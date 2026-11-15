"""Bounded Playwright provider for natural Tieba page interactions."""

from __future__ import annotations

import asyncio
import contextvars
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

from .dom import (
    DOMExtractionResult,
    extract_comment_author_growth,
    extract_dom_candidates,
    target_dom_structure_ready,
)
from .identity import ImmutableRequestContext, ObservationCandidate, Target, make_request_context
from .network import (
    NESTED_FLOOR_PATH,
    PAGE_PC_PATH,
    USER_CARD_PATH,
    NetworkExtractionResult,
    extract_nested_floor,
    extract_page_pc_comment,
    extract_page_pc_post,
    extract_page_pc_thread_counts,
)

_CALLBACK_CLEANUP_TIMEOUT_SECONDS = 0.05
_COMMENT_GROWTH_HYDRATION_WAIT_SECONDS = 2.0
_COMMENT_GROWTH_HYDRATION_POLL_SECONDS = 0.05
_MAX_COMMENT_GROWTH_HOVERS = 3

_COMMENT_GROWTH_HOVER_POINTS_SCRIPT = r"""({parentPid, portrait, limit}) => {
  const parent = Array.from(document.querySelectorAll('.pb-comment-item[data-id]'))
    .find((node) => node.getAttribute('data-id') === String(parentPid));
  if (!parent) return [];
  const contents = Array.from(parent.children).filter((node) => node.classList.contains('comment-content'));
  if (contents.length !== 1) return [];
  let wrappers = Array.from(contents[0].children).filter((node) => node.classList.contains('lzl-wrapper'));
  if (!wrappers.length) {
    wrappers = Array.from(parent.children).filter((node) => node.classList.contains('lzl-wrapper'));
  }
  if (wrappers.length !== 1) return [];
  const owners = Array.from(wrappers[0].children).filter((node) => node.classList.contains('pb-lzl-item'));
  const boundary = '.pb-comment-item, .pb-lzl-item';
  const ownedBy = (node, owner) => {
    let current = node.parentElement;
    while (current && current !== owner) {
      if (current.matches(boundary)) return false;
      current = current.parentElement;
    }
    return current === owner;
  };
  const points = [];
  for (const owner of owners) {
    const headers = Array.from(owner.querySelectorAll('.head-line.user-info'))
      .filter((header) => ownedBy(header, owner));
    if (headers.length !== 1) continue;
    for (const header of headers) {
      const nameInfos = Array.from(header.querySelectorAll('.name-info'));
      if (nameInfos.length !== 1) continue;
      const links = Array.from(nameInfos[0].querySelectorAll('a.head-name[href]'));
      if (links.length !== 1) continue;
      let portraitId = null;
      try {
        const profile = new URL(links[0].href, location.href);
        const ids = profile.searchParams.getAll('id');
        if (profile.protocol !== 'https:' || profile.hostname.toLowerCase() !== 'tieba.baidu.com'
            || profile.pathname.replace(/\/$/, '') !== '/home/main' || ids.length !== 1
            || !ids[0] || /^[0-9]+$/.test(ids[0])) continue;
        portraitId = ids[0];
      } catch (_) {
        continue;
      }
      if (portraitId !== portrait) continue;
      const icons = Array.from(header.querySelectorAll('.icon_tshows'))
        .filter((icon) => ownedBy(icon, header));
      for (const icon of icons) {
        const refs = icon.classList.contains('tooltip__reference')
          ? [icon]
          : Array.from(icon.querySelectorAll('.tooltip__reference'))
            .filter((ref) => ownedBy(ref, header));
        for (const ref of refs) {
          if (ref.closest('.icon_tshows') !== icon) continue;
          const rect = ref.getBoundingClientRect();
          if (rect.width > 0 && rect.height > 0) {
            points.push({x: rect.left + rect.width / 2, y: rect.top + rect.height / 2});
            if (points.length >= limit) return points;
          }
        }
      }
    }
  }
  return points;
}"""


@dataclass(frozen=True)
class BrowserRunResult:
    status: str
    reason: str | None
    candidates: tuple[ObservationCandidate, ...]
    diagnostics: dict[str, Any]
    field_outcomes: dict[str, dict[str, str]] = field(default_factory=dict)
    identity_verified: bool = False
    identity_methods: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _CachedPostPage:
    page: Any
    tid: str
    page_url: str
    request_context: ImmutableRequestContext
    response_text: str
    response_status: int


class BrowserProviderConfigurationError(RuntimeError):
    """The provider lacks an explicit isolated profile or request budget."""


class PlaywrightWebFallbackProvider:
    """Collect from a dedicated profile or an injected context, one task at a time."""

    def __init__(
        self,
        *,
        budget: Any,
        profile_dir: str | Path | None = None,
        context: Any | None = None,
        max_expansions: int = 1,
        timeout_seconds: float = 30.0,
        max_pages: int = 1,
        headless: bool = True,
        keep_login_page: bool = False,
        on_blocked: Any | None = None,
        playwright_factory: Any | None = None,
    ) -> None:
        if budget is None or not hasattr(budget, "__aenter__"):
            raise BrowserProviderConfigurationError("an async request budget is required")
        if context is None and profile_dir is None:
            raise BrowserProviderConfigurationError(
                "an explicit isolated profile_dir or Playwright context is required"
            )
        if not isinstance(max_expansions, int) or isinstance(max_expansions, bool) or max_expansions < 0:
            raise ValueError("max_expansions must be a non-negative integer")
        if not isinstance(max_pages, int) or isinstance(max_pages, bool) or max_pages < 1:
            raise ValueError("max_pages must be a positive integer")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not isinstance(headless, bool) or not isinstance(keep_login_page, bool):
            raise ValueError("headless and keep_login_page must be booleans")
        if keep_login_page and headless:
            raise BrowserProviderConfigurationError("keep_login_page requires headless=False")
        if profile_dir is not None:
            _reject_default_chrome_profile(Path(profile_dir))
        self.budget = budget
        self.profile_dir = Path(profile_dir) if profile_dir is not None else None
        self._external_context = context
        self._context = context
        self._owned_context = False
        self._playwright_factory = playwright_factory
        self._manager = None
        self._playwright = None
        self._max_expansions = max_expansions
        self._timeout_seconds = timeout_seconds
        self._max_pages = max_pages
        self._headless = headless
        self._keep_login_page = keep_login_page
        self._on_blocked = on_blocked
        self._target_semaphore = asyncio.Semaphore(1)
        self._closed = False
        self._manual_login_page = None
        self._cached_post_page: _CachedPostPage | None = None
        self._active_collect_task: asyncio.Task | None = None
        self._close_task: asyncio.Task | None = None
        self._close_deferred = False
        self._callback_context = contextvars.ContextVar(
            f"web_fallback_callback_{id(self)}", default=False
        )

    @property
    def has_manual_login_page(self) -> bool:
        return self._manual_login_page is not None

    async def collect(self, target: Target) -> BrowserRunResult:
        """Visit at most max_pages and naturally trigger bounded expansions."""
        if self._closed:
            raise RuntimeError("provider is closed")
        state: dict[str, Any] = {
            "requests": 0,
            "budgeted_actions": 0,
            "observed_subresource_requests": 0,
            "nested_floor_requests": 0,
            "page_pc_requests": 0,
            "user_card_requests": 0,
            "pages": 0,
            "page_cache_hits": 0,
            "post_owner_scrolls": 0,
            "growth_hover_attempts": 0,
            "expansions": 0,
            "user_card_status": "NOT_TESTED",
            "user_card_reason": None,
            "blocked_reason": None,
            "block_callback_notified": False,
        }
        deadline = asyncio.get_running_loop().time() + self._timeout_seconds
        worker = asyncio.create_task(self._collect_worker(target, state, deadline))
        try:
            result = await asyncio.shield(worker)
        except asyncio.CancelledError:
            worker.cancel()
            await _await_cleanup(asyncio.create_task(asyncio.wait({worker})))
            await self._finish_deferred_close()
            raise
        await self._finish_deferred_close()
        return result

    async def _collect_worker(
        self, target: Target, state: dict[str, Any], deadline: float
    ) -> BrowserRunResult:
        try:
            async with asyncio.timeout(self._timeout_seconds):
                async with self._target_semaphore:
                    if self._closed:
                        raise RuntimeError("provider is closed")
                    if self.has_manual_login_page:
                        raise BrowserProviderConfigurationError(
                            "close the retained login page before collecting another target"
                        )
                    current_task = asyncio.current_task()
                    self._active_collect_task = current_task
                    try:
                        return await self._collect(target, state, deadline)
                    finally:
                        if self._active_collect_task is current_task:
                            self._active_collect_task = None
        except TimeoutError:
            return BrowserRunResult("timeout", "browser_task_timeout", (), dict(state))

    async def _finish_deferred_close(self) -> None:
        if not self._close_deferred:
            return
        self._close_deferred = False
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close_resources())
        await _await_cleanup(self._close_task)

    async def _collect(self, target: Target, state: dict[str, Any], deadline: float) -> BrowserRunResult:
        loop = asyncio.get_running_loop()
        context = await self._ensure_context()
        cached_page = self._cached_post_page
        reuse_page = (
            target.target_type == "post"
            and cached_page is not None
            and cached_page.tid == target.tid
        )
        if cached_page is not None and not reuse_page:
            self._cached_post_page = None
            await _await_cleanup(asyncio.create_task(cached_page.page.close()))
        page = cached_page.page if reuse_page else await context.new_page()
        request_contexts: dict[Any, ImmutableRequestContext] = {}
        request_page_numbers: dict[Any, int] = {}
        request_page_urls: dict[Any, str] = {}
        page_network_results: dict[int, list[NetworkExtractionResult]] = {}
        capture_tasks: dict[Any, asyncio.Task] = {}
        response_tasks: set[asyncio.Task] = set()
        callback_tasks: set[asyncio.Task] = set()
        network_results: list[NetworkExtractionResult] = []
        identity_mismatch = False
        post_page_snapshot: _CachedPostPage | None = None

        async def capture_response(response) -> None:
            key = _request_key(response.request)
            task = capture_tasks.get(key)
            if task is None:
                task = asyncio.create_task(read_and_parse_response(response, key))
                capture_tasks[key] = task
            await task

        async def read_and_parse_response(response, key: int) -> None:
            nonlocal identity_mismatch, post_page_snapshot
            context_snapshot = request_contexts.pop(key, None)
            response_page_number = request_page_numbers.pop(key, None)
            response_page_url = request_page_urls.pop(key, None)
            if context_snapshot is None:
                identity_mismatch = True
                state["network_request_context_unavailable"] = True
                return
            path = urlparse(response.request.url).path
            try:
                response_text = "" if response.status != 200 else await asyncio.wait_for(
                    response.text(), timeout=self._timeout_seconds
                )
            except TimeoutError:
                network_results.append(NetworkExtractionResult("timeout", "response_text_timeout", False, ()))
                return
            if path == PAGE_PC_PATH:
                parser = (
                    extract_page_pc_thread_counts
                    if target.target_type == "thread"
                    else extract_page_pc_post
                    if target.target_type == "post"
                    else extract_page_pc_comment
                )
            else:
                parser = extract_nested_floor
            result = parser(
                target,
                response_text,
                context_snapshot,
                response_status=response.status,
            )
            network_results.append(result)
            if (
                path == PAGE_PC_PATH
                and target.target_type == "post"
                and result.identity_verified
                and context_snapshot.target.target_type == "post"
            ):
                post_page_snapshot = _CachedPostPage(
                    page=page,
                    tid=target.tid,
                    page_url=response_page_url or page.url,
                    request_context=context_snapshot,
                    response_text=response_text,
                    response_status=response.status,
                )
            if response_page_number is not None:
                page_network_results.setdefault(response_page_number, []).append(result)
            if result.status == "identity_unverified" and result.reason not in {
                "cid_missing_from_response",
                "parent_post_missing_from_response",
            }:
                state["identity_failure"] = result.reason

        def on_request(request) -> None:
            state["requests"] += 1
            if not _is_document_request(request):
                state["observed_subresource_requests"] += 1
            path = urlparse(request.url).path
            if path == USER_CARD_PATH:
                state["user_card_requests"] += 1
                state["user_card_status"] = "needs_evidence"
                state["user_card_reason"] = "unverified_source_schema"
            if path == NESTED_FLOOR_PATH:
                state["nested_floor_requests"] += 1
            if path == PAGE_PC_PATH and target.target_type in {"thread", "post", "comment"}:
                state["page_pc_requests"] += 1
            if path == NESTED_FLOOR_PATH or (
                path == PAGE_PC_PATH and target.target_type in {"thread", "post", "comment"}
            ):
                request_contexts[_request_key(request)] = make_request_context(
                    target,
                    page_url=page.url,
                    request_url=request.url,
                    request_method=request.method,
                    post_data=request.post_data,
                    context_id=(
                        f"{target.target_type}:{target.target_id}:request-{state['nested_floor_requests']}"
                        f":{uuid.uuid4().hex}"
                    ),
                )
                request_page_numbers[_request_key(request)] = state["pages"]
                request_page_urls[_request_key(request)] = page.url

        def schedule_blocked(reason: str) -> None:
            if state["block_callback_notified"]:
                return
            state["block_callback_notified"] = True

            async def invoke_callback() -> None:
                token = self._callback_context.set(True)
                try:
                    try:
                        await self._notify_blocked(reason)
                    except Exception:
                        return
                finally:
                    self._callback_context.reset(token)

            if self._on_blocked is not None:
                task = asyncio.create_task(invoke_callback())
                callback_tasks.add(task)
                task.add_done_callback(callback_tasks.discard)

        async def notify_blocked_once(reason: str) -> None:
            if state["block_callback_notified"]:
                return
            state["block_callback_notified"] = True
            await self._notify_blocked(reason)

        def on_response(response) -> None:
            path = urlparse(response.request.url).path
            status = response.status
            if status == 429:
                state["blocked_reason"] = "http_429"
                schedule_blocked("http_429")
            elif status in {401, 403} and (
                path in {NESTED_FLOOR_PATH, USER_CARD_PATH}
                or (path == PAGE_PC_PATH and target.target_type in {"thread", "post", "comment"})
                or _is_document_request(response.request)
            ):
                state["blocked_reason"] = "http_auth_or_verification"
                schedule_blocked("http_auth_or_verification")
            if path == NESTED_FLOOR_PATH or (
                path == PAGE_PC_PATH and target.target_type in {"thread", "post", "comment"}
            ):
                task = asyncio.create_task(capture_response(response))
                response_tasks.add(task)

        page.on("request", on_request)
        page.on("response", on_response)
        dom_result: DOMExtractionResult | None = None
        cache_ready = False
        try:
            if reuse_page and cached_page is not None:
                cached_context = replace(
                    cached_page.request_context,
                    target=target,
                    context_id=f"{cached_page.request_context.context_id}:cached:{uuid.uuid4().hex}",
                )
                cached_result = extract_page_pc_post(
                    target,
                    cached_page.response_text,
                    cached_context,
                    response_status=cached_page.response_status,
                )
                if cached_result.status == "ok" and cached_result.identity_verified:
                    network_results.append(cached_result)
                    post_page_snapshot = _CachedPostPage(
                        page=page,
                        tid=target.tid,
                        page_url=cached_page.page_url,
                        request_context=cached_context,
                        response_text=cached_page.response_text,
                        response_status=cached_page.response_status,
                    )
                    state["page_cache_hits"] = 1
                    dom_result = await _extract_post_dom_with_scroll(
                        page, target, deadline, state
                    )
                    if dom_result.identity_verified:
                        dom_result = await _hover_growth_and_extract_dom(
                            page, target, dom_result, network_results, deadline, state
                        )
                    cache_ready = True
                else:
                    self._cached_post_page = None

            for page_number in (() if cache_ready else range(1, self._max_pages + 1)):
                state["pages"] += 1
                page_url = _target_page_url(target, page_number)
                async with self._budget(state):
                    response = await page.goto(
                        page_url,
                        wait_until="domcontentloaded",
                        timeout=self._remaining_ms(deadline),
                    )
                if response is not None and response.status == 429:
                    state["blocked_reason"] = "http_429"
                    await notify_blocked_once("http_429")
                elif response is not None and response.status in {401, 403}:
                    state["blocked_reason"] = "http_auth_or_verification"
                    await notify_blocked_once("http_auth_or_verification")
                if state["blocked_reason"] == "http_auth_or_verification":
                    explicit_gate = await _page_gate_reason(page)
                    if explicit_gate in {"needs_login", "blocked"}:
                        state["blocked_reason"] = explicit_gate
                        state["manual_gate_detected"] = True
                if state["blocked_reason"]:
                    break

                pages_left = self._max_pages - page_number + 1
                remaining = max(0.0, deadline - loop.time())
                page_deadline = loop.time() + remaining / pages_left
                readiness = await self._wait_for_target(
                    page,
                    target,
                    page_deadline,
                    deadline,
                    state,
                    page_network_results,
                    page_number,
                )
                if readiness in {"needs_login", "blocked"}:
                    state["blocked_reason"] = readiness
                    state["manual_gate_detected"] = True
                    await notify_blocked_once(readiness)
                    break
                if readiness == "timeout":
                    return BrowserRunResult("timeout", "browser_task_timeout", (), dict(state))
                if readiness in {"network_ready", "network_invalid"}:
                    dom_result = (
                        await _extract_post_dom_with_scroll(page, target, deadline, state)
                        if target.target_type == "post" and readiness == "network_ready"
                        else _extract_dom_or_empty(target, page.url, await page.content())
                    )
                    if dom_result.identity_verified:
                        dom_result = await _hover_growth_and_extract_dom(
                            page, target, dom_result, network_results, deadline, state
                        )
                    break
                if readiness != "ready":
                    if target.target_type in {"thread", "post"}:
                        dom_result = extract_dom_candidates(target, page.url, await page.content())
                    continue

                if target.target_type in {"thread", "post"}:
                    dom_result = _extract_dom_or_empty(target, page.url, await page.content())
                    if dom_result.identity_verified:
                        dom_result = await _hover_growth_and_extract_dom(
                            page, target, dom_result, network_results, deadline, state
                        )
                    if dom_result.identity_verified:
                        break
                    if dom_result.reason not in {"post_data_id_missing", "first_floor_owner_missing"}:
                        break
                elif target.target_type == "comment":
                    await self._expand_comment_owner(page, target, state, capture_response, deadline)
                    dom_result = _extract_dom_or_empty(target, page.url, await page.content())
                    if dom_result.identity_verified:
                        dom_result = await _hover_growth_and_extract_dom(
                            page, target, dom_result, network_results, deadline, state
                        )
                    comment_growth_result = None
                    if not state.get("identity_failure") and not state["blocked_reason"]:
                        comment_growth_result = await _extract_comment_author_growth(
                            page, target, network_results, dom_result, deadline, state
                        )
                    if comment_growth_result is not None:
                        if (
                            comment_growth_result.status == "identity_unverified"
                            and _comment_growth_identity_conflict(comment_growth_result)
                        ):
                            state["identity_failure"] = comment_growth_result.reason
                        elif comment_growth_result.identity_verified:
                            dom_result = _merge_dom_results(dom_result, comment_growth_result)
                    if state["nested_floor_requests"] or state["blocked_reason"] or state.get("identity_failure"):
                        break

            if response_tasks:
                await asyncio.gather(*tuple(response_tasks), return_exceptions=True)
            if state["blocked_reason"]:
                return BrowserRunResult("blocked", state["blocked_reason"], (), dict(state))
            if target.target_type == "post" and dom_result is not None and dom_result.status == "identity_unverified":
                return BrowserRunResult(
                    "identity_unverified",
                    dom_result.reason,
                    (),
                    dict(state),
                    _field_outcomes(target, network_results, None),
                    False,
                )
            if identity_mismatch:
                return BrowserRunResult("identity_unverified", "network_request_context_unavailable", (), dict(state))
            if state.get("identity_failure"):
                return BrowserRunResult("identity_unverified", state["identity_failure"], (), dict(state))
            if any(
                result.status == "identity_unverified"
                and result.reason
                not in {"cid_missing_from_response", "parent_post_missing_from_response"}
                for result in network_results
            ):
                return BrowserRunResult("identity_unverified", "network_identity_mismatch", (), dict(state))

            if (
                target.target_type == "post"
                and any(result.identity_verified for result in network_results)
                and dom_result is not None
                and not dom_result.identity_verified
            ):
                dom_result = None

            candidates = _merge_candidates(network_results, dom_result)
            identity_verified = any(result.identity_verified for result in network_results) or (
                dom_result is not None and dom_result.identity_verified
            )
            identity_methods = _identity_methods(target, network_results, dom_result)
            if target.target_type == "post" and identity_verified and post_page_snapshot is not None:
                self._cached_post_page = post_page_snapshot
            if candidates:
                return BrowserRunResult(
                    "ok",
                    None,
                    tuple(candidates),
                    dict(state),
                    _field_outcomes(target, network_results, dom_result),
                    identity_verified,
                    identity_methods,
                )
            if identity_verified:
                return BrowserRunResult(
                    "ok",
                    None,
                    (),
                    dict(state),
                    _field_outcomes(target, network_results, dom_result),
                    True,
                    identity_methods,
                )
            if (
                target.target_type == "post"
                and dom_result is not None
                and not dom_result.identity_verified
                and any(
                    result.status == "not_tested"
                    and result.reason == "target_post_missing_from_response"
                    for result in network_results
                )
            ):
                return BrowserRunResult(
                    "not_tested",
                    "target_post_missing_from_response",
                    (),
                    dict(state),
                    _field_outcomes(target, network_results, None),
                    False,
                )
            if target.target_type == "comment":
                reason = next(
                    (result.reason for result in reversed(network_results) if result.reason),
                    "target_cid_not_observed",
                )
                return BrowserRunResult(
                    "identity_unverified",
                    reason,
                    (),
                    dict(state),
                    _field_outcomes(target, network_results, dom_result),
                    False,
                )
            if dom_result is not None:
                return BrowserRunResult(
                    dom_result.status,
                    dom_result.reason,
                    (),
                    dict(state),
                    _field_outcomes(target, network_results, dom_result),
                    dom_result.identity_verified,
                )
            return BrowserRunResult("dom_changed", "target_owner_not_found", (), dict(state))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if _looks_like_timeout(error):
                return BrowserRunResult("timeout", "browser_operation_timeout", (), dict(state))
            return BrowserRunResult("dom_changed", "browser_operation_failed", (), dict(state))
        finally:
            _remove_page_listener(page, "request", on_request)
            _remove_page_listener(page, "response", on_response)
            keep_page = (
                self._keep_login_page
                and not self._closed
                and not state.get("blocked_reason") == "http_429"
                and state.get("manual_gate_detected") is True
            )
            cleanup = asyncio.create_task(
                _cleanup_capture_tasks(response_tasks, callback_tasks)
            )
            try:
                await _await_cleanup(cleanup)
            finally:
                if keep_page:
                    self._manual_login_page = page
                elif (
                    self._cached_post_page is not None
                    and self._cached_post_page.page is page
                    and not self._closed
                    and state.get("blocked_reason") != "http_429"
                ):
                    pass
                else:
                    if self._cached_post_page is not None and self._cached_post_page.page is page:
                        self._cached_post_page = None
                    await _await_cleanup(asyncio.create_task(page.close()))

    async def _wait_for_target(
        self,
        page,
        target,
        page_deadline,
        task_deadline,
        state,
        page_network_results=None,
        page_number=None,
    ) -> str:
        loop = asyncio.get_running_loop()
        previous_html = None
        stable_count = 0
        while loop.time() < page_deadline:
            if loop.time() >= task_deadline:
                return "timeout"
            gate_reason = await _page_gate_reason(page)
            if gate_reason:
                return gate_reason
            if target.target_type == "post":
                current_results = (
                    page_network_results.get(page_number, ())
                    if page_network_results is not None and page_number is not None
                    else ()
                )
                if state.get("network_request_context_unavailable") or state.get("identity_failure"):
                    return "network_invalid"
                if any(result.status == "identity_unverified" for result in current_results):
                    return "network_invalid"
                if any(result.identity_verified for result in current_results):
                    return "network_ready"
                if any(
                    result.status == "not_tested"
                    and result.reason == "target_post_missing_from_response"
                    for result in current_results
                ):
                    return "ready"
            if target.target_type in {"thread", "post"}:
                html_text = await page.content()
                if target_dom_structure_ready(target, page.url, html_text):
                    return "ready"
            else:
                owner = page.locator(f'.pb-comment-item[data-id="{target.parent_post_pid}"]')
                if await owner.count() == 1:
                    wrapper = owner.locator(":scope > .comment-content > .lzl-wrapper")
                    if await wrapper.count() == 1:
                        expansion = wrapper.locator(":scope > .show-more-lzl")
                        nested_items = wrapper.locator(":scope > .pb-lzl-item")
                        if (
                            state["nested_floor_requests"]
                            or await expansion.count() == 1
                            or await nested_items.count() > 0
                        ):
                            return "ready"
                        html_text = await page.content()
                        if html_text == previous_html:
                            stable_count += 1
                            if stable_count >= 3:
                                return "ready"
                        else:
                            previous_html = html_text
                            stable_count = 0
            remaining = min(page_deadline, task_deadline) - loop.time()
            if remaining > 0:
                await asyncio.sleep(min(0.05, remaining))
        return "timeout" if loop.time() >= task_deadline else "not_ready"

    async def _expand_comment_owner(self, page, target, state, capture_response, deadline) -> None:
        owner = page.locator(f'.pb-comment-item[data-id="{target.parent_post_pid}"]')
        if await owner.count() != 1:
            return
        wrapper = owner.locator(":scope > .comment-content > .lzl-wrapper")
        if await wrapper.count() != 1:
            return
        expansion = wrapper.locator(":scope > .show-more-lzl")
        while state["expansions"] < self._max_expansions:
            if await expansion.count() != 1:
                return
            try:
                async with page.expect_response(
                    lambda response: urlparse(response.request.url).path == NESTED_FLOOR_PATH,
                    timeout=self._remaining_ms(deadline),
                ) as response_info:
                    async with self._budget(state):
                        await expansion.first.click(timeout=self._remaining_ms(deadline))
                response = await response_info.value
            except Exception as error:
                if _looks_like_timeout(error):
                    return
                raise
            state["expansions"] += 1
            await capture_response(response)
            owner = page.locator(f'.pb-comment-item[data-id="{target.parent_post_pid}"]')
            if await owner.count() != 1:
                return
            wrapper = owner.locator(":scope > .comment-content > .lzl-wrapper")
            if await wrapper.count() != 1:
                return
            expansion = wrapper.locator(":scope > .show-more-lzl")
            if state["blocked_reason"]:
                return

    async def _ensure_context(self):
        if self._context is not None:
            return self._context
        if self.profile_dir is None:
            raise BrowserProviderConfigurationError("no browser context available")
        if self._playwright_factory is None:
            try:
                from playwright.async_api import async_playwright
            except ImportError:
                raise BrowserProviderConfigurationError(
                    "Playwright is not installed. Run `uv sync`, then "
                    "`uv run playwright install chromium`."
                ) from None
            factory = async_playwright
        else:
            factory = self._playwright_factory
        self._manager = factory()
        self._playwright = await self._manager.start()
        self._context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir=str(self.profile_dir),
            headless=self._headless,
        )
        self._owned_context = True
        return self._context

    @asynccontextmanager
    async def _budget(self, state: dict[str, Any] | None = None):
        bypass_auth_gate = getattr(self.budget, "bypass_auth_gate", None)
        if callable(bypass_auth_gate):
            async with bypass_auth_gate():
                async with self.budget:
                    if state is not None:
                        state["budgeted_actions"] += 1
                    yield
        else:
            async with self.budget:
                if state is not None:
                    state["budgeted_actions"] += 1
                yield

    def _remaining_ms(self, deadline: float | None = None) -> int:
        if deadline is None:
            return max(1, int(self._timeout_seconds * 1000))
        return max(1, int((deadline - asyncio.get_running_loop().time()) * 1000))

    async def _notify_blocked(self, reason: str) -> None:
        if self._on_blocked is None:
            return
        result = self._on_blocked(reason)
        if asyncio.iscoroutine(result):
            await result

    async def close(self) -> None:
        """Close after active collection cleanup; callback self-close is deferred."""
        current_task = asyncio.current_task()
        if self._callback_context.get() or current_task is self._active_collect_task:
            self._closed = True
            self._close_deferred = True
            return

        if self._close_task is None:
            self._closed = True
            self._close_deferred = False
            self._close_task = asyncio.create_task(self._close_resources())
        await _await_cleanup(self._close_task)

    async def _close_resources(self) -> None:
        active_task = self._active_collect_task
        if active_task is not None and active_task is not asyncio.current_task():
            active_task.cancel()
            await asyncio.gather(active_task, return_exceptions=True)
        await self.close_manual_login_page()
        cached_page = self._cached_post_page
        self._cached_post_page = None
        if cached_page is not None:
            await _await_cleanup(asyncio.create_task(cached_page.page.close()))
        if self._owned_context and self._context is not None:
            await self._context.close()
        self._context = self._external_context
        if self._playwright is not None:
            await self._playwright.stop()
        elif self._manager is not None:
            await self._manager.stop()

    async def close_manual_login_page(self) -> None:
        page = self._manual_login_page
        self._manual_login_page = None
        if page is not None:
            await page.close()


def _field_outcomes(target, network_results, dom_result):
    outcomes = {}
    candidates = []
    for result in network_results:
        outcomes.update(result.field_outcomes)
        candidates.extend(result.candidates)
    if dom_result is not None:
        outcomes.update(dom_result.field_outcomes)
        candidates.extend(dom_result.candidates)
    candidate_fields = {candidate.field_name for candidate in candidates}
    for field_name in candidate_fields:
        outcomes.pop(field_name, None)
    for field_name in target.requested_fields:
        if field_name not in candidate_fields:
            outcomes.setdefault(
                field_name,
                {"status": "not_tested", "reason": "not_observed_in_bounded_page"},
            )
    return outcomes


async def _extract_post_dom_with_scroll(page, target: Target, deadline: float, state: dict[str, Any]):
    """Give a virtualized post owner a bounded chance to render before DOM extraction."""
    result: DOMExtractionResult | None = None
    loop = asyncio.get_running_loop()
    hydration_deadline = min(deadline - 0.1, loop.time() + 1.0)
    for attempt in range(21):
        html_text = await page.content()
        result = _extract_dom_or_empty(target, page.url, html_text)
        if result.identity_verified or result.reason != "post_data_id_missing":
            if result.reason != "dom_content_empty":
                return result
        remaining = hydration_deadline - loop.time()
        if attempt == 20 or remaining <= 0.01:
            break

        selector = f'.pb-comment-item[data-id="{target.pid}"]'
        try:
            locator = page.locator(selector)
            if await locator.count() == 1:
                scroll_into_view = getattr(locator, "scroll_into_view_if_needed", None)
                if callable(scroll_into_view):
                    await scroll_into_view(timeout=max(1, min(1000, int(remaining * 1000))))
        except Exception:
            # Vue may detach the exact owner between the locator check and scroll.
            pass

        evaluate = getattr(page, "evaluate", None)
        if not callable(evaluate):
            break
        try:
            await evaluate("() => window.scrollBy(0, Math.max(window.innerHeight * 0.8, 600))")
        except Exception:
            break
        state["post_owner_scrolls"] += 1
        await asyncio.sleep(min(0.05, max(0, hydration_deadline - loop.time())))
    return result or _extract_dom_or_empty(target, page.url, "")


def _extract_dom_or_empty(target: Target, page_url: str, html_text: str) -> DOMExtractionResult:
    if not html_text.strip():
        return DOMExtractionResult("not_tested", "dom_content_empty", False, ())
    return extract_dom_candidates(target, page_url, html_text)


async def _extract_comment_author_growth(
    page,
    target: Target,
    network_results,
    dom_result: DOMExtractionResult | None,
    deadline: float,
    state: dict[str, Any],
) -> DOMExtractionResult | None:
    growth_fields = {"global_growth_level", "global_growth_icon_url"}
    if target.target_type != "comment" or not set(target.requested_fields) & growth_fields:
        return None
    if dom_result is not None and dom_result.identity_verified:
        return None
    if any(
        candidate.field_name in growth_fields
        for result in network_results
        for candidate in result.candidates
    ):
        return None

    network_identities = [
        result.identity_evidence
        for result in network_results
        if result.status == "ok"
        and result.identity_verified
        and result.identity_evidence is not None
    ]
    if not network_identities:
        return None
    network_identity = network_identities[-1]

    loop = asyncio.get_running_loop()
    hydration_deadline = min(deadline, loop.time() + _COMMENT_GROWTH_HYDRATION_WAIT_SECONDS)
    last_result = None
    while True:
        try:
            html_text = await page.content()
        except Exception:
            html_text = ""
        result = (
            extract_comment_author_growth(
                target,
                page.url,
                html_text,
                network_identity,
            )
            if html_text.strip()
            else DOMExtractionResult("not_tested", "dom_content_empty", False, ())
        )
        last_result = result
        if _comment_growth_identity_conflict(result):
            return result
        if result.identity_verified and result.reason != "author_profile_header_not_observed":
            if any(candidate.field_name in growth_fields for candidate in result.candidates):
                return result
            if _comment_growth_needs_hover(result):
                result = await _hover_comment_author_growth(
                    page,
                    target,
                    network_identity,
                    result,
                    deadline,
                    state,
                )
                if _comment_growth_identity_conflict(result):
                    return result
            return result

        remaining = hydration_deadline - loop.time()
        if remaining <= 0:
            return last_result
        state["comment_growth_hydration_waits"] = (
            state.get("comment_growth_hydration_waits", 0) + 1
        )
        await asyncio.sleep(min(_COMMENT_GROWTH_HYDRATION_POLL_SECONDS, remaining))


def _comment_growth_identity_conflict(result: DOMExtractionResult) -> bool:
    if result.status != "identity_unverified":
        return False
    transient_dom_reasons = {
        "parent_post_data_id_missing",
        "comment_content_not_unique",
        "comment_wrapper_not_unique",
        "comment_owner_not_present",
        "thread_context_not_exposed",
    }
    return result.reason not in transient_dom_reasons


def _comment_growth_needs_hover(result: DOMExtractionResult) -> bool:
    hover_reasons = {
        "icon_semantics_unverified",
        "numeric_level_not_exposed_in_page",
        "growth_icon_image_not_found",
    }
    return any(
        outcome.get("status") == "needs_evidence"
        and outcome.get("reason") in hover_reasons
        for outcome in result.field_outcomes.values()
    )


async def _hover_comment_author_growth(
    page,
    target: Target,
    network_identity,
    current: DOMExtractionResult,
    deadline: float,
    state: dict[str, Any],
) -> DOMExtractionResult:
    loop = asyncio.get_running_loop()
    remaining = deadline - loop.time()
    if remaining <= 0:
        return current
    evaluate = getattr(page, "evaluate", None)
    mouse = getattr(page, "mouse", None)
    move = getattr(mouse, "move", None)
    if not callable(evaluate) or not callable(move):
        return current

    try:
        async with asyncio.timeout(min(1.0, remaining)):
            points = await evaluate(
                _COMMENT_GROWTH_HOVER_POINTS_SCRIPT,
                {
                    "parentPid": target.parent_post_pid,
                    "portrait": target.author_portrait,
                    "limit": _MAX_COMMENT_GROWTH_HOVERS,
                },
            )
    except Exception:
        return current
    if not isinstance(points, (list, tuple)):
        return current

    result = current
    for point in points[:_MAX_COMMENT_GROWTH_HOVERS]:
        if not isinstance(point, dict):
            continue
        x, y = point.get("x"), point.get("y")
        if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
            continue
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        try:
            async with asyncio.timeout(min(1.0, remaining)):
                await move(x, y)
                state["growth_hover_attempts"] += 1
                await asyncio.sleep(min(0.1, max(0, deadline - loop.time())))
                html_text = await page.content()
        except Exception:
            continue
        updated = extract_comment_author_growth(
            target,
            page.url,
            html_text,
            network_identity,
        )
        if _comment_growth_identity_conflict(updated):
            return updated
        if updated.identity_verified:
            result = updated
            if any(
                candidate.field_name in {"global_growth_level", "global_growth_icon_url"}
                for candidate in updated.candidates
            ):
                return updated
    return result


def _merge_dom_results(
    current: DOMExtractionResult | None,
    auxiliary: DOMExtractionResult,
) -> DOMExtractionResult:
    results = [
        result
        for result in (current, auxiliary)
        if result is not None and result.identity_verified
    ]
    candidates = []
    outcomes = {}
    for result in results:
        candidates.extend(result.candidates)
        outcomes.update(result.field_outcomes)
    candidate_fields = {candidate.field_name for candidate in candidates}
    for field_name in candidate_fields:
        outcomes.pop(field_name, None)
    return DOMExtractionResult("ok", None, bool(results), tuple(candidates), outcomes)


async def _hover_growth_and_extract_dom(
    page,
    target: Target,
    current: DOMExtractionResult,
    network_results,
    deadline: float,
    state: dict[str, Any],
) -> DOMExtractionResult:
    growth_fields = {"global_growth_level", "global_growth_icon_url"}
    if not set(target.requested_fields) & growth_fields:
        return current
    if any(candidate.field_name in growth_fields for result in network_results for candidate in result.candidates):
        return current
    if any(candidate.field_name in growth_fields for candidate in current.candidates):
        return current

    try:
        if target.target_type == "post":
            owner = page.locator(f'.pb-comment-item[data-id="{target.pid}"]')
        elif target.target_type == "comment":
            parent = page.locator(f'.pb-comment-item[data-id="{target.parent_post_pid}"]')
            if await parent.count() != 1:
                return current
            owner = parent.locator(
                f':scope .pb-lzl-item[data-cid="{target.cid}"], '
                f':scope .pb-lzl-item[data-comment-id="{target.cid}"], '
                f':scope .pb-lzl-item[data-id="{target.cid}"], '
                f':scope .pb-lzl-item[cid="{target.cid}"]'
            )
        else:
            owner = page.locator(
                f'.image-text:has(.pb-content-wrap [tid="{target.tid}"][fid="{target.fid}"])'
            )
        if await owner.count() != 1:
            return current
        headers = owner.locator(":scope > .head-line.user-info")
        if await headers.count() != 1:
            return current
        references = headers.locator(
            ".icon_tshows.tooltip__reference, .icon_tshows .tooltip__reference"
        )
        reference_count = min(3, await references.count())
    except Exception:
        return current

    result = current
    for index in range(reference_count):
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        try:
            await references.nth(index).hover(timeout=max(1, min(1000, int(remaining * 1000))))
            state["growth_hover_attempts"] += 1
            await asyncio.sleep(min(0.1, max(0, deadline - asyncio.get_running_loop().time())))
            updated = _extract_dom_or_empty(target, page.url, await page.content())
        except Exception:
            continue
        if updated.identity_verified:
            result = updated
            if any(candidate.field_name in growth_fields for candidate in updated.candidates):
                return updated
    return result


def _identity_methods(target, network_results, dom_result) -> tuple[str, ...]:
    methods = {
        method
        for result in network_results
        for candidate in result.candidates
        for method in candidate.identity.match_methods
    }
    if (
        target.target_type == "thread"
        and any(result.status == "ok" and result.identity_verified for result in network_results)
    ):
        # extract_page_pc_thread_counts returns ok only after its exact TID/FID,
        # creator, and first-floor author checks have all passed.
        methods.update(
            {
                "response_forum_id",
                "response_thread_id",
                "thread_post_id_matches_first_floor_id",
                "response_author_id",
            }
        )
    if dom_result is not None:
        methods.update(
            method
            for candidate in dom_result.candidates
            for method in candidate.identity.match_methods
        )
    return tuple(sorted(methods))


def _target_page_url(target: Target, page_number: int) -> str:
    if target.target_type == "post":
        query = {"pid": target.pid}
    elif target.target_type == "comment":
        query = {"pid": target.parent_post_pid}
    else:
        query = {"pn": page_number}
    return f"https://tieba.baidu.com/p/{target.tid}?{urlencode(query)}"


def _merge_candidates(network_results, dom_result):
    candidates = [candidate for result in network_results for candidate in result.candidates]
    if dom_result is not None and dom_result.identity_verified:
        candidates.extend(dom_result.candidates)
    candidates.sort(
        key=lambda item: (
            0 if item.source_path.startswith("page_pc.") else 1 if item.source_kind == "web_network" else 2
        )
    )
    merged = []
    for candidate in candidates:
        if any(
            existing.field_name == candidate.field_name
            and existing.normalized_value == candidate.normalized_value
            for existing in merged
        ):
            continue
        merged.append(candidate)
    return merged


def _request_key(request):
    return getattr(request, "_impl_obj", request)


def _remove_page_listener(page, event: str, callback) -> None:
    remover = getattr(page, "remove_listener", None)
    if remover is not None:
        remover(event, callback)


async def _await_cleanup(task: asyncio.Task):
    """Finish essential cleanup under cancellation, then preserve cancellation."""
    interrupted = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            interrupted = True
    if task.cancelled():
        raise asyncio.CancelledError
    result = task.result()
    if interrupted:
        raise asyncio.CancelledError
    return result


async def _cleanup_capture_tasks(
    response_tasks: set[asyncio.Task], callback_tasks: set[asyncio.Task]
) -> None:
    responses = tuple(response_tasks)
    for task in responses:
        if not task.done():
            task.cancel()
    if responses:
        await asyncio.gather(*responses, return_exceptions=True)

    callbacks = tuple(callback_tasks)
    if callbacks:
        _done, pending = await asyncio.wait(
            callbacks, timeout=_CALLBACK_CLEANUP_TIMEOUT_SECONDS
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


def _is_document_request(request) -> bool:
    try:
        return request.resource_type == "document"
    except Exception:
        return False


async def _page_gate_reason(page) -> str | None:
    parsed = urlparse(page.url)
    host = (parsed.hostname or "").lower()
    if host == "passport.baidu.com":
        return "needs_login"
    if host == "wappass.baidu.com" and any(
        marker in parsed.path.lower() for marker in ("verify", "captcha", "appeal")
    ):
        return "blocked"
    title = " ".join((await page.title()).split())
    if title in {"百度账号登录", "百度帐号登录", "百度登录", "贴吧登录"}:
        return "needs_login"
    if title in {"百度安全验证", "安全验证", "百度安全中心验证"}:
        return "blocked"
    return None


def _looks_like_timeout(error: Exception) -> bool:
    return "timeout" in type(error).__name__.lower() or "timed out" in str(error).lower()


def _reject_default_chrome_profile(path: Path) -> None:
    resolved = path.expanduser().resolve()
    home = Path.home().resolve()
    default_roots = (
        home / ".config" / "google-chrome",
        home / ".config" / "chromium",
        home / "Library" / "Application Support" / "Google" / "Chrome",
    )
    windows_default_root = home / "AppData" / "Local" / "Google" / "Chrome" / "User Data"
    if any(resolved == root or root in resolved.parents for root in (*default_roots, windows_default_root)):
        raise BrowserProviderConfigurationError("the user's default Chrome profile cannot be used")
