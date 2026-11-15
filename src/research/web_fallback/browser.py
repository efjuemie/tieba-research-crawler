"""Bounded Playwright provider for natural Tieba page interactions."""

from __future__ import annotations

import asyncio
import contextvars
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

from .dom import DOMExtractionResult, extract_dom_candidates, target_dom_structure_ready
from .identity import ImmutableRequestContext, ObservationCandidate, Target, make_request_context
from .network import (
    NESTED_FLOOR_PATH,
    PAGE_PC_PATH,
    USER_CARD_PATH,
    NetworkExtractionResult,
    extract_nested_floor,
    extract_page_pc_comment,
    extract_page_pc_thread_counts,
)

_CALLBACK_CLEANUP_TIMEOUT_SECONDS = 0.05


@dataclass(frozen=True)
class BrowserRunResult:
    status: str
    reason: str | None
    candidates: tuple[ObservationCandidate, ...]
    diagnostics: dict[str, Any]
    field_outcomes: dict[str, dict[str, str]] = field(default_factory=dict)
    identity_verified: bool = False


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
        page = await context.new_page()
        request_contexts: dict[Any, ImmutableRequestContext] = {}
        capture_tasks: dict[Any, asyncio.Task] = {}
        response_tasks: set[asyncio.Task] = set()
        callback_tasks: set[asyncio.Task] = set()
        network_results: list[NetworkExtractionResult] = []
        identity_mismatch = False

        async def capture_response(response) -> None:
            key = _request_key(response.request)
            task = capture_tasks.get(key)
            if task is None:
                task = asyncio.create_task(read_and_parse_response(response, key))
                capture_tasks[key] = task
            await task

        async def read_and_parse_response(response, key: int) -> None:
            nonlocal identity_mismatch
            context_snapshot = request_contexts.pop(key, None)
            if context_snapshot is None:
                identity_mismatch = True
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
            if path == PAGE_PC_PATH and target.target_type in {"thread", "comment"}:
                state["page_pc_requests"] += 1
            if path == NESTED_FLOOR_PATH or (
                path == PAGE_PC_PATH and target.target_type in {"thread", "comment"}
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
                or (path == PAGE_PC_PATH and target.target_type in {"thread", "comment"})
                or _is_document_request(response.request)
            ):
                state["blocked_reason"] = "http_auth_or_verification"
                schedule_blocked("http_auth_or_verification")
            if path == NESTED_FLOOR_PATH or (
                path == PAGE_PC_PATH and target.target_type in {"thread", "comment"}
            ):
                task = asyncio.create_task(capture_response(response))
                response_tasks.add(task)

        page.on("request", on_request)
        page.on("response", on_response)
        dom_result: DOMExtractionResult | None = None
        try:
            for page_number in range(1, self._max_pages + 1):
                state["pages"] += 1
                page_url = f"https://tieba.baidu.com/p/{target.tid}?{urlencode({'pn': page_number})}"
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
                readiness = await self._wait_for_target(page, target, page_deadline, deadline, state)
                if readiness in {"needs_login", "blocked"}:
                    state["blocked_reason"] = readiness
                    state["manual_gate_detected"] = True
                    await notify_blocked_once(readiness)
                    break
                if readiness == "timeout":
                    return BrowserRunResult("timeout", "browser_task_timeout", (), dict(state))
                if readiness != "ready":
                    if target.target_type in {"thread", "post"}:
                        dom_result = extract_dom_candidates(target, page.url, await page.content())
                    continue

                if target.target_type in {"thread", "post"}:
                    dom_result = extract_dom_candidates(target, page.url, await page.content())
                    if dom_result.identity_verified:
                        break
                    if dom_result.reason not in {"post_data_id_missing", "first_floor_owner_missing"}:
                        break
                elif target.target_type == "comment":
                    await self._expand_comment_owner(page, target, state, capture_response, deadline)
                    if state["nested_floor_requests"] or state["blocked_reason"] or state.get("identity_failure"):
                        break

            if response_tasks:
                await asyncio.gather(*tuple(response_tasks), return_exceptions=True)
            if state["blocked_reason"]:
                return BrowserRunResult("blocked", state["blocked_reason"], (), dict(state))
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

            candidates = _merge_candidates(network_results, dom_result)
            identity_verified = any(result.identity_verified for result in network_results) or (
                dom_result is not None and dom_result.identity_verified
            )
            if candidates:
                return BrowserRunResult(
                    "ok",
                    None,
                    tuple(candidates),
                    dict(state),
                    _field_outcomes(target, network_results, dom_result),
                    identity_verified,
                )
            if identity_verified:
                return BrowserRunResult(
                    "ok",
                    None,
                    (),
                    dict(state),
                    _field_outcomes(target, network_results, dom_result),
                    True,
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
                else:
                    await _await_cleanup(asyncio.create_task(page.close()))

    async def _wait_for_target(self, page, target, page_deadline, task_deadline, state) -> str:
        loop = asyncio.get_running_loop()
        previous_html = None
        stable_count = 0
        while loop.time() < page_deadline:
            if loop.time() >= task_deadline:
                return "timeout"
            gate_reason = await _page_gate_reason(page)
            if gate_reason:
                return gate_reason
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
                    "Playwright is not installed. Run `uv sync --group login`, then "
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
