from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from lxml import html

from src.core.auth import GateAwareSemaphore
from src.research.web_fallback import browser as browser_module
from src.research.web_fallback.browser import (
    BrowserProviderConfigurationError,
    PlaywrightWebFallbackProvider,
)
from src.research.web_fallback.diagnostics import sanitize_web_diagnostics
from src.research.web_fallback.dom import DOMExtractionResult
from src.research.web_fallback.identity import IdentityError, Target
from src.research.web_fallback.network import (
    NESTED_FLOOR_PATH,
    PAGE_PC_PATH,
    USER_CARD_PATH,
    NetworkExtractionResult,
)
from src.research.web_fallback.service import _safe_diagnostics

TID = "81001"
FID = "120"
POST_ID = "45002"
CID = "9007199254740993"
AUTHOR_ID = "77003"
PORTRAIT = "tb.1.synthetic"


class FakeBudget:
    def __init__(self):
        self.entries = 0
        self.active = 0
        self.max_active = 0

    async def __aenter__(self):
        self.entries += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        return self

    async def __aexit__(self, *_exc):
        self.active -= 1


class FakeRequest:
    def __init__(self, url, *, method="GET", post_data=None, resource_type="xhr"):
        self.url = url
        self.method = method
        self.post_data = post_data
        self.resource_type = resource_type


class FakeResponse:
    def __init__(self, request, *, status=200, text="", delay=0):
        self.request = request
        self.url = request.url
        self.status = status
        self._text = text
        self.delay = delay

    async def text(self):
        if self.delay:
            await asyncio.sleep(self.delay)
        return self._text


class FakeExpectResponse:
    def __init__(self, page, predicate, timeout):
        self.page = page
        self.predicate = predicate
        self.timeout = timeout
        self.future = None

    async def __aenter__(self):
        self.future = asyncio.get_running_loop().create_future()
        self.page.expected_responses.append(self)
        return self

    async def __aexit__(self, *_exc):
        return None

    @property
    def value(self):
        return self.future


class FakeBodyLocator:
    def __init__(self, page):
        self.page = page

    async def inner_text(self, **_kwargs):
        return self.page.body_text


class FakeExpansion:
    def __init__(self, page):
        self.page = page

    @property
    def first(self):
        return self

    async def count(self):
        return int(self.page.expansion_remaining > 0)

    async def click(self, **_kwargs):
        self.page.expansion_remaining -= 1
        plan = self.page.nested_plan or [("0", self.page.nested_response_text, 0)]
        requests = []
        for offset, text, delay in plan:
            request = FakeRequest(
                f"https://tieba.baidu.com{NESTED_FLOOR_PATH}",
                method="POST",
                post_data=f"thread_id={TID}&post_id={POST_ID}&fid={FID}&offset={offset}",
            )
            self.page.emit("request", request)
            requests.append((request, text, delay))
        for request, text, delay in reversed(requests):
            response = FakeResponse(request, text=text, delay=delay)
            self.page.emit("response", response)
            self.page.match_expected_response(response)


class FakeMouse:
    def __init__(self, page):
        self.page = page

    async def move(self, x, y):
        self.page.hover_positions.append((x, y))
        if self.page.hovered_html_text is not None:
            self.page.html_text = self.page.hovered_html_text


class FakeWrapper:
    def __init__(self, page):
        self.page = page

    async def count(self):
        return int(self.page.has_wrapper)

    def locator(self, selector):
        if selector == ":scope > .show-more-lzl":
            return FakeExpansion(self.page)
        if selector == ":scope > .pb-lzl-item":
            return FakeNestedItems(self.page)
        raise AssertionError(f"unexpected wrapper selector {selector}")


class FakeNestedItems:
    def __init__(self, page):
        self.page = page

    async def count(self):
        return int(self.page.has_nested_items)


class FakePostLocator:
    def __init__(self, page):
        self.page = page

    async def count(self):
        if self.page.render_after and self.page._navigation_started is not None:
            if asyncio.get_running_loop().time() - self.page._navigation_started >= self.page.render_after:
                return int(self.page.rendered_has_post)
        return int(self.page.has_post)

    def locator(self, selector):
        assert selector == ":scope > .comment-content > .lzl-wrapper"
        return FakeWrapper(self.page)


class FakePage:
    def __init__(
        self,
        *,
        html_text="",
        body_text="正常页面",
        status=200,
        nested_response_text="{}",
        page_pc_response_text=None,
        page_pc_response_delay=0,
        expansion_remaining=1,
        has_post=True,
        emit_user_card=False,
        navigation_delay=0,
        nested_plan=None,
        title="贴吧",
        redirect_url=None,
        render_after=0,
        rendered_html_text=None,
        rendered_has_post=True,
        has_wrapper=True,
        has_nested_items=False,
        close_delay=0,
        close_error=None,
        scroll_html_sequence=None,
        comment_growth_hover_points=None,
        hovered_html_text=None,
        diagnostic_result=None,
        evaluate_error=None,
        content_delay=0,
        diagnostic_title=None,
        selector_readiness_result=None,
    ):
        self.url = "about:blank"
        self.html_text = html_text
        self.body_text = body_text
        self.goto_status = status
        self.nested_response_text = nested_response_text
        self.page_pc_response_text = page_pc_response_text
        self.page_pc_response_delay = page_pc_response_delay
        self.expansion_remaining = expansion_remaining
        self.has_post = has_post
        self.emit_user_card = emit_user_card
        self.navigation_delay = navigation_delay
        self.nested_plan = nested_plan
        self.title_text = title
        self.redirect_url = redirect_url
        self.render_after = render_after
        self.rendered_html_text = rendered_html_text
        self.rendered_has_post = rendered_has_post
        self.has_wrapper = has_wrapper
        self.has_nested_items = has_nested_items
        self.close_delay = close_delay
        self.close_error = close_error
        self.scroll_html_sequence = list(scroll_html_sequence or ())
        self.comment_growth_hover_points = list(comment_growth_hover_points or ())
        self.hovered_html_text = hovered_html_text
        self.diagnostic_result = diagnostic_result
        self.evaluate_error = evaluate_error
        self.content_delay = content_delay
        self.diagnostic_title = diagnostic_title
        self.selector_readiness_result = selector_readiness_result or {
            "readyState": "complete",
            "visibleOwnerCount": 0,
        }
        self.hover_positions = []
        self.comment_growth_hover_args = None
        self.mouse = FakeMouse(self)
        self.content_calls = 0
        self._navigation_started = None
        self.handlers = {}
        self.expected_responses = []
        self.closed = False
        self.goto_urls = []
        self.goto_started = asyncio.Event()

    def on(self, event, callback):
        self.handlers.setdefault(event, []).append(callback)

    def emit(self, event, value):
        for callback in self.handlers.get(event, []):
            callback(value)

    def remove_listener(self, event, callback):
        self.handlers[event].remove(callback)

    def match_expected_response(self, response):
        for expected in tuple(self.expected_responses):
            if expected.predicate(response) and expected.future is not None and not expected.future.done():
                expected.future.set_result(response)
                self.expected_responses.remove(expected)
                return

    async def goto(self, url, **_kwargs):
        self.goto_urls.append(url)
        self.goto_started.set()
        self.url = self.redirect_url or url
        self._navigation_started = asyncio.get_running_loop().time()
        document = FakeRequest(url, resource_type="document")
        self.emit("request", document)
        self.emit("response", FakeResponse(document, status=self.goto_status))
        if self.emit_user_card:
            user_card = FakeRequest(f"https://tieba.baidu.com{USER_CARD_PATH}", method="GET")
            self.emit("request", user_card)
            self.emit("response", FakeResponse(user_card))
        if self.page_pc_response_text is not None:
            page_pc = FakeRequest(
                f"https://tieba.baidu.com{PAGE_PC_PATH}",
                method="POST",
                post_data=f"kz={TID}&pn=1",
            )
            self.emit("request", page_pc)
            response = FakeResponse(page_pc, text=self.page_pc_response_text)
            if self.page_pc_response_delay:
                async def emit_delayed_response():
                    await asyncio.sleep(self.page_pc_response_delay)
                    self.emit("response", response)

                asyncio.create_task(emit_delayed_response())
            else:
                self.emit("response", response)
        if self.navigation_delay:
            await asyncio.sleep(self.navigation_delay)
        return FakeResponse(document, status=self.goto_status)

    def locator(self, selector):
        if selector == "body":
            return FakeBodyLocator(self)
        if selector.startswith(".pb-comment-item[data-id="):
            return FakePostLocator(self)
        raise AssertionError(f"unexpected selector {selector}")

    async def title(self):
        return self.title_text

    def expect_response(self, predicate, *, timeout):
        return FakeExpectResponse(self, predicate, timeout)

    async def content(self):
        self.content_calls += 1
        if self.content_delay:
            await asyncio.sleep(self.content_delay)
        if (
            self.render_after
            and self._navigation_started is not None
            and asyncio.get_running_loop().time() - self._navigation_started >= self.render_after
            and self.rendered_html_text is not None
        ):
            return self.rendered_html_text
        return self.html_text

    async def evaluate(self, script, arg=None):
        if script == browser_module._COMMENT_SELECTOR_STABILITY_SCRIPT:
            if self.evaluate_error is not None:
                raise self.evaluate_error
            return self.selector_readiness_result
        if script == browser_module._PAGE_READINESS_DIAGNOSTIC_SCRIPT:
            if self.evaluate_error is not None:
                raise self.evaluate_error
            if self.diagnostic_title is not None:
                self.title_text = self.diagnostic_title
            return self.diagnostic_result
        if script == browser_module._COMMENT_GROWTH_HOVER_POINTS_SCRIPT:
            self.comment_growth_hover_args = arg
            return self.comment_growth_hover_points
        if self.scroll_html_sequence:
            self.html_text = self.scroll_html_sequence.pop(0)

    async def close(self):
        if self.close_delay:
            await asyncio.sleep(self.close_delay)
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


class PaginatedPagePcFakePage(FakePage):
    def __init__(
        self,
        *,
        page_pc_responses,
        has_post_by_page,
        page_pc_delays=None,
        html_text="",
        nested_response_text="{}",
    ):
        super().__init__(html_text=html_text, has_post=False, nested_response_text=nested_response_text)
        self.page_pc_responses = page_pc_responses
        self.has_post_by_page = has_post_by_page
        self.page_pc_delays = page_pc_delays or [0] * len(page_pc_responses)
        self.page_number = 0

    async def goto(self, url, **kwargs):
        self.page_number += 1
        self.page_pc_response_text = self.page_pc_responses[self.page_number - 1]
        self.page_pc_response_delay = self.page_pc_delays[self.page_number - 1]
        self.has_post = self.has_post_by_page[self.page_number - 1]
        return await super().goto(url, **kwargs)


class FakeContext:
    def __init__(self, page_factory):
        self.page_factory = page_factory
        self.pages = []
        self.closed = False

    async def new_page(self):
        page = self.page_factory()
        self.pages.append(page)
        return page

    async def close(self):
        self.closed = True


def _thread_target(tid=TID, **changes):
    values = {
        "target_type": "thread",
        "target_id": tid,
        "tid": tid,
        "fid": FID,
        "author_user_id": AUTHOR_ID,
        "author_portrait": PORTRAIT,
        "requested_fields": ("ip_location_text",),
    }
    values.update(changes)
    return Target(**values)


def _comment_target(**changes):
    values = {
        "target_type": "comment",
        "target_id": CID,
        "tid": TID,
        "fid": FID,
        "cid": CID,
        "parent_post_pid": POST_ID,
        "author_user_id": AUTHOR_ID,
        "author_portrait": PORTRAIT,
        "requested_fields": ("author_nickname",),
        "scope_generation": 0,
        "parent_scope_generation": 0,
    }
    values.update(changes)
    return Target(**values)


def _post_target(**changes):
    values = {
        "target_type": "post",
        "target_id": "45003",
        "tid": TID,
        "fid": FID,
        "pid": "45003",
        "author_user_id": AUTHOR_ID,
        "author_portrait": PORTRAIT,
        "requested_fields": (
            "ip_location_text",
            "global_growth_level",
            "global_growth_icon_url",
            "agree_count",
        ),
    }
    values.update(changes)
    return Target(**values)


def _thread_html(tid=TID):
    return f"""
    <html><body><div class="image-text">
      <div class="pb-content-wrap">
        <div class="richtext-item"><span tid="{tid}" fid="{FID}" index="0"></span></div>
      </div>
      <div class="head-line user-info"><div class="name-info">
        <a class="head-name" href="https://tieba.baidu.com/home/main?id={PORTRAIT}">synthetic</a>
      </div></div>
      <div class="desc-info"><span class="ip-address">合成地区</span></div>
      <div class="pc-pb-first-floor-interactive"></div>
    </div></body></html>
    """


def _post_html(*, pid="45003", tid=TID, fid=FID, portrait="tb.1.other"):
    return f"""
    <html><body><div class="pb-comment-item" data-id="{pid}">
      <div class="pb-content-wrap">
        <div class="richtext-item"><span tid="{tid}" fid="{fid}" index="1"></span></div>
      </div>
      <div class="head-line user-info"><div class="name-info">
        <a class="head-name" href="https://tieba.baidu.com/home/main?id={portrait}">synthetic</a>
      </div></div>
    </div></body></html>
    """


def _nested_response(*, cid=CID, author_id=AUTHOR_ID):
    return json.dumps(
        {
            "error_code": 0,
            "data": {
                "post_list": [
                    {
                        "id": cid,
                        "author_id": author_id,
                        "author": {"id": author_id, "name_show": "合成昵称"},
                    }
                ]
            }
        },
        ensure_ascii=False,
    )


def _comment_author_growth_html(
    *,
    include_nested_owner=True,
    include_growth_icon=False,
    growth_tooltip=False,
    expose_cid=False,
    conflicting_cid_aliases=False,
    include_avatar=False,
    portrait=PORTRAIT,
):
    nested_owner = ""
    if include_nested_owner:
        cid_attribute = f' data-cid="{CID}"' if expose_cid else ""
        if conflicting_cid_aliases:
            cid_attribute = f' data-cid="{CID}" data-id="{int(CID) + 1}"'
        growth_icon = ""
        avatar = (
            '<img class="avatar" src="https://tieba-ares.cdn.bcebos.com/avatar.webp">'
            if include_avatar
            else ""
        )
        if include_growth_icon:
            tooltip_attributes = ' aria-describedby="growth-tip"' if growth_tooltip else ""
            growth_icon = (
                '<span class="icon_tshows tooltip__reference"'
                f'{tooltip_attributes}>'
                '<img class="icon_tshows_img" '
                'src="https://tieba-ares.cdn.bcebos.com/synthetic/comment-growth.webp">'
                "</span>"
            )
        nested_owner = f"""
          <div class="pb-lzl-item"{cid_attribute}>
            <div class="head-line user-info"><div class="name-info">
              {avatar}
              <a class="head-name" href="https://tieba.baidu.com/home/main?id={portrait}">author</a>
            </div>{growth_icon}</div>
          </div>"""
    tooltip = '<div id="growth-tip">贴吧成长等级：Lv. 7</div>' if growth_tooltip else ""
    return f"""
    <html><body>
      <div class="pb-comment-item" data-id="{POST_ID}">
        <div class="pb-content-wrap"><div class="richtext-item">
          <span tid="{TID}" fid="{FID}" index="1"></span>
        </div></div>
        <div class="comment-content"><div class="lzl-wrapper">
          <button class="show-more-lzl">Expand replies</button>
          {nested_owner}
        </div></div>
      </div>
      {tooltip}
    </body></html>"""


def _page_pc_response(*, cid=CID):
    return json.dumps(
        {
            "error_code": 0,
            "forum": {"id": int(FID)},
            "thread": {
                "id": int(TID),
                "post_id": int(POST_ID),
                "author": {"id": int(AUTHOR_ID)},
                "reply_num": 8,
                "agree": {"agree_num": 5, "disagree_num": 2},
                "collect_num": 0,
                "share_num": 1,
            },
            "first_floor": {"id": int(POST_ID), "author_id": int(AUTHOR_ID)},
            "post_list": [
                {
                    "id": int(POST_ID),
                    "sub_post_list": {
                        "pid": int(POST_ID),
                        "sub_post_list": [
                            {
                                "id": int(cid),
                                "author_id": int(AUTHOR_ID),
                                "agree": {"agree_num": 3, "disagree_num": 0},
                            }
                        ],
                    },
                }
            ],
            "user_list": [{"id": int(AUTHOR_ID), "ip_address": "合成地区"}],
        },
        ensure_ascii=False,
    )


def _page_pc_parent_response(comments):
    return json.dumps(
        {
            "error_code": 0,
            "forum": {"id": int(FID)},
            "thread": {
                "id": int(TID),
                "post_id": int(POST_ID),
                "author": {"id": int(AUTHOR_ID)},
            },
            "first_floor": {"id": int(POST_ID), "author_id": int(AUTHOR_ID)},
            "post_list": [
                {
                    "id": int(POST_ID),
                    "sub_post_list": {
                        "pid": int(POST_ID),
                        "sub_post_list": [
                            {"id": int(cid), "author_id": int(author_id)}
                            for cid, author_id, _ip in comments
                        ],
                    },
                }
            ],
            "user_list": [
                {"id": int(author_id), "ip_address": ip_address}
                for _cid, author_id, ip_address in comments
            ],
        },
        ensure_ascii=False,
    )


def _post_page_pc_response(
    *, post_id="45003", ip_address="  ", growth_level=0, include_growth_icon=False
):
    user = {
        "id": int(AUTHOR_ID),
        "ip_address": ip_address,
        "user_growth": {"level_id": growth_level},
    }
    if include_growth_icon:
        user["user_show_info"] = {
            "feed_head": {
                "main_data": [
                    {
                        "business_info_map": {"icon_tip": "贴吧成长等级"},
                        "icon": {"url": "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp"},
                    }
                ]
            }
        }
    return json.dumps(
        {
            "error_code": 0,
            "forum": {"id": int(FID)},
            "thread": {"id": int(TID)},
            "post_list": [{"id": int(post_id), "author_id": int(AUTHOR_ID)}],
            "user_list": [user],
        },
        ensure_ascii=False,
    )


def _provider(context, budget=None, **kwargs):
    return PlaywrightWebFallbackProvider(
        context=context,
        budget=budget or FakeBudget(),
        **kwargs,
    )


def _gate_aware_budget(auth_ready, network_ready):
    capacity = asyncio.Semaphore(1)

    async def wait_all_gates():
        await auth_ready.wait()
        await network_ready.wait()

    async def is_all_gates_open():
        return auth_ready.is_set() and network_ready.is_set()

    async def wait_network_gate():
        await network_ready.wait()

    async def is_network_gate_open():
        return network_ready.is_set()

    return (
        GateAwareSemaphore(
            capacity,
            wait_all_gates,
            is_all_gates_open,
            wait_network_gate,
            is_network_gate_open,
        ),
        capacity,
    )


@pytest.mark.asyncio
async def test_web_budget_ignores_api_auth_gate_but_waits_for_network_gate():
    auth_ready = asyncio.Event()
    network_ready = asyncio.Event()
    budget, _capacity = _gate_aware_budget(auth_ready, network_ready)
    page = FakePage(html_text=_thread_html())
    provider = _provider(FakeContext(lambda: page), budget=budget)

    web_task = asyncio.create_task(provider.collect(_thread_target()))
    await asyncio.sleep(0.03)

    assert not page.goto_started.is_set()
    assert not web_task.done()
    network_ready.set()
    result = await asyncio.wait_for(web_task, timeout=1)

    assert result.status == "ok"
    assert not auth_ready.is_set()
    api_task = asyncio.create_task(budget.acquire())
    await asyncio.sleep(0.03)
    assert not api_task.done()
    auth_ready.set()
    await asyncio.wait_for(api_task, timeout=1)
    budget.release()
    await provider.close()


@pytest.mark.asyncio
async def test_web_budget_cancellation_restores_capacity_and_auth_gate():
    auth_ready = asyncio.Event()
    network_ready = asyncio.Event()
    network_ready.set()
    budget, capacity = _gate_aware_budget(auth_ready, network_ready)
    page = FakePage(html_text=_thread_html(), navigation_delay=5)
    provider = _provider(FakeContext(lambda: page), budget=budget)

    web_task = asyncio.create_task(provider.collect(_thread_target()))
    await asyncio.wait_for(page.goto_started.wait(), timeout=1)
    assert capacity._value == 0

    web_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await web_task

    assert capacity._value == 1
    api_task = asyncio.create_task(budget.acquire())
    await asyncio.sleep(0.03)
    assert not api_task.done()
    auth_ready.set()
    await asyncio.wait_for(api_task, timeout=1)
    budget.release()
    await provider.close()


@pytest.mark.asyncio
async def test_provider_uses_natural_expansion_response_and_request_context():
    page = FakePage(nested_response_text=_nested_response())
    context = FakeContext(lambda: page)
    budget = FakeBudget()
    provider = _provider(context, budget=budget, max_expansions=1, max_pages=1)

    target = _comment_target(requested_fields=("author_nickname", "ip_location_text", "agree_count"))
    result = await provider.collect(target)

    assert result.status == "ok"
    assert result.identity_verified is True
    assert [candidate.raw_value for candidate in result.candidates] == ["合成昵称"]
    assert result.candidates[0].identity.request_offset == "0"
    assert result.diagnostics["nested_floor_requests"] == 1
    assert result.diagnostics["expansions"] == 1
    assert result.diagnostics["pages"] == 1
    assert budget.entries == 2
    assert result.diagnostics["budgeted_actions"] == 2
    assert result.diagnostics["observed_subresource_requests"] == 1
    assert "author_nickname" not in result.field_outcomes
    assert result.field_outcomes["ip_location_text"] == {
        "status": "not_tested",
        "reason": "not_observed_in_response",
    }
    assert page.closed is True
    await provider.close()


@pytest.mark.asyncio
async def test_provider_expands_synthetic_comment_content_wrapper():
    """The real owner shape nests lzl-wrapper under comment-content."""
    fixture = html.fromstring(
        (Path(__file__).parent / "fixtures/web_comment_owner_synthetic.html").read_text()
    )
    owners = fixture.xpath(
        "//*[contains(concat(' ', normalize-space(@class), ' '), ' pb-comment-item ') "
        "and @data-id='45002']"
    )
    assert len(owners) == 1
    contents = owners[0].xpath(
        "./*[contains(concat(' ', normalize-space(@class), ' '), ' comment-content ')]"
    )
    wrappers = contents[0].xpath(
        "./*[contains(concat(' ', normalize-space(@class), ' '), ' lzl-wrapper ')]"
    )
    expanders = wrappers[0].xpath(
        "./*[contains(concat(' ', normalize-space(@class), ' '), ' show-more-lzl ')]"
    )
    assert len(contents) == len(wrappers) == len(expanders) == 1

    page = FakePage(nested_response_text=_nested_response())
    provider = _provider(FakeContext(lambda: page), max_expansions=1)

    result = await provider.collect(_comment_target())

    assert result.status == "ok"
    assert result.diagnostics["expansions"] == 1
    assert result.diagnostics["nested_floor_requests"] == 1
    await provider.close()


@pytest.mark.asyncio
async def test_verified_comment_without_requested_network_value_is_ok():
    page = FakePage(nested_response_text=_nested_response())
    provider = _provider(FakeContext(lambda: page))
    target = _comment_target(requested_fields=("global_growth_level",))

    result = await provider.collect(target)

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.candidates == ()
    assert result.field_outcomes["global_growth_level"] == {
        "status": "not_tested",
        "reason": "not_observed_in_response",
    }
    await provider.close()


@pytest.mark.asyncio
async def test_comment_growth_waits_for_hydrated_exact_author_header_and_uses_parent_pid():
    page = FakePage(
        html_text=_comment_author_growth_html(include_nested_owner=False),
        rendered_html_text=_comment_author_growth_html(
            include_growth_icon=True,
            growth_tooltip=True,
        ),
        render_after=0.5,
        nested_response_text=_nested_response(),
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=3)
    target = _comment_target(
        requested_fields=("global_growth_level", "global_growth_icon_url", "ip_location_text")
    )

    result = await provider.collect(target)

    assert result.status == "ok"
    assert page.goto_urls == [f"https://tieba.baidu.com/p/{TID}?pid={POST_ID}"]
    assert result.diagnostics["comment_growth_hydration_waits"] > 0
    assert {candidate.field_name for candidate in result.candidates} == {
        "global_growth_level",
        "global_growth_icon_url",
    }
    assert all(
        "same_author_profile_growth" in candidate.identity.match_methods
        for candidate in result.candidates
    )
    assert result.field_outcomes["ip_location_text"]["status"] == "not_tested"
    assert page.hover_positions == []
    await provider.close()


@pytest.mark.asyncio
async def test_comment_growth_hovers_only_verified_author_tooltip_and_stops_on_growth():
    page = FakePage(
        html_text=_comment_author_growth_html(include_growth_icon=True),
        hovered_html_text=_comment_author_growth_html(
            include_growth_icon=True,
            growth_tooltip=True,
        ),
        comment_growth_hover_points=[{"x": 10, "y": 20}, {"x": 30, "y": 40}],
        nested_response_text=_nested_response(),
    )
    provider = _provider(FakeContext(lambda: page))
    target = _comment_target(requested_fields=("global_growth_level", "global_growth_icon_url"))

    result = await provider.collect(target)

    assert result.status == "ok"
    assert {candidate.field_name for candidate in result.candidates} == {
        "global_growth_level",
        "global_growth_icon_url",
    }
    assert page.comment_growth_hover_args == {
        "parentPid": POST_ID,
        "portrait": PORTRAIT,
        "limit": 3,
    }
    assert page.hover_positions == [(10, 20)]
    assert result.diagnostics["growth_hover_attempts"] == 1
    assert ".searchParams.getAll('id')" in browser_module._COMMENT_GROWTH_HOVER_POINTS_SCRIPT
    assert "classList.contains('tooltip__reference')" in browser_module._COMMENT_GROWTH_HOVER_POINTS_SCRIPT
    assert "querySelectorAll('.icon_tshows')" in browser_module._COMMENT_GROWTH_HOVER_POINTS_SCRIPT
    await provider.close()


@pytest.mark.asyncio
async def test_comment_growth_requires_verified_network_identity_for_auxiliary_dom(monkeypatch):
    page = FakePage(nested_response_text=_nested_response())
    provider = _provider(FakeContext(lambda: page))
    target = _comment_target(requested_fields=("global_growth_icon_url",))

    monkeypatch.setattr(
        browser_module,
        "extract_nested_floor",
        lambda *_args, **_kwargs: NetworkExtractionResult(
            "ok",
            None,
            True,
            (),
            {"global_growth_icon_url": {"status": "not_tested", "reason": "no_evidence"}},
        ),
    )
    monkeypatch.setattr(
        browser_module,
        "extract_comment_author_growth",
        lambda *_args, **_kwargs: pytest.fail("auxiliary DOM must require network identity proof"),
    )

    result = await provider.collect(target)

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.candidates == ()
    await provider.close()


@pytest.mark.asyncio
async def test_comment_growth_marks_missing_global_badge_without_using_avatar_image():
    page = FakePage(
        html_text=_comment_author_growth_html(include_avatar=True),
        nested_response_text=_nested_response(),
    )
    provider = _provider(FakeContext(lambda: page))
    target = _comment_target(requested_fields=("global_growth_level", "global_growth_icon_url"))

    result = await provider.collect(target)

    assert result.status == "ok"
    assert result.candidates == ()
    for field_name in ("global_growth_level", "global_growth_icon_url"):
        assert result.field_outcomes[field_name] == {
            "status": "absent_on_page",
            "reason": "global_growth_icon_not_present",
        }
    await provider.close()


@pytest.mark.asyncio
async def test_exact_dom_comment_preserves_network_ip_without_auxiliary_growth_conflict(monkeypatch):
    page = FakePage(
        html_text=_comment_author_growth_html(expose_cid=True),
        page_pc_response_text=_page_pc_response(),
        nested_response_text=_nested_response(),
    )
    provider = _provider(FakeContext(lambda: page))
    target = _comment_target(requested_fields=("ip_location_text", "global_growth_icon_url"))
    monkeypatch.setattr(
        browser_module,
        "extract_comment_author_growth",
        lambda *_args, **_kwargs: pytest.fail(
            "exact comment DOM identity should skip the CID-less auxiliary extractor"
        ),
    )

    result = await provider.collect(target)

    assert result.status == "ok"
    assert result.identity_verified is True
    assert [candidate.field_name for candidate in result.candidates] == ["ip_location_text"]
    assert result.candidates[0].normalized_value == "合成地区"
    assert result.field_outcomes["global_growth_icon_url"] == {
        "status": "absent_on_page",
        "reason": "global_growth_icon_not_present",
    }
    await provider.close()


@pytest.mark.asyncio
async def test_comment_growth_rejects_conflicting_dom_cid_aliases():
    page = FakePage(
        html_text=_comment_author_growth_html(conflicting_cid_aliases=True),
        nested_response_text=_nested_response(),
    )
    provider = _provider(FakeContext(lambda: page))
    target = _comment_target(requested_fields=("global_growth_icon_url",))

    result = await provider.collect(target)

    assert result.status == "identity_unverified"
    assert result.reason == "comment_data_id_invalid"
    assert result.candidates == ()
    await provider.close()


@pytest.mark.asyncio
async def test_verified_thread_without_requested_dom_value_is_ok():
    page = FakePage(html_text=_thread_html())
    provider = _provider(FakeContext(lambda: page))
    target = _thread_target(requested_fields=("global_growth_level",))

    result = await provider.collect(target)

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.candidates == ()
    assert result.field_outcomes["global_growth_level"] == {
        "status": "absent_on_page",
        "reason": "global_growth_icon_not_present",
    }
    await provider.close()


@pytest.mark.asyncio
async def test_thread_prefers_page_pc_counts_and_preserves_dom_conflicts():
    counters = """
      <div class="pc-pb-first-floor-interactive">
        <div class="action-item"><svg><use href="#agree_pb"></use></svg><span class="action-number">5</span></div>
        <div class="action-item"><svg><use href="#comment_pb"></use></svg><span class="action-number">7</span></div>
        <div class="action-item"><svg><use href="#collect"></use></svg><span class="action-number">收藏</span></div>
        <div class="action-item"><svg><use href="#share_pb"></use></svg><span class="action-number">1</span></div>
      </div>
    """
    page = FakePage(
        html_text=_thread_html().replace(
            '<div class="pc-pb-first-floor-interactive"></div>', counters
        ),
        page_pc_response_text=_page_pc_response(),
    )
    provider = _provider(FakeContext(lambda: page))

    result = await provider.collect(
        _thread_target(
            requested_fields=("agree_count", "reply_count", "collect_count", "share_count")
        )
    )

    by_field = {}
    for candidate in result.candidates:
        by_field.setdefault(candidate.field_name, []).append(candidate)
    assert result.status == "ok"
    assert [item.source_kind for item in by_field["agree_count"]] == ["web_network"]
    assert [item.normalized_value for item in by_field["reply_count"]] == [8, 7]
    assert by_field["reply_count"][0].source_kind == "web_network"
    assert by_field["reply_count"][1].source_kind == "web_dom"
    assert [(item.source_kind, item.raw_value) for item in by_field["reply_count"]] == [
        ("web_network", 8),
        ("web_dom", "7"),
    ]
    assert by_field["collect_count"][0].normalized_value == 0
    assert by_field["collect_count"][0].source_path == "page_pc.thread.collect_num"
    assert "collect_count" not in result.field_outcomes
    await provider.close()


@pytest.mark.asyncio
async def test_comment_page_pc_candidates_do_not_stop_nested_floor_fallback():
    page = FakePage(
        nested_response_text=_nested_response(),
        page_pc_response_text=_page_pc_response(),
    )
    provider = _provider(FakeContext(lambda: page), max_expansions=1)

    result = await provider.collect(
        _comment_target(
            requested_fields=("author_nickname", "ip_location_text", "agree_count", "disagree_count")
        )
    )

    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert result.status == "ok"
    assert result.diagnostics["page_pc_requests"] == 1
    assert result.diagnostics["nested_floor_requests"] == 1
    assert candidates["author_nickname"].raw_value == "合成昵称"
    assert candidates["ip_location_text"].source_path == "page_pc.user_list[0].ip_address"
    assert candidates["agree_count"].normalized_value == 3
    await provider.close()


@pytest.mark.asyncio
async def test_post_page_pc_identity_finishes_without_dom_owner_and_keeps_empty_outcomes():
    page = FakePage(
        html_text="<html><body></body></html>",
        has_post=False,
        page_pc_response_text=_post_page_pc_response(),
    )
    provider = _provider(FakeContext(lambda: page), max_pages=2, timeout_seconds=0.3)

    result = await provider.collect(_post_target())

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.candidates == ()
    assert result.diagnostics["pages"] == 1
    assert result.diagnostics["page_pc_requests"] == 1
    assert result.field_outcomes["ip_location_text"]["reason"] == "ip_location_empty_in_response"
    assert result.field_outcomes["global_growth_level"]["reason"] == "global_growth_level_zero_is_unknown"
    assert result.field_outcomes["agree_count"]["reason"] == "not_observed_in_response"
    assert "pid=45003" in page.goto_urls[0]
    await provider.close()


@pytest.mark.asyncio
async def test_post_page_snapshot_reuses_one_live_page_for_other_exact_post_ids():
    payload = json.loads(_post_page_pc_response())
    payload["post_list"].append({"id": 45004, "author_id": int(AUTHOR_ID)})
    page = FakePage(
        html_text=_post_html(portrait=PORTRAIT),
        page_pc_response_text=json.dumps(payload, ensure_ascii=False),
    )
    context = FakeContext(lambda: page)
    provider = _provider(context)

    first = await provider.collect(_post_target())
    second = await provider.collect(
        _post_target(target_id="45004", pid="45004")
    )

    assert first.status == second.status == "ok"
    assert first.identity_verified is second.identity_verified is True
    assert second.diagnostics["page_cache_hits"] == 1
    assert second.diagnostics["pages"] == 0
    assert len(context.pages) == 1
    assert len(page.goto_urls) == 1
    assert page.closed is False
    await provider.close()
    assert page.closed is True


@pytest.mark.asyncio
async def test_comment_parent_snapshot_reuses_page_pc_and_nested_floor_per_cid_and_resets_by_run():
    sibling_cid = "9007199254740994"
    sibling_author = "77004"
    anchor = _comment_target(requested_fields=("ip_location_text",))
    sibling = _comment_target(
        target_id=sibling_cid,
        cid=sibling_cid,
        author_user_id=sibling_author,
        requested_fields=("ip_location_text",),
    )
    shared_response = _page_pc_parent_response(
        ((CID, AUTHOR_ID, "黑龙江"), (sibling_cid, sibling_author, "浙江"))
    )
    old_context = FakeContext(
        lambda: FakePage(page_pc_response_text=shared_response, nested_response_text=_nested_response())
    )
    old_provider = _provider(old_context, reuse_parent_snapshots=False)
    old_first = await old_provider.collect(anchor)
    old_second = await old_provider.collect(sibling)
    assert old_first.status == old_second.status == "ok"
    assert sum(len(page.goto_urls) for page in old_context.pages) == 2
    assert sum(result.diagnostics["page_pc_requests"] for result in (old_first, old_second)) == 2
    assert sum(result.diagnostics["nested_floor_requests"] for result in (old_first, old_second)) == 2
    await old_provider.close()

    pages = [
        FakePage(page_pc_response_text=shared_response, nested_response_text=_nested_response()),
        FakePage(
            page_pc_response_text=shared_response,
            nested_response_text=_nested_response(cid=sibling_cid, author_id=sibling_author),
        ),
    ]
    context = FakeContext(lambda: pages.pop(0))
    provider = _provider(context, reuse_parent_snapshots=True)
    provider.set_run_scope("run-one")

    first = await provider.collect(anchor)
    second = await provider.collect(sibling)

    assert first.status == second.status == "ok"
    assert first.diagnostics["page_pc_requests"] == 1
    assert first.diagnostics["nested_floor_requests"] == 1
    assert second.diagnostics["parent_snapshot_hits"] == 1
    assert second.diagnostics["pages"] == 0
    second_candidate = next(
        candidate for candidate in second.candidates if candidate.field_name == "ip_location_text"
    )
    assert second_candidate.normalized_value == "浙江"
    assert second_candidate.identity.cid == sibling_cid
    assert second_candidate.identity.author_user_id == sibling_author
    assert len(context.pages) == 1
    assert len(context.pages[0].goto_urls) == 1

    provider.set_run_scope("run-two")
    third = await provider.collect(sibling)

    assert third.status == "ok"
    assert third.diagnostics["parent_snapshot_hits"] == 0
    assert third.diagnostics["pages"] == 1
    assert len(context.pages) == 2
    assert len(context.pages[1].goto_urls) == 1
    await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope_generation", "parent_scope_generation"),
    [(1, 0), (0, 1)],
)
async def test_parent_snapshot_does_not_cross_scope_generations(scope_generation, parent_scope_generation):
    sibling_cid = str(int(CID) + 1)
    sibling_author = str(int(AUTHOR_ID) + 1)
    shared_response = _page_pc_parent_response(
        ((CID, AUTHOR_ID, "黑龙江"), (sibling_cid, sibling_author, "浙江"))
    )
    pages = [
        FakePage(page_pc_response_text=shared_response, expansion_remaining=0),
        FakePage(page_pc_response_text=shared_response, expansion_remaining=0),
    ]
    context = FakeContext(lambda: pages.pop(0))
    provider = _provider(context, reuse_parent_snapshots=True)

    anchor = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))
    sibling = await provider.collect(
        _comment_target(
            target_id=sibling_cid,
            cid=sibling_cid,
            author_user_id=sibling_author,
            requested_fields=("ip_location_text",),
            scope_generation=scope_generation,
            parent_scope_generation=parent_scope_generation,
        )
    )

    assert anchor.status == sibling.status == "ok"
    assert sibling.diagnostics["parent_snapshot_hits"] == 0
    assert sibling.diagnostics["parent_snapshot_misses"] == 1
    assert sibling.diagnostics["pages"] == 1
    assert len(context.pages) == 2
    await provider.close()


@pytest.mark.asyncio
async def test_parent_snapshot_cache_is_disabled_without_generation_context():
    shared_response = _page_pc_parent_response(((CID, AUTHOR_ID, "黑龙江"),))
    pages = [
        FakePage(page_pc_response_text=shared_response, expansion_remaining=0),
        FakePage(page_pc_response_text=shared_response, expansion_remaining=0),
    ]
    context = FakeContext(lambda: pages.pop(0))
    provider = _provider(context, reuse_parent_snapshots=True)
    target = _comment_target(
        requested_fields=("ip_location_text",),
        scope_generation=None,
        parent_scope_generation=None,
    )

    first = await provider.collect(target)
    second = await provider.collect(target)

    assert first.status == second.status == "ok"
    assert first.diagnostics["parent_snapshot_hits"] == 0
    assert second.diagnostics["parent_snapshot_hits"] == 0
    assert second.diagnostics["pages"] == 1
    assert provider._cached_parent_snapshot is None
    assert len(context.pages) == 2
    await provider.close()


@pytest.mark.parametrize("generation", [True, -1, 1.5, "0"])
def test_target_rejects_invalid_scope_generation(generation):
    with pytest.raises(IdentityError, match="invalid_scope_generation"):
        _comment_target(scope_generation=generation)


@pytest.mark.asyncio
async def test_comment_parent_snapshot_publishes_only_after_full_identity_success():
    page = FakePage(
        page_pc_response_text=_page_pc_parent_response(((CID, AUTHOR_ID, "黑龙江"),)),
        nested_response_text=_nested_response(author_id=str(int(AUTHOR_ID) + 1)),
    )
    provider = _provider(FakeContext(lambda: page), reuse_parent_snapshots=True)

    result = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))

    assert result.status == "identity_unverified"
    assert result.reason is not None
    assert provider._cached_parent_snapshot is None
    await provider.close()


@pytest.mark.asyncio
async def test_parent_snapshot_identity_conflict_clears_cached_frame():
    sibling_cid = str(int(CID) + 1)
    sibling_author = str(int(AUTHOR_ID) + 1)
    shared_response = _page_pc_parent_response(
        ((CID, AUTHOR_ID, "黑龙江"), (sibling_cid, sibling_author, "浙江"))
    )
    page = FakePage(page_pc_response_text=shared_response, expansion_remaining=0)
    provider = _provider(FakeContext(lambda: page), reuse_parent_snapshots=True)
    anchor = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))
    cached_snapshot = provider._cached_parent_snapshot
    assert anchor.status == "ok"
    assert cached_snapshot is not None

    conflicting_target = _comment_target(
        target_id=sibling_cid,
        cid=sibling_cid,
        author_user_id=str(int(sibling_author) + 1),
        requested_fields=("ip_location_text",),
    )
    result = provider._collect_from_parent_snapshot(
        conflicting_target,
        {"parent_snapshot_hits": 0, "parent_snapshot_misses": 0},
    )

    assert result is not None
    assert result.status == "identity_unverified"
    assert result.reason == "response_author_id_mismatch"
    assert provider._cached_parent_snapshot is None
    await provider.close()


@pytest.mark.asyncio
async def test_parent_snapshot_growth_miss_falls_back_to_exact_cid_dom():
    sibling_cid = str(int(CID) + 1)
    sibling_author = str(int(AUTHOR_ID) + 1)
    other_cid = str(int(CID) + 2)
    other_author = str(int(AUTHOR_ID) + 2)
    shared_response = _page_pc_parent_response(
        (
            (CID, AUTHOR_ID, "黑龙江"),
            (sibling_cid, sibling_author, "浙江"),
            (other_cid, other_author, "吉林"),
        )
    )
    anchor = _comment_target(requested_fields=("ip_location_text",))
    sibling = _comment_target(
        target_id=sibling_cid,
        cid=sibling_cid,
        author_user_id=sibling_author,
        requested_fields=("global_growth_level", "global_growth_icon_url"),
    )
    other_sibling = _comment_target(
        target_id=other_cid,
        cid=other_cid,
        author_user_id=other_author,
        requested_fields=("ip_location_text",),
    )

    old_pages = [
        FakePage(
            page_pc_response_text=shared_response,
            nested_response_text=_nested_response(),
        ),
        FakePage(
            html_text=_comment_author_growth_html(include_growth_icon=True, growth_tooltip=True),
            page_pc_response_text=shared_response,
            nested_response_text=_nested_response(cid=sibling_cid, author_id=sibling_author),
        ),
    ]
    old_context = FakeContext(lambda: old_pages.pop(0))
    old_provider = _provider(old_context)
    await old_provider.collect(anchor)
    old_result = await old_provider.collect(sibling)
    await old_provider.close()

    pages = [
        FakePage(
            page_pc_response_text=shared_response,
            nested_response_text=_nested_response(),
        ),
        FakePage(
            html_text=_comment_author_growth_html(include_growth_icon=True, growth_tooltip=True),
            page_pc_response_text=shared_response,
            nested_response_text=_nested_response(cid=sibling_cid, author_id=sibling_author),
        ),
    ]
    context = FakeContext(lambda: pages.pop(0))
    provider = _provider(context, reuse_parent_snapshots=True)
    await provider.collect(anchor)
    cached_snapshot = provider._cached_parent_snapshot
    assert cached_snapshot is not None
    new_result = await provider.collect(sibling)

    assert old_result.status == new_result.status == "ok", (
        old_result.status,
        old_result.reason,
        new_result.status,
        new_result.reason,
    )
    assert {candidate.field_name: candidate.normalized_value for candidate in old_result.candidates} == {
        candidate.field_name: candidate.normalized_value for candidate in new_result.candidates
    }
    assert {candidate.field_name for candidate in new_result.candidates} == {
        "global_growth_level",
        "global_growth_icon_url",
    }
    assert new_result.diagnostics["parent_snapshot_misses"] == 1
    assert new_result.diagnostics["pages"] == 1
    assert len(context.pages) == 2
    assert provider._cached_parent_snapshot is cached_snapshot
    assert sibling_cid in cached_snapshot.missed_target_ids

    repeated_state = {"parent_snapshot_hits": 0, "parent_snapshot_misses": 0}
    assert provider._collect_from_parent_snapshot(sibling, repeated_state) is None
    assert repeated_state["parent_snapshot_misses"] == 0
    assert provider._cached_parent_snapshot is cached_snapshot

    other_result = await provider.collect(other_sibling)
    assert other_result.status == "ok"
    assert other_result.diagnostics["parent_snapshot_hits"] == 1
    assert other_result.diagnostics["pages"] == 0
    other_candidate = next(
        candidate for candidate in other_result.candidates if candidate.field_name == "ip_location_text"
    )
    assert other_candidate.identity.cid == other_cid
    assert len(context.pages) == 2
    await provider.close()


@pytest.mark.asyncio
async def test_comment_parent_snapshot_has_ttl_and_eight_target_cap():
    siblings = [
        (str(9007199254741000 + index), str(77100 + index), f"地区{index}")
        for index in range(8)
    ]
    comments = [(CID, AUTHOR_ID, "黑龙江"), *siblings]
    shared_response = _page_pc_parent_response(comments)
    pages = [
        FakePage(page_pc_response_text=shared_response, nested_response_text=_nested_response())
        for _ in range(3)
    ]
    context = FakeContext(lambda: pages.pop(0))
    provider = _provider(context, reuse_parent_snapshots=True)
    provider.set_run_scope("synthetic-run")

    anchor = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))
    hits = []
    for cid, author_id, _region in siblings[:7]:
        result = await provider.collect(
            _comment_target(
                target_id=cid,
                cid=cid,
                author_user_id=author_id,
                requested_fields=("ip_location_text",),
            )
        )
        hits.append(result)

    assert anchor.status == "ok"
    assert all(result.status == "ok" and result.diagnostics["parent_snapshot_hits"] == 1 for result in hits)
    assert len(context.pages) == 1
    capped = await provider.collect(
        _comment_target(
            target_id=siblings[7][0],
            cid=siblings[7][0],
            author_user_id=siblings[7][1],
            requested_fields=("ip_location_text",),
        )
    )
    assert capped.status == "ok"
    assert capped.diagnostics["parent_snapshot_hits"] == 0
    assert capped.diagnostics["pages"] == 1
    assert len(context.pages) == 2

    assert provider._cached_parent_snapshot is not None
    provider._cached_parent_snapshot.cached_at = 0
    expired = await provider.collect(
        _comment_target(
            target_id=siblings[0][0],
            cid=siblings[0][0],
            author_user_id=siblings[0][1],
            requested_fields=("ip_location_text",),
        )
    )
    assert expired.status == "ok"
    assert expired.diagnostics["parent_snapshot_hits"] == 0
    assert expired.diagnostics["pages"] == 1
    assert len(context.pages) == 3
    await provider.close()


@pytest.mark.asyncio
async def test_post_waits_for_virtualized_owner_after_exact_network_identity():
    page = FakePage(
        html_text="<html><body></body></html>",
        page_pc_response_text=_post_page_pc_response(),
        scroll_html_sequence=[_post_html(portrait=PORTRAIT)],
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=0.3)

    result = await provider.collect(_post_target())

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.diagnostics["post_owner_scrolls"] == 1
    await provider.close()


@pytest.mark.asyncio
async def test_growth_hover_uses_owner_header_and_includes_self_tooltip_reference(monkeypatch):
    selectors = []

    class HoverReference:
        def __init__(self, page):
            self.page = page

        async def hover(self, **_kwargs):
            self.page.hovered = True

    class ReferenceLocator:
        def __init__(self, page, count):
            self.page = page
            self.count_value = count

        async def count(self):
            return self.count_value

        def nth(self, _index):
            return HoverReference(self.page)

    class HeaderLocator:
        def __init__(self, page, count):
            self.page = page
            self.count_value = count

        async def count(self):
            return self.count_value

        def locator(self, selector):
            selectors.append(selector)
            count = 1 if selector == (
                ".icon_tshows.tooltip__reference, .icon_tshows .tooltip__reference"
            ) else 0
            return ReferenceLocator(self.page, count)

    class OwnerLocator:
        def __init__(self, page):
            self.page = page

        async def count(self):
            return 1

        def locator(self, selector):
            selectors.append(selector)
            return HeaderLocator(self.page, 1 if selector == ":scope > .head-line.user-info" else 2)

    class FakePage:
        url = f"https://tieba.baidu.com/p/{TID}?pid=45003"

        def __init__(self):
            self.hovered = False

        def locator(self, selector):
            assert selector == '.pb-comment-item[data-id="45003"]'
            return OwnerLocator(self)

        async def content(self):
            return "<html></html>"

    target = _post_target(
        requested_fields=("global_growth_level", "global_growth_icon_url")
    )
    current = DOMExtractionResult("ok", None, True, ())
    found = DOMExtractionResult(
        "ok", None, True, (SimpleNamespace(field_name="global_growth_icon_url"),)
    )
    page = FakePage()
    monkeypatch.setattr(
        browser_module,
        "extract_dom_candidates",
        lambda *_args: found if page.hovered else current,
    )

    result = await browser_module._hover_growth_and_extract_dom(
        page,
        target,
        current,
        (),
        asyncio.get_running_loop().time() + 1,
        {"growth_hover_attempts": 0},
    )

    assert result.candidates[0].field_name == "global_growth_icon_url"
    assert ":scope > .head-line.user-info" in selectors
    assert ".icon_tshows.tooltip__reference, .icon_tshows .tooltip__reference" in selectors


@pytest.mark.asyncio
async def test_post_page_pc_waits_for_delayed_exact_response_on_later_page():
    page = PaginatedPagePcFakePage(
        html_text="<html><body></body></html>",
        page_pc_responses=[
            _post_page_pc_response(post_id="45004"),
            _post_page_pc_response(ip_address="合成地区", growth_level=3),
        ],
        page_pc_delays=[0, 0.02],
        has_post_by_page=[False, False],
    )
    provider = _provider(FakeContext(lambda: page), max_pages=2, timeout_seconds=0.5)

    result = await provider.collect(_post_target())

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.diagnostics["pages"] == 2
    assert result.diagnostics["page_pc_requests"] == 2
    assert {candidate.field_name for candidate in result.candidates} == {
        "ip_location_text",
        "global_growth_level",
    }
    await provider.close()


@pytest.mark.asyncio
async def test_post_page_pc_keeps_explicit_dom_author_conflict_fail_closed():
    page = FakePage(
        html_text=_post_html(),
        page_pc_response_text=_post_page_pc_response(ip_address="合成地区", growth_level=3),
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=0.3)

    result = await provider.collect(_post_target())

    assert result.status == "identity_unverified"
    assert result.reason == "owner_author_not_verified"
    assert result.identity_verified is False
    assert result.candidates == ()
    await provider.close()


@pytest.mark.asyncio
async def test_comment_page_pc_missing_cid_keeps_exact_nested_expansion_active():
    page = FakePage(
        nested_response_text=_nested_response(),
        page_pc_response_text=_page_pc_response(cid="9007199254740994"),
    )
    provider = _provider(FakeContext(lambda: page), max_expansions=1)

    result = await provider.collect(_comment_target())

    assert result.status == "ok"
    assert result.diagnostics["nested_floor_requests"] == 1
    assert [candidate.field_name for candidate in result.candidates] == ["author_nickname"]
    await provider.close()


@pytest.mark.asyncio
async def test_comment_selector_wait_hands_off_to_verified_page_pc_without_owner(monkeypatch):
    monkeypatch.setattr(browser_module, "_COMMENT_SELECTOR_STABLE_SECONDS", 0.05)
    page = FakePage(
        page_pc_response_text=_page_pc_response(),
        has_post=False,
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 0},
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=1.0)

    result = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))

    assert result.status == "ok"
    assert result.identity_verified is True
    candidate = next(candidate for candidate in result.candidates if candidate.field_name == "ip_location_text")
    assert candidate.source_path == "page_pc.user_list[0].ip_address"
    assert candidate.identity.cid == CID
    assert result.diagnostics["verified_page_pc_response_pages"] == [1]
    assert result.diagnostics["pages"] == 1
    await provider.close()


@pytest.mark.asyncio
async def test_comment_selector_wait_does_not_trust_page_pc_for_another_parent(monkeypatch):
    monkeypatch.setattr(browser_module, "_COMMENT_SELECTOR_STABLE_SECONDS", 0.05)
    wrong_parent = json.loads(_page_pc_response())
    wrong_parent_pid = str(int(POST_ID) + 1)
    wrong_parent["post_list"][0]["id"] = int(wrong_parent_pid)
    wrong_parent["post_list"][0]["sub_post_list"]["pid"] = int(wrong_parent_pid)
    page = FakePage(
        page_pc_response_text=json.dumps(wrong_parent, ensure_ascii=False),
        has_post=False,
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 0},
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=0.2)

    result = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))

    assert result.status == "timeout"
    assert result.reason == "browser_task_timeout"
    assert result.diagnostics["verified_page_pc_response_pages"] == []
    assert result.diagnostics["readiness_step"] == "selector_wait"
    await provider.close()


@pytest.mark.asyncio
async def test_comment_selector_wait_uses_stable_visible_owners_without_marking_identity(monkeypatch):
    monkeypatch.setattr(browser_module, "_COMMENT_SELECTOR_STABLE_SECONDS", 0.05)
    page = FakePage(
        html_text='<div class="pb-comment-item" data-id="99999"></div>',
        has_post=False,
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 1},
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=1.0)

    result = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))

    assert result.status == "identity_unverified"
    assert result.identity_verified is False
    assert result.candidates == ()
    assert result.diagnostics["verified_page_pc_response_pages"] == []
    assert result.diagnostics["pages"] == 1
    await provider.close()


@pytest.mark.asyncio
async def test_comment_selector_wait_preserves_gate_priority():
    page = FakePage(
        status=429,
        page_pc_response_text=_page_pc_response(),
        has_post=False,
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 1},
    )
    provider = _provider(FakeContext(lambda: page))

    result = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))

    assert result.status == "blocked"
    assert result.reason == "http_429"
    await provider.close()


@pytest.mark.asyncio
async def test_page_pc_without_parent_on_page_one_allows_valid_page_two_candidate():
    first_page_payload = json.loads(_page_pc_response())
    first_page_payload["post_list"] = []
    page = PaginatedPagePcFakePage(
        page_pc_responses=[
            json.dumps(first_page_payload, ensure_ascii=False),
            _page_pc_response(),
        ],
        has_post_by_page=[False, True],
        nested_response_text=_nested_response(),
    )
    provider = _provider(
        FakeContext(lambda: page),
        max_pages=2,
        max_expansions=1,
        timeout_seconds=0.2,
    )

    result = await provider.collect(
        _comment_target(requested_fields=("author_nickname", "ip_location_text", "agree_count"))
    )

    assert result.status == "ok"
    assert result.diagnostics["pages"] == 2
    assert result.diagnostics["page_pc_requests"] == 2
    assert result.diagnostics["nested_floor_requests"] == 1
    assert {candidate.field_name for candidate in result.candidates} == {
        "ip_location_text",
        "agree_count",
        "author_nickname",
    }
    await provider.close()


@pytest.mark.asyncio
async def test_provider_rejects_same_author_response_for_another_cid():
    page = FakePage(nested_response_text=_nested_response(cid="9007199254740994"))
    provider = _provider(FakeContext(lambda: page))

    result = await provider.collect(_comment_target())

    assert result.status == "identity_unverified"
    assert result.candidates == ()
    assert result.reason == "cid_missing_from_response"
    await provider.close()


@pytest.mark.asyncio
async def test_response_order_reversal_keeps_each_request_offset_with_its_body():
    page = FakePage(
        nested_plan=[
            ("0", _nested_response(cid="9007199254740994"), 0),
            ("20", _nested_response(cid=CID), 0),
        ]
    )
    provider = _provider(FakeContext(lambda: page), max_expansions=1)

    result = await provider.collect(_comment_target())

    assert result.status == "ok"
    assert len(result.candidates) == 1
    assert result.candidates[0].identity.cid == CID
    assert result.candidates[0].identity.request_offset == "20"
    assert ":request-2:" in result.candidates[0].identity.context_id
    await provider.close()


@pytest.mark.asyncio
async def test_provider_serializes_tasks_and_reuses_injected_context():
    pages_active = 0
    max_pages_active = 0

    def make_page():
        nonlocal pages_active, max_pages_active

        page = FakePage(html_text=_thread_html())
        original_close = page.close

        async def close():
            nonlocal pages_active
            await original_close()
            pages_active -= 1

        async def goto(url, **kwargs):
            nonlocal pages_active, max_pages_active
            pages_active += 1
            max_pages_active = max(max_pages_active, pages_active)
            return await FakePage.goto(page, url, **kwargs)

        page.close = close
        page.goto = goto
        return page

    context = FakeContext(make_page)
    provider = _provider(context)
    first, second = await asyncio.gather(
        provider.collect(_thread_target()),
        provider.collect(_thread_target(tid="81002")),
    )

    assert first.status == "ok"
    assert second.status == "identity_unverified"
    assert max_pages_active == 1
    assert len(context.pages) == 2
    assert all(page.closed for page in context.pages)
    await provider.close()


@pytest.mark.asyncio
async def test_provider_stops_on_429_and_notifies_shared_gate():
    blocked = []
    page = FakePage(status=429)
    provider = _provider(FakeContext(lambda: page), on_blocked=blocked.append)

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert result.reason == "http_429"
    assert blocked == ["http_429"]
    await provider.close()


@pytest.mark.asyncio
async def test_429_remains_sticky_when_later_403_and_response_reset_arrive(monkeypatch):
    blocked = []
    page = FakePage(
        status=429,
        title="百度安全验证",
        page_pc_response_text=_page_pc_response(),
    )
    original_goto = page.goto
    original_text = FakeResponse.text

    async def goto_with_later_403(url, **kwargs):
        await original_goto(url, **kwargs)
        document = FakeRequest(url, resource_type="document")
        page.emit("response", FakeResponse(document, status=403))
        return FakeResponse(document, status=403)

    async def response_body(self):
        if PAGE_PC_PATH in self.url:
            raise ConnectionResetError("synthetic response reset")
        return await original_text(self)

    page.goto = goto_with_later_403
    monkeypatch.setattr(FakeResponse, "text", response_body)
    provider = _provider(FakeContext(lambda: page), on_blocked=blocked.append)

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert result.reason == "http_429"
    assert result.diagnostics["transport_error_type"] == "ConnectionResetError"
    assert blocked == ["http_429"]
    assert provider.has_manual_login_page is False
    await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("readiness", ["security_verification", "timeout"])
async def test_429_during_readiness_is_not_overwritten_by_verification_title(
    monkeypatch, readiness
):
    page = FakePage(title="百度安全验证")
    provider = _provider(FakeContext(lambda: page), headless=False, keep_login_page=True)

    async def gate_races_with_rate_limit(
        _self, _page, _target, _page_deadline, _deadline, state, *_args
    ):
        state["blocked_reason"] = "http_429"
        return readiness

    monkeypatch.setattr(provider, "_wait_for_target", gate_races_with_rate_limit.__get__(provider))

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert result.reason == "http_429"
    assert result.diagnostics.get("manual_gate_detected") is not True
    assert provider.has_manual_login_page is False
    await provider.close()


@pytest.mark.asyncio
async def test_provider_records_login_gate_without_retrying():
    page = FakePage(redirect_url="https://passport.baidu.com/v3/", body_text="正常正文")
    provider = _provider(FakeContext(lambda: page))

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert result.reason == "needs_login"
    await provider.close()


@pytest.mark.asyncio
async def test_page_navigation_connection_reset_is_retryable_and_component_scoped():
    page = FakePage()

    async def reset(_url, **_kwargs):
        raise ConnectionResetError("synthetic reset")

    page.goto = reset
    provider = _provider(FakeContext(lambda: page))

    result = await provider.collect(_thread_target())

    assert result.status == "failed_retryable"
    assert result.reason == "transport_reset"
    assert result.diagnostics["transport_error_type"] == "ConnectionResetError"
    assert result.diagnostics["transport_component"] == "page_navigation"
    assert result.diagnostics["transport_error_at"].endswith("+00:00")
    assert page.closed is True
    await provider.close()


@pytest.mark.asyncio
async def test_page_close_connection_reset_is_retryable_without_leaving_parent_snapshot():
    page = FakePage(
        page_pc_response_text=_page_pc_parent_response(((CID, AUTHOR_ID, "黑龙江"),)),
        nested_response_text=_nested_response(),
        close_error=ConnectionResetError("synthetic page close reset"),
    )
    provider = _provider(FakeContext(lambda: page), reuse_parent_snapshots=True)

    result = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))

    assert result.status == "failed_retryable"
    assert result.reason == "transport_reset"
    assert result.diagnostics["transport_component"] == "page_close"
    assert provider._cached_parent_snapshot is None
    await provider.close()


@pytest.mark.asyncio
async def test_post_body_mentions_login_and_verification_words_without_blocking():
    page = FakePage(
        html_text=_thread_html(),
        body_text="文章讨论验证码、扫码登录和安全验证这些词。",
    )
    provider = _provider(FakeContext(lambda: page))

    result = await provider.collect(_thread_target())

    assert result.status == "ok"
    await provider.close()


@pytest.mark.asyncio
async def test_explicit_security_verification_title_stops_and_notifies_gate():
    blocked = []
    page = FakePage(title="百度安全验证")
    provider = _provider(FakeContext(lambda: page), on_blocked=blocked.append)

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert result.reason == "security_verification"
    assert result.diagnostics["blocked_reason"] == "security_verification"
    assert blocked == ["security_verification"]
    await provider.close()


@pytest.mark.asyncio
async def test_security_verification_title_requires_a_trusted_https_baidu_host():
    page = FakePage(title="百度安全验证")

    page.url = "https://attacker.example/verify"
    assert await browser_module._page_gate_reason(page) is None

    page.url = "http://tieba.baidu.com/p/81001"
    assert await browser_module._page_gate_reason(page) is None

    page.url = "https://user@tieba.baidu.com/p/81001"
    assert await browser_module._page_gate_reason(page) is None

    page.url = "https://tieba.baidu.com/p/81001"
    assert await browser_module._page_gate_reason(page) == "security_verification"


@pytest.mark.asyncio
async def test_explicit_security_verification_redirect_after_http_403_remains_blocked():
    blocked = []
    page = FakePage(
        status=403,
        redirect_url="https://wappass.baidu.com/verify/captcha",
    )
    provider = _provider(FakeContext(lambda: page), on_blocked=blocked.append)

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert result.reason == "security_verification"
    assert result.diagnostics["blocked_reason"] == "security_verification"
    assert blocked == ["http_auth_or_verification"]
    await provider.close()


@pytest.mark.asyncio
async def test_provider_waits_for_delayed_vue_owner_and_thread_context():
    page = FakePage(
        html_text="<html><body><div id='skeleton'></div></body></html>",
        rendered_html_text=_thread_html(),
        render_after=0.08,
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=0.5)

    result = await provider.collect(_thread_target())

    assert result.status == "ok"
    await provider.close()


@pytest.mark.asyncio
async def test_provider_waits_for_delayed_comment_owner_before_expansion():
    page = FakePage(
        nested_response_text=_nested_response(),
        has_post=False,
        render_after=0.08,
        rendered_has_post=True,
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=0.5)

    result = await provider.collect(_comment_target())

    assert result.status == "ok"
    assert result.diagnostics["expansions"] == 1
    await provider.close()


@pytest.mark.asyncio
async def test_provider_waits_past_one_second_for_vue_owner_within_task_budget():
    page = FakePage(
        html_text="<html><body><div id='skeleton'></div></body></html>",
        rendered_html_text=_thread_html(),
        render_after=1.2,
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=3.0)

    result = await provider.collect(_thread_target())

    assert result.status == "ok"
    await provider.close()


@pytest.mark.asyncio
async def test_manual_login_page_is_retained_unhooked_and_closed_explicitly():
    page = FakePage(redirect_url="https://passport.baidu.com/v3/")
    provider = _provider(
        FakeContext(lambda: page),
        headless=False,
        keep_login_page=True,
    )

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert result.reason == "needs_login"
    assert provider.has_manual_login_page is True
    assert page.closed is False
    assert page.handlers == {"request": [], "response": []}
    page.emit("request", FakeRequest("https://tieba.baidu.com/c/f/pb/nestedFloor", method="POST"))
    await provider.close_manual_login_page()
    assert page.closed is True
    assert provider.has_manual_login_page is False
    await provider.close()


@pytest.mark.asyncio
async def test_provider_close_cleans_up_manual_page_in_injected_context():
    page = FakePage(title="百度安全验证")
    provider = _provider(
        FakeContext(lambda: page),
        headless=False,
        keep_login_page=True,
    )

    result = await provider.collect(_thread_target())
    assert result.status == "blocked"
    assert provider.has_manual_login_page is True

    await provider.close()

    assert page.closed is True
    assert provider.has_manual_login_page is False


@pytest.mark.asyncio
async def test_manual_page_is_never_retained_after_http_429():
    page = FakePage(status=429)
    provider = _provider(
        FakeContext(lambda: page),
        headless=False,
        keep_login_page=True,
    )

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert result.reason == "http_429"
    assert provider.has_manual_login_page is False
    assert page.closed is True
    await provider.close()


def test_manual_page_retention_requires_a_visible_browser():
    with pytest.raises(BrowserProviderConfigurationError, match="headless=False"):
        _provider(FakeContext(lambda: FakePage()), keep_login_page=True)


@pytest.mark.asyncio
async def test_user_card_route_stays_unverified_until_schema_evidence_exists():
    page = FakePage(html_text=_thread_html(), emit_user_card=True)
    provider = _provider(FakeContext(lambda: page))

    result = await provider.collect(_thread_target())

    assert result.status == "ok"
    assert result.diagnostics["user_card_requests"] == 1
    assert result.diagnostics["user_card_status"] == "needs_evidence"
    assert result.diagnostics["user_card_reason"] == "unverified_source_schema"
    await provider.close()


@pytest.mark.asyncio
async def test_provider_obeys_max_pages_when_owner_is_not_on_current_page():
    page = FakePage(
        html_text=_thread_html(),
        has_post=False,
        page_pc_response_text=_page_pc_response(),
    )
    target = Target(
        target_type="post",
        target_id="45004",
        tid=TID,
        fid=FID,
        pid="45004",
        author_user_id=AUTHOR_ID,
        author_portrait=PORTRAIT,
        requested_fields=("ip_location_text",),
    )
    provider = _provider(FakeContext(lambda: page), max_pages=2, timeout_seconds=0.5)

    result = await provider.collect(target)

    assert result.status == "not_tested"
    assert result.reason == "target_post_missing_from_response"
    assert result.diagnostics["pages"] == 2
    assert result.diagnostics["page_pc_requests"] == 2
    await provider.close()


def test_provider_requires_budget_and_rejects_default_chrome_profile():
    with pytest.raises(BrowserProviderConfigurationError, match="request budget"):
        PlaywrightWebFallbackProvider(context=object(), budget=None)
    with pytest.raises(BrowserProviderConfigurationError, match="default Chrome"):
        PlaywrightWebFallbackProvider(
            profile_dir="~/Library/Application Support/Google/Chrome/Default",
            budget=FakeBudget(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("headless", [True, False])
async def test_explicit_profile_launch_is_persistent_and_isolated(tmp_path, headless):
    context = FakeContext(lambda: FakePage())
    calls = {}

    class FakeChromium:
        async def launch_persistent_context(self, **kwargs):
            calls.update(kwargs)
            return context

    class FakePlaywright:
        chromium = FakeChromium()

        async def stop(self):
            calls["stopped"] = True

    class FakeManager:
        async def start(self):
            return FakePlaywright()

    provider = PlaywrightWebFallbackProvider(
        profile_dir=tmp_path / "dedicated-profile",
        budget=FakeBudget(),
        playwright_factory=FakeManager,
        headless=headless,
    )

    assert await provider._ensure_context() is context
    await provider.close()
    assert calls["user_data_dir"] == str(tmp_path / "dedicated-profile")
    assert calls["headless"] is headless
    assert context.closed is True
    assert calls["stopped"] is True


@pytest.mark.asyncio
async def test_missing_optional_playwright_shows_install_command(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "playwright.async_api", None)
    provider = PlaywrightWebFallbackProvider(
        profile_dir=tmp_path / "dedicated-profile",
        budget=FakeBudget(),
    )

    with pytest.raises(BrowserProviderConfigurationError, match="uv sync.*playwright install chromium"):
        await provider._ensure_context()

    await provider.close()


@pytest.mark.asyncio
async def test_timeout_reports_requests_already_observed_and_closes_page():
    page = FakePage(html_text=_thread_html(), navigation_delay=0.1)
    provider = _provider(FakeContext(lambda: page), timeout_seconds=0.01)

    result = await provider.collect(_thread_target())

    assert result.status == "timeout"
    assert result.diagnostics["pages"] == 1
    assert result.diagnostics["requests"] == 1
    assert page.closed is True
    await provider.close()


@pytest.mark.asyncio
async def test_page_readiness_timeout_records_safe_dom_counts_without_accepting_target():
    page = FakePage(
        html_text=(
            '<div class="image-text"><div class="pc-pb-first-floor-interactive"></div>'
            '<div class="pb-content-wrap"><div class="richtext-item"></div></div></div>'
        ),
        status=200,
        redirect_url=f"https://tieba.baidu.com/p/{TID}?token=private-value",
        content_delay=0.1,
        diagnostic_result={
            "readyState": "interactive",
            "selectors": {
                "first_floor_interactive": {"count": 1, "visible": 0},
                "image_text_owner": {"count": 1, "visible": 1},
                "pb_content_wrap": {"count": 1, "visible": 1},
                "richtext_item": {"count": 0, "visible": 0},
                "l_post": {"count": 1, "visible": 1},
                "data_field": {"count": 1, "visible": 1},
                "data_pid": {"count": 0, "visible": 0},
                "identity": {"count": 0, "visible": 0},
            },
        },
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=0.05)

    result = await provider.collect(_thread_target())

    assert result.status == "timeout"
    assert result.reason == "browser_task_timeout"
    assert result.diagnostics["readiness_diagnostic_status"] == "ok"
    assert result.diagnostics["readiness_url_class"] == "official_thread"
    assert result.diagnostics["readiness_document_state"] == "interactive"
    assert result.diagnostics["readiness_step"] == "content_snapshot"
    assert result.diagnostics["readiness_headed"] is False
    assert result.diagnostics["readiness_main_document_status"] == 200
    assert result.diagnostics["readiness_first_floor_interactive_count"] == 1
    assert result.diagnostics["readiness_first_floor_interactive_visible"] == 0
    assert result.diagnostics["readiness_budget_pages_exhausted"] is True
    assert result.diagnostics["readiness_resource_request_budget"] == "unknown"
    assert "private-value" not in json.dumps(result.diagnostics)
    for safe_diagnostics in (
        sanitize_web_diagnostics(result.diagnostics),
        _safe_diagnostics(result.diagnostics),
    ):
        assert safe_diagnostics["readiness_url_class"] == "official_thread"
        assert safe_diagnostics["readiness_step"] == "content_snapshot"
        assert safe_diagnostics["readiness_first_floor_interactive_count"] == 1
        assert "private-value" not in json.dumps(safe_diagnostics)
    assert page.closed is True
    await provider.close()


@pytest.mark.asyncio
async def test_page_readiness_diagnostic_failure_keeps_timeout_and_does_not_leak_error():
    page = FakePage(
        html_text="",
        content_delay=0.1,
        evaluate_error=RuntimeError("private-value"),
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=0.05)

    result = await provider.collect(_thread_target())

    assert result.status == "timeout"
    assert result.reason == "browser_task_timeout"
    assert result.diagnostics["readiness_diagnostic_status"] == "unavailable"
    assert "private-value" not in json.dumps(result.diagnostics)
    assert page.closed is True
    await provider.close()


@pytest.mark.asyncio
async def test_page_readiness_diagnostic_preserves_late_gate_and_429_priority():
    page = FakePage(
        html_text="",
        content_delay=0.1,
        diagnostic_result={"readyState": "complete", "selectors": {}},
        diagnostic_title="百度安全验证",
    )

    async def on_blocked(reason):
        assert reason == "security_verification"
        request = FakeRequest(
            f"https://tieba.baidu.com{PAGE_PC_PATH}", resource_type="xhr"
        )
        page.emit("response", FakeResponse(request, status=429))
        await asyncio.sleep(0.25)

    provider = _provider(
        FakeContext(lambda: page),
        timeout_seconds=0.05,
        on_blocked=on_blocked,
    )

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert result.reason == "http_429"
    assert result.diagnostics["readiness_url_class"] == "official_gate"
    assert result.diagnostics["readiness_gate_reason"] == "security_verification"
    assert result.diagnostics["blocked_reason"] == "http_429"
    assert result.diagnostics["manual_gate_detected"] is True
    await provider.close()


@pytest.mark.asyncio
async def test_page_readiness_page_close_failure_preserves_timeout_outcome():
    page = FakePage(
        html_text="",
        content_delay=0.1,
        close_error=ConnectionResetError("private-value"),
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=0.05)

    result = await provider.collect(_thread_target())

    assert result.status == "timeout"
    assert result.reason == "browser_task_timeout"
    assert result.status != "ok"
    assert "private-value" not in json.dumps(result.diagnostics)
    await provider.close()


@pytest.mark.asyncio
async def test_http_429_during_blocked_callback_cleanup_stays_blocked_and_closes_page():
    callback_cancelled = asyncio.Event()

    async def blocked(_reason):
        try:
            await asyncio.sleep(0.15)
        except asyncio.CancelledError:
            callback_cancelled.set()
            raise

    page = FakePage(status=429, close_delay=0.03)
    provider = _provider(
        FakeContext(lambda: page), timeout_seconds=0.02, on_blocked=blocked
    )

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert result.reason == "http_429"
    assert callback_cancelled.is_set()
    assert page.closed is True
    assert len(page.goto_urls) == 1
    await provider.close()


@pytest.mark.asyncio
async def test_blocked_callback_cleanup_is_bounded_and_closes_page():
    callback_cancelled = asyncio.Event()

    async def blocked(_reason):
        try:
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            callback_cancelled.set()
            raise

    page = FakePage(status=429)
    provider = _provider(FakeContext(lambda: page), on_blocked=blocked)

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert callback_cancelled.is_set()
    assert page.closed is True
    await provider.close()


@pytest.mark.asyncio
async def test_explicit_collect_cancellation_finishes_callback_and_page_cleanup():
    callback_started = asyncio.Event()
    callback_cancelled = asyncio.Event()

    async def blocked(_reason):
        callback_started.set()
        try:
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            callback_cancelled.set()
            raise

    page = FakePage(status=429)
    provider = _provider(FakeContext(lambda: page), on_blocked=blocked)
    task = asyncio.create_task(provider.collect(_thread_target()))
    await asyncio.wait_for(callback_started.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert callback_cancelled.is_set()
    assert page.closed is True
    await provider.close()


@pytest.mark.asyncio
async def test_close_cancels_active_collect_and_queued_collect_cannot_open_page():
    pages = []
    page_created = asyncio.Event()

    def make_page():
        page = FakePage(html_text=_thread_html(), navigation_delay=0.2)
        pages.append(page)
        page_created.set()
        return page

    context = FakeContext(make_page)
    provider = _provider(context, timeout_seconds=1)
    first = asyncio.create_task(provider.collect(_thread_target()))
    await asyncio.wait_for(page_created.wait(), timeout=1)
    await asyncio.wait_for(pages[0].goto_started.wait(), timeout=1)
    second = asyncio.create_task(provider.collect(_thread_target(tid="81002")))
    await asyncio.sleep(0)

    await asyncio.gather(provider.close(), provider.close())

    with pytest.raises(asyncio.CancelledError):
        await first
    with pytest.raises(RuntimeError, match="provider is closed"):
        await second
    assert len(pages) == 1
    assert pages[0].closed is True
    assert context.closed is False
    await provider.close()


@pytest.mark.asyncio
async def test_owner_finally_close_does_not_deadlock_with_external_close():
    page_created = asyncio.Event()
    page = FakePage(html_text=_thread_html(), navigation_delay=0.2)

    def make_page():
        page_created.set()
        return page

    context = FakeContext(make_page)
    provider = _provider(context, timeout_seconds=1)

    async def owner():
        try:
            return await provider.collect(_thread_target())
        finally:
            await provider.close()

    owner_task = asyncio.create_task(owner())
    await asyncio.wait_for(page_created.wait(), timeout=1)
    await asyncio.wait_for(page.goto_started.wait(), timeout=1)
    close_task = asyncio.create_task(provider.close())

    done, pending = await asyncio.wait({owner_task, close_task}, timeout=1)

    assert not pending
    assert done == {owner_task, close_task}
    with pytest.raises(asyncio.CancelledError):
        await owner_task
    await close_task
    assert page.closed is True
    assert context.closed is False


@pytest.mark.asyncio
async def test_on_blocked_can_close_provider_without_self_await_deadlock():
    page = FakePage(status=429)
    context = FakeContext(lambda: page)
    provider = None

    async def blocked(_reason):
        await provider.close()

    provider = _provider(context, on_blocked=blocked)
    result = await asyncio.wait_for(provider.collect(_thread_target()), timeout=1)

    assert result.status == "blocked"
    assert page.closed is True
    assert provider._closed is True
    assert context.closed is False
    await provider.close()


@pytest.mark.asyncio
async def test_cancelled_late_response_from_previous_target_cannot_cross_tasks():
    slow_page = FakePage(
        nested_response_text=_nested_response(cid=CID),
        nested_plan=[("0", _nested_response(cid=CID), 0.1)],
    )
    next_cid = "9007199254740994"
    next_page = FakePage(nested_response_text=_nested_response(cid=next_cid))
    pages = iter((slow_page, next_page))
    provider = _provider(FakeContext(lambda: next(pages)), timeout_seconds=0.02)

    first_result = await provider.collect(_comment_target())
    second_target = _comment_target(target_id=next_cid, cid=next_cid)
    second_result = await provider.collect(second_target)

    assert first_result.status == "timeout"
    assert first_result.candidates == ()
    assert slow_page.closed is True
    assert second_result.status == "ok"
    assert all(candidate.identity.cid == next_cid for candidate in second_result.candidates)
    assert all(":request-1:" in candidate.identity.context_id for candidate in second_result.candidates)
    await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target_factory",
    [
        lambda: _comment_target(requested_fields=("ip_location_text",)),
        lambda: _thread_target(requested_fields=("ip_location_text",)),
        lambda: _post_target(requested_fields=("ip_location_text",)),
    ],
)
async def test_deleted_thread_page_exits_fast_as_not_applicable(target_factory):
    page = FakePage(
        html_text="<html><body><div>帖子可能已被删除</div></body></html>",
        has_post=False,
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 0},
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=1.0)

    result = await provider.collect(target_factory())

    assert result.status == "not_applicable"
    assert result.reason == "thread_deleted_or_unavailable"
    assert result.identity_verified is False
    assert result.candidates == ()
    assert result.diagnostics["pages"] == 1
    assert result.field_outcomes == {
        "ip_location_text": {"status": "not_applicable", "reason": "thread_deleted_or_unavailable"}
    }
    await provider.close()


@pytest.mark.asyncio
async def test_deleted_thread_marker_respects_gate_priority_untrusted_host_and_live_owners(monkeypatch):
    monkeypatch.setattr(browser_module, "_COMMENT_SELECTOR_STABLE_SECONDS", 0.05)

    blocked_page = FakePage(
        status=429,
        html_text="<html><body><div>帖子可能已被删除</div></body></html>",
        has_post=False,
    )
    blocked_provider = _provider(FakeContext(lambda: blocked_page), timeout_seconds=1.0)
    blocked_result = await blocked_provider.collect(_comment_target(requested_fields=("ip_location_text",)))
    assert blocked_result.status == "blocked"
    assert blocked_result.reason == "http_429"
    await blocked_provider.close()

    untrusted_page = FakePage(
        redirect_url=f"https://example.com/p/{TID}",
        html_text="<html><body><div>帖子可能已被删除</div></body></html>",
        has_post=False,
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 0},
    )
    untrusted_provider = _provider(FakeContext(lambda: untrusted_page), timeout_seconds=0.15)
    untrusted_result = await untrusted_provider.collect(_comment_target(requested_fields=("ip_location_text",)))
    assert untrusted_result.status == "timeout"
    assert untrusted_result.reason == "browser_task_timeout"
    await untrusted_provider.close()

    live_owner_page = FakePage(
        html_text='<div class="pb-comment-item" data-id="99999">帖子可能已被删除</div>',
        has_post=False,
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 1},
    )
    live_owner_provider = _provider(FakeContext(lambda: live_owner_page), timeout_seconds=1.0)
    live_owner_result = await live_owner_provider.collect(_comment_target(requested_fields=("ip_location_text",)))
    assert live_owner_result.status == "identity_unverified"
    await live_owner_provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target_factory",
    [
        lambda: _comment_target(requested_fields=("ip_location_text",)),
        lambda: _thread_target(requested_fields=("ip_location_text",)),
    ],
)
async def test_nonzero_page_pc_error_exits_fast_instead_of_timing_out(monkeypatch, target_factory):
    monkeypatch.setattr(browser_module, "_COMMENT_SELECTOR_STABLE_SECONDS", 0.05)
    page = FakePage(
        html_text="<html><body><div>未知错误</div></body></html>",
        page_pc_response_text=json.dumps({"error_code": 110001, "error_msg": "未知错误"}, ensure_ascii=False),
        has_post=False,
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 0},
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=1.0)

    result = await provider.collect(target_factory())

    assert result.status == "dom_changed"
    assert result.reason == "response_error_code_nonzero"
    assert result.identity_verified is False
    assert result.field_outcomes == {
        "ip_location_text": {"status": "not_tested", "reason": "response_error_code_nonzero"}
    }
    await provider.close()


@pytest.mark.asyncio
async def test_thread_with_verified_page_pc_and_missing_richtext_exits_fast_as_network_ready(monkeypatch):
    monkeypatch.setattr(browser_module, "_THREAD_NETWORK_READY_STABLE_SECONDS", 0.02)
    payload = json.loads(_page_pc_response())
    payload["thread"]["author"] = {
        "id": int(AUTHOR_ID),
        "ip_address": "合成地区",
        "user_growth": {"level_id": 5},
    }
    modern_thread_html = """
    <html><body><div class="image-text">
      <div class="pb-content-wrap"></div>
      <div class="pc-pb-first-floor-interactive"></div>
    </div></body></html>
    """
    page = FakePage(
        html_text=modern_thread_html,
        page_pc_response_text=json.dumps(payload, ensure_ascii=False),
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 0},
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=1.0)

    result = await provider.collect(
        _thread_target(requested_fields=("ip_location_text", "global_growth_level", "reply_count"))
    )

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.diagnostics["pages"] == 1
    assert result.field_outcomes == {}
    by_field = {candidate.field_name: candidate for candidate in result.candidates}
    assert by_field["ip_location_text"].normalized_value == "合成地区"
    assert by_field["global_growth_level"].normalized_value == 5
    assert by_field["reply_count"].normalized_value == 8
    await provider.close()


@pytest.mark.asyncio
async def test_video_thread_with_mutating_dom_and_verified_page_pc_exits_fast_without_timeout(monkeypatch):
    monkeypatch.setattr(browser_module, "_THREAD_NETWORK_READY_STABLE_SECONDS", 0.02)
    payload = json.loads(_page_pc_response())
    payload["thread"]["author"] = {
        "id": int(AUTHOR_ID),
        "ip_address": "合成地区",
        "user_growth": {"level_id": 5},
    }

    class _MutatingVideoFakePage(FakePage):
        async def content(self):
            tick = self.content_calls + 1
            self.html_text = (
                f'<html><body><div class="image-text"><video data-time="{tick}"></video>'
                '<div class="pc-pb-first-floor-interactive"></div></div></body></html>'
            )
            return await super().content()

    page = _MutatingVideoFakePage(
        page_pc_response_text=json.dumps(payload, ensure_ascii=False),
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 0},
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=0.5)

    result = await provider.collect(
        _thread_target(requested_fields=("ip_location_text", "global_growth_level"))
    )

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.diagnostics["pages"] == 1
    assert page.content_calls >= 3
    by_field = {candidate.field_name: candidate for candidate in result.candidates}
    assert by_field["ip_location_text"].normalized_value == "合成地区"
    assert by_field["global_growth_level"].normalized_value == 5
    await provider.close()


@pytest.mark.asyncio
async def test_thread_with_unverified_page_pc_exits_fast_as_identity_unverified():
    payload = json.loads(_page_pc_response())
    payload["first_floor"] = None
    page = FakePage(
        html_text=(
            '<html><body><div class="image-text">'
            '<div class="pc-pb-first-floor-interactive"></div>'
            "</div></body></html>"
        ),
        page_pc_response_text=json.dumps(payload, ensure_ascii=False),
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 0},
    )
    provider = _provider(FakeContext(lambda: page), timeout_seconds=1.0)

    result = await provider.collect(_thread_target(requested_fields=("ip_location_text",)))

    assert result.status == "identity_unverified"
    assert result.reason == "page_pc_identity_structure_missing"
    assert result.identity_verified is False
    assert result.candidates == ()
    assert result.diagnostics["pages"] == 1
    await provider.close()


@pytest.mark.asyncio
async def test_configurable_parent_snapshot_max_targets_avoids_reopen():
    siblings = [
        (str(9007199254742000 + index), str(78100 + index), f"地区{index}")
        for index in range(12)
    ]
    comments = [(CID, AUTHOR_ID, "黑龙江"), *siblings]
    shared_response = _page_pc_parent_response(comments)
    pages = [
        FakePage(page_pc_response_text=shared_response, nested_response_text=_nested_response())
        for _ in range(2)
    ]
    context = FakeContext(lambda: pages.pop(0))
    provider = _provider(
        context,
        reuse_parent_snapshots=True,
        parent_snapshot_ttl_seconds=300.0,
        parent_snapshot_max_targets=50,
    )
    provider.set_run_scope("synthetic-run")

    anchor = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))
    assert anchor.status == "ok"
    for cid, author_id, _region in siblings:
        res = await provider.collect(
            _comment_target(
                target_id=cid,
                cid=cid,
                author_user_id=author_id,
                requested_fields=("ip_location_text",),
            )
        )
        assert res.status == "ok"
        assert res.diagnostics["parent_snapshot_hits"] == 1
    assert len(context.pages) == 1
    await provider.close()


@pytest.mark.asyncio
async def test_block_nonessential_resources_aborts_images_fonts_and_analytics():
    class _RoutingFakeContext(FakeContext):
        def __init__(self, page_factory):
            super().__init__(page_factory)
            self.route_patterns = []
            self.route_handler = None

        async def route(self, pattern, handler):
            self.route_patterns.append(pattern)
            self.route_handler = handler

    class _FakeRoute:
        def __init__(self, url, resource_type):
            self.request = FakeRequest(url, resource_type=resource_type)
            self.aborted = False
            self.continued = False

        async def abort(self):
            self.aborted = True

        async def continue_(self):
            self.continued = True

    page = FakePage(
        html_text=_thread_html(),
        page_pc_response_text=_page_pc_response(),
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 0},
    )
    context = _RoutingFakeContext(lambda: page)
    provider = _provider(context, block_nonessential_resources=True)
    res = await provider.collect(_thread_target(requested_fields=("ip_location_text",)))
    assert res.status == "ok"
    assert context.route_patterns == ["**/*"]
    assert context.route_handler is not None

    img_route = _FakeRoute("https://tiebapic.baidu.com/pic.jpg", "image")
    hm_route = _FakeRoute("https://hm.baidu.com/hm.js", "script")
    doc_route = _FakeRoute("https://tieba.baidu.com/p/81001", "document")
    xhr_route = _FakeRoute("https://tieba.baidu.com/p/page_pc", "xhr")
    await context.route_handler(img_route)
    await context.route_handler(hm_route)
    await context.route_handler(doc_route)
    await context.route_handler(xhr_route)
    assert img_route.aborted is True
    assert hm_route.aborted is True
    assert doc_route.continued is True
    assert xhr_route.continued is True
    await provider.close()


@pytest.mark.asyncio
async def test_same_thread_reuses_page_snapshot_from_thread_to_post():
    payload = json.loads(_page_pc_response())
    payload["thread"]["author"] = {
        "id": int(AUTHOR_ID),
        "ip_address": "合成地区",
        "user_growth": {"level_id": 5},
    }
    payload["post_list"] = [
        {
            "id": 45003,
            "author_id": int(AUTHOR_ID),
            "floor": 2,
            "author": {
                "id": int(AUTHOR_ID),
                "portrait": PORTRAIT,
                "ip_address": "广东",
                "user_growth": {"level_id": 6},
            },
        }
    ]
    shared_html = (
        f'<html><body><div class="image-text">'
        f'<div class="pc-pb-first-floor-interactive"></div></div>'
        f'<div class="pb-comment-item" data-id="45003">'
        f'<div class="pb-content-wrap"><div class="richtext-item"><span tid="{TID}" fid="{FID}" index="1"></span></div></div>'
        f'<div class="head-line user-info"><div class="name-info"><a class="head-name" href="https://tieba.baidu.com/home/main?id={PORTRAIT}">synthetic</a></div></div>'
        f'<div class="desc-info"><span class="ip-address">广东</span></div>'
        f"</div></body></html>"
    )
    page = FakePage(
        html_text=shared_html,
        page_pc_response_text=json.dumps(payload, ensure_ascii=False),
        selector_readiness_result={"readyState": "complete", "visibleOwnerCount": 0},
    )
    context = FakeContext(lambda: page)
    provider = _provider(context)

    t_res = await provider.collect(_thread_target(requested_fields=("ip_location_text",)))
    assert t_res.status == "ok"
    assert t_res.diagnostics["pages"] == 1

    p_res = await provider.collect(_post_target(requested_fields=("ip_location_text",)))
    assert p_res.status == "ok"
    assert p_res.diagnostics["pages"] == 0
    assert p_res.diagnostics["page_cache_hits"] == 1
    assert len(context.pages) == 1
    await provider.close()


@pytest.mark.asyncio
async def test_multi_page_nested_floor_caches_all_pages_and_exhausted_snapshot():
    page1_cid = CID
    page1_author = AUTHOR_ID
    page2_cid = str(int(CID) + 1)
    page2_author = str(int(AUTHOR_ID) + 1)
    missing_cid = str(int(CID) + 2)
    missing_author = str(int(AUTHOR_ID) + 2)

    class _MultiPageNestedExpansion(FakeExpansion):
        async def click(self, **_kwargs):
            idx = 2 - self.page.expansion_remaining
            self.page.expansion_remaining -= 1
            offset, text = self.page.step_responses[idx]
            request = FakeRequest(
                f"https://tieba.baidu.com{NESTED_FLOOR_PATH}",
                method="POST",
                post_data=f"thread_id={TID}&post_id={POST_ID}&fid={FID}&offset={offset}",
            )
            self.page.emit("request", request)
            response = FakeResponse(request, text=text)
            self.page.emit("response", response)
            self.page.match_expected_response(response)

    class _MultiPageFakePage(FakePage):
        def __init__(self, step_responses, **kwargs):
            super().__init__(**kwargs)
            self.step_responses = step_responses

        def locator(self, selector):
            if selector.startswith(".pb-comment-item[data-id="):
                parent_locator = FakePostLocator(self)
                wrapper = FakeWrapper(self)
                wrapper.locator = (
                    lambda sel: _MultiPageNestedExpansion(self)
                    if sel == ":scope > .show-more-lzl"
                    else FakeNestedItems(self)
                )
                parent_locator.locator = lambda _sel: wrapper
                return parent_locator
            return super().locator(selector)

    def _nested_with_ip(cid, author_id, ip_address):
        return json.dumps(
            {
                "error_code": 0,
                "data": {
                    "post_list": [
                        {
                            "id": cid,
                            "author_id": author_id,
                            "author": {
                                "id": author_id,
                                "name_show": "合成昵称",
                                "ip_address": ip_address,
                            },
                        }
                    ]
                },
            },
            ensure_ascii=False,
        )

    page = _MultiPageFakePage(
        step_responses=[
            ("0", _nested_with_ip(page1_cid, page1_author, "北京")),
            ("10", _nested_with_ip(page2_cid, page2_author, "上海")),
        ],
        page_pc_response_text=_page_pc_parent_response(((page1_cid, page1_author, "北京"),)),
        expansion_remaining=2,
    )
    context = FakeContext(lambda: page)
    provider = _provider(
        context,
        max_expansions=10,
        reuse_parent_snapshots=True,
        parent_snapshot_ttl_seconds=300.0,
        parent_snapshot_max_targets=50,
    )
    provider.set_run_scope("multi-page-run")

    anchor = await provider.collect(
        _comment_target(
            target_id=page1_cid,
            cid=page1_cid,
            author_user_id=page1_author,
            requested_fields=("ip_location_text",),
        )
    )
    assert anchor.status == "ok"
    assert anchor.diagnostics["expansions"] == 2

    # Missing sibling on an exhausted parent resolves from snapshot without re-opening or wiping cache.
    missing = await provider.collect(
        _comment_target(
            target_id=missing_cid,
            cid=missing_cid,
            author_user_id=missing_author,
            requested_fields=("ip_location_text",),
        )
    )
    assert missing.status == "identity_unverified"
    assert missing.reason == "cid_missing_from_response"
    assert missing.diagnostics["pages"] == 0
    assert missing.diagnostics["parent_snapshot_hits"] == 1

    # Sibling on page 2 of nestedFloor hits cached snapshot without opening a new page.
    second = await provider.collect(
        _comment_target(
            target_id=page2_cid,
            cid=page2_cid,
            author_user_id=page2_author,
            requested_fields=("ip_location_text",),
        )
    )
    assert second.status == "ok"
    assert second.diagnostics["pages"] == 0
    assert second.diagnostics["parent_snapshot_hits"] == 1
    assert next(c.normalized_value for c in second.candidates if c.field_name == "ip_location_text") == "上海"
    assert len(context.pages) == 1
    await provider.close()


@pytest.mark.asyncio
async def test_unexhausted_parent_snapshot_missing_cid_preserves_cache_for_next_sibling():
    page1_cid = CID
    page1_author = AUTHOR_ID
    page2_cid = str(int(CID) + 1)
    page2_author = str(int(AUTHOR_ID) + 1)
    missing_cid = str(int(CID) + 2)
    missing_author = str(int(AUTHOR_ID) + 2)

    shared_response = _page_pc_parent_response(
        ((page1_cid, page1_author, "黑龙江"), (page2_cid, page2_author, "浙江"))
    )
    pages = [
        FakePage(
            page_pc_response_text=shared_response,
            nested_response_text=_nested_response(cid=page1_cid, author_id=page1_author),
            expansion_remaining=2,  # hits max_expansions=1 -> NOT exhausted
        ),
        FakePage(
            page_pc_response_text=shared_response,
            nested_response_text=_nested_response(cid=page1_cid, author_id=page1_author),
            expansion_remaining=1,
        ),
    ]
    context = FakeContext(lambda: pages.pop(0))
    provider = _provider(context, max_expansions=1, reuse_parent_snapshots=True)
    provider.set_run_scope("unexhausted-run")

    anchor = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))
    assert anchor.status == "ok"
    cached = provider._cached_parent_snapshot
    assert cached is not None
    assert cached.exhausted is False

    # Missing CID misses cache and falls back to fresh page, without destroying cached snapshot.
    missing = await provider.collect(
        _comment_target(
            target_id=missing_cid,
            cid=missing_cid,
            author_user_id=missing_author,
            requested_fields=("ip_location_text",),
        )
    )
    assert missing.status == "identity_unverified"
    assert missing.diagnostics["pages"] == 1
    assert provider._cached_parent_snapshot is cached

    # Next valid sibling still reuses the preserved snapshot!
    sibling = await provider.collect(
        _comment_target(
            target_id=page2_cid,
            cid=page2_cid,
            author_user_id=page2_author,
            requested_fields=("ip_location_text",),
        )
    )
    assert sibling.status == "ok"
    assert sibling.diagnostics["parent_snapshot_hits"] == 1
    assert sibling.diagnostics["pages"] == 0
    assert len(context.pages) == 2
    await provider.close()


@pytest.mark.asyncio
async def test_parent_snapshot_reuses_expanded_dom_for_sibling_growth_and_absent_growth():
    anchor_cid = CID
    anchor_author = AUTHOR_ID
    anchor_portrait = PORTRAIT
    icon_cid = str(int(CID) + 1)
    icon_author = str(int(AUTHOR_ID) + 1)
    icon_portrait = "tb.1.sibling_icon"
    no_icon_cid = str(int(CID) + 2)
    no_icon_author = str(int(AUTHOR_ID) + 2)
    no_icon_portrait = "tb.1.sibling_none"

    expanded_html = f"""
    <html><body>
      <div class="pb-comment-item" data-id="{POST_ID}">
        <div class="pb-content-wrap"><div class="richtext-item">
          <span tid="{TID}" fid="{FID}" index="1"></span>
        </div></div>
        <div class="comment-content"><div class="lzl-wrapper">
          <div class="pb-lzl-item">
            <div class="head-line user-info"><div class="name-info">
              <a class="head-name" href="https://tieba.baidu.com/home/main?id={anchor_portrait}">anchor</a>
            </div></div>
          </div>
          <div class="pb-lzl-item">
            <div class="head-line user-info"><div class="name-info">
              <a class="head-name" href="https://tieba.baidu.com/home/main?id={icon_portrait}">with_icon</a>
            </div><span class="icon_tshows tooltip__reference" aria-describedby="sibling-tip"><img
              class="icon_tshows_img"
              src="https://tieba-ares.cdn.bcebos.com/synthetic/sibling-growth.webp"></span></div>
          </div>
          <div class="pb-lzl-item">
            <div class="head-line user-info"><div class="name-info">
              <a class="head-name" href="https://tieba.baidu.com/home/main?id={no_icon_portrait}">no_icon</a>
            </div></div>
          </div>
        </div></div>
      </div>
      <div id="sibling-tip">贴吧成长等级：Lv. 7</div>
    </body></html>"""

    shared_response = _page_pc_parent_response(
        (
            (anchor_cid, anchor_author, "北京"),
            (icon_cid, icon_author, "上海"),
            (no_icon_cid, no_icon_author, "广东"),
        )
    )
    page = FakePage(
        html_text=expanded_html,
        page_pc_response_text=shared_response,
        nested_response_text=_nested_response(cid=anchor_cid, author_id=anchor_author),
        expansion_remaining=0,
    )
    context = FakeContext(lambda: page)
    provider = _provider(context, reuse_parent_snapshots=True)
    provider.set_run_scope("growth-dom-reuse-run")

    anchor = await provider.collect(
        _comment_target(
            target_id=anchor_cid,
            cid=anchor_cid,
            author_user_id=anchor_author,
            author_portrait=anchor_portrait,
            requested_fields=("ip_location_text",),
        )
    )
    assert anchor.status == "ok"

    # Sibling with growth icon in expanded DOM hits snapshot with 0 page loads.
    with_icon = await provider.collect(
        _comment_target(
            target_id=icon_cid,
            cid=icon_cid,
            author_user_id=icon_author,
            author_portrait=icon_portrait,
            requested_fields=("global_growth_icon_url", "global_growth_level", "ip_location_text"),
        )
    )
    assert with_icon.status == "ok"
    assert with_icon.diagnostics["parent_snapshot_hits"] == 1
    assert with_icon.diagnostics["pages"] == 0
    with_icon_map = {c.field_name: c.normalized_value for c in with_icon.candidates}
    assert with_icon_map["ip_location_text"] == "上海"
    assert with_icon_map["global_growth_level"] == 7
    assert with_icon_map["global_growth_icon_url"] == (
        "https://tieba-ares.cdn.bcebos.com/synthetic/sibling-growth.webp"
    )

    # Sibling with no growth icon in expanded DOM also hits snapshot and marks absent cleanly.
    without_icon = await provider.collect(
        _comment_target(
            target_id=no_icon_cid,
            cid=no_icon_cid,
            author_user_id=no_icon_author,
            author_portrait=no_icon_portrait,
            requested_fields=("global_growth_icon_url", "global_growth_level", "ip_location_text"),
        )
    )
    assert without_icon.status == "ok"
    assert without_icon.diagnostics["parent_snapshot_hits"] == 1
    assert without_icon.diagnostics["pages"] == 0
    without_icon_map = {c.field_name: c.normalized_value for c in without_icon.candidates}
    assert without_icon_map == {"ip_location_text": "广东"}
    assert without_icon.field_outcomes["global_growth_icon_url"]["status"] == "absent_on_page"
    assert without_icon.field_outcomes["global_growth_level"]["status"] == "absent_on_page"
    assert len(context.pages) == 1
    await provider.close()


@pytest.mark.asyncio
async def test_parent_snapshot_falls_back_when_cached_dom_growth_needs_hover():
    anchor_cid = CID
    anchor_author = AUTHOR_ID
    sibling_cid = str(int(CID) + 1)
    sibling_author = str(int(AUTHOR_ID) + 1)
    sibling_portrait = "tb.1.needs_hover"

    # Unmapped/empty-image icon_tshows triggers growth_icon_image_not_found -> needs hover on live page.
    initial_html = f"""
    <html><body>
      <div class="pb-comment-item" data-id="{POST_ID}">
        <div class="pb-content-wrap"><div class="richtext-item">
          <span tid="{TID}" fid="{FID}" index="1"></span>
        </div></div>
        <div class="comment-content"><div class="lzl-wrapper">
          <div class="pb-lzl-item">
            <div class="head-line user-info"><div class="name-info">
              <a class="head-name" href="https://tieba.baidu.com/home/main?id={PORTRAIT}">anchor</a>
            </div></div>
          </div>
          <div class="pb-lzl-item">
            <div class="head-line user-info"><div class="name-info">
              <a class="head-name" href="https://tieba.baidu.com/home/main?id={sibling_portrait}">hover_needed</a>
            </div><span class="icon_tshows tooltip__reference"></span></div>
          </div>
        </div></div>
      </div>
    </body></html>"""
    hovered_html = initial_html.replace(
        '<span class="icon_tshows tooltip__reference"></span>',
        '<span class="icon_tshows tooltip__reference" aria-describedby="hover-tip"></span>',
    ).replace("</body>", '<div id="hover-tip">贴吧成长等级：Lv. 9</div></body>')

    shared_response = _page_pc_parent_response(
        ((anchor_cid, anchor_author, "北京"), (sibling_cid, sibling_author, "浙江"))
    )
    pages = [
        FakePage(html_text=initial_html, page_pc_response_text=shared_response, expansion_remaining=0),
        FakePage(
            html_text=initial_html,
            hovered_html_text=hovered_html,
            comment_growth_hover_points=[{"x": 10, "y": 10}],
            page_pc_response_text=shared_response,
            expansion_remaining=0,
        ),
    ]
    context = FakeContext(lambda: pages.pop(0))
    provider = _provider(context, reuse_parent_snapshots=True)
    provider.set_run_scope("hover-fallback-run")

    anchor = await provider.collect(_comment_target(requested_fields=("ip_location_text",)))
    assert anchor.status == "ok"

    sibling = await provider.collect(
        _comment_target(
            target_id=sibling_cid,
            cid=sibling_cid,
            author_user_id=sibling_author,
            author_portrait=sibling_portrait,
            requested_fields=("global_growth_level", "global_growth_icon_url"),
        )
    )
    assert sibling.status == "ok"
    assert sibling.diagnostics["parent_snapshot_misses"] == 1
    assert sibling.diagnostics["pages"] == 1
    assert next(c.normalized_value for c in sibling.candidates if c.field_name == "global_growth_level") == 9
    assert len(context.pages) == 2
    await provider.close()


@pytest.mark.asyncio
async def test_owned_context_target_closed_on_new_page_auto_relaunches_and_succeeds(tmp_path):
    class TargetClosedError(Exception):
        pass

    launch_calls = 0

    class DeadNewPageContext:
        async def new_page(self):
            raise TargetClosedError(
                "BrowserContext.new_page: Target page, context or browser has been closed"
            )

        async def close(self):
            pass

    healthy_context = FakeContext(lambda: FakePage(html_text=_thread_html()))

    class FakeChromium:
        async def launch_persistent_context(self, **_kwargs):
            nonlocal launch_calls
            launch_calls += 1
            return DeadNewPageContext() if launch_calls == 1 else healthy_context

    class FakePlaywright:
        chromium = FakeChromium()

        async def stop(self):
            pass

    class FakeManager:
        async def start(self):
            return FakePlaywright()

    provider = PlaywrightWebFallbackProvider(
        profile_dir=tmp_path / "dedicated-profile",
        budget=FakeBudget(),
        playwright_factory=FakeManager,
    )

    result = await provider.collect(_thread_target())

    assert result.status == "ok"
    assert launch_calls == 2
    assert result.diagnostics["browser_relaunch_attempts"] == 1
    assert len(healthy_context.pages) == 1
    await provider.close()


@pytest.mark.asyncio
async def test_owned_context_disconnected_browser_auto_relaunches_in_ensure_context(tmp_path):
    connected = True
    launch_calls = 0

    class FakeBrowser:
        def is_connected(self):
            return connected

    class DisconnectingContext(FakeContext):
        def __init__(self):
            super().__init__(lambda: FakePage(html_text=_thread_html()))
            self.browser = FakeBrowser()

    contexts = [DisconnectingContext(), FakeContext(lambda: FakePage(html_text=_thread_html()))]

    class FakeChromium:
        async def launch_persistent_context(self, **_kwargs):
            nonlocal launch_calls
            launch_calls += 1
            return contexts[launch_calls - 1]

    class FakePlaywright:
        chromium = FakeChromium()

        async def stop(self):
            pass

    class FakeManager:
        async def start(self):
            return FakePlaywright()

    provider = PlaywrightWebFallbackProvider(
        profile_dir=tmp_path / "dedicated-profile",
        budget=FakeBudget(),
        playwright_factory=FakeManager,
    )

    first = await provider.collect(_thread_target())
    assert first.status == "ok"
    assert launch_calls == 1

    connected = False
    second = await provider.collect(_thread_target())
    assert second.status == "ok"
    assert launch_calls == 2
    await provider.close()


@pytest.mark.asyncio
async def test_owned_context_target_closed_mid_task_resets_context_for_next_target(tmp_path):
    class TargetClosedError(Exception):
        pass

    launch_calls = 0
    broken_page = FakePage(close_error=TargetClosedError("Target page, context or browser has been closed"))

    async def closed_goto(_url, **_kwargs):
        raise TargetClosedError("Page.goto: Target page, context or browser has been closed")

    broken_page.goto = closed_goto
    broken_context = FakeContext(lambda: broken_page)
    healthy_context = FakeContext(lambda: FakePage(html_text=_thread_html()))

    class FakeChromium:
        async def launch_persistent_context(self, **_kwargs):
            nonlocal launch_calls
            launch_calls += 1
            return broken_context if launch_calls == 1 else healthy_context

    class FakePlaywright:
        chromium = FakeChromium()

        async def stop(self):
            pass

    class FakeManager:
        async def start(self):
            return FakePlaywright()

    provider = PlaywrightWebFallbackProvider(
        profile_dir=tmp_path / "dedicated-profile",
        budget=FakeBudget(),
        playwright_factory=FakeManager,
    )

    first = await provider.collect(_thread_target())

    assert first.status == "failed_retryable"
    assert first.reason == "browser_context_closed"
    assert provider._context is None

    second = await provider.collect(_thread_target())
    assert second.status == "ok"
    assert launch_calls == 2
    await provider.close()


@pytest.mark.asyncio
async def test_owned_context_repeated_target_closed_returns_browser_context_closed(tmp_path):
    class TargetClosedError(Exception):
        pass

    launch_calls = 0

    class DeadNewPageContext:
        async def new_page(self):
            raise TargetClosedError("Target page, context or browser has been closed")

        async def close(self):
            pass

    class FakeChromium:
        async def launch_persistent_context(self, **_kwargs):
            nonlocal launch_calls
            launch_calls += 1
            return DeadNewPageContext()

    class FakePlaywright:
        chromium = FakeChromium()

        async def stop(self):
            pass

    class FakeManager:
        async def start(self):
            return FakePlaywright()

    provider = PlaywrightWebFallbackProvider(
        profile_dir=tmp_path / "dedicated-profile",
        budget=FakeBudget(),
        playwright_factory=FakeManager,
    )

    result = await provider.collect(_thread_target())

    assert result.status == "failed_retryable"
    assert result.reason == "browser_context_closed"
    assert result.diagnostics["browser_relaunch_attempts"] == 1
    assert launch_calls == 2
    await provider.close()
