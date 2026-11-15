from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest
from lxml import html

from src.research.web_fallback.browser import (
    BrowserProviderConfigurationError,
    PlaywrightWebFallbackProvider,
)
from src.research.web_fallback.identity import Target
from src.research.web_fallback.network import NESTED_FLOOR_PATH, PAGE_PC_PATH, USER_CARD_PATH

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
    ):
        self.url = "about:blank"
        self.html_text = html_text
        self.body_text = body_text
        self.goto_status = status
        self.nested_response_text = nested_response_text
        self.page_pc_response_text = page_pc_response_text
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
        self._navigation_started = None
        self.handlers = {}
        self.expected_responses = []
        self.closed = False
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
            self.emit("response", FakeResponse(page_pc, text=self.page_pc_response_text))
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
        if (
            self.render_after
            and self._navigation_started is not None
            and asyncio.get_running_loop().time() - self._navigation_started >= self.render_after
            and self.rendered_html_text is not None
        ):
            return self.rendered_html_text
        return self.html_text

    async def close(self):
        if self.close_delay:
            await asyncio.sleep(self.close_delay)
        self.closed = True


class PaginatedPagePcFakePage(FakePage):
    def __init__(self, *, page_pc_responses, has_post_by_page, nested_response_text="{}"):
        super().__init__(has_post=False, nested_response_text=nested_response_text)
        self.page_pc_responses = page_pc_responses
        self.has_post_by_page = has_post_by_page
        self.page_number = 0

    async def goto(self, url, **kwargs):
        self.page_number += 1
        self.page_pc_response_text = self.page_pc_responses[self.page_number - 1]
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


def _provider(context, budget=None, **kwargs):
    return PlaywrightWebFallbackProvider(
        context=context,
        budget=budget or FakeBudget(),
        **kwargs,
    )


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
async def test_provider_records_login_gate_without_retrying():
    page = FakePage(redirect_url="https://passport.baidu.com/v3/", body_text="正常正文")
    provider = _provider(FakeContext(lambda: page))

    result = await provider.collect(_thread_target())

    assert result.status == "blocked"
    assert result.reason == "needs_login"
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
    assert result.reason == "blocked"
    assert blocked == ["blocked"]
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
        target_id=POST_ID,
        tid=TID,
        fid=FID,
        pid=POST_ID,
        author_user_id=AUTHOR_ID,
        author_portrait=PORTRAIT,
        requested_fields=("ip_location_text",),
    )
    provider = _provider(FakeContext(lambda: page), max_pages=2, timeout_seconds=0.1)

    result = await provider.collect(target)

    assert result.status in {"dom_changed", "timeout"}
    assert result.diagnostics["pages"] == 2
    assert result.diagnostics["page_pc_requests"] == 0
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

    with pytest.raises(BrowserProviderConfigurationError, match="uv sync --group login"):
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
async def test_total_timeout_during_blocked_callback_cleanup_still_closes_page():
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

    assert result.status == "timeout"
    assert callback_cancelled.is_set()
    assert page.closed is True
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
