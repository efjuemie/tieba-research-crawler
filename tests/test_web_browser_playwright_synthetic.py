from __future__ import annotations

import json

import pytest

from src.research.web_fallback import browser as browser_module
from src.research.web_fallback.browser import PlaywrightWebFallbackProvider
from src.research.web_fallback.identity import Target


class _RequestBudget:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None


@pytest.mark.asyncio
async def test_windows_chromium_collects_only_fully_intercepted_synthetic_responses(
    monkeypatch,
):
    playwright_api = pytest.importorskip("playwright.async_api")
    async with playwright_api.async_playwright() as playwright:
        try:
            browser = await playwright.chromium.launch(headless=True)
        except Exception as error:
            pytest.skip(f"Chromium is not installed: {type(error).__name__}")
        context = await browser.new_context()
        routed_urls = []

        async def synthetic_route(route):
            request = route.request
            routed_urls.append(request.url)
            if request.url.endswith("/c/f/pb/page_pc"):
                await route.fulfill(
                    status=200,
                    content_type="application/json",
                    body=json.dumps(
                        {
                            "error_code": 0,
                            "forum": {"id": 120},
                            "thread": {
                                "id": 81001,
                                "post_id": 81002,
                                "author": {"id": 77003},
                                "reply_num": 8,
                                "agree": {"agree_num": 5, "disagree_num": 2},
                                "collect_num": 0,
                                "share_num": 1,
                            },
                            "first_floor": {"id": 81002, "author_id": 77003},
                        }
                    ),
                )
                return
            if "/p/81001" in request.url:
                await route.fulfill(
                    status=200,
                    content_type="text/html",
                    body=(
                        '<div class="image-text"><div class="pb-content-wrap">'
                        '<div class="richtext-item"><span tid="81001" fid="120" index="0">'
                        '</span></div></div><div class="head-line user-info"><div class="name-info">'
                        '<a class="head-name" href="https://tieba.baidu.com/home/main?id=tb.1.synthetic">'
                        'synthetic</a></div></div><div class="desc-info"><span class="ip-address">合成地区'
                        '</span></div><div class="pc-pb-first-floor-interactive"></div>'
                        '<script>fetch("https://tieba.baidu.com/c/f/pb/page_pc", {'
                        'method:"POST", headers:{"Content-Type":"application/x-www-form-urlencoded"}, '
                        'body:"kz=81001&pn=1"});</script>'
                    ),
                )
                return
            await route.fulfill(status=204, body="")

        await context.route("**/*", synthetic_route)
        monkeypatch.setattr(
            browser_module,
            "_target_page_url",
            lambda _target, _page_number: "https://tieba.baidu.com/p/81001?pn=1",
        )
        provider = PlaywrightWebFallbackProvider(
            context=context,
            budget=_RequestBudget(),
            timeout_seconds=10,
            max_pages=1,
        )
        target = Target(
            target_type="thread",
            target_id="81001",
            tid="81001",
            fid="120",
            author_user_id="77003",
            author_portrait="tb.1.synthetic",
            requested_fields=("reply_count", "agree_count", "disagree_count", "collect_count", "share_count"),
        )

        try:
            result = await provider.collect(target)
            assert result.status == "ok"
            assert result.identity_verified is True
            assert {
                candidate.field_name: candidate.normalized_value
                for candidate in result.candidates
            } == {
                "reply_count": 8,
                "agree_count": 5,
                "disagree_count": 2,
                "collect_count": 0,
                "share_count": 1,
            }
            assert result.diagnostics["page_pc_requests"] == 1
            assert all(url.startswith("https://tieba.baidu.com/") for url in routed_urls)
            assert len(context.pages) == 0
        finally:
            await provider.close()
            await context.close()
            await browser.close()
