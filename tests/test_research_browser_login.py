import asyncio
import sys
from pathlib import Path

import pytest

from src.research import browser_login as browser_login_module


class FakeContext:
    def __init__(self, cookie_batches):
        self.cookie_batches = list(cookie_batches)
        self.cookie_urls = []
        self.closed = False
        self.page = None

    async def new_page(self):
        self.page = FakePage()
        return self.page

    async def cookies(self, urls):
        self.cookie_urls.append(urls)
        if len(self.cookie_batches) > 1:
            return self.cookie_batches.pop(0)
        return self.cookie_batches[0]

    async def close(self):
        self.closed = True


class FakePage:
    def __init__(self):
        self.goto_args = None

    async def goto(self, *args, **kwargs):
        self.goto_args = (args, kwargs)


class FakeBrowser:
    def __init__(self, context):
        self.context = context
        self.launch_args = None
        self.closed = False
        self.new_context_requested = False

    async def new_context(self):
        self.new_context_requested = True
        return self.context

    async def launch_persistent_context(self, **_kwargs):
        pytest.fail("existing browser profiles must not be read")

    async def close(self):
        self.closed = True


class FakePlaywright:
    def __init__(self, browser):
        self.chromium = self
        self.browser = browser
        self.stopped = False

    async def launch(self, **kwargs):
        self.browser.launch_args = kwargs
        return self.browser

    async def stop(self):
        self.stopped = True


class FakePlaywrightManager:
    def __init__(self, playwright):
        self.playwright = playwright
        self.started = False

    async def start(self):
        self.started = True
        return self.playwright


def make_playwright(cookie_batches):
    context = FakeContext(cookie_batches)
    browser = FakeBrowser(context)
    playwright = FakePlaywright(browser)
    manager = FakePlaywrightManager(playwright)
    return context, browser, playwright, manager, lambda: manager


@pytest.mark.asyncio
async def test_manual_login_saves_only_baidu_bduss_and_closes_context(monkeypatch, tmp_path):
    account_file = tmp_path / "account.json"
    cookie = {"name": "BDUSS", "value": "synthetic-cookie-value", "domain": ".baidu.com"}
    context, browser, playwright, manager, factory = make_playwright([[], [cookie]])
    saved = []

    def save_bduss(value, path):
        saved.append((value, path))
        return Path(path)

    monkeypatch.setattr("src.research.credentials.save_bduss", save_bduss, raising=False)
    monkeypatch.setattr(browser_login_module, "POLL_INTERVAL_SECONDS", 0.001)

    result = await browser_login_module._browser_login_with_playwright(
        factory,
        account_file=account_file,
        timeout_seconds=1.0,
    )

    assert result == account_file
    assert saved == [(cookie["value"], account_file)]
    assert context.cookie_urls == [list(browser_login_module.BAIDU_COOKIE_URLS)] * 2
    assert browser.launch_args == {"headless": False}
    assert browser.new_context_requested
    assert context.page.goto_args[0] == (browser_login_module.TIEBA_HOME_URL,)
    assert context.closed
    assert browser.closed
    assert playwright.stopped
    assert manager.started


@pytest.mark.asyncio
async def test_ignores_bduss_cookie_from_non_baidu_domain(monkeypatch):
    context, browser, playwright, _, factory = make_playwright(
        [[{"name": "BDUSS", "value": "synthetic", "domain": ".evil.example"}]]
    )
    monkeypatch.setattr("src.research.credentials.save_bduss", lambda *_: pytest.fail("must not save"), raising=False)

    with pytest.raises(browser_login_module.BrowserLoginTimeoutError):
        await browser_login_module._browser_login_with_playwright(
            factory,
            account_file=None,
            timeout_seconds=0.01,
        )

    assert context.cookie_urls == [list(browser_login_module.BAIDU_COOKIE_URLS)]
    assert context.closed
    assert browser.closed
    assert playwright.stopped


@pytest.mark.asyncio
async def test_timeout_closes_browser_and_playwright(monkeypatch):
    context, browser, playwright, _, factory = make_playwright([[]])
    monkeypatch.setattr("src.research.credentials.save_bduss", lambda *_: pytest.fail("must not save"), raising=False)

    with pytest.raises(browser_login_module.BrowserLoginTimeoutError):
        await browser_login_module._browser_login_with_playwright(
            factory,
            account_file=None,
            timeout_seconds=0.01,
        )

    assert context.closed
    assert browser.closed
    assert playwright.stopped


@pytest.mark.asyncio
async def test_cancellation_closes_browser_and_playwright(monkeypatch):
    entered_cookies = asyncio.Event()
    never_finish = asyncio.Event()
    context, browser, playwright, _, factory = make_playwright([[]])

    async def wait_forever(_urls):
        entered_cookies.set()
        await never_finish.wait()

    context.cookies = wait_forever
    monkeypatch.setattr("src.research.credentials.save_bduss", lambda *_: pytest.fail("must not save"), raising=False)
    task = asyncio.create_task(
        browser_login_module._browser_login_with_playwright(
            factory,
            account_file=None,
            timeout_seconds=60,
        )
    )
    await entered_cookies.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert context.closed
    assert browser.closed
    assert playwright.stopped


@pytest.mark.asyncio
async def test_missing_playwright_shows_free_setup_and_cookie_fallback(monkeypatch):
    monkeypatch.setitem(sys.modules, "playwright.async_api", None)

    with pytest.raises(browser_login_module.BrowserLoginUnavailableError) as error:
        await browser_login_module.browser_login()

    assert "uv sync --group login" in str(error.value)
    assert "uv run playwright install chromium" in str(error.value)
    assert "save_bduss_interactively" in str(error.value)
    assert "synthetic-cookie-value" not in str(error.value)


@pytest.mark.asyncio
async def test_missing_chromium_shows_free_setup(monkeypatch):
    _, _, playwright, _, factory = make_playwright([[]])

    async def missing_executable(**_kwargs):
        raise RuntimeError("Executable doesn't exist; please run playwright install chromium")

    monkeypatch.setattr(playwright.chromium, "launch", missing_executable)

    with pytest.raises(browser_login_module.BrowserLoginUnavailableError) as error:
        await browser_login_module._browser_login_with_playwright(
            factory,
            account_file=None,
            timeout_seconds=1,
        )

    assert "uv run playwright install chromium" in str(error.value)
    assert playwright.stopped
