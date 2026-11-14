"""Optional headed browser helper for a user-completed Baidu Tieba login."""

from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

TIEBA_HOME_URL = "https://tieba.baidu.com/"
BAIDU_COOKIE_URLS = ("https://tieba.baidu.com/", "https://passport.baidu.com/")
POLL_INTERVAL_SECONDS = 1.0
DEFAULT_TIMEOUT_SECONDS = 600.0

_BROWSER_SETUP_HELP = (
    "Install the optional browser dependency with `uv sync --group login`, then install Chromium with "
    "`uv run playwright install chromium`. To save a BDUSS or Cookie header through a hidden local prompt "
    "instead, run `uv run python -c 'from src.research.credentials import save_bduss_interactively; "
    "save_bduss_interactively()'`. Never paste credentials into chat or a command argument."
)


class BrowserLoginUnavailableError(RuntimeError):
    """Raised when the optional Playwright package or its browser is unavailable."""


class BrowserLoginTimeoutError(TimeoutError):
    """Raised when the user does not complete login before the configured timeout."""


async def browser_login(
    account_file: str | Path | None = None,
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> Path:
    """Wait for a manual Tieba login in a temporary browser and save its BDUSS locally.

    This opens a headed, non-persistent Playwright context. It does not inspect any
    existing browser profile or write a Playwright storage-state file.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        raise BrowserLoginUnavailableError(f"Playwright is not installed. {_BROWSER_SETUP_HELP}") from None

    return await _browser_login_with_playwright(
        async_playwright,
        account_file=account_file,
        timeout_seconds=timeout_seconds,
    )


async def _browser_login_with_playwright(
    playwright_factory: Any,
    *,
    account_file: str | Path | None,
    timeout_seconds: float,
) -> Path:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    from .credentials import save_bduss

    manager = playwright_factory()
    playwright = None
    browser = None
    context = None
    try:
        playwright = await manager.start()
        try:
            browser = await playwright.chromium.launch(headless=False)
        except Exception as error:
            if _looks_like_missing_browser(error):
                raise BrowserLoginUnavailableError(_BROWSER_SETUP_HELP) from None
            raise
        context = await browser.new_context()
        page = await context.new_page()
        try:
            await page.goto(
                TIEBA_HOME_URL,
                wait_until="domcontentloaded",
                timeout=max(1, int(timeout_seconds * 1000)),
            )
        except Exception as error:
            if _looks_like_missing_browser(error):
                raise BrowserLoginUnavailableError(_BROWSER_SETUP_HELP) from None
            raise

        deadline = time.monotonic() + timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BrowserLoginTimeoutError(
                    "Timed out while waiting for login in the opened Tieba window. "
                    "You can resume the command and try again."
                )

            cookies = await context.cookies(list(BAIDU_COOKIE_URLS))
            for cookie in cookies:
                if cookie.get("name") != "BDUSS" or not _is_baidu_cookie(cookie):
                    continue
                value = cookie.get("value")
                if not isinstance(value, str):
                    continue
                return save_bduss(value, account_file)

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BrowserLoginTimeoutError(
                    "Timed out while waiting for login in the opened Tieba window. "
                    "You can resume the command and try again."
                )
            await asyncio.sleep(min(POLL_INTERVAL_SECONDS, remaining))
    finally:
        if context is not None:
            with suppress(Exception):
                await context.close()
        if browser is not None:
            with suppress(Exception):
                await browser.close()
        if playwright is not None:
            with suppress(Exception):
                await playwright.stop()


def _is_baidu_cookie(cookie: dict[str, Any]) -> bool:
    domain = cookie.get("domain")
    if not isinstance(domain, str):
        return False
    normalized = domain.lstrip(".").lower()
    return normalized == "baidu.com" or normalized.endswith(".baidu.com")


def _looks_like_missing_browser(error: Exception) -> bool:
    message = str(error).lower()
    return "executable doesn't exist" in message or (
        "please run" in message and "playwright install" in message
    )
