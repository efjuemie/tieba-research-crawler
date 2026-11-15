"""Safe local credential watching and bounded authentication checks."""

from __future__ import annotations

import asyncio
import contextvars
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from aiotieba.enums import ReqUInfo
from tiebameow.client.tieba_client import ErrorHandler, RetriableApiError, UnretriableApiError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from aiolimiter import AsyncLimiter


class GateAwareSemaphore:
    """Semaphore adapter that rechecks request gates after acquiring capacity."""

    def __init__(
        self,
        semaphore: asyncio.Semaphore,
        wait_until_ready: Callable[[], Awaitable[None]],
        is_ready: Callable[[], Awaitable[bool]],
        wait_network_ready: Callable[[], Awaitable[None]],
        is_network_ready: Callable[[], Awaitable[bool]],
        minimum_interval_seconds: float = 0.0,
    ) -> None:
        self._semaphore = semaphore
        self._wait_until_ready = wait_until_ready
        self._is_ready = is_ready
        self._wait_network_ready = wait_network_ready
        self._is_network_ready = is_network_ready
        self._minimum_interval_seconds = max(0.0, minimum_interval_seconds)
        self._next_request_at = 0.0
        self._admission_lock = asyncio.Lock()
        self._bypass_auth = contextvars.ContextVar(f"research_auth_bypass_{id(self)}", default=False)
        self._api_stop_event = contextvars.ContextVar(f"research_api_stop_event_{id(self)}", default=None)

    async def acquire(self) -> bool:
        stop_event = self._api_stop_event.get()
        while True:
            await self._await_or_api_stop(self._wait_for_request_slot(), stop_event)
            if stop_event is not None and stop_event.is_set():
                raise asyncio.CancelledError

            capacity_wait = asyncio.create_task(self._semaphore.acquire())
            try:
                await self._await_or_api_stop(capacity_wait, stop_event)
            except BaseException:
                if self._task_succeeded(capacity_wait):
                    self._semaphore.release()
                raise

            granted = False
            remaining = 0.0
            try:
                if stop_event is not None and stop_event.is_set():
                    raise asyncio.CancelledError
                if self._minimum_interval_seconds == 0:
                    gate_is_open = await self._await_or_api_stop(self._active_gate_is_open(), stop_event)
                    granted = gate_is_open
                else:
                    async def check_admission() -> tuple[bool, float]:
                        async with self._admission_lock:
                            gate_is_open = await self._await_or_api_stop(
                                self._active_gate_is_open(), stop_event
                            )
                            now = asyncio.get_running_loop().time()
                            wait_seconds = self._next_request_at - now
                            is_granted = gate_is_open and wait_seconds <= 0
                            if is_granted:
                                self._next_request_at = now + self._minimum_interval_seconds
                            return is_granted, wait_seconds

                    granted, remaining = await self._await_or_api_stop(check_admission(), stop_event)
                if stop_event is not None and stop_event.is_set():
                    granted = False
                    raise asyncio.CancelledError
                if granted:
                    return True
            finally:
                if not granted:
                    self._semaphore.release()

            if remaining > 0:
                await self._await_or_api_stop(asyncio.sleep(remaining), stop_event)

    @staticmethod
    def _task_succeeded(task: asyncio.Future[object]) -> bool:
        if not task.done() or task.cancelled():
            return False
        try:
            return task.exception() is None and bool(task.result())
        except BaseException:
            return False

    @staticmethod
    async def _await_or_api_stop(awaitable: Awaitable[object], stop_event: asyncio.Event | None) -> object:
        if stop_event is None:
            return await awaitable
        if stop_event.is_set():
            if asyncio.iscoroutine(awaitable):
                awaitable.close()
            elif isinstance(awaitable, asyncio.Future):
                awaitable.cancel()
                await asyncio.gather(awaitable, return_exceptions=True)
            raise asyncio.CancelledError

        operation = asyncio.ensure_future(awaitable)
        stopped = asyncio.create_task(stop_event.wait())
        try:
            done, _pending = await asyncio.wait(
                {operation, stopped},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stopped in done and operation not in done:
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)
                raise asyncio.CancelledError
            return await operation
        finally:
            if not operation.done():
                operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            if not stopped.done():
                stopped.cancel()
            await asyncio.gather(stopped, return_exceptions=True)

    async def _active_gate_is_open(self) -> bool:
        if self._bypass_auth.get():
            return await self._is_network_ready()
        return await self._is_ready()

    async def _wait_for_request_slot(self) -> None:
        while True:
            if self._bypass_auth.get():
                await self._wait_network_ready()
            else:
                await self._wait_until_ready()

            if self._minimum_interval_seconds == 0:
                return

            async with self._admission_lock:
                remaining = self._next_request_at - asyncio.get_running_loop().time()
            if remaining <= 0:
                return
            await asyncio.sleep(remaining)

    def release(self) -> None:
        self._semaphore.release()

    async def __aenter__(self) -> GateAwareSemaphore:
        await self.acquire()
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        self.release()

    @asynccontextmanager
    async def api_stop_guard(self, stop_event: asyncio.Event) -> AsyncGenerator[None, None]:
        token = self._api_stop_event.set(stop_event)
        try:
            yield
        finally:
            self._api_stop_event.reset(token)

    @asynccontextmanager
    async def bypass_auth_gate(self) -> AsyncGenerator[None, None]:
        token = self._bypass_auth.set(True)
        try:
            yield
        finally:
            self._bypass_auth.reset(token)


class ResearchAuthError(RuntimeError):
    """Authentication result with a safe run status and reason."""

    def __init__(self, status: str, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


def account_file_fingerprint(path: str | Path) -> tuple[int, int, int, int] | None:
    """Return metadata only; never read or expose the credential file contents."""
    file_path = resolve_account_file(path)
    try:
        stat = file_path.stat()
    except OSError:
        return None
    return stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_size


def resolve_account_file(path: str | Path) -> Path:
    file_path = Path(path)
    if file_path.is_absolute():
        return file_path
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / file_path


async def wait_for_account_file_update(
    path: str | Path,
    previous_fingerprint: tuple[int, int, int, int] | None,
    *,
    poll_interval: float = 1.0,
) -> tuple[str, tuple[int, int, int, int]]:
    """Wait for an explicit local file change and return its validated BDUSS.

    Invalid intermediate edits are ignored until the file changes again. The
    caller can cancel this coroutine at any time; it holds no other resources.
    """
    from ..research.credentials import load_bduss

    latest_fingerprint = previous_fingerprint
    file_path = resolve_account_file(path)
    while True:
        current_fingerprint = account_file_fingerprint(file_path)
        if current_fingerprint != latest_fingerprint:
            latest_fingerprint = current_fingerprint
            if latest_fingerprint is not None:
                try:
                    return load_bduss(file_path), latest_fingerprint
                except ValueError:
                    pass
        await asyncio.sleep(poll_interval)


async def verify_research_credentials(
    client: object,
    limiter: AsyncLimiter | None,
    *,
    max_attempts: int,
    retry_wait_seconds: float,
) -> None:
    """Verify a loaded credential with get_self_info under bounded retries."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")

    for attempt in range(1, max_attempts + 1):
        try:
            if limiter is None:
                response = await client.get_self_info(require=ReqUInfo.BASIC)  # type: ignore[attr-defined]
            else:
                async with limiter:
                    response = await client.get_self_info(require=ReqUInfo.BASIC)  # type: ignore[attr-defined]
            ErrorHandler.check(response)
        except RetriableApiError as error:
            if error.code == 4011:
                raise ResearchAuthError("paused_auth", "account_validation_rejected_code_4011") from None
            if attempt == max_attempts:
                status = "paused_rate_limit" if error.code in {429, 220034} else "incomplete"
                raise ResearchAuthError(status, f"account_validation_failed_code_{error.code}") from None
            await asyncio.sleep(retry_wait_seconds * attempt)
            continue
        except UnretriableApiError as error:
            raise ResearchAuthError("blocked", f"account_validation_blocked_code_{error.code}") from None
        except Exception:
            if attempt == max_attempts:
                raise ResearchAuthError("incomplete", "account_validation_network_failure") from None
            await asyncio.sleep(retry_wait_seconds * attempt)
            continue

        try:
            user_id = int(getattr(response, "user_id", 0) or 0)
        except (TypeError, ValueError):
            user_id = 0
        if user_id <= 0:
            raise ResearchAuthError("paused_auth", "account_validation_returned_no_user")
        return
