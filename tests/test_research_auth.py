"""Synthetic request-gate and shared API response-streak regressions."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.core.auth import GateAwareSemaphore
from src.core.config import RateLimitConfig
from src.core.container import Container
from src.core.lock import MemoryLockManager
from src.scraper.router import QueueRouter
from src.scraper.worker import FullScanPostsTaskHandler


@pytest.mark.asyncio
async def test_api_gate_at_half_rps_admits_at_zero_two_and_four_seconds(monkeypatch):
    class FakeClock:
        now = 0.0

        def time(self):
            return self.now

        async def sleep(self, seconds):
            self.now += seconds

    clock = FakeClock()

    async def ready():
        return None

    async def is_ready():
        return True

    gate = GateAwareSemaphore(
        asyncio.Semaphore(1),
        ready,
        is_ready,
        ready,
        is_ready,
        minimum_interval_seconds=1 / RateLimitConfig().global_rps,
    )
    admissions = []
    with monkeypatch.context() as patch:
        patch.setattr("src.core.auth.asyncio.get_running_loop", lambda: clock)
        patch.setattr("src.core.auth.asyncio.sleep", clock.sleep)
        for _ in range(3):
            await gate.acquire()
            admissions.append(clock.now)
            gate.release()

    assert admissions == [0.0, 2.0, 4.0]


@pytest.mark.asyncio
async def test_api_gate_rotates_waiting_lanes_work_conservingly(monkeypatch):
    real_sleep = asyncio.sleep

    class FakeClock:
        now = 0.0

        def time(self):
            return self.now

        async def sleep(self, seconds):
            target = self.now + seconds
            await real_sleep(0)
            self.now = max(self.now, target)

    clock = FakeClock()

    async def ready():
        return None

    async def is_ready():
        return True

    gate = GateAwareSemaphore(
        asyncio.Semaphore(8), ready, is_ready, ready, is_ready, minimum_interval_seconds=2
    )
    admissions = []

    async def request(lane):
        async with gate.api_lane(lane):
            await gate.acquire()
            admissions.append((lane, clock.now))
            gate.release()

    with monkeypatch.context() as patch:
        patch.setattr("src.core.auth.asyncio.get_running_loop", lambda: clock)
        patch.setattr("src.core.auth.asyncio.sleep", clock.sleep)
        tasks = [
            asyncio.create_task(request(lane))
            for lane in ("threads", "posts", "posts", "comments")
        ]
        await asyncio.gather(*tasks)

    assert admissions == [
        ("threads", 0.0),
        ("posts", 2.0),
        ("posts", 4.0),
        ("comments", 6.0),
    ]


@pytest.mark.asyncio
async def test_api_gate_lone_lane_uses_every_slot_and_cancel_removes_waiter(monkeypatch):
    real_sleep = asyncio.sleep

    class FakeClock:
        now = 0.0

        def time(self):
            return self.now

        async def sleep(self, seconds):
            target = self.now + seconds
            await real_sleep(0)
            self.now = max(self.now, target)

    clock = FakeClock()

    async def ready():
        return None

    async def is_ready():
        return True

    gate = GateAwareSemaphore(
        asyncio.Semaphore(3), ready, is_ready, ready, is_ready, minimum_interval_seconds=2
    )
    admissions = []

    async def request():
        async with gate.api_lane("posts"):
            await gate.acquire()
            admissions.append(clock.now)
            gate.release()

    with monkeypatch.context() as patch:
        patch.setattr("src.core.auth.asyncio.get_running_loop", lambda: clock)
        patch.setattr("src.core.auth.asyncio.sleep", clock.sleep)
        blocked = asyncio.create_task(request())
        await asyncio.sleep(0)
        blocked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await blocked
        await asyncio.gather(request(), request())

    assert admissions == [0.0, 2.0]
    assert all(not waiters for waiters in gate._api_waiters.values())


@pytest.mark.asyncio
async def test_api_gate_does_not_admit_if_stop_races_with_lane_grant():
    async def ready():
        return None

    async def is_ready():
        return True

    capacity = asyncio.Semaphore(1)
    gate = GateAwareSemaphore(capacity, ready, is_ready, ready, is_ready, minimum_interval_seconds=2)
    stop_event = asyncio.Event()

    async def grant_then_stop(_api_type):
        stop_event.set()

    gate._wait_for_api_lane_slot = grant_then_stop
    async with gate.api_lane("posts"), gate.api_stop_guard(stop_event):
        with pytest.raises(asyncio.CancelledError):
            await gate.acquire()

    assert capacity._value == 1


@pytest.mark.asyncio
async def test_api_lane_releases_capacity_while_auth_gate_closes():
    auth_ready = asyncio.Event()
    auth_ready.set()
    lane_checked = asyncio.Event()

    async def wait_auth():
        await auth_ready.wait()

    async def is_auth_ready():
        return auth_ready.is_set()

    capacity = asyncio.Semaphore(1)
    gate = GateAwareSemaphore(capacity, wait_auth, is_auth_ready, wait_auth, is_auth_ready, 2)
    original_wait = gate._wait_for_api_lane_slot
    first = True
    admitted = []

    async def close_auth_once(api_type):
        nonlocal first
        if first:
            first = False
            auth_ready.clear()
            lane_checked.set()
            return False
        return await original_wait(api_type)

    gate._wait_for_api_lane_slot = close_auth_once

    async def request():
        async with gate.api_lane("posts"):
            await gate.acquire()
            admitted.append(True)
            gate.release()

    task = asyncio.create_task(request())
    await asyncio.wait_for(lane_checked.wait(), timeout=1)
    assert await asyncio.wait_for(capacity.acquire(), timeout=1)
    capacity.release()
    await asyncio.sleep(0)
    assert not task.done()

    auth_ready.set()
    await task
    assert admitted == [True]


@pytest.mark.asyncio
async def test_request_api_sets_lane_context_during_client_call():
    observed = []

    async def ready():
        return None

    async def is_ready():
        return True

    gate = GateAwareSemaphore(asyncio.Semaphore(1), ready, is_ready, ready, is_ready)
    response = object()

    class Client:
        async def get_posts_dto(self, *_args, **_kwargs):
            observed.append(gate._api_type.get())
            return response

    container = SimpleNamespace(
        config=SimpleNamespace(research_enabled=False),
        tb_client=Client(),
        tb_clients=[],
        posts_limiter=AsyncMock(),
        threads_limiter=None,
        comments_limiter=None,
        lock_manager=MemoryLockManager(),
        semaphore=gate,
    )
    handler = FullScanPostsTaskHandler(0, container, SimpleNamespace(), QueueRouter())

    fetched = await handler._request_api("posts", "get_posts_research", "get_posts_dto", (1, 1), {})

    assert fetched.response is response
    assert observed == ["posts"]


@pytest.mark.asyncio
async def test_suspicious_empty_streak_is_shared_and_valid_response_resets_it():
    container = Container(SimpleNamespace())

    outcomes = await asyncio.gather(
        *(container.record_research_api_response(suspicious_empty=True) for _ in range(3))
    )
    assert outcomes == [False, False, True]

    assert await container.record_research_api_response(suspicious_empty=True) is False
    assert await container.record_research_api_response(suspicious_empty=True) is False
    assert await container.record_research_api_response(suspicious_empty=False) is False

    assert await container.record_research_api_response(suspicious_empty=True) is False
    assert await container.record_research_api_response(suspicious_empty=True) is False
    assert await container.record_research_api_response(suspicious_empty=True) is True
