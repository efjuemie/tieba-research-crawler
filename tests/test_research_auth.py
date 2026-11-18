"""Synthetic request-gate and shared API response-streak regressions."""

import asyncio
from types import SimpleNamespace

import pytest

from src.core.auth import GateAwareSemaphore
from src.core.config import RateLimitConfig
from src.core.container import Container


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
