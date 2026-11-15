from __future__ import annotations

import asyncio

import pytest

from src.core.auth import GateAwareSemaphore


async def _wait_ready() -> None:
    return None


async def _is_ready() -> bool:
    return True


def _gate(capacity: asyncio.Semaphore) -> GateAwareSemaphore:
    return GateAwareSemaphore(capacity, _wait_ready, _is_ready, _wait_ready, _is_ready)


@pytest.mark.asyncio
async def test_api_waiting_for_global_capacity_is_cancelled_before_http_starts():
    capacity = asyncio.Semaphore(0)
    gate = _gate(capacity)
    api_stop = asyncio.Event()
    http_started = asyncio.Event()

    async def api_request():
        async with gate.api_stop_guard(api_stop):
            async with gate:
                http_started.set()

    request = asyncio.create_task(api_request())
    await asyncio.sleep(0)
    api_stop.set()
    capacity.release()

    with pytest.raises(asyncio.CancelledError):
        await request

    assert not http_started.is_set()
    assert capacity._value == 1


@pytest.mark.asyncio
async def test_admitted_api_request_finishes_and_web_can_use_shared_gate_after_stop():
    capacity = asyncio.Semaphore(1)
    gate = _gate(capacity)
    api_stop = asyncio.Event()
    http_started = asyncio.Event()
    release_http = asyncio.Event()
    page_persisted = asyncio.Event()

    async def api_request():
        async with gate.api_stop_guard(api_stop):
            async with gate:
                http_started.set()
                await release_http.wait()
                page_persisted.set()

    request = asyncio.create_task(api_request())
    await asyncio.wait_for(http_started.wait(), timeout=1)
    api_stop.set()
    assert not request.done()
    release_http.set()
    await request
    assert page_persisted.is_set()

    async with gate:
        assert capacity._value == 0
    assert capacity._value == 1


@pytest.mark.asyncio
async def test_cancelled_api_capacity_wait_does_not_leak_a_late_grant():
    capacity = asyncio.Semaphore(0)
    gate = _gate(capacity)
    api_stop = asyncio.Event()
    http_started = asyncio.Event()

    async def api_request():
        async with gate.api_stop_guard(api_stop):
            async with gate:
                http_started.set()

    request = asyncio.create_task(api_request())
    await asyncio.sleep(0)
    request.cancel()
    capacity.release()
    with pytest.raises(asyncio.CancelledError):
        await request

    assert not http_started.is_set()
    assert capacity._value == 1
