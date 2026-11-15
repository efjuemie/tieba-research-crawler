from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.core.auth import GateAwareSemaphore
from src.core.lock import MemoryLockManager
from src.scraper.router import QueueRouter
from src.scraper.tasks import DeepScanTask, FullScanPostsTask, Priority, Task
from src.scraper.worker import DeepScanTaskHandler, Worker


def _worker(router: QueueRouter, semaphore=None) -> Worker:
    container = SimpleNamespace(lock_manager=MemoryLockManager(), semaphore=semaphore)
    return Worker(0, router.posts_queue, container, SimpleNamespace(), router, lane="posts")


@pytest.mark.asyncio
async def test_worker_dispatches_deep_scan_task_to_its_handler():
    router = QueueRouter()
    worker = _worker(router)
    handler = worker.handlers[DeepScanTask]
    handler.handle = AsyncMock()
    task = DeepScanTask(tid=12, total_pages=4)

    await worker._process_task(Task(priority=Priority.BACKGROUND, content=task))

    assert isinstance(handler, DeepScanTaskHandler)
    handler.handle.assert_awaited_once_with(task)


@pytest.mark.asyncio
async def test_cancelled_api_gate_wait_cancels_and_joins_its_operation():
    router = QueueRouter()
    handler = _worker(router).handlers[FullScanPostsTask]
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def operation():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    waiting = asyncio.create_task(handler._await_api_gate(operation()))
    await asyncio.wait_for(started.wait(), timeout=1)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_cancelled_enqueue_cleans_up_bounded_queue_waiter():
    router = QueueRouter(maxsize=1)
    handler = _worker(router).handlers[FullScanPostsTask]
    first = Task(priority=Priority.MEDIUM, content=FullScanPostsTask(tid=1))
    second = Task(priority=Priority.MEDIUM, content=FullScanPostsTask(tid=2))
    router.put_nowait(first)

    enqueue = asyncio.create_task(handler._enqueue_task_unless_stopping(second))
    await asyncio.sleep(0)
    enqueue.cancel()
    with pytest.raises(asyncio.CancelledError):
        await enqueue

    assert router.posts_queue.qsize() == 1
    assert router.tracker.pending == 1
    assert router.posts_queue.get_nowait() == first
    router.posts_queue.task_done(first)
    await asyncio.sleep(0)
    assert router.posts_queue.empty()
    assert router.tracker.pending == 0


@pytest.mark.asyncio
async def test_cancelled_worker_get_does_not_steal_a_later_queue_item():
    router = QueueRouter()
    worker = _worker(router)
    waiting = asyncio.create_task(worker._get_next_task())
    await asyncio.sleep(0)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    later = Task(priority=Priority.MEDIUM, content=FullScanPostsTask(tid=3))
    router.put_nowait(later)
    await asyncio.sleep(0)

    assert router.posts_queue.qsize() == 1
    assert router.tracker.pending == 1
    assert router.posts_queue.get_nowait() == later
    router.posts_queue.task_done(later)
    assert router.tracker.pending == 0


@pytest.mark.asyncio
async def test_worker_accounts_for_item_if_stop_wins_after_queue_get():
    router = QueueRouter()
    worker = _worker(router)
    task = Task(priority=Priority.MEDIUM, content=FullScanPostsTask(tid=4))
    router.put_nowait(task)
    original_get = worker.queue.get

    async def get_then_stop():
        item = await original_get()
        worker.stop_after_current()
        return item

    worker.queue.get = get_then_stop

    assert await worker._get_next_task() is None
    assert worker.queue.empty()
    assert router.tracker.pending == 0


@pytest.mark.asyncio
async def test_worker_stop_prevents_shared_gate_admission_but_not_current_page_commit():
    async def wait_ready():
        return None

    async def is_ready():
        return True

    capacity = asyncio.Semaphore(0)
    gate = GateAwareSemaphore(capacity, wait_ready, is_ready, wait_ready, is_ready)
    router = QueueRouter()
    worker = _worker(router, gate)
    handler = worker.handlers[FullScanPostsTask]
    http_started = asyncio.Event()

    async def blocked_api(_task):
        async with gate:
            http_started.set()

    handler.handle = blocked_api
    processing = asyncio.create_task(
        worker._process_task(Task(priority=Priority.MEDIUM, content=FullScanPostsTask(tid=5)))
    )
    await asyncio.sleep(0)
    worker.stop_after_current()
    capacity.release()

    with pytest.raises(asyncio.CancelledError):
        await processing
    assert not http_started.is_set()
    assert capacity._value == 1

    capacity = asyncio.Semaphore(1)
    gate = GateAwareSemaphore(capacity, wait_ready, is_ready, wait_ready, is_ready)
    worker = _worker(QueueRouter(), gate)
    handler = worker.handlers[FullScanPostsTask]
    http_started = asyncio.Event()
    release_http = asyncio.Event()
    page_persisted = asyncio.Event()

    async def admitted_api(_task):
        async with gate:
            http_started.set()
            await release_http.wait()
            page_persisted.set()

    handler.handle = admitted_api
    processing = asyncio.create_task(
        worker._process_task(Task(priority=Priority.MEDIUM, content=FullScanPostsTask(tid=6)))
    )
    await asyncio.wait_for(http_started.wait(), timeout=1)
    worker.stop_after_current()
    assert not processing.done()
    release_http.set()
    await processing
    assert page_persisted.is_set()
