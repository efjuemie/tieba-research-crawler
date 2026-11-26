"""Application-level failure propagation tests."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import main as app_main
from src.scraper.router import QueueRouter
from src.scraper.scheduler import BackfillIncompleteError


@pytest.mark.asyncio
async def test_parent_zero_revalidation_flag_requires_resume_before_initialization(monkeypatch):
    initialize = AsyncMock()
    monkeypatch.setattr(app_main, "initialize_application", initialize)

    with pytest.raises(ValueError, match="--revalidate-failed-comment-page-zero requires --resume"):
        await app_main.main(revalidate_failed_comment_page_zero=True)

    initialize.assert_not_awaited()


def test_parse_ip_location_min_date_accepts_date_none_and_rejects_invalid():
    parsed = app_main._parse_ip_location_min_date("2022-04-28")
    assert parsed is not None
    assert (parsed.year, parsed.month, parsed.day) == (2022, 4, 28)
    assert app_main._parse_ip_location_min_date("none") is None
    assert app_main._parse_ip_location_min_date("OFF") is None
    assert app_main._parse_ip_location_min_date(None) is None
    with pytest.raises(ValueError, match="--ip-location-min-date"):
        app_main._parse_ip_location_min_date("2022/04/28")


@pytest.mark.asyncio
async def test_main_propagates_incomplete_backfill_failure(monkeypatch):
    class FakeDataStore:
        def __init__(self, _container):
            pass

        async def get_all_pending_thread_scans(self):
            return []

        async def get_all_pending_comment_scans(self):
            return []

        async def close(self):
            pass

    class FakeScheduler:
        def __init__(self, router, container, datastore=None):
            pass

        def stop(self):
            return None

        async def run(self, mode):
            raise BackfillIncompleteError(["synthetic incomplete list stream"])

    class FakeContainer:
        config = SimpleNamespace(
            metrics_enabled=False,
            worker_threads=0,
            worker_posts=0,
            worker_comments=0,
        )
        forums = []

        async def teardown(self):
            pass

    container = FakeContainer()
    router = QueueRouter()

    async def initialize(*, mode):
        return container, router

    monkeypatch.setattr(app_main, "initialize_application", initialize)
    monkeypatch.setattr(app_main, "DataStore", FakeDataStore)
    monkeypatch.setattr(app_main, "Scheduler", FakeScheduler)

    with pytest.raises(BackfillIncompleteError):
        await app_main.main("backfill")


class _HybridScheduler:
    def __init__(self, *, fail_after_stop=False, fail_immediately=False):
        self.stopped = asyncio.Event()
        self.periodic_started = asyncio.Event()
        self.backfill_started = asyncio.Event()
        self.worker_started = asyncio.Event()
        self.fail_after_stop = fail_after_stop
        self.fail_immediately = fail_immediately

    async def run(self, mode):
        if mode == "periodic":
            self.periodic_started.set()
            await self.stopped.wait()
            return

        self.backfill_started.set()
        await self.worker_started.wait()
        if self.fail_immediately:
            raise BackfillIncompleteError(["synthetic backfill failure"])
        await self.stopped.wait()
        if self.fail_after_stop:
            await asyncio.sleep(0.01)
            raise BackfillIncompleteError(["synthetic failure during shutdown"])

    def stop(self):
        self.stopped.set()


class _WaitingWorker:
    lane = "threads"
    worker_id = 0

    def __init__(self, started):
        self.started = started
        self.done = False

    async def run(self):
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.done = True


async def _start_hybrid(scheduler, worker):
    task = asyncio.create_task(app_main._run_hybrid_mode(scheduler, [worker], QueueRouter()))
    await asyncio.wait_for(scheduler.periodic_started.wait(), timeout=1)
    await asyncio.wait_for(scheduler.backfill_started.wait(), timeout=1)
    await asyncio.wait_for(scheduler.worker_started.wait(), timeout=1)
    return task


@pytest.mark.asyncio
async def test_hybrid_shutdown_cancels_producers_and_reaps_workers():
    scheduler = _HybridScheduler(fail_after_stop=True)
    worker = _WaitingWorker(scheduler.worker_started)
    task = await _start_hybrid(scheduler, worker)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert scheduler.stopped.is_set()
    assert worker.done


@pytest.mark.asyncio
async def test_hybrid_normal_shutdown_reaps_workers_and_preserves_cancellation():
    scheduler = _HybridScheduler()
    worker = _WaitingWorker(scheduler.worker_started)
    task = await _start_hybrid(scheduler, worker)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert worker.done


@pytest.mark.asyncio
async def test_hybrid_scheduler_exception_reaps_internal_workers():
    scheduler = _HybridScheduler(fail_immediately=True)
    worker = _WaitingWorker(scheduler.worker_started)
    task = await _start_hybrid(scheduler, worker)

    with pytest.raises(BackfillIncompleteError, match="synthetic backfill failure"):
        await task

    assert worker.done
