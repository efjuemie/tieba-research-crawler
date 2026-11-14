"""Application orchestration tests for run checkpoint reconciliation."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import main as app_main
from src.scraper.router import QueueRouter


def _container():
    class FakeContainer:
        config = SimpleNamespace(
            metrics_enabled=False,
            worker_threads=0,
            worker_posts=0,
            worker_comments=0,
            queue_maxsize=1,
            research_enabled=True,
            fetch_user_forum_info=False,
            max_backfill_pages=0,
        )
        forums = [SimpleNamespace(fid=7, fname="synthetic")]

        async def teardown(self):
            return None

    return FakeContainer()


@pytest.mark.asyncio
async def test_backfill_reconciles_after_scheduler_checkpoint_and_detail_drain(monkeypatch):
    events = []
    container = _container()
    router = QueueRouter(maxsize=container.config.queue_maxsize)
    original_wait_idle = router.wait_idle

    async def tracked_wait_idle():
        events.append("drain_started")
        await original_wait_idle()
        events.append("drain_finished")

    router.wait_idle = tracked_wait_idle

    class FakeDataStore:
        async def start_research_run(self, **_kwargs):
            events.append("run_created")
            return "synthetic-run"

        async def adopt_legacy_research_scopes(self, run_id, fids):
            assert run_id == "synthetic-run"
            assert fids == {7}
            events.append("legacy_scopes_adopted")

        async def iter_pending_research_task_scopes(self, run_id, fids, *, batch_size):
            assert run_id == "synthetic-run"
            assert fids == {7}
            assert batch_size == 500
            events.append("research_scopes_iterated")
            if False:
                yield []

        async def iter_pending_thread_scans(self, _fids, *, batch_size):
            assert batch_size == 500
            if False:
                yield []

        async def iter_pending_comment_scans(self, _fids, *, batch_size):
            assert batch_size == 500
            if False:
                yield []

        async def reconcile_research_run_status_after_drain(self, run_id):
            assert run_id == "synthetic-run"
            assert events[-1] == "drain_finished"
            events.append("run_reconciled")
            return "completed"

        async def close(self):
            return None

    datastore = FakeDataStore()

    class FakeScheduler:
        def __init__(self, *, router, container, datastore):
            assert router is not None
            assert container is not None
            assert datastore is datastore_instance
            events.append("scheduler_received_datastore")

        async def run(self, mode):
            assert mode == "backfill"
            events.append("list_scheduler_finished")

    datastore_instance = datastore

    async def initialize(*, mode):
        assert mode == "backfill"
        return container, router

    monkeypatch.setattr(app_main, "initialize_application", initialize)
    monkeypatch.setattr(app_main, "DataStore", lambda _container: datastore)
    monkeypatch.setattr(app_main, "Scheduler", FakeScheduler)

    await app_main.main("backfill")

    assert events == [
        "scheduler_received_datastore",
        "run_created",
        "legacy_scopes_adopted",
        "research_scopes_iterated",
        "list_scheduler_finished",
        "drain_started",
        "drain_finished",
        "run_reconciled",
    ]


@pytest.mark.asyncio
async def test_hybrid_reconciles_only_after_stopping_producers_and_draining(monkeypatch):
    events = []
    scheduler_stopped = asyncio.Event()
    periodic_started = asyncio.Event()
    backfill_started = asyncio.Event()

    class FakeScheduler:
        def stop(self):
            events.append("scheduler_stopped")
            scheduler_stopped.set()

        async def run(self, mode):
            events.append(f"{mode}_started")
            if mode == "periodic":
                periodic_started.set()
            else:
                backfill_started.set()
            await scheduler_stopped.wait()
            if mode == "backfill":
                events.append("backfill_finished")

    class FakeRouter:
        async def wait_idle(self):
            events.append("queue_drained")

        def total_qsize(self):
            return 0

    class FakeDataStore:
        async def reconcile_research_run_status_after_drain(self, run_id):
            assert run_id == "synthetic-run"
            assert events[-1] == "queue_drained"
            events.append("run_reconciled")
            return "completed"

    task = asyncio.create_task(
        app_main._run_hybrid_mode(
            FakeScheduler(),
            [],
            FakeRouter(),
            worker_tasks=[],
            datastore=FakeDataStore(),
            research_run_id="synthetic-run",
        )
    )
    await asyncio.wait_for(asyncio.gather(periodic_started.wait(), backfill_started.wait()), timeout=1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert events.index("scheduler_stopped") < events.index("backfill_finished")
    assert events.index("backfill_finished") < events.index("queue_drained")
    assert events.index("queue_drained") < events.index("run_reconciled")


@pytest.mark.asyncio
async def test_hybrid_drain_timeout_marks_interrupted_without_reconciliation(monkeypatch):
    scheduler_stopped = asyncio.Event()

    class FakeScheduler:
        def stop(self):
            scheduler_stopped.set()

        async def run(self, mode):
            assert mode in {"periodic", "backfill"}
            await scheduler_stopped.wait()

    class FakeRouter:
        async def wait_idle(self):
            await asyncio.Event().wait()

        def total_qsize(self):
            return 1

    class FakeDataStore:
        def __init__(self):
            self.status_updates = []
            self.reconciled = False

        async def set_research_run_status_if(self, *args):
            self.status_updates.append(args)
            return True

        async def reconcile_research_run_status_after_drain(self, _run_id):
            self.reconciled = True

    datastore = FakeDataStore()
    monkeypatch.setattr(app_main, "GRACEFUL_SHUTDOWN_TIMEOUT", 0.01)
    task = asyncio.create_task(
        app_main._run_hybrid_mode(
            FakeScheduler(),
            [],
            FakeRouter(),
            worker_tasks=[],
            datastore=datastore,
            research_run_id="synthetic-run",
        )
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert datastore.status_updates == [
        (
            "synthetic-run",
            {"running", "completed", "incomplete"},
            "interrupted",
            "hybrid_shutdown_before_backfill_drain",
        )
    ]
    assert not datastore.reconciled
