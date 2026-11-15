"""Application orchestration tests for run checkpoint reconciliation."""

from __future__ import annotations

import asyncio
import os
import signal
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

        def stop(self):
            return None

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
async def test_sigterm_stops_api_page_then_drains_live_web_before_teardown(monkeypatch):
    events = []
    api_page_started = asyncio.Event()
    web_started = asyncio.Event()
    api_active = False
    discovery_calls = 0

    class FakeContainer:
        config = SimpleNamespace(
            metrics_enabled=False,
            worker_threads=0,
            worker_posts=1,
            worker_comments=0,
            research_enabled=True,
            fetch_user_forum_info=False,
            max_backfill_pages=0,
            pydantic_config=SimpleNamespace(
                web_fallback=SimpleNamespace(browser_profile="isolated-test-profile")
            ),
        )
        forums = [SimpleNamespace(fid=7, fname="synthetic")]
        semaphore = object()

        async def teardown(self):
            events.append("container_torn_down")

    class FakeDataStore:
        async_sessionmaker = object()

        async def start_research_run(self, **_kwargs):
            return "synthetic-run"

        async def reconcile_research_run_status_after_drain(self, run_id):
            assert run_id == "synthetic-run"
            assert events.index("web_completed") < len(events)
            events.append("run_reconciled")
            return "completed"

        async def set_research_run_status_if(self, *args):
            events.append(("run_status", args[-2], args[-1]))
            return True

        async def close(self):
            events.append("database_closed")

    class FakeScheduler:
        def __init__(self, **_kwargs):
            self.stopped = asyncio.Event()

        def stop(self):
            self.stopped.set()

        async def run(self, mode):
            assert mode == "periodic"
            await self.stopped.wait()

    class FakeWorker:
        def __init__(self, worker_id, *_args, lane, **_kwargs):
            self.worker_id = worker_id
            self.lane = lane
            self.stop_requested = asyncio.Event()

        async def run(self):
            nonlocal api_active
            api_active = True
            events.append("api_page_started")
            api_page_started.set()
            await self.stop_requested.wait()
            events.append("api_page_committed")
            api_active = False

        def stop_after_current(self):
            self.stop_requested.set()

    async def initialize(*, mode):
        assert mode == "periodic"
        return FakeContainer(), QueueRouter()

    async def no_recovery(*_args, **_kwargs):
        return None

    async def pending_run_ids(_sessions, current_run_id, *, responsible_forums):
        assert current_run_id == "synthetic-run"
        assert responsible_forums == ((7, "synthetic"),)
        return (current_run_id,)

    async def discover(_sessions, run_ids):
        nonlocal discovery_calls
        assert run_ids == ("synthetic-run",)
        discovery_calls += 1
        if discovery_calls > 1:
            assert "api_page_committed" in events
        events.append("web_target_rescan_startup" if discovery_calls == 1 else "web_target_rescan_shutdown")
        return 1

    async def live_web_worker(_sessions, _run_ids, *, stop_event, **_kwargs):
        await api_page_started.wait()
        assert api_active
        events.append("web_started_while_api_active")
        web_started.set()
        await stop_event.wait()
        assert "web_target_rescan_shutdown" in events
        events.append("web_completed")
        return SimpleNamespace(unresolved_count=0)

    datastore = FakeDataStore()
    original_await_mode_or_shutdown = app_main._await_mode_or_shutdown

    async def tracked_await_mode_or_shutdown(mode_task, shutdown_event):
        result = await original_await_mode_or_shutdown(mode_task, shutdown_event)
        events.append(("shutdown_result", result, shutdown_event.is_set()))
        return result

    monkeypatch.setattr(app_main, "initialize_application", initialize)
    monkeypatch.setattr(app_main, "DataStore", lambda _container: datastore)
    monkeypatch.setattr(app_main, "Scheduler", FakeScheduler)
    monkeypatch.setattr(app_main, "Worker", FakeWorker)
    monkeypatch.setattr(app_main, "_recover_pending_tasks", no_recovery)
    monkeypatch.setattr(app_main, "pending_web_run_ids", pending_run_ids)
    monkeypatch.setattr(app_main, "discover_missing_web_targets", discover)
    monkeypatch.setattr(app_main, "run_live_web_worker", live_web_worker)
    monkeypatch.setattr(app_main, "_await_mode_or_shutdown", tracked_await_mode_or_shutdown)

    task = asyncio.create_task(app_main.main("periodic"))
    await asyncio.wait_for(web_started.wait(), timeout=1)
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(task, timeout=1)

    assert events.index("web_started_while_api_active") < events.index("api_page_committed")
    assert events.index("api_page_committed") < events.index("web_target_rescan_shutdown")
    assert events.index("web_target_rescan_shutdown") < events.index("web_completed")
    assert events.index("web_completed") < events.index("run_reconciled")
    assert events.index("web_completed") < events.index("database_closed") < events.index("container_torn_down")
    assert any(
        event[0] == "run_status" and event[1] == "interrupted"
        for event in events
        if isinstance(event, tuple)
    ), [event for event in events if isinstance(event, tuple)]
    assert ("shutdown_result", True, True) in events


@pytest.mark.asyncio
async def test_backfill_stops_api_producers_if_live_web_worker_exits_early():
    events = []
    worker_started = asyncio.Event()
    worker_stop = asyncio.Event()

    class FakeScheduler:
        def __init__(self):
            self.stopped = asyncio.Event()

        def stop(self):
            self.stopped.set()

        async def run(self, mode):
            assert mode == "backfill"
            await self.stopped.wait()

    class FakeWorker:
        async def run(self):
            worker_started.set()
            await worker_stop.wait()
            events.append("api_page_committed")

        def stop_after_current(self):
            worker_stop.set()

    class FakeRouter:
        async def wait_idle(self):
            await asyncio.Event().wait()

    async def fail_web_worker():
        await worker_started.wait()
        events.append("web_worker_failed")
        raise RuntimeError("synthetic web worker failure")

    worker = FakeWorker()
    worker_task = asyncio.create_task(worker.run())
    web_worker_task = asyncio.create_task(fail_web_worker())

    async def producers_stopped():
        assert "api_page_committed" in events
        events.append("producers_stopped")

    with pytest.raises(RuntimeError, match="synthetic web worker failure"):
        await app_main._run_backfill_mode(
            FakeScheduler(),
            [worker],
            FakeRouter(),
            [worker_task],
            web_worker_task=web_worker_task,
            on_api_stopped=producers_stopped,
        )

    assert events.index("web_worker_failed") < events.index("api_page_committed")
    assert events.index("api_page_committed") < events.index("producers_stopped")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["periodic", "hybrid", "backfill"])
async def test_normal_live_web_summary_stops_api_producers_in_every_mode(mode):
    events = []
    api_stop = asyncio.Event()
    web_stop = asyncio.Event()

    class FakeScheduler:
        def __init__(self):
            self.stopped = asyncio.Event()

        def stop(self):
            self.stopped.set()

        async def run(self, mode):
            assert mode in {"periodic", "backfill"}
            await self.stopped.wait()

    class FakeWorker:
        async def run(self):
            events.append("api_started")
            await api_stop.wait()
            events.append("api_finished")

        def stop_after_current(self):
            api_stop.set()

    class FakeRouter:
        async def wait_idle(self):
            await asyncio.Event().wait()

    async def returned_summary():
        return SimpleNamespace(stopped_reason="manual_login_not_confirmed", unresolved_count=1)

    async def producers_stopped():
        assert "api_finished" in events
        events.append("producers_stopped")
        web_stop.set()

    scheduler = FakeScheduler()
    worker = FakeWorker()
    worker_task = asyncio.create_task(worker.run())
    web_worker_task = asyncio.create_task(returned_summary())
    run_mode = {
        "periodic": lambda: app_main._run_periodic_mode(
            scheduler,
            [worker],
            FakeRouter(),
            worker_tasks=[worker_task],
            web_worker_task=web_worker_task,
            web_stop_event=web_stop,
            on_api_stopped=producers_stopped,
        ),
        "hybrid": lambda: app_main._run_hybrid_mode(
            scheduler,
            [worker],
            FakeRouter(),
            worker_tasks=[worker_task],
            web_worker_task=web_worker_task,
            web_stop_event=web_stop,
            on_api_stopped=producers_stopped,
        ),
        "backfill": lambda: app_main._run_backfill_mode(
            scheduler,
            [worker],
            FakeRouter(),
            [worker_task],
            web_worker_task=web_worker_task,
            web_stop_event=web_stop,
            on_api_stopped=producers_stopped,
        ),
    }[mode]

    with pytest.raises(RuntimeError, match="returned before shutdown was requested"):
        await run_mode()

    assert events == ["api_started", "api_finished", "producers_stopped"]
    assert scheduler.stopped.is_set()
    assert web_stop.is_set()
