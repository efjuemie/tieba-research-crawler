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
async def test_backfill_api_completion_waits_for_recovery_producer():
    events = []
    recovery_started = asyncio.Event()
    recovery_finished = asyncio.Event()
    allow_recovery_to_finish = asyncio.Event()
    scheduler_started = asyncio.Event()

    class FakeScheduler:
        def stop(self):
            return None

        async def run(self, mode):
            assert mode == "backfill"
            events.append("scheduler_started")
            scheduler_started.set()

    class FakeRouter:
        async def wait_idle(self):
            events.append("router_idle_wait_started")
            await recovery_finished.wait()
            events.append("router_idle")

    async def recovery_producer():
        events.append("recovery_started")
        recovery_started.set()
        await allow_recovery_to_finish.wait()
        events.append("recovery_enqueued")
        recovery_finished.set()

    recovery_task = asyncio.create_task(recovery_producer())

    async def api_finished():
        events.append("api_finished")

    mode_task = asyncio.create_task(
        app_main._run_backfill_mode(
            FakeScheduler(),
            [],
            FakeRouter(),
            [],
            web_worker_task=None,
            recovery_task=recovery_task,
            on_api_stopped=api_finished,
        )
    )
    await asyncio.wait_for(recovery_started.wait(), timeout=1)
    await asyncio.wait_for(scheduler_started.wait(), timeout=1)
    assert not recovery_finished.is_set()
    assert "api_finished" not in events

    allow_recovery_to_finish.set()
    await asyncio.wait_for(mode_task, timeout=1)
    assert events.index("scheduler_started") < events.index("recovery_enqueued")
    assert events.index("recovery_enqueued") < events.index("router_idle") < events.index("api_finished")


@pytest.mark.asyncio
async def test_user_stop_during_web_drain_interrupts_web_without_waiting_for_full_drain():
    events = []
    api_done_event = asyncio.Event()
    web_stop_event = asyncio.Event()
    web_drain_started = asyncio.Event()

    class FakeScheduler:
        def stop(self):
            return None

        async def run(self, mode):
            assert mode == "backfill"

    class FakeRouter:
        async def wait_idle(self):
            events.append("api_queue_idle")

    async def live_web_worker():
        await api_done_event.wait()
        events.append("web_drain_started")
        web_drain_started.set()
        await web_stop_event.wait()
        events.append("web_stopped_for_user")
        return SimpleNamespace(unresolved_count=1)

    web_worker_task = asyncio.create_task(live_web_worker())

    async def api_stopped():
        events.append("api_finished")
        api_done_event.set()
        await asyncio.shield(web_worker_task)

    async def api_interrupted():
        web_stop_event.set()
        await asyncio.shield(web_worker_task)

    mode_task = asyncio.create_task(
        app_main._run_backfill_mode(
            FakeScheduler(),
            [],
            FakeRouter(),
            [],
            web_worker_task=web_worker_task,
            on_api_stopped=api_stopped,
            on_api_interrupted=api_interrupted,
        )
    )
    await asyncio.wait_for(web_drain_started.wait(), timeout=1)
    mode_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await mode_task

    assert events == ["api_queue_idle", "api_finished", "web_drain_started", "web_stopped_for_user"]
    assert web_worker_task.done()


@pytest.mark.asyncio
async def test_user_stop_stops_api_and_web_without_draining_all_queued_web_work(monkeypatch):
    events = []
    api_page_started = asyncio.Event()
    web_started = asyncio.Event()
    scheduler_started = asyncio.Event()
    recovery_finished = asyncio.Event()
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
                web_fallback=SimpleNamespace(browser_profile="isolated-test-profile", rps=0.2)
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
            events.append("scheduler_started")
            scheduler_started.set()
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

    async def delayed_recovery(*_args, **_kwargs):
        events.append("recovery_started")
        await web_started.wait()
        await scheduler_started.wait()
        events.append("recovery_finished")
        recovery_finished.set()

    async def pending_run_ids(_sessions, current_run_id, *, responsible_forums):
        assert current_run_id == "synthetic-run"
        assert responsible_forums == ((7, "synthetic"),)
        return (current_run_id,)

    async def discover(_sessions, run_ids):
        nonlocal discovery_calls
        assert run_ids == ("synthetic-run",)
        discovery_calls += 1
        events.append("web_target_rescan_startup" if discovery_calls == 1 else "web_target_rescan_shutdown")
        return 1

    async def live_web_worker(_sessions, _run_ids, *, stop_event, api_done_event, **kwargs):
        assert api_done_event is not None
        assert "request_budget" not in kwargs
        await api_page_started.wait()
        assert api_active
        events.append("web_started_while_api_active")
        web_started.set()
        await stop_event.wait()
        events.append("web_stopped_for_user")
        return SimpleNamespace(unresolved_count=1)

    datastore = FakeDataStore()
    original_await_mode_or_shutdown = app_main._await_mode_or_shutdown

    async def trigger_user_stop(mode_task, shutdown_event):
        await web_started.wait()
        await scheduler_started.wait()
        await recovery_finished.wait()
        shutdown_event.set()
        result = await original_await_mode_or_shutdown(mode_task, shutdown_event)
        events.append(("shutdown_result", result, shutdown_event.is_set()))
        return result

    monkeypatch.setattr(app_main, "initialize_application", initialize)
    monkeypatch.setattr(app_main, "DataStore", lambda _container: datastore)
    monkeypatch.setattr(app_main, "Scheduler", FakeScheduler)
    monkeypatch.setattr(app_main, "Worker", FakeWorker)
    monkeypatch.setattr(app_main, "_recover_pending_tasks", delayed_recovery)
    monkeypatch.setattr(app_main, "pending_web_run_ids", pending_run_ids)
    monkeypatch.setattr(app_main, "discover_missing_web_targets", discover)
    monkeypatch.setattr(app_main, "run_live_web_worker", live_web_worker)
    monkeypatch.setattr(app_main, "_await_mode_or_shutdown", trigger_user_stop)

    task = asyncio.create_task(app_main.main("periodic"))
    await asyncio.wait_for(web_started.wait(), timeout=1)
    await asyncio.wait_for(task, timeout=1)

    assert events.index("web_started_while_api_active") < events.index("recovery_finished")
    assert events.index("scheduler_started") < events.index("recovery_finished")
    assert events.index("web_started_while_api_active") < events.index("api_page_committed")
    assert events.index("api_page_committed") < events.index("web_stopped_for_user")
    assert discovery_calls == 1
    assert "run_reconciled" not in events
    assert events.index("web_stopped_for_user") < events.index("database_closed") < events.index("container_torn_down")
    assert any(
        event[0] == "run_status" and event[1] == "interrupted"
        for event in events
        if isinstance(event, tuple)
    ), [event for event in events if isinstance(event, tuple)]
    assert ("shutdown_result", True, True) in events


@pytest.mark.asyncio
async def test_backfill_web_failure_does_not_cancel_api_producer():
    events = []
    worker_started = asyncio.Event()
    worker_stop = asyncio.Event()
    allow_api_drain = asyncio.Event()

    class FakeScheduler:
        def __init__(self):
            self.stopped = asyncio.Event()

        def stop(self):
            self.stopped.set()

        async def run(self, mode):
            assert mode == "backfill"
            return None

    class FakeWorker:
        async def run(self):
            worker_started.set()
            await worker_stop.wait()
            events.append("api_page_committed")

        def stop_after_current(self):
            worker_stop.set()

    class FakeRouter:
        async def wait_idle(self):
            events.append("api_drain_waiting")
            await allow_api_drain.wait()

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

    mode_task = asyncio.create_task(
        app_main._run_backfill_mode(
            FakeScheduler(),
            [worker],
            FakeRouter(),
            [worker_task],
            web_worker_task=web_worker_task,
            on_api_stopped=producers_stopped,
        )
    )
    await asyncio.wait_for(worker_started.wait(), timeout=1)
    with pytest.raises(RuntimeError, match="synthetic web worker failure"):
        await asyncio.wait_for(web_worker_task, timeout=1)
    await asyncio.sleep(0)
    assert not mode_task.done()
    assert not worker_stop.is_set()
    allow_api_drain.set()
    await asyncio.wait_for(mode_task, timeout=1)

    assert events.index("web_worker_failed") < events.index("api_page_committed")
    assert events.index("api_page_committed") < events.index("producers_stopped")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["periodic", "hybrid", "backfill"])
async def test_early_web_exit_does_not_cancel_api_producers(mode):
    events = []
    api_stop = asyncio.Event()
    web_stop = asyncio.Event()
    allow_backfill_drain = asyncio.Event()

    class FakeScheduler:
        def __init__(self):
            self.stopped = asyncio.Event()

        def stop(self):
            self.stopped.set()

        async def run(self, mode):
            assert mode in {"periodic", "backfill"}
            if mode != "backfill":
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
            await allow_backfill_drain.wait()

    async def returned_summary():
        events.append("web_exited_early")
        return SimpleNamespace(stopped_reason="manual_login_not_confirmed", unresolved_count=1)

    async def producers_stopped():
        assert "api_finished" in events
        events.append("producers_stopped")

    async def producers_interrupted():
        web_stop.set()
        events.append("producers_interrupted")

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
            on_api_interrupted=producers_interrupted,
        ),
        "hybrid": lambda: app_main._run_hybrid_mode(
            scheduler,
            [worker],
            FakeRouter(),
            worker_tasks=[worker_task],
            web_worker_task=web_worker_task,
            web_stop_event=web_stop,
            on_api_stopped=producers_stopped,
            on_api_interrupted=producers_interrupted,
        ),
        "backfill": lambda: app_main._run_backfill_mode(
            scheduler,
            [worker],
            FakeRouter(),
            [worker_task],
            web_worker_task=web_worker_task,
            web_stop_event=web_stop,
            on_api_stopped=producers_stopped,
            on_api_interrupted=producers_interrupted,
        ),
    }[mode]

    mode_task = asyncio.create_task(run_mode())
    await asyncio.wait_for(web_worker_task, timeout=1)
    await asyncio.sleep(0)
    assert not mode_task.done()
    assert not api_stop.is_set()
    if mode == "backfill":
        allow_backfill_drain.set()
        await asyncio.wait_for(mode_task, timeout=1)
        assert events[-2:] == ["api_finished", "producers_stopped"]
        assert not web_stop.is_set()
    else:
        mode_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await mode_task
        assert events[-2:] == ["api_finished", "producers_interrupted"]
        assert scheduler.stopped.is_set()
        assert web_stop.is_set()
