"""Research task-scope restart recovery tests."""

from __future__ import annotations

import asyncio
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest
from tiebameow.utils.time_utils import SHANGHAI_TZ

import main as app_main
from main import _dispatch_pending_comment_scopes, _recover_pending_tasks
from src.scraper.router import QueueRouter
from src.scraper.tasks import FullScanCommentsTask, FullScanPostsTask, Priority, Task


class _ScopeStore:
    def __init__(self, scopes):
        self.scopes = scopes
        self.events: list[str] = []
        self.checkpoints: list[object] = []

    async def adopt_legacy_research_scopes(self, run_id, fids):
        assert run_id == "run-synthetic"
        assert fids == {7}
        self.events.append("adopted")

    async def iter_pending_research_task_scopes(self, run_id, fids, *, batch_size, scope_type=None):
        assert run_id == "run-synthetic"
        assert fids == {7}
        assert batch_size == 500
        assert self.events[0] == "adopted"
        self.events.append(f"iterated:{scope_type}")
        pending = [
            scope
            for scope in self.scopes
            if scope.status != "completed"
            and scope.fid in fids
            and (scope_type is None or scope.scope_type == scope_type)
        ]
        if pending:
            yield pending


class _Router:
    def __init__(self):
        self.tasks: list[Any] = []

    async def put(self, task):
        self.tasks.append(task)


def _scope(
    *,
    logical_key: str,
    scope_type: str,
    task_type: str,
    generation: int,
    tid: int,
    parent_pid: int | None = None,
    parent_kind: str | None = None,
    parent_generation: int | None = None,
):
    return SimpleNamespace(
        logical_task_key=logical_key,
        scope_type=scope_type,
        task_type=task_type,
        generation=generation,
        status="pending",
        fid=7,
        fname="synthetic",
        tid=tid,
        parent_pid=parent_pid,
        parent_kind=parent_kind,
        parent_logical_task_key=None,
        parent_generation=parent_generation,
        backfill=True,
        rn=17,
    )


@pytest.mark.asyncio
async def test_recovery_enqueues_current_generation_without_page_checkpoint():
    store = _ScopeStore(
        [
            _scope(
                logical_key="post_comments:101:1001",
                scope_type="post_comments",
                task_type="full_scan_comments",
                generation=5,
                tid=101,
                parent_pid=1001,
                parent_kind="first_post",
                parent_generation=3,
            ),
            _scope(
                logical_key="thread_replies:101",
                scope_type="thread_replies",
                task_type="full_scan_posts",
                generation=3,
                tid=101,
            ),
        ]
    )
    router = _Router()

    await _recover_pending_tasks(store, router, {7}, research_run_id="run-synthetic")

    assert store.events == ["adopted", "iterated:thread_replies", "iterated:post_comments"]
    assert store.checkpoints == []
    post_task = next(task.content for task in router.tasks if isinstance(task.content, FullScanPostsTask))
    comment_task = next(task.content for task in router.tasks if isinstance(task.content, FullScanCommentsTask))
    assert (post_task.run_id, post_task.tid, post_task.generation, post_task.fid, post_task.rn) == (
        "run-synthetic",
        101,
        3,
        7,
        17,
    )
    assert (
        comment_task.run_id,
        comment_task.tid,
        comment_task.pid,
        comment_task.generation,
        comment_task.parent_generation,
        comment_task.parent_kind,
    ) == ("run-synthetic", 101, 1001, 5, 3, "first_post")
    assert router.tasks[0].content is post_task
    assert comment_task.pending_only is False


@pytest.mark.asyncio
async def test_cancelled_enqueue_leaves_current_scope_recoverable():
    scope = _scope(
        logical_key="thread_replies:202",
        scope_type="thread_replies",
        task_type="full_scan_posts",
        generation=4,
        tid=202,
    )
    store = _ScopeStore([scope])
    enqueue_started = asyncio.Event()

    class CancelBeforeEnqueue:
        async def put(self, _task):
            enqueue_started.set()
            await asyncio.Event().wait()

    cancelled = asyncio.create_task(
        _recover_pending_tasks(store, CancelBeforeEnqueue(), {7}, research_run_id="run-synthetic")
    )
    await asyncio.wait_for(enqueue_started.wait(), timeout=1)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled

    assert scope.status == "pending"
    assert store.checkpoints == []

    router = _Router()
    await _recover_pending_tasks(store, router, {7}, research_run_id="run-synthetic")

    assert len(router.tasks) == 1
    recovered = router.tasks[0].content
    assert isinstance(recovered, FullScanPostsTask)
    assert (recovered.tid, recovered.generation, recovered.run_id) == (202, 4, "run-synthetic")


class _CommentScopeStore:
    def __init__(self, scopes):
        self.scopes = scopes
        self.read_count = 0
        self.read_event = asyncio.Event()

    async def iter_pending_research_task_scopes(
        self, run_id, fids, *, batch_size, scope_type, status
    ):
        assert (run_id, fids, batch_size, scope_type, status) == (
            "run-synthetic",
            {7},
            500,
            "post_comments",
            "pending",
        )
        self.read_count += 1
        self.read_event.set()
        pending = [scope for scope in self.scopes if scope.scope_type == scope_type and scope.status == status]
        if pending:
            yield pending


@pytest.mark.asyncio
async def test_comment_dispatcher_finds_late_child_and_waits_for_real_router_idle():
    store = _CommentScopeStore([])
    router = QueueRouter(maxsize=1)
    producers_done = asyncio.Event()
    scope = _scope(
        logical_key="post_comments:202:2020",
        scope_type="post_comments",
        task_type="full_scan_comments",
        generation=2,
        tid=202,
        parent_pid=2020,
        parent_kind="reply_post",
        parent_generation=1,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def consume_one():
        item = await router.comments_queue.get()
        assert item.content.pending_only is True
        started.set()
        await release.wait()
        scope.status = "completed"
        router.comments_queue.task_done(item)

    consumer = asyncio.create_task(consume_one())
    dispatcher = asyncio.create_task(
        _dispatch_pending_comment_scopes(store, router, {7}, "run-synthetic", producers_done=producers_done)
    )
    await asyncio.wait_for(store.read_event.wait(), timeout=1)
    store.scopes.append(scope)
    producers_done.set()

    await asyncio.wait_for(started.wait(), timeout=1)
    assert router.tracker.pending == 1
    assert not dispatcher.done()
    await asyncio.sleep(0.01)
    assert not dispatcher.done()

    release.set()
    assert await asyncio.wait_for(dispatcher, timeout=1) is True
    await consumer
    assert router.tracker.pending == 0
    assert store.read_count >= 2


@pytest.mark.asyncio
async def test_comment_dispatcher_cancellation_rolls_back_full_queue_waiter():
    scope = _scope(
        logical_key="post_comments:202:2020",
        scope_type="post_comments",
        task_type="full_scan_comments",
        generation=2,
        tid=202,
        parent_pid=2020,
        parent_kind="reply_post",
        parent_generation=1,
    )
    store = _CommentScopeStore([scope])
    router = QueueRouter(maxsize=1)
    placeholder = Task(priority=Priority.LOW, content=FullScanCommentsTask(tid=999, pid=9999))
    router.put_nowait(placeholder)
    dispatcher = asyncio.create_task(
        _dispatch_pending_comment_scopes(
            store,
            router,
            {7},
            "run-synthetic",
            producers_done=asyncio.Event(),
        )
    )
    await asyncio.wait_for(store.read_event.wait(), timeout=1)
    await asyncio.sleep(0)
    dispatcher.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dispatcher

    assert router.comments_queue.qsize() == 1
    assert router.comments_queue._unique_set == {placeholder.unique_key}
    assert router.tracker.pending == 1
    assert scope.status == "pending"


@pytest.mark.asyncio
async def test_main_cancellation_marks_running_research_run_interrupted(monkeypatch):
    events: list[str] = []
    scheduler_started = asyncio.Event()

    class FakeDataStore:
        async def start_research_run(self, **_kwargs):
            return "run-synthetic"

        async def adopt_legacy_research_scopes(self, _run_id, _fids):
            return None

        async def iter_pending_research_task_scopes(self, _run_id, _fids, *, batch_size, scope_type=None):
            assert batch_size == 500
            if False:
                yield []

        async def set_research_run_status_if(self, run_id, expected, status, reason):
            assert run_id == "run-synthetic"
            assert expected == {"running", "completed", "incomplete"}
            assert status == "interrupted"
            assert reason == "cooperative_shutdown_after_current_api_page"
            events.append("run_interrupted")
            return True

        async def close(self):
            return None

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

    class FakeScheduler:
        def __init__(self, **_kwargs):
            pass

        def stop(self):
            return None

        async def run(self, mode):
            assert mode == "backfill"
            scheduler_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append("scheduler_cancelled")

    container = FakeContainer()
    datastore = FakeDataStore()

    async def initialize(*, mode):
        assert mode == "backfill"
        return container, QueueRouter()

    monkeypatch.setattr(app_main, "initialize_application", initialize)
    monkeypatch.setattr(app_main, "DataStore", lambda _container: datastore)
    monkeypatch.setattr(app_main, "Scheduler", FakeScheduler)

    main_task = asyncio.create_task(app_main.main("backfill"))
    await asyncio.wait_for(scheduler_started.wait(), timeout=1)
    main_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await main_task

    assert events == ["scheduler_cancelled", "run_interrupted"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["backfill", "hybrid"])
async def test_empty_research_forums_fail_before_creating_run(mode):
    class FakeDataStore:
        def __init__(self):
            self.start_calls = 0

        async def start_research_run(self, **_kwargs):
            self.start_calls += 1
            return "run-synthetic"

    container = SimpleNamespace(
        config=SimpleNamespace(research_enabled=True),
        forums=[],
    )
    datastore = FakeDataStore()

    with pytest.raises(ValueError, match=f"Research {mode} requires at least one initialized forum"):
        await app_main._prepare_research_run(
            container,
            datastore,
            mode,
            resume=False,
            run_id=None,
        )

    assert datastore.start_calls == 0
    assert not hasattr(container, "research_run_id")


@pytest.mark.asyncio
async def test_reply_backfill_prepares_separate_run_only_with_research_enabled():
    class FakeDataStore:
        def __init__(self):
            self.start_args = None

        async def start_research_run(self, **kwargs):
            self.start_args = kwargs
            return "reply-run-synthetic"

    container = SimpleNamespace(
        config=SimpleNamespace(
            research_enabled=True,
            fetch_user_forum_info=False,
            max_backfill_pages=9,
        ),
        forums=[SimpleNamespace(fid=7, fname="synthetic")],
    )
    datastore = FakeDataStore()

    run_id = await app_main._prepare_research_run(
        container,
        datastore,
        "backfill",
        backfill_sort="reply",
        resume=False,
        run_id=None,
    )

    assert run_id == "reply-run-synthetic"
    assert datastore.start_args["mode"] == "reply_backfill"
    assert datastore.start_args["forums"] == [(7, "synthetic")]
    assert container.research_run_id == run_id

    container.config.research_enabled = False
    datastore.start_args = None
    with pytest.raises(ValueError, match="requires research.enabled=true"):
        await app_main._prepare_research_run(
            container,
            datastore,
            "backfill",
            backfill_sort="reply",
            resume=False,
            run_id=None,
        )
    assert datastore.start_args is None


def test_min_thread_create_time_conversion_and_validation():
    assert app_main._min_thread_create_time(None) is None
    assert app_main._min_thread_create_time(2021) == datetime(2021, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ)
    for bad_year in (1989, 2101):
        with pytest.raises(ValueError, match="between 1990 and 2100"):
            app_main._min_thread_create_time(bad_year)


@pytest.mark.asyncio
async def test_recover_and_dispatch_forward_min_thread_create_time_only_when_set():
    cutoff = datetime(2021, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ)
    seen_recover_kwargs: list[dict[str, Any]] = []
    seen_dispatch_kwargs: list[dict[str, Any]] = []

    class CaptureStore:
        async def adopt_legacy_research_scopes(self, _run_id, _fids):
            return None

        async def iter_pending_research_task_scopes(self, _run_id, _fids, **kwargs):
            if kwargs.get("scope_type") == "post_comments" and kwargs.get("status") == "pending":
                seen_dispatch_kwargs.append(kwargs)
            else:
                seen_recover_kwargs.append(kwargs)
            if False:
                yield []

    store = CaptureStore()
    router = QueueRouter()
    producers_done = asyncio.Event()
    producers_done.set()

    await _recover_pending_tasks(store, router, {7}, research_run_id="run-synthetic")
    await _dispatch_pending_comment_scopes(
        store,
        router,
        {7},
        "run-synthetic",
        producers_done=producers_done,
    )
    assert all("min_thread_create_time" not in kw for kw in seen_recover_kwargs)
    assert all("min_thread_create_time" not in kw for kw in seen_dispatch_kwargs)

    seen_recover_kwargs.clear()
    seen_dispatch_kwargs.clear()
    await _recover_pending_tasks(
        store,
        router,
        {7},
        research_run_id="run-synthetic",
        min_thread_create_time=cutoff,
    )
    await _dispatch_pending_comment_scopes(
        store,
        router,
        {7},
        "run-synthetic",
        producers_done=producers_done,
        min_thread_create_time=cutoff,
    )
    assert seen_recover_kwargs
    assert seen_dispatch_kwargs
    assert all(kw["min_thread_create_time"] == cutoff for kw in seen_recover_kwargs)
    assert all(kw["min_thread_create_time"] == cutoff for kw in seen_dispatch_kwargs)
    assert all("newest_first" not in kw for kw in seen_recover_kwargs)
    assert all("newest_first" not in kw for kw in seen_dispatch_kwargs)

    seen_recover_kwargs.clear()
    seen_dispatch_kwargs.clear()
    await _recover_pending_tasks(
        store,
        router,
        {7},
        research_run_id="run-synthetic",
        min_thread_create_time=cutoff,
        newest_first=True,
    )
    await _dispatch_pending_comment_scopes(
        store,
        router,
        {7},
        "run-synthetic",
        producers_done=producers_done,
        min_thread_create_time=cutoff,
        newest_first=True,
    )
    assert seen_recover_kwargs
    assert seen_dispatch_kwargs
    assert all(kw["newest_first"] is True for kw in seen_recover_kwargs)
    assert all(kw["newest_first"] is True for kw in seen_dispatch_kwargs)
