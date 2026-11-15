"""Drain-pending selection, recovery, and worker integration tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import make_comment, make_comments_response, make_post, make_posts_response
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from tiebameow.client.tieba_client import RetriableApiError

import main as app_main
from src.core.datastore import DataStore
from src.core.lock import MemoryLockManager
from src.core.models import ResearchRun, ResearchTaskScope
from src.research.client import ResearchPage
from src.scraper.router import QueueRouter
from src.scraper.worker import FullScanPostsTaskHandler, _retry_delay


class _SessionContext:
    def __init__(self, engine):
        self.engine = engine
        self.session: Session | None = None

    async def __aenter__(self):
        self.session = Session(self.engine)
        return _AsyncSession(self.session)

    async def __aexit__(self, *_args):
        assert self.session is not None
        self.session.close()


class _AsyncSession:
    def __init__(self, session: Session):
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

    def add(self, value):
        self.session.add(value)

    async def commit(self):
        self.session.commit()


def _scope(
    logical_key: str,
    scope_type: str,
    tid: int,
    *,
    generation: int = 0,
    status: str = "pending",
    parent_pid: int | None = None,
    parent_kind: str | None = None,
    parent_generation: int | None = None,
):
    return SimpleNamespace(
        run_id="run-synthetic",
        logical_task_key=logical_key,
        scope_type=scope_type,
        task_type="full_scan_posts" if scope_type == "thread_replies" else "full_scan_comments",
        generation=generation,
        status=status,
        fid=7,
        fname="synthetic",
        tid=tid,
        parent_pid=parent_pid,
        parent_kind=parent_kind,
        parent_generation=parent_generation,
        parent_logical_task_key=None,
        backfill=True,
        rn=17,
    )


def _stored_scope(
    run_id: str,
    logical_key: str,
    scope_type: str,
    tid: int,
    fid: int,
    *,
    status: str = "pending",
):
    return ResearchTaskScope(
        run_id=run_id,
        logical_task_key=logical_key,
        generation=0,
        task_type="full_scan_posts" if scope_type == "thread_replies" else "full_scan_comments",
        scope_type=scope_type,
        fid=fid,
        fname=f"forum-{fid}",
        tid=tid,
        parent_pid=tid * 10 if scope_type == "post_comments" else None,
        parent_kind="first_post" if scope_type == "post_comments" else None,
        parent_generation=0 if scope_type == "post_comments" else None,
        backfill=True,
        rn=30,
        status=status,
    )


@pytest.mark.asyncio
async def test_pending_scope_query_filters_run_forum_tid_and_global_limit():
    engine = create_engine("sqlite:///:memory:")
    ResearchTaskScope.__table__.create(engine)
    with Session(engine) as session:
        session.add_all(
            [
                _stored_scope("run-a", "post_comments:202:2020", "post_comments", 202, 7),
                _stored_scope("run-a", "thread_replies:202", "thread_replies", 202, 7),
                _stored_scope("run-a", "post_comments:101:1010", "post_comments", 101, 7),
                _stored_scope("run-a", "thread_replies:303", "thread_replies", 303, 8),
                _stored_scope("run-a", "post_comments:404:4040", "post_comments", 404, 7, status="completed"),
                _stored_scope("run-b", "post_comments:202:2020", "post_comments", 202, 7),
            ]
        )
        session.commit()

    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _SessionContext(engine)

    async def collect(**kwargs):
        rows = []
        async for batch in store.iter_pending_research_task_scopes("run-a", {7}, **kwargs):
            rows.extend(batch)
        return rows

    filtered = await collect(tid=202, limit=1, batch_size=1)
    assert [row.logical_task_key for row in filtered] == ["post_comments:202:2020"]

    all_pending = await collect(batch_size=2)
    assert [row.logical_task_key for row in all_pending] == [
        "post_comments:101:1010",
        "post_comments:202:2020",
        "thread_replies:202",
    ]
    engine.dispose()


@pytest.mark.asyncio
async def test_drain_run_loader_checks_scope_and_completed_reopen_is_drain_only():
    engine = create_engine("sqlite:///:memory:")
    ResearchRun.__table__.create(engine)
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _SessionContext(engine)
    forums = [(7, "synthetic")]
    run_id = await store.start_research_run(
        mode="backfill",
        forums=forums,
        max_backfill_pages=0,
    )
    await store.set_research_run_status(run_id, "completed")

    assert await store.load_research_run_for_drain(run_id, forums) == "completed"
    with pytest.raises(ValueError, match="does not match the current forum scope"):
        await store.load_research_run_for_drain(run_id, [(8, "other")])

    assert await store.resume_research_run_for_drain(run_id, "completed")
    with Session(engine) as session:
        run = session.get(ResearchRun, run_id)
        assert run is not None
        assert run.status == "running"
        assert run.reason == "drain_reopened_completed_run"

    await store.set_research_run_status(run_id, "completed")
    with pytest.raises(ValueError, match="No resumable research run matches"):
        await store.start_research_run(
            mode="backfill",
            forums=forums,
            max_backfill_pages=0,
            resume=True,
            run_id=run_id,
        )
    engine.dispose()


class _ResearchStore:
    """Small in-memory scope/checkpoint store for real Worker/router tests."""

    def __init__(self, scopes, *, run_status="incomplete", run_reason=None):
        self.scopes = {(scope.run_id, scope.logical_task_key): scope for scope in scopes}
        self.checkpoints: dict[tuple[str, str, int], dict] = {}
        self.run_status = run_status
        self.run_reason = run_reason
        self.selection = None
        self.child_scopes_created = asyncio.Event()
        self.posts_page_committed = asyncio.Event()
        self.children_created = 0
        self.drain_resume_calls = []
        self.drain_resume_reasons = []
        self.async_sessionmaker = lambda: _Session()

    async def load_research_run_for_drain(self, run_id, forums):
        assert run_id == "run-synthetic"
        assert forums == [(7, "synthetic")]
        return self.run_status

    async def resume_research_run_for_drain(self, run_id, expected_status):
        assert run_id == "run-synthetic"
        self.drain_resume_calls.append((run_id, expected_status))
        if self.run_status != expected_status:
            return False
        self.run_reason = "drain_reopened_completed_run" if expected_status == "completed" else None
        self.drain_resume_reasons.append(self.run_reason)
        self.run_status = "running"
        return True

    async def adopt_legacy_research_scopes(self, *_args):
        pytest.fail("drain-pending must not adopt global legacy pending rows")

    async def iter_pending_research_task_scopes(self, run_id, fids, *, batch_size, tid=None, limit=None):
        self.selection = (run_id, set(fids), batch_size, tid, limit)
        selected = []
        for scope_type in ("post_comments", "thread_replies"):
            selected.extend(
                sorted(
                    (
                        scope
                        for (scope_run_id, _), scope in self.scopes.items()
                        if scope_run_id == run_id
                        and scope.scope_type == scope_type
                        and scope.status != "completed"
                        and scope.fid in fids
                        and (tid is None or scope.tid == tid)
                    ),
                    key=lambda scope: scope.logical_task_key,
                )
            )
        if limit is not None:
            selected = selected[:limit]
        for offset in range(0, len(selected), batch_size):
            yield selected[offset : offset + batch_size]

    async def set_research_run_status_if(self, run_id, expected, status, reason):
        assert run_id == "run-synthetic"
        if self.run_status in expected:
            self.run_status = status
            self.run_reason = reason
            return True
        return False

    async def resume_research_run_if_status(self, run_id, expected_status):
        return await self.set_research_run_status_if(run_id, {expected_status}, "running", None)

    async def reconcile_research_run_status_after_drain(self, run_id):
        assert run_id == "run-synthetic"
        if self.run_status in {"paused_auth", "paused_rate_limit", "blocked", "paused_limit", "interrupted"}:
            return self.run_status
        if any(
            scope.status != "completed"
            for (scope_run_id, _), scope in self.scopes.items()
            if scope_run_id == run_id
        ):
            self.run_status = "incomplete"
            self.run_reason = "latest_checkpoint_not_terminal"
        else:
            self.run_status = "completed"
            self.run_reason = None
        return self.run_status

    async def get_research_task_scope_state(self, run_id, logical_key):
        scope = self.scopes.get((run_id, logical_key))
        return None if scope is None else (scope.generation, scope.status, scope.parent_generation)

    async def get_research_task_scope_for_update(self, _session, run_id, logical_key):
        return self.scopes.get((run_id, logical_key))

    async def get_research_resume_state(self, run_id, task_key):
        matching = [
            (page_number, value)
            for (checkpoint_run_id, checkpoint_key, page_number), value in self.checkpoints.items()
            if checkpoint_run_id == run_id and checkpoint_key == task_key
        ]
        if not matching:
            return 1, None, None
        page_number, checkpoint = max(matching)
        if checkpoint["status"] == "completed" and checkpoint["has_more"] is False:
            return None, checkpoint["scope_status"], checkpoint["continuation_json"]
        if checkpoint["status"] == "completed":
            return checkpoint["next_page"], checkpoint["scope_status"], checkpoint["continuation_json"]
        return page_number, checkpoint["scope_status"], checkpoint["continuation_json"]

    async def get_research_first_post_pids(self, _tid, *, session=None):
        return {100}

    async def ensure_research_task_scope(self, *, run_id, logical_task_key, task_type, scope_type, **kwargs):
        scope = self.scopes.get((run_id, logical_task_key))
        if scope is None:
            scope = _scope(
                logical_task_key,
                scope_type,
                kwargs["tid"],
                parent_pid=kwargs.get("parent_pid"),
                parent_kind=kwargs.get("parent_kind"),
                parent_generation=kwargs.get("parent_generation"),
            )
            scope.task_type = task_type
            scope.fid = kwargs.get("fid", 7)
            scope.fname = kwargs.get("fname", "synthetic")
            scope.generation = 0
            scope.parent_logical_task_key = kwargs.get("parent_logical_task_key")
            self.scopes[(run_id, logical_task_key)] = scope
        self.children_created += 1
        if self.children_created >= 1:
            self.child_scopes_created.set()
        return scope

    async def add_pending_comment_scan_in_session(self, *_args, **_kwargs):
        return None

    async def save_items_in_session(self, *_args, **_kwargs):
        return None

    async def record_research_checkpoint(self, **values):
        key = (values["run_id"], values["task_key"], values["page_number"])
        logical_key = values["task_key"].rsplit(":g", 1)[0]
        scope = self.scopes.get((values["run_id"], logical_key))
        if scope is None or (values.get("generation") is not None and scope.generation != values["generation"]):
            return False
        self.checkpoints[key] = values
        scope.status = values.get("scope_status") or values["status"]
        if values["scope_type"] == "thread_replies":
            self.posts_page_committed.set()
        return True

    async def remove_pending_thread_scan_in_session(self, *_args):
        return None

    async def remove_pending_comment_scan_in_session(self, *_args, **_kwargs):
        return None

    async def remove_pending_thread_scan(self, *_args, **_kwargs):
        return None

    async def remove_pending_comment_scan(self, *_args, **_kwargs):
        return None

    async def filter_new_ids(self, *_args, **_kwargs):
        return set()

    async def close(self):
        return None


class _Session:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def begin(self):
        return self


class _SyntheticClient:
    def __init__(self, *, posts=None, comments=None):
        self.posts = posts
        self.comments = comments
        self.post_pages: list[tuple[int, int]] = []
        self.comment_pages: list[tuple[int, int, int]] = []

    async def get_posts_research(self, tid, page_number, **_kwargs):
        self.post_pages.append((tid, page_number))
        return await self.posts(tid, page_number)

    async def get_comments_research(self, tid, pid, page_number):
        self.comment_pages.append((tid, pid, page_number))
        return await self.comments(tid, pid, page_number)


def _container(client, *, queue_maxsize=1):
    config = SimpleNamespace(
        metrics_enabled=False,
        worker_threads=1,
        worker_posts=1,
        worker_comments=1,
        queue_maxsize=queue_maxsize,
        research_enabled=True,
        fetch_user_forum_info=False,
        max_backfill_pages=0,
        research_max_api_retries=1,
        research_retry_wait_seconds=0,
        cooldown_seconds_429=0.01,
        pydantic_config=SimpleNamespace(
            web_fallback=SimpleNamespace(browser_profile="isolated-test-profile")
        ),
    )
    return SimpleNamespace(
        config=config,
        forums=[SimpleNamespace(fid=7, fname="synthetic")],
        tb_client=client,
        tb_clients=None,
        posts_limiter=AsyncMock(),
        comments_limiter=AsyncMock(),
        threads_limiter=AsyncMock(),
        lock_manager=MemoryLockManager(),
        research_run_id=None,
        semaphore=asyncio.Semaphore(1),
        research_auth_ready=asyncio.Event(),
        wait_for_research_ready=AsyncMock(),
        wait_for_auth_refresh=AsyncMock(),
        wait_for_rate_limit=AsyncMock(),
        block_research_network=AsyncMock(),
        teardown=AsyncMock(),
    )


def _posts_page(tid: int, posts=None, *, has_more=False, current_page=1, total_page=1):
    posts = posts or []
    dto = make_posts_response(
        posts,
        has_more=has_more,
        current_page=current_page,
        total_page=total_page,
    )
    raw = SimpleNamespace(
        thread=SimpleNamespace(
            pid=100,
            tid=tid,
            fid=7,
            fname="synthetic",
            reply_num=sum(post.floor != 1 for post in posts),
        ),
    )
    return ResearchPage(raw, dto, datetime.now(UTC), "aiotieba.get_posts")


def _comments_page(comments, *, has_more=False, current_page=1, total_page=1):
    dto = make_comments_response(
        comments,
        has_more=has_more,
        current_page=current_page,
        total_page=total_page,
    )
    return ResearchPage(SimpleNamespace(objs=comments), dto, datetime.now(UTC), "aiotieba.get_comments")


def _patch_drain_runtime(monkeypatch, container, router, datastore):
    async def initialize(*, mode):
        assert mode == "backfill"
        return container, router

    container.async_sessionmaker = datastore.async_sessionmaker
    monkeypatch.setattr(app_main, "initialize_application", initialize)
    monkeypatch.setattr(app_main, "DataStore", lambda _container: datastore)
    monkeypatch.setattr(
        app_main,
        "Scheduler",
        lambda **_kwargs: pytest.fail("drain-pending must not start a scheduler"),
    )

    async def pending_run_ids(_sessions, active_run_id, *, responsible_forums):
        assert responsible_forums == ((7, "synthetic"),)
        return (active_run_id,)

    async def discover_missing(_sessions, _run_ids):
        return 0

    async def live_web_worker(_sessions, _run_ids, *, stop_event, **_kwargs):
        await stop_event.wait()
        return SimpleNamespace(unresolved_count=0)

    monkeypatch.setattr(app_main, "pending_web_run_ids", pending_run_ids)
    monkeypatch.setattr(app_main, "discover_missing_web_targets", discover_missing)
    monkeypatch.setattr(app_main, "run_live_web_worker", live_web_worker)
    monkeypatch.setattr(
        "src.research.web_fallback.live.enqueue_missing_web_targets",
        AsyncMock(return_value=()),
    )


@pytest.mark.asyncio
async def test_drain_pending_uses_real_workers_only_for_selected_run_and_scans_first_floor_comments(monkeypatch):
    thread_scope = _scope("thread_replies:1", "thread_replies", 1, generation=4)
    other_scope = _scope("thread_replies:2", "thread_replies", 2)

    async def get_posts(tid, page_number):
        assert (tid, page_number) == (1, 1)
        first_post = make_post(pid=100, tid=tid, fid=7, fname="synthetic", floor=1, reply_num=2)
        response = _posts_page(tid, [first_post])
        assert response.raw.thread.reply_num == 0
        assert response.dto.objs[0].floor == 1
        return response

    async def get_comments(tid, pid, page_number):
        assert (tid, pid) == (1, 100)
        if page_number == 1:
            return _comments_page(
                [make_comment(cid=500, pid=100, tid=1, fid=7, fname="synthetic")],
                has_more=True,
                current_page=1,
                total_page=2,
            )
        return _comments_page(
            [make_comment(cid=501, pid=100, tid=1, fid=7, fname="synthetic")],
            current_page=2,
            total_page=2,
        )

    client = _SyntheticClient(posts=get_posts, comments=get_comments)
    container = _container(client)
    router = QueueRouter(maxsize=1)
    store = _ResearchStore([thread_scope, other_scope])
    _patch_drain_runtime(monkeypatch, container, router, store)
    monkeypatch.setattr("src.research.persistence.persist_research_page", AsyncMock())

    count = await app_main.drain_pending(run_id="run-synthetic", tid=1, limit=1, max_seconds=2)

    assert count == 1
    assert store.drain_resume_calls == [("run-synthetic", "incomplete")]
    assert store.selection == ("run-synthetic", {7}, 1, 1, 1)
    assert client.post_pages == [(1, 1)]
    assert client.comment_pages == [(1, 100, 1), (1, 100, 2)]
    assert not hasattr(client, "get_threads_research")
    assert store.scopes[("run-synthetic", "thread_replies:1")].generation == 4
    assert store.scopes[("run-synthetic", "thread_replies:1")].status == "completed"
    assert store.scopes[("run-synthetic", "post_comments:1:100")].status == "completed"
    assert store.scopes[("run-synthetic", "thread_replies:2")].status == "pending"
    assert store.run_status == "incomplete"


@pytest.mark.asyncio
async def test_drain_completed_run_without_pending_scopes_is_noop_without_auth(monkeypatch):
    container = _container(_SyntheticClient())
    container.config.fetch_user_forum_info = True
    container.validate_research_auth = AsyncMock()
    container.wait_for_initial_auth = AsyncMock()
    router = QueueRouter(maxsize=1)
    store = _ResearchStore([], run_status="completed", run_reason="completed_before_drain")
    _patch_drain_runtime(monkeypatch, container, router, store)

    count = await app_main.drain_pending(run_id="run-synthetic", limit=1, max_seconds=1)

    assert count == 0
    assert store.run_status == "completed"
    assert store.run_reason == "completed_before_drain"
    assert store.drain_resume_calls == []
    container.validate_research_auth.assert_not_awaited()
    container.wait_for_initial_auth.assert_not_awaited()


@pytest.mark.asyncio
async def test_drain_reopens_completed_run_with_pending_scope_and_audits_reason(monkeypatch):
    scope = _scope("thread_replies:1", "thread_replies", 1, generation=5)

    async def posts(tid, _page_number):
        first_post = make_post(pid=100, tid=tid, fid=7, fname="synthetic", floor=1, reply_num=0)
        return _posts_page(tid, [first_post])

    async def comments(_tid, _pid, _page_number):
        return _comments_page([])

    container = _container(_SyntheticClient(posts=posts, comments=comments))
    router = QueueRouter(maxsize=1)
    store = _ResearchStore([scope], run_status="completed")
    _patch_drain_runtime(monkeypatch, container, router, store)
    monkeypatch.setattr("src.research.persistence.persist_research_page", AsyncMock())

    count = await app_main.drain_pending(run_id="run-synthetic", limit=1, max_seconds=1)

    assert count == 1
    assert store.drain_resume_calls == [("run-synthetic", "completed")]
    assert store.drain_resume_reasons == ["drain_reopened_completed_run"]
    assert store.run_status == "completed"
    assert scope.status == "completed"


@pytest.mark.asyncio
async def test_drain_deadline_keeps_current_scope_recoverable_and_pauses_run(monkeypatch):
    started = asyncio.Event()
    release_request = asyncio.Event()
    scope = _scope("thread_replies:1", "thread_replies", 1, generation=3)

    async def blocked_posts(tid, page_number):
        started.set()
        await release_request.wait()
        return _posts_page(
            tid,
            [make_post(pid=100, tid=tid, fid=7, fname="synthetic", floor=1, reply_num=0)],
        )

    client = _SyntheticClient(posts=blocked_posts, comments=lambda *_args: None)
    container = _container(client)
    router = QueueRouter(maxsize=1)
    store = _ResearchStore([scope])
    _patch_drain_runtime(monkeypatch, container, router, store)
    monkeypatch.setattr("src.research.persistence.persist_research_page", AsyncMock())

    result = asyncio.create_task(
        app_main.drain_pending(run_id="run-synthetic", limit=1, max_seconds=0.05)
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.sleep(0.1)
    assert store.run_status == "paused_limit"
    assert not result.done()
    release_request.set()
    assert await asyncio.wait_for(result, timeout=1) == 1

    assert store.run_status == "paused_limit"
    assert scope.status == "completed"
    assert store.checkpoints[("run-synthetic", "thread_replies:1:g3", 1)]["status"] == "completed"


@pytest.mark.asyncio
async def test_drain_cancellation_can_resume_same_run_without_losing_checkpoint(monkeypatch):
    started = asyncio.Event()
    release_request = asyncio.Event()
    scope = _scope("thread_replies:1", "thread_replies", 1, generation=2)
    post_calls = 0

    async def posts(tid, page_number):
        nonlocal post_calls
        post_calls += 1
        if post_calls == 1:
            started.set()
            await release_request.wait()
            return _posts_page(
                tid,
                [make_post(pid=100, tid=tid, fid=7, fname="synthetic", floor=1, reply_num=0)],
                has_more=True,
                current_page=1,
                total_page=2,
            )
        assert page_number == 2
        return _posts_page(
            tid,
            [make_post(pid=100, tid=tid, fid=7, fname="synthetic", floor=1, reply_num=0)],
            current_page=2,
            total_page=2,
        )

    async def comments(_tid, _pid, _page_number):
        return _comments_page([])

    client = _SyntheticClient(posts=posts, comments=comments)
    store = _ResearchStore([scope])
    monkeypatch.setattr("src.research.persistence.persist_research_page", AsyncMock())

    def start_attempt(limit=1):
        container = _container(client)
        router = QueueRouter(maxsize=1)
        _patch_drain_runtime(monkeypatch, container, router, store)
        return app_main.drain_pending(run_id="run-synthetic", limit=limit, max_seconds=0)

    first_attempt = asyncio.create_task(start_attempt())
    await asyncio.wait_for(started.wait(), timeout=1)
    first_attempt.cancel()
    await asyncio.sleep(0)
    release_request.set()
    with pytest.raises(asyncio.CancelledError):
        await first_attempt
    assert store.run_status == "interrupted"
    assert scope.status == "running"
    assert store.checkpoints[("run-synthetic", "thread_replies:1:g2", 1)]["next_page"] == 2

    # The first page creates a durable comment child. Drain comments-first
    # ordering needs both that child and its parent to resume the second page.
    assert await start_attempt(limit=2) == 2
    assert post_calls == 2
    assert store.drain_resume_calls == [
        ("run-synthetic", "incomplete"),
        ("run-synthetic", "interrupted"),
    ]
    assert store.run_status == "completed"
    assert scope.status == "completed"


@pytest.mark.asyncio
async def test_drain_stops_api_when_live_web_returns_summary_early(monkeypatch):
    api_started = asyncio.Event()
    release_page = asyncio.Event()
    web_returned = asyncio.Event()
    scope = _scope("thread_replies:1", "thread_replies", 1, generation=6)
    post_calls = []

    async def posts(tid, page_number):
        post_calls.append(page_number)
        if page_number == 1:
            api_started.set()
            await release_page.wait()
            return _posts_page(
                tid,
                [make_post(pid=100, tid=tid, fid=7, fname="synthetic", floor=1, reply_num=0)],
                has_more=True,
                current_page=1,
                total_page=2,
            )
        pytest.fail("API page 2 must not start after the live WEB worker exits")

    client = _SyntheticClient(posts=posts, comments=lambda *_args: _comments_page([]))
    container = _container(client)
    router = QueueRouter(maxsize=1)
    store = _ResearchStore([scope])
    _patch_drain_runtime(monkeypatch, container, router, store)
    monkeypatch.setattr("src.research.persistence.persist_research_page", AsyncMock())

    async def returned_summary(_sessions, _run_ids, *, stop_event, **_kwargs):
        await api_started.wait()
        web_returned.set()
        return SimpleNamespace(stopped_reason="manual_login_not_confirmed", unresolved_count=1)

    monkeypatch.setattr(app_main, "run_live_web_worker", returned_summary)

    drain = asyncio.create_task(
        app_main.drain_pending(run_id="run-synthetic", limit=1, max_seconds=0)
    )
    await asyncio.wait_for(web_returned.wait(), timeout=1)
    await asyncio.sleep(0)
    assert not drain.done()

    release_page.set()
    with pytest.raises(RuntimeError, match="returned before shutdown was requested"):
        await asyncio.wait_for(drain, timeout=2)

    assert post_calls == [1]
    assert scope.status == "running"
    assert store.checkpoints[("run-synthetic", "thread_replies:1:g6", 1)]["next_page"] == 2
    assert store.run_status == "incomplete"


@pytest.mark.asyncio
async def test_bounded_queue_cancellation_keeps_generated_children_and_is_not_idle(monkeypatch):
    comment_started = asyncio.Event()
    release_comment = asyncio.Event()
    parent_scope = _scope("thread_replies:1", "thread_replies", 1, generation=4)
    initial_comment = _scope(
        "post_comments:1:90",
        "post_comments",
        1,
        parent_pid=90,
        parent_kind="reply_post",
        parent_generation=4,
    )

    async def posts(tid, _page_number):
        replies = [make_post(pid=pid, tid=tid, fid=7, fname="synthetic") for pid in (101, 102, 103)]
        return _posts_page(tid, replies)

    async def comments(tid, pid, _page_number):
        if pid == 90:
            comment_started.set()
            await release_comment.wait()
        return _comments_page([])

    client = _SyntheticClient(posts=posts, comments=comments)
    container = _container(client)
    router = QueueRouter(maxsize=1)
    store = _ResearchStore([parent_scope, initial_comment])
    _patch_drain_runtime(monkeypatch, container, router, store)
    monkeypatch.setattr("src.research.persistence.persist_research_page", AsyncMock())

    drain_task = asyncio.create_task(
        app_main.drain_pending(run_id="run-synthetic", limit=2, max_seconds=0)
    )
    await asyncio.wait_for(comment_started.wait(), timeout=1)
    await asyncio.wait_for(store.posts_page_committed.wait(), timeout=1)
    await asyncio.sleep(0)
    child_keys = {
        logical_key
        for (run_id, logical_key), scope in store.scopes.items()
        if run_id == "run-synthetic" and scope.parent_logical_task_key == "thread_replies:1"
    }
    assert child_keys == {
        "post_comments:1:100",
        "post_comments:1:101",
        "post_comments:1:102",
        "post_comments:1:103",
    }
    assert router.total_qsize() == 1
    assert router.tracker.pending >= 3

    drain_task.cancel()
    await asyncio.sleep(0)
    release_comment.set()
    with pytest.raises(asyncio.CancelledError):
        await drain_task

    assert store.run_status == "interrupted"
    assert all(store.scopes[("run-synthetic", key)].status == "pending" for key in child_keys)
    assert router.total_qsize() == 1
    assert router.tracker.pending == 1
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(router.wait_idle(), timeout=0.02)


@pytest.mark.asyncio
async def test_non_auth_retries_use_exponential_delay(monkeypatch):
    expected = object()
    method = AsyncMock(
        side_effect=[RetriableApiError(10, "synthetic"), RetriableApiError(11, "synthetic"), expected]
    )
    client = SimpleNamespace(get_posts_research=method)
    container = _container(client)
    container.config.research_max_api_retries = 3
    container.config.research_retry_wait_seconds = 0.5
    container.research_run_id = "run-synthetic"
    container.wait_for_research_ready = AsyncMock()
    store = SimpleNamespace(resume_research_run_if_status=AsyncMock(return_value=False))
    delays = []

    async def record_sleep(delay):
        delays.append(delay)

    monkeypatch.setattr("src.scraper.worker.asyncio.sleep", record_sleep)
    monkeypatch.setattr("src.scraper.worker.random.uniform", lambda low, high: 1.1)
    handler = FullScanPostsTaskHandler(0, container, store, QueueRouter())

    fetched = await handler._request_api("posts", "get_posts_research", "get_posts_dto", (1, 1), {})

    assert fetched.response is expected
    assert method.await_count == 3
    assert delays == pytest.approx([0.55, 1.1])
    assert _retry_delay(100, 1) == 30
