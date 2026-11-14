"""Synthetic regressions for research detail pagination and checkpoint ordering."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, call

import pytest
from conftest import (
    make_comment,
    make_comments_response,
    make_post,
    make_posts_response,
    make_thread,
    make_threads_response,
)
from tiebameow.client.tieba_client import RetriableApiError

from src.research.client import ResearchPage
from src.scraper.router import QueueRouter
from src.scraper.tasks import FullScanCommentsTask, FullScanPostsTask, Priority, Task
from src.scraper.worker import (
    FetchedPage,
    FullScanCommentsTaskHandler,
    FullScanPostsTaskHandler,
    IncrementalScanPostsTaskHandler,
    PageIntegrityError,
    ThreadsTaskHandler,
    Worker,
)


def create_mock_container(tb_client):
    return cast(
        "Any",
        SimpleNamespace(
            tb_client=tb_client,
            tb_clients=None,
            threads_limiter=AsyncMock(),
            posts_limiter=AsyncMock(),
            comments_limiter=AsyncMock(),
            config=SimpleNamespace(
                research_enabled=False,
                fetch_user_forum_info=False,
                research_max_api_retries=1,
                research_retry_wait_seconds=0,
            cooldown_seconds_429=0,
            ),
            lock_manager=SimpleNamespace(acquire=AsyncMock(return_value=True), release=AsyncMock()),
        ),
    )


def make_research_container(tb_client):
    container = create_mock_container(tb_client)
    container.config.research_enabled = True
    container.config.research_max_api_retries = 1
    container.research_run_id = "synthetic-run"
    container.wait_for_research_ready = AsyncMock()
    container.wait_for_auth_refresh = AsyncMock()
    container.wait_for_rate_limit = AsyncMock()
    container.block_research_network = AsyncMock()
    return container


@pytest.mark.asyncio
async def test_full_scan_posts_keeps_pending_after_unexpected_empty_page():
    """中间页异常为空时不能把一级回复扫描标成完成。"""
    post = make_post(pid=100, tid=1, floor=2, reply_num=0)
    first_page = make_posts_response([post], has_more=True, current_page=1, total_page=2)
    empty_page = make_posts_response([], has_more=False, current_page=2, total_page=2)
    tb_client = SimpleNamespace(get_posts_dto=AsyncMock(side_effect=[first_page, empty_page]))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={post.pid}),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
        remove_pending_thread_scan=AsyncMock(),
        add_pending_comment_scan=AsyncMock(),
    )
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    await handler.handle(FullScanPostsTask(tid=1, backfill=True))

    datastore.remove_pending_thread_scan.assert_not_awaited()


@pytest.mark.asyncio
async def test_full_scan_comments_scans_later_pages_after_duplicate_ids():
    """已保存评论命中去重时仍须检查后续楼中楼分页。"""
    first = make_comment(cid=1000, pid=100, tid=1)
    later = make_comment(cid=1001, pid=100, tid=1)
    pages = {
        2: make_comments_response([first], has_more=True, current_page=2, total_page=3),
        3: make_comments_response([later], current_page=3, total_page=3),
    }
    first_page = make_comments_response([first], has_more=True, current_page=1, total_page=3)
    tb_client = SimpleNamespace(get_comments_dto=AsyncMock(side_effect=[first_page, pages[2], pages[3]]))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(side_effect=[{first.cid}, set(), set(), {later.cid}, set()]),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
        remove_pending_comment_scan=AsyncMock(),
    )
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    await handler.handle(FullScanCommentsTask(tid=1, pid=100, backfill=True))

    assert tb_client.get_comments_dto.await_args_list == [call(1, 100, 1), call(1, 100, 2), call(1, 100, 3)]
    datastore.remove_pending_comment_scan.assert_awaited_once_with(1, 100)


@pytest.mark.asyncio
async def test_full_scan_comments_keeps_pending_after_unexpected_empty_page():
    """非终态范围内的空页必须保持楼中楼任务待处理。"""
    comment = make_comment(cid=1000, pid=100, tid=1)
    first_page = make_comments_response([comment], has_more=True, current_page=1, total_page=2)
    empty_page = make_comments_response([], has_more=True, current_page=2, total_page=2)
    tb_client = SimpleNamespace(get_comments_dto=AsyncMock(side_effect=[first_page, empty_page]))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={comment.cid}),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
        remove_pending_comment_scan=AsyncMock(),
    )
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    await handler.handle(FullScanCommentsTask(tid=1, pid=100, backfill=True))

    datastore.remove_pending_comment_scan.assert_not_awaited()


@pytest.mark.asyncio
async def test_full_scan_posts_checkpoints_each_page_and_scans_first_post_comments():
    first_post = make_post(pid=100, tid=1, floor=1)
    reply = make_post(pid=101, tid=1, floor=2)
    page1 = make_posts_response([first_post, reply], has_more=True, current_page=1, total_page=2)
    page2 = make_posts_response([reply], current_page=2, total_page=2)
    raw = SimpleNamespace(thread=SimpleNamespace(pid=100, tid=1, fid=10, fname="bar"))
    research_pages = [
        ResearchPage(raw, dto, datetime.now(UTC), "aiotieba.get_posts")
        for dto in (page1, page2)
    ]
    tb_client = SimpleNamespace(get_posts_research=AsyncMock(side_effect=research_pages))
    events = []
    datastore = SimpleNamespace(
        get_research_resume_state=AsyncMock(return_value=(1, None, None)),
        record_research_checkpoint=AsyncMock(side_effect=lambda **_kwargs: events.append("checkpoint")),
        filter_new_ids=AsyncMock(return_value=set()),
        save_items=AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("base_write")),
        push_object_event=AsyncMock(),
        add_pending_comment_scan=AsyncMock(),
        remove_pending_thread_scan=AsyncMock(),
        set_research_run_status_if=AsyncMock(),
        resume_research_run_if_status=AsyncMock(return_value=False),
    )
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=make_research_container(tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler._persist_research_page = AsyncMock(side_effect=lambda _page: events.append("research_write"))

    await handler.handle(FullScanPostsTask(tid=1, backfill=True))

    assert [call.args[0:2] for call in tb_client.get_posts_research.await_args_list] == [(1, 1), (1, 2)]
    assert [entry.kwargs["page_number"] for entry in datastore.record_research_checkpoint.await_args_list] == [1, 2]
    assert [entry.kwargs["next_page"] for entry in datastore.record_research_checkpoint.await_args_list] == [2, None]
    assert events.index("research_write") < events.index("base_write")
    queued = [task.content for task in handler.router.comments_queue._q._queue]
    assert {(task.pid, task.parent_kind) for task in queued} == {(100, "first_post"), (101, "reply_post")}
    datastore.remove_pending_thread_scan.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_first_post_comments_use_floor_one_pid_when_raw_thread_pid_is_zero():
    first_post = make_post(pid=500, tid=9, floor=1, reply_num=20)
    posts_page = make_posts_response([first_post])
    research_response = ResearchPage(
        raw=SimpleNamespace(thread=SimpleNamespace(pid=0, fid=0, fname="")),
        dto=posts_page,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )
    datastore = SimpleNamespace(
        save_items=AsyncMock(),
        add_pending_comment_scan=AsyncMock(),
        get_research_first_post_pids=AsyncMock(return_value=set()),
    )
    router = QueueRouter()
    handler = FullScanPostsTaskHandler(
        0,
        make_research_container(SimpleNamespace()),
        cast("Any", datastore),
        router,
    )

    await handler._process_posts_on_page(9, posts_page, True, research_response=research_response)

    datastore.add_pending_comment_scan.assert_awaited_once_with(
        tid=9,
        pid=500,
        fid=10,
        fname="bar",
        backfill=True,
        task_kind="full",
    )
    task = router.comments_queue._q._queue[0].content
    assert isinstance(task, FullScanCommentsTask)
    assert task.pid == 500
    assert task.parent_kind == "first_post"


@pytest.mark.asyncio
async def test_first_post_comments_fall_back_to_stored_first_pid():
    posts_page = make_posts_response([])
    research_response = ResearchPage(
        raw=SimpleNamespace(thread=SimpleNamespace(pid=0, fid=0, fname="")),
        dto=posts_page,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )
    datastore = SimpleNamespace(
        save_items=AsyncMock(),
        add_pending_comment_scan=AsyncMock(),
        get_research_first_post_pids=AsyncMock(return_value={501}),
    )
    router = QueueRouter()
    handler = FullScanPostsTaskHandler(
        0,
        make_research_container(SimpleNamespace()),
        cast("Any", datastore),
        router,
    )

    await handler._process_posts_on_page(9, posts_page, True, research_response=research_response)

    datastore.get_research_first_post_pids.assert_awaited_once_with(9)
    datastore.add_pending_comment_scan.assert_awaited_once_with(
        tid=9,
        pid=501,
        fid=10,
        fname="bar",
        backfill=True,
        task_kind="full",
    )


@pytest.mark.asyncio
async def test_managed_posts_page_uses_stored_first_pid_inside_page_transaction(monkeypatch):
    reply = make_post(pid=701, tid=9, floor=2)
    posts_page = SimpleNamespace(
        objs=[reply],
        forum=SimpleNamespace(fid=10, fname="bar"),
        page=SimpleNamespace(has_more=False, total_page=1, total_count=0),
    )
    response = ResearchPage(
        raw=SimpleNamespace(
            thread=SimpleNamespace(pid=0, tid=9, fid=10, fname="bar", reply_num=20),
            objs=[SimpleNamespace(pid=701, tid=9, floor=2)],
        ),
        dto=posts_page,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )
    events = []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def begin(self):
            return self

    class ManagedDataStore:
        def __init__(self):
            self.session = Session()
            self.async_sessionmaker = lambda: self.session

        async def get_research_task_scope_for_update(self, session, run_id, logical_key):
            assert run_id == "synthetic-run"
            assert logical_key == "thread_replies:9"
            assert session is self.session
            events.append("parent_locked")
            return SimpleNamespace(generation=4, status="running")

        async def get_research_first_post_pids(self, tid, *, session=None):
            assert tid == 9
            assert session is self.session
            events.append("stored_first_pid_lookup")
            return {501}

        async def save_items_in_session(self, session, _items):
            assert session is self.session

        async def ensure_research_task_scope(self, *, session, logical_task_key, parent_generation, **kwargs):
            assert session is self.session
            assert parent_generation == 4
            events.append(("ensure_child", logical_task_key, kwargs["parent_kind"]))
            return SimpleNamespace(generation=0, status="pending")

        async def add_pending_comment_scan_in_session(self, session, tid, pid, **_kwargs):
            assert session is self.session
            events.append(("pending_child", tid, pid))

        async def record_research_checkpoint(self, **values):
            events.append(("checkpoint", values))
            self.checkpoint = values
            return True

        async def remove_pending_thread_scan_in_session(self, session, tid):
            assert session is self.session
            assert tid == 9

    async def persist(_maker, _page, *, session):
        assert session is datastore.session
        events.append("research_page_persisted")

    monkeypatch.setattr("src.research.persistence.persist_research_page", persist)
    datastore = ManagedDataStore()
    router = QueueRouter(maxsize=3)
    handler = FullScanPostsTaskHandler(
        0,
        make_research_container(SimpleNamespace()),
        cast("Any", datastore),
        router,
    )
    handler.ensure_users = AsyncMock()
    handler._prepare_research_page = AsyncMock()
    task = FullScanPostsTask(tid=9, run_id="synthetic-run", generation=4, fid=10, fname="bar", backfill=True)

    committed, children = await handler._commit_research_posts_page(
        task,
        posts_page,
        response,
        page_number=1,
        cursor={},
        attempts=1,
    )

    assert committed is True
    assert events.index("parent_locked") < events.index("stored_first_pid_lookup")
    assert ("ensure_child", "post_comments:9:501", "first_post") in events
    assert ("pending_child", 9, 501) in events
    assert datastore.checkpoint["reported_count_missing_reason"] == "not_provided_by_aiotieba_posts_page"
    first_post_task = next(child for child in children if child.parent_kind == "first_post")
    assert first_post_task.pid == 501
    assert first_post_task.parent_generation == 4
    assert len(router.comments_queue._q._queue) == 2


@pytest.mark.asyncio
async def test_research_posts_empty_page_is_incomplete_when_raw_page_has_no_total_count():
    posts_page = make_posts_response([])
    raw_page = SimpleNamespace(has_more=False, total_page=1)
    response = ResearchPage(
        raw=SimpleNamespace(
            thread=SimpleNamespace(pid=501, tid=9, fid=10, fname="bar", reply_num=20),
            objs=[],
            page=raw_page,
        ),
        dto=posts_page,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )
    assert not hasattr(raw_page, "total_count")

    container = make_research_container(SimpleNamespace())
    container.lock_manager = SimpleNamespace(acquire=AsyncMock(return_value=True), release=AsyncMock())
    datastore = SimpleNamespace(
        get_research_task_scope_state=AsyncMock(return_value=(0, "running", None)),
        get_research_resume_state=AsyncMock(return_value=(1, "running", {})),
        record_research_checkpoint=AsyncMock(return_value=True),
        set_research_run_status_if=AsyncMock(return_value=True),
        remove_pending_thread_scan=AsyncMock(),
    )
    handler = FullScanPostsTaskHandler(0, container, cast("Any", datastore), QueueRouter())
    handler.get_posts = AsyncMock(return_value=FetchedPage(response, attempts=1))

    await handler.handle(
        FullScanPostsTask(tid=9, run_id="synthetic-run", generation=0, backfill=True, fid=10, fname="bar")
    )

    checkpoint = datastore.record_research_checkpoint.await_args.kwargs
    assert checkpoint["status"] == "failed"
    assert checkpoint["scope_status"] == "incomplete"
    assert checkpoint["error_type"] == "PageIntegrityError"
    datastore.remove_pending_thread_scan.assert_not_awaited()


@pytest.mark.asyncio
async def test_thread_growth_invalidates_terminal_checkpoint_before_full_scan_request():
    thread = make_thread(tid=9, fname="bar", reply_num=3)
    old_thread = SimpleNamespace(tid=9, reply_num=2, last_time=thread.last_time)
    state = {"terminal": True, "pending": False}
    requested: list[tuple[int, int]] = []

    async def save_threads_and_pending_scans(_threads, **kwargs):
        assert kwargs["refresh_research_run_id"] == "synthetic-run"
        assert kwargs["upsert_threads"] is True
        state["terminal"] = False
        state["pending"] = True

    async def get_resume_state(_run_id, _task_key):
        return (None, "completed", {}) if state["terminal"] else (1, None, None)

    async def record_checkpoint(**kwargs):
        state["terminal"] = kwargs["status"] == "completed" and kwargs["has_more"] is False

    datastore = SimpleNamespace(
        get_threads_by_tids=AsyncMock(return_value=[old_thread]),
        get_pending_thread_scan_tids=AsyncMock(return_value=set()),
        needs_research_thread_detail=AsyncMock(return_value=set()),
        save_threads_and_pending_scans=AsyncMock(side_effect=save_threads_and_pending_scans),
        get_research_resume_state=AsyncMock(side_effect=get_resume_state),
        record_research_checkpoint=AsyncMock(side_effect=record_checkpoint),
        remove_pending_thread_scan=AsyncMock(side_effect=lambda _tid: state.update(pending=False)),
        resume_research_run_if_status=AsyncMock(return_value=False),
    )
    router = QueueRouter()
    container = make_research_container(SimpleNamespace())
    list_handler = ThreadsTaskHandler(0, container, cast("Any", datastore), router)
    await list_handler._process_old_threads(make_threads_response([thread]), {9}, backfill=True)
    content = router.posts_queue._q._queue[0].content
    assert isinstance(content, FullScanPostsTask)

    posts_page = make_posts_response([make_post(pid=901, tid=9, floor=2)])
    research_page = ResearchPage(
        raw=SimpleNamespace(thread=SimpleNamespace(tid=9, pid=0)),
        dto=posts_page,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )

    async def get_posts(tid, pn, **_kwargs):
        requested.append((tid, pn))
        return research_page

    container.tb_client.get_posts_research = AsyncMock(side_effect=get_posts)
    posts_handler = FullScanPostsTaskHandler(0, container, cast("Any", datastore), router)
    posts_handler._persist_research_page = AsyncMock()
    posts_handler._process_posts_on_page = AsyncMock()

    await posts_handler.handle(content)

    assert requested == [(9, 1)]
    datastore.save_threads_and_pending_scans.assert_awaited_once()
    assert state["pending"] is False


@pytest.mark.asyncio
async def test_comment_growth_refreshes_completed_scope_and_resumes_after_interruption():
    task_key = "post_comments:9:500"

    class RefreshDataStore:
        def __init__(self):
            self.pages = {
                1: {
                    "status": "completed",
                    "scope_status": "completed",
                    "has_more": False,
                    "next_page": None,
                    "continuation_json": {},
                }
            }
            self.pending = False

        async def get_posts_by_pids(self, _pids):
            return [SimpleNamespace(pid=500, reply_num=1)]

        async def add_pending_comment_scan(self, **kwargs):
            assert kwargs["refresh_research_run_id"] == "synthetic-run"
            self.pages.clear()
            self.pending = True

        async def save_items(self, *_args, **_kwargs):
            return None

        async def get_research_resume_state(self, _run_id, _task_key):
            if not self.pages:
                return 1, None, None
            page_number = max(self.pages)
            row = self.pages[page_number]
            if row["status"] == "completed" and row["has_more"]:
                return row["next_page"], "running", row["continuation_json"]
            if row["status"] == "completed":
                return None, "completed", row["continuation_json"]
            return page_number, row["scope_status"], row["continuation_json"]

        async def record_research_checkpoint(self, **kwargs):
            self.pages[kwargs["page_number"]] = kwargs

        async def filter_new_ids(self, *_args):
            return set()

        async def remove_pending_comment_scan(self, _tid, _pid):
            self.pending = False

        async def set_research_run_status_if(self, *_args):
            return True

    datastore = RefreshDataStore()
    assert await datastore.get_research_resume_state("synthetic-run", task_key) == (
        None,
        "completed",
        {},
    )
    router = QueueRouter()
    container = make_research_container(SimpleNamespace())
    posts_handler = IncrementalScanPostsTaskHandler(0, container, cast("Any", datastore), router)
    updated_post = make_post(pid=500, tid=9, reply_num=2)

    await posts_handler._process_old_posts({500}, [updated_post], backfill=True)

    assert datastore.pending is True
    assert await datastore.get_research_resume_state("synthetic-run", task_key) == (1, None, None)
    task = router.comments_queue._q._queue[0].content
    assert isinstance(task, FullScanCommentsTask)

    calls: list[int] = []
    page1 = make_comments_response(
        [make_comment(cid=7001, pid=500, tid=9)], has_more=True, current_page=1, total_page=2
    )
    page2 = make_comments_response([make_comment(cid=7002, pid=500, tid=9)], current_page=2, total_page=2)
    fail_page2_once = True

    async def get_comments(_tid, _pid, pn=1):
        nonlocal fail_page2_once
        calls.append(pn)
        if pn == 1:
            return SimpleNamespace(response=page1, attempts=1)
        if fail_page2_once:
            fail_page2_once = False
            raise RetriableApiError(11, "synthetic interruption")
        return SimpleNamespace(response=page2, attempts=1)

    comments_handler = FullScanCommentsTaskHandler(0, container, cast("Any", datastore), router)
    comments_handler.get_comments = get_comments
    await comments_handler.handle(task)

    assert datastore.pending is True
    assert datastore.pages[2]["status"] == "failed"

    resumed_handler = FullScanCommentsTaskHandler(0, container, cast("Any", datastore), router)
    resumed_handler.get_comments = get_comments
    await resumed_handler.handle(task)

    assert calls == [1, 2, 2]
    assert datastore.pages[2]["status"] == "completed"
    assert datastore.pending is False


@pytest.mark.asyncio
@pytest.mark.parametrize("old_fails", [False, True])
async def test_inflight_old_comment_worker_cannot_erase_refreshed_child_scope(monkeypatch, old_fails):
    old_request_started = asyncio.Event()
    release_old_request = asyncio.Event()
    new_child_registered = asyncio.Event()
    new_request_started = asyncio.Event()
    release_new_request = asyncio.Event()
    thread = make_thread(tid=9, fname="bar", reply_num=2)
    reply = make_post(pid=500, tid=9, fid=thread.fid, fname=thread.fname, floor=2, reply_num=2)
    old_comment = make_comment(cid=700, tid=9, pid=500)
    old_comment_response = make_comments_response([old_comment], current_page=1, total_page=1)
    refreshed_post_page = make_posts_response([reply], current_page=1, total_page=1)
    refreshed_posts_response = ResearchPage(
        raw=SimpleNamespace(thread=SimpleNamespace(tid=9, pid=0, fid=thread.fid, fname=thread.fname)),
        dto=refreshed_post_page,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def begin(self):
            return self

    class RacingDataStore:
        def __init__(self):
            self.pending_comments = {(9, 500)}
            self.checkpoints = {}
            self.scopes = {
                "thread_replies:9": {"generation": 0, "status": "running", "parent_generation": None},
                "post_comments:9:500": {"generation": 0, "status": "running", "parent_generation": 0},
            }
            self.async_sessionmaker = FakeSession

        async def get_research_resume_state(self, _run_id, _task_key):
            return 1, None, None

        async def get_research_task_scope_state(self, _run_id, logical_key):
            state = self.scopes.get(logical_key)
            return None if state is None else (
                state["generation"],
                state["status"],
                state["parent_generation"],
            )

        async def get_research_task_scope_for_update(self, _session, _run_id, logical_key):
            state = self.scopes.get(logical_key)
            if state is None:
                return None
            return SimpleNamespace(**state)

        async def ensure_research_task_scope(
            self, *, logical_task_key, parent_logical_task_key, parent_generation, **kwargs
        ):
            parent = self.scopes[parent_logical_task_key]
            if parent["generation"] != parent_generation:
                return None
            state = self.scopes.setdefault(
                logical_task_key,
                {"generation": 0, "status": "pending", "parent_generation": parent_generation},
            )
            return SimpleNamespace(**state)

        async def record_research_checkpoint(self, **values):
            logical_key = values["task_key"].rsplit(":g", 1)[0]
            scope = self.scopes.get(logical_key)
            if scope is not None and values.get("generation") is not None:
                if scope["generation"] != values["generation"]:
                    return False
                scope["status"] = values.get("scope_status", values["status"])
            self.checkpoints[(values["task_key"], values["page_number"])] = values
            return True

        async def remove_pending_comment_scan(self, tid, pid, *, run_id=None, generation=None):
            scope = self.scopes[f"post_comments:{tid}:{pid}"]
            if run_id is not None and scope["generation"] != generation:
                return False
            self.pending_comments.discard((tid, pid))
            return True

        async def add_pending_comment_scan_in_session(self, _session, tid, pid, **_kwargs):
            self.pending_comments.add((tid, pid))
            new_child_registered.set()

        async def remove_pending_comment_scan_in_session(self, _session, tid, pid, **_kwargs):
            self.pending_comments.discard((tid, pid))

        async def remove_pending_thread_scan_in_session(self, *_args):
            return None

        async def save_items_in_session(self, *_args, **_kwargs):
            return None

        async def save_threads_and_pending_scans(self, _threads, **kwargs):
            assert kwargs["refresh_research_run_id"] == "synthetic-run"
            self.scopes["thread_replies:9"]["generation"] += 1
            self.scopes["thread_replies:9"]["status"] = "pending"
            self.scopes["post_comments:9:500"]["generation"] += 1
            self.scopes["post_comments:9:500"]["parent_generation"] = 1
            self.scopes["post_comments:9:500"]["status"] = "pending"
            return {9: 1}

        async def get_threads_by_tids(self, _tids):
            return [SimpleNamespace(tid=9, reply_num=1, last_time=thread.last_time)]

        async def get_pending_thread_scan_tids(self, _tids):
            return set()

        async def needs_research_thread_detail(self, _tids):
            return set()

        async def filter_new_ids(self, _item_type, _ids):
            return set()

        async def save_items(self, *_args, **_kwargs):
            return None

        async def remove_pending_thread_scan(self, _tid, **_kwargs):
            return None

        async def resume_research_run_if_status(self, *_args):
            return False

        async def set_research_run_status_if(self, *_args):
            return False

    datastore = RacingDataStore()
    monkeypatch.setattr("src.research.persistence.persist_research_page", AsyncMock(return_value={}))
    router = QueueRouter(maxsize=1)
    container = make_research_container(SimpleNamespace())
    container.async_sessionmaker = FakeSession

    comment_worker = Worker(0, router.comments_queue, container, cast("Any", datastore), router, lane="comments")
    comment_handler = comment_worker.handlers[FullScanCommentsTask]
    request_count = 0

    async def blocked_comment_request(_tid, _pid, _pn=1):
        nonlocal request_count
        request_count += 1
        if request_count == 1:
            old_request_started.set()
            await release_old_request.wait()
            if old_fails:
                raise RuntimeError("synthetic old-generation failure")
            return FetchedPage(response=old_comment_response, attempts=1)
        new_request_started.set()
        await release_new_request.wait()
        return FetchedPage(response=old_comment_response, attempts=1)

    comment_handler.get_comments = blocked_comment_request
    comment_worker_task = asyncio.create_task(comment_worker.run())
    await router.put(
        Task(
            priority=Priority.BACKFILL_POSTS,
            content=FullScanCommentsTask(
                tid=9,
                pid=500,
                backfill=True,
                parent_kind="reply_post",
                run_id="synthetic-run",
                generation=0,
                parent_generation=0,
                fid=thread.fid,
                fname=thread.fname,
            ),
        )
    )

    posts_worker = Worker(1, router.posts_queue, container, cast("Any", datastore), router, lane="posts")
    posts_handler = posts_worker.handlers[FullScanPostsTask]

    async def refreshed_post_request(_tid, _pn, **_kwargs):
        return FetchedPage(response=refreshed_posts_response, attempts=1)

    posts_handler.get_posts = refreshed_post_request
    posts_worker_task = asyncio.create_task(posts_worker.run())

    try:
        await asyncio.wait_for(old_request_started.wait(), timeout=1)

        list_handler = ThreadsTaskHandler(2, container, cast("Any", datastore), router)
        await list_handler._process_old_threads(make_threads_response([thread]), {9}, backfill=True)
        await asyncio.wait_for(new_child_registered.wait(), timeout=1)
        assert datastore.scopes["thread_replies:9"]["generation"] == 1
        assert datastore.scopes["post_comments:9:500"]["generation"] == 1
        assert router.comments_queue.qsize() == 1
        assert (9, 500) in datastore.pending_comments

        release_old_request.set()
        await asyncio.wait_for(new_request_started.wait(), timeout=1)

        assert router.comments_queue.qsize() == 0
        assert (9, 500) in datastore.pending_comments
        assert not any(key[0].startswith("post_comments:") for key in datastore.checkpoints)

    finally:
        release_old_request.set()
        release_new_request.set()
        comment_worker_task.cancel()
        posts_worker_task.cancel()
        await asyncio.gather(comment_worker_task, posts_worker_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_comment_page_scope_mismatch_is_rejected_before_persistence():
    wrong_parent = make_comment(cid=1000, pid=101, tid=1)
    response = make_comments_response([wrong_parent], current_page=1, total_page=1)
    tb_client = SimpleNamespace(get_comments_dto=AsyncMock(return_value=response))
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client),
        datastore=cast("Any", SimpleNamespace()),
        router=QueueRouter(),
    )

    with pytest.raises(PageIntegrityError, match="parent scopes"):
        await handler.get_comments(1, 100, 1)


@pytest.mark.asyncio
async def test_auth_refresh_callback_rate_limit_is_paused_then_bounded_retry_resumes():
    response = make_threads_response([], current_page=1, total_page=1)
    tb_client = SimpleNamespace(
        get_threads_research=AsyncMock(
            side_effect=[
                RetriableApiError(4011, "synthetic auth"),
                RetriableApiError(429, "synthetic rate limit"),
                ResearchPage(SimpleNamespace(objs=[]), response, datetime.now(UTC), "aiotieba.get_threads"),
            ]
        )
    )
    container = make_research_container(tb_client)
    container.config.research_max_api_retries = 2

    async def run_callback(callback, **_kwargs):
        return await callback()

    container.wait_for_auth_refresh = AsyncMock(side_effect=run_callback)
    datastore = SimpleNamespace(
        set_research_run_status_if=AsyncMock(return_value=True),
        resume_research_run_if_status=AsyncMock(return_value=True),
    )
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=container,
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    fetched = await handler.get_threads("bar", pn=1)

    assert fetched.response.dto is response
    assert tb_client.get_threads_research.await_count == 3
    assert container.wait_for_rate_limit.await_count == 1
    assert datastore.set_research_run_status_if.await_args_list[1].args[1:3] == ({"paused_auth"}, "paused_rate_limit")
    datastore.resume_research_run_if_status.assert_any_await("synthetic-run", "paused_rate_limit")


@pytest.mark.asyncio
async def test_exhausted_rate_limit_retries_close_shared_research_network_gate():
    tb_client = SimpleNamespace(
        get_threads_research=AsyncMock(
            side_effect=[
                RetriableApiError(429, "synthetic rate limit"),
                RetriableApiError(429, "synthetic rate limit"),
            ]
        )
    )
    container = make_research_container(tb_client)
    container.config.research_max_api_retries = 2
    datastore = SimpleNamespace(
        set_research_run_status_if=AsyncMock(return_value=True),
        resume_research_run_if_status=AsyncMock(return_value=True),
    )
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=container,
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    with pytest.raises(RetriableApiError) as error:
        await handler.get_threads("bar", pn=1)

    assert error.value.code == 429
    assert tb_client.get_threads_research.await_count == 2
    assert container.wait_for_rate_limit.await_count == 2
    container.block_research_network.assert_awaited_once()
    assert datastore.set_research_run_status_if.await_args_list[-1].args[1:3] == (
        {"running", "paused_rate_limit"},
        "paused_rate_limit",
    )


@pytest.mark.asyncio
async def test_auth_refresh_callback_challenge_blocks_network_without_overwriting_state():
    tb_client = SimpleNamespace(
        get_threads_research=AsyncMock(
            side_effect=[
                RetriableApiError(4011, "synthetic auth"),
                RetriableApiError(220034, "synthetic challenge"),
            ]
        )
    )
    container = make_research_container(tb_client)

    async def run_callback(callback, **_kwargs):
        return await callback()

    container.wait_for_auth_refresh = AsyncMock(side_effect=run_callback)
    datastore = SimpleNamespace(
        set_research_run_status_if=AsyncMock(return_value=True),
        resume_research_run_if_status=AsyncMock(return_value=True),
    )
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=container,
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    with pytest.raises(RetriableApiError) as error:
        await handler.get_threads("bar", pn=1)

    assert error.value.code == 220034
    assert container.block_research_network.await_count == 1
    assert datastore.set_research_run_status_if.await_args_list[-1].args[1:3] == ({"paused_auth"}, "blocked")


@pytest.mark.asyncio
async def test_detail_rate_limit_failure_is_checkpointed_as_paused_not_incomplete():
    datastore = SimpleNamespace(
        record_research_checkpoint=AsyncMock(),
        set_research_run_status_if=AsyncMock(),
    )
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=create_mock_container(SimpleNamespace()),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    await handler._record_detail_failure(
        run_id="synthetic-run",
        task_key="thread_replies:1",
        page_number=2,
        task_type="full_scan_posts",
        scope_type="thread_replies",
        tid=1,
        error=RetriableApiError(429, "synthetic rate limit"),
        continuation_json={"recent_page_fingerprints": [[100]]},
    )

    checkpoint = datastore.record_research_checkpoint.await_args.kwargs
    assert checkpoint["status"] == "paused_rate_limit"
    assert checkpoint["scope_status"] == "paused_rate_limit"
    assert checkpoint["next_page"] == 2
    datastore.set_research_run_status_if.assert_not_awaited()
