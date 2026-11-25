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
    make_user,
)
from tiebameow.client.tieba_client import RetriableApiError
from tiebameow.models.dto import PostUserDTO
from tiebameow.models.orm import Comment as CommentModel
from tiebameow.models.orm import Post as PostModel
from tiebameow.utils.time_utils import SHANGHAI_TZ

from src.research.client import ResearchPage
from src.scraper.router import QueueRouter
from src.scraper.tasks import FullScanCommentsTask, FullScanPostsTask, Priority, ScanThreadsTask, Task
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
    container.config.cooldown_seconds_429 = 5
    container.research_run_id = "synthetic-run"
    container.wait_for_research_ready = AsyncMock()
    container.wait_for_auth_refresh = AsyncMock()
    container.wait_for_rate_limit = AsyncMock()
    container.block_research_network = AsyncMock()
    empty_response_streak = 0

    async def record_research_api_response(*, suspicious_empty):
        nonlocal empty_response_streak
        if not suspicious_empty:
            empty_response_streak = 0
            return False
        empty_response_streak += 1
        if empty_response_streak < 3:
            return False
        empty_response_streak = 0
        return True

    container.record_research_api_response = AsyncMock(side_effect=record_research_api_response)
    return container


def make_research_comment_page(
    *, current_page: int, total_page: int, has_more: bool, total_count: int = 158
) -> ResearchPage:
    dto = make_comments_response(
        [], current_page=current_page, total_page=total_page, has_more=has_more
    )
    dto.page.total_count = total_count
    raw_page = SimpleNamespace(
        current_page=current_page,
        total_page=total_page,
        total_count=total_count,
        has_more=has_more,
    )
    return ResearchPage(
        raw=SimpleNamespace(page=raw_page),
        dto=dto,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_comments",
    )


def make_research_posts_page(
    posts,
    *,
    current_page: int,
    total_page: int,
    has_more: bool,
    tid: int = 1,
    fid: int = 10,
    first_pid: int | None = 1,
) -> ResearchPage:
    dto = make_posts_response(
        posts,
        has_more=has_more,
        current_page=current_page,
        total_page=total_page,
    )
    dto.forum.fid = fid
    raw = SimpleNamespace(
        page=SimpleNamespace(
            current_page=current_page,
            total_page=total_page,
            has_more=has_more,
        ),
        forum=SimpleNamespace(fid=fid),
        thread=SimpleNamespace(fid=fid, tid=tid, pid=first_pid),
    )
    return ResearchPage(
        raw=raw,
        dto=dto,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )


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
async def test_full_scan_posts_bounded_prefetches_nonempty_page_after_false_has_more():
    first_post = make_post(pid=100, tid=1, fid=10, floor=2)
    second_post = make_post(pid=101, tid=1, fid=10, floor=3)
    first_page = make_research_posts_page(
        [first_post],
        current_page=1,
        total_page=2,
        has_more=False,
        first_pid=153591092432,
    )
    second_page = make_research_posts_page(
        [second_post],
        current_page=2,
        total_page=2,
        has_more=False,
        first_pid=154010138191,
    )
    datastore = SimpleNamespace(
        record_research_checkpoint=AsyncMock(return_value=True),
        remove_pending_thread_scan=AsyncMock(),
    )
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=make_research_container(SimpleNamespace()),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler.get_posts = AsyncMock(
        side_effect=[FetchedPage(first_page, 1), FetchedPage(second_page, 1)]
    )
    handler._persist_research_page = AsyncMock()
    handler._prepare_research_page = AsyncMock()
    handler._process_posts_on_page = AsyncMock()

    await handler.handle(FullScanPostsTask(tid=1, fid=10, backfill=True))

    assert [call.args[:2] for call in handler.get_posts.await_args_list] == [(1, 1), (1, 2)]
    checkpoints = [call.kwargs for call in datastore.record_research_checkpoint.await_args_list]
    assert checkpoints[0]["page_number"] == 1
    assert checkpoints[0]["scope_status"] == "running"
    assert checkpoints[0]["has_more"] is True
    assert checkpoints[0]["next_page"] == 2
    assert checkpoints[0]["continuation_json"]["validated_terminal_prefetch"] == {
        "page": 2,
        "original_has_more": False,
        "total_pages": 2,
    }
    assert checkpoints[1]["page_number"] == 2
    assert checkpoints[1]["scope_status"] == "completed"
    assert checkpoints[1]["has_more"] is False
    datastore.remove_pending_thread_scan.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_scope_commit_keeps_bounded_prefetch_evidence(monkeypatch):
    first_post = make_post(pid=100, tid=1, fid=10, floor=1)
    response = make_research_posts_page(
        [first_post], current_page=1, total_page=2, has_more=False
    )
    prefetch_evidence = {
        "page": 2,
        "original_has_more": False,
        "total_pages": 2,
    }

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def begin(self):
            return self

    class DataStore:
        def __init__(self):
            self.session = Session()
            self.async_sessionmaker = lambda: self.session
            self.checkpoints = []

        async def get_research_task_scope_for_update(self, *_args):
            return SimpleNamespace(generation=4, status="running")

        async def get_research_first_post_pids(self, *_args, **_kwargs):
            return {100}

        async def save_items_in_session(self, *_args, **_kwargs):
            return None

        async def ensure_research_task_scope(self, **_kwargs):
            return SimpleNamespace(
                generation=2,
                status="completed",
                parent_generation=4,
                parent_logical_task_key="thread_replies:1",
            )

        async def record_research_checkpoint(self, **values):
            self.checkpoints.append(values)
            return True

        async def remove_pending_thread_scan_in_session(self, *_args):
            return None

    async def persist(_maker, _response, *, session, run_id=None):
        assert session is datastore.session
        assert run_id == "synthetic-run"

    monkeypatch.setattr("src.research.persistence.persist_research_page", persist)
    datastore = DataStore()
    handler = FullScanPostsTaskHandler(
        0,
        make_research_container(SimpleNamespace()),
        cast("Any", datastore),
        QueueRouter(),
    )
    handler.ensure_users = AsyncMock()
    handler._prepare_research_page = AsyncMock()
    handler._enqueue_missing_web_targets_after_commit = AsyncMock()

    committed, _children = await handler._commit_research_posts_page(
        FullScanPostsTask(tid=1, run_id="synthetic-run", generation=4, fid=10, backfill=True),
        response.dto,
        response,
        page_number=1,
        cursor={"validated_terminal_prefetch": prefetch_evidence},
        attempts=1,
        effective_has_more=True,
    )

    assert committed is True
    parent_checkpoint = next(
        checkpoint for checkpoint in datastore.checkpoints if checkpoint["scope_type"] == "thread_replies"
    )
    assert parent_checkpoint["scope_status"] == "running"
    assert parent_checkpoint["has_more"] is True
    assert parent_checkpoint["next_page"] == 2
    assert parent_checkpoint["continuation_json"]["validated_terminal_prefetch"] == prefetch_evidence


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("next_page", "reason"),
    [
        (
            make_research_posts_page(
                [make_post(pid=101, tid=1, fid=10)],
                current_page=2,
                total_page=2,
                has_more=False,
                fid=11,
            ),
            "forum identity conflict",
        ),
        (
            make_research_posts_page(
                [make_post(pid=101, tid=1, fid=10)],
                current_page=2,
                total_page=2,
                has_more=False,
                tid=2,
            ),
            "thread identity conflict",
        ),
        (
            make_research_posts_page(
                [make_post(pid=101, tid=1, fid=10)],
                current_page=2,
                total_page=3,
                has_more=False,
            ),
            "page total changed",
        ),
    ],
)
async def test_full_scan_posts_rejects_unverified_bounded_prefetch(next_page, reason):
    first_page = make_research_posts_page(
        [make_post(pid=100, tid=1, fid=10)],
        current_page=1,
        total_page=2,
        has_more=False,
    )
    datastore = SimpleNamespace(
        record_research_checkpoint=AsyncMock(return_value=True),
        set_research_run_status_if=AsyncMock(),
    )
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=make_research_container(SimpleNamespace()),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler.get_posts = AsyncMock(
        side_effect=[FetchedPage(first_page, 1), FetchedPage(next_page, 1)]
    )
    handler._persist_research_page = AsyncMock()
    handler._prepare_research_page = AsyncMock()
    handler._process_posts_on_page = AsyncMock()

    await handler.handle(FullScanPostsTask(tid=1, fid=10, backfill=True))

    assert reason
    assert datastore.record_research_checkpoint.await_args.kwargs["status"] == "failed"
    assert datastore.record_research_checkpoint.await_args.kwargs["scope_status"] == "incomplete"
    assert datastore.record_research_checkpoint.await_args.kwargs["error_type"] == "PageIntegrityError"


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
async def test_research_comment_empty_page_accepts_confirmed_deleted_tail_metadata():
    pages = {
        5: make_research_comment_page(current_page=5, total_page=6, has_more=True),
        6: make_research_comment_page(current_page=6, total_page=6, has_more=False),
    }
    tb_client = SimpleNamespace(
        get_comments_research=AsyncMock(side_effect=lambda _tid, _pid, pn: pages[pn])
    )
    datastore = SimpleNamespace(resume_research_run_if_status=AsyncMock())
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=make_research_container(tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    middle = await handler.get_comments(1, 100, 5)
    tail = await handler.get_comments(1, 100, 6)

    assert middle.response.dto.page.current_page == 5
    assert middle.response.dto.page.has_more is True
    assert tail.response.dto.page.current_page == 6
    assert tail.response.dto.page.has_more is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "page",
    [
        make_research_comment_page(current_page=0, total_page=0, has_more=False),
        make_research_comment_page(current_page=5, total_page=0, has_more=True),
        make_research_comment_page(current_page=5, total_page=6, has_more=False),
        make_research_comment_page(current_page=5, total_page=5, has_more=True),
    ],
    ids=["zero-page", "missing-total-pages", "terminal-before-reported-end", "more-beyond-reported-end"],
)
async def test_research_comment_empty_page_rejects_untrusted_or_inconsistent_metadata(page):
    tb_client = SimpleNamespace(get_comments_research=AsyncMock(return_value=page))
    datastore = SimpleNamespace(resume_research_run_if_status=AsyncMock())
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=make_research_container(tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    with pytest.raises(PageIntegrityError):
        await handler.get_comments(1, 100, 5)


@pytest.mark.asyncio
async def test_full_scan_comments_continues_from_empty_intermediate_page_to_confirmed_tail():
    pages = {
        5: make_research_comment_page(current_page=5, total_page=6, has_more=True),
        6: make_research_comment_page(current_page=6, total_page=6, has_more=False),
    }
    tb_client = SimpleNamespace(
        get_comments_research=AsyncMock(side_effect=lambda _tid, _pid, pn: pages[pn])
    )
    datastore = SimpleNamespace(
        resume_research_run_if_status=AsyncMock(),
        record_research_checkpoint=AsyncMock(return_value=True),
        set_research_run_status_if=AsyncMock(),
        remove_pending_comment_scan=AsyncMock(),
    )
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=make_research_container(tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler._persist_research_page = AsyncMock()
    handler._process_comments_on_page = AsyncMock()

    await handler.handle(
        FullScanCommentsTask(
            tid=1,
            pid=100,
            start_page=5,
            run_id="synthetic-run",
            fid=10,
        )
    )

    assert tb_client.get_comments_research.await_args_list == [call(1, 100, 5), call(1, 100, 6)]
    page_checkpoints = datastore.record_research_checkpoint.await_args_list
    assert [
        (call.kwargs["page_number"], call.kwargs["scope_status"], call.kwargs["has_more"])
        for call in page_checkpoints
    ] == [
        (5, "running", True),
        (6, "completed", False),
    ]
    assert page_checkpoints[0].kwargs["next_page"] == 6
    assert page_checkpoints[1].kwargs["page_item_count"] == 0
    assert page_checkpoints[1].kwargs["continuation_json"]["empty_tail_reason"] == (
        "server_confirmed_empty_terminal_page"
    )
    datastore.remove_pending_comment_scan.assert_awaited_once_with(
        1, 100
    )


@pytest.mark.asyncio
async def test_scope_managed_comment_scan_continues_after_empty_intermediate_page():
    pages = {
        5: make_research_comment_page(current_page=5, total_page=6, has_more=True),
        6: make_research_comment_page(current_page=6, total_page=6, has_more=False),
    }
    tb_client = SimpleNamespace(
        get_comments_research=AsyncMock(side_effect=lambda _tid, _pid, pn: pages[pn])
    )
    datastore = SimpleNamespace(
        get_research_task_scope_state=AsyncMock(
            side_effect=[(6, "running", 4), (4, "completed", None)]
        ),
        claim_research_comments_scope=AsyncMock(return_value=True),
        resume_research_run_if_status=AsyncMock(),
        remove_pending_comment_scan=AsyncMock(),
    )
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=make_research_container(tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler._commit_research_comments_page = AsyncMock(return_value=True)

    await handler.handle(
        FullScanCommentsTask(
            tid=1,
            pid=100,
            start_page=5,
            run_id="synthetic-run",
            generation=6,
            parent_generation=4,
            fid=10,
        )
    )

    assert tb_client.get_comments_research.await_args_list == [call(1, 100, 5), call(1, 100, 6)]
    commits = handler._commit_research_comments_page.await_args_list
    assert [commit.kwargs["page_number"] for commit in commits] == [5, 6]
    assert commits[0].kwargs["cursor"]["empty_comment_page_total_limit"] == 6
    assert commits[0].kwargs["cursor"]["empty_comment_page_count"] == 1
    assert commits[1].kwargs["cursor"]["empty_tail_reason"] == (
        "server_confirmed_empty_terminal_page"
    )


@pytest.mark.asyncio
async def test_empty_comment_page_bound_survives_resume_when_total_pages_grow():
    page = make_research_comment_page(current_page=6, total_page=7, has_more=True)
    tb_client = SimpleNamespace(get_comments_research=AsyncMock(return_value=page))
    datastore = SimpleNamespace(
        get_research_resume_state=AsyncMock(
            return_value=(
                6,
                "running",
                {
                    "empty_comment_page_total_limit": 6,
                    "empty_comment_page_count": 1,
                    "recent_page_fingerprints": [[]],
                },
            )
        ),
        resume_research_run_if_status=AsyncMock(),
        record_research_checkpoint=AsyncMock(return_value=True),
        set_research_run_status_if=AsyncMock(),
        remove_pending_comment_scan=AsyncMock(),
    )
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=make_research_container(tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler._persist_research_page = AsyncMock()

    await handler.handle(
        FullScanCommentsTask(tid=1, pid=100, run_id="synthetic-run", fid=10)
    )

    tb_client.get_comments_research.assert_awaited_once_with(1, 100, 6)
    failure = datastore.record_research_checkpoint.await_args.kwargs
    assert failure["page_number"] == 6
    assert failure["scope_status"] == "incomplete"
    assert failure["error_type"] == "PageIntegrityError"
    datastore.remove_pending_comment_scan.assert_not_awaited()


@pytest.mark.asyncio
async def test_comment_scan_stops_after_ten_consecutive_empty_pages():
    tb_client = SimpleNamespace(
        get_comments_research=AsyncMock(
            side_effect=lambda _tid, _pid, pn: make_research_comment_page(
                current_page=pn, total_page=12, has_more=pn < 12
            )
        )
    )
    datastore = SimpleNamespace(
        resume_research_run_if_status=AsyncMock(),
        record_research_checkpoint=AsyncMock(return_value=True),
        set_research_run_status_if=AsyncMock(),
        remove_pending_comment_scan=AsyncMock(),
    )
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=make_research_container(tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler._persist_research_page = AsyncMock()

    await handler.handle(
        FullScanCommentsTask(tid=1, pid=100, start_page=2, run_id="synthetic-run", fid=10)
    )

    assert [call.args[2] for call in tb_client.get_comments_research.await_args_list] == list(range(2, 13))
    assert datastore.record_research_checkpoint.await_args.kwargs["page_number"] == 12
    assert datastore.record_research_checkpoint.await_args.kwargs["scope_status"] == "incomplete"
    datastore.remove_pending_comment_scan.assert_not_awaited()


@pytest.mark.asyncio
async def test_scope_managed_empty_comment_commit_records_intermediate_then_terminal_page(monkeypatch):
    from src.research import persistence

    async def persist_research_page(*_args, **_kwargs):
        return None

    monkeypatch.setattr(persistence, "persist_research_page", persist_research_page)
    parent_scope = SimpleNamespace(generation=4)
    child_scope = SimpleNamespace(generation=6, parent_generation=4)

    class Session:
        def begin(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class SessionMaker:
        def __call__(self):
            return Session()

    async def get_scope(_session, _run_id, logical_key):
        return parent_scope if logical_key == "thread_replies:1" else child_scope

    datastore = SimpleNamespace(
        async_sessionmaker=SessionMaker(),
        get_research_task_scope_for_update=AsyncMock(side_effect=get_scope),
        save_items_in_session=AsyncMock(),
        record_research_checkpoint=AsyncMock(return_value=True),
        remove_pending_comment_scan_in_session=AsyncMock(),
    )
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=make_research_container(SimpleNamespace()),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler.ensure_users = AsyncMock()
    handler._prepare_research_page = AsyncMock()
    handler._enqueue_missing_web_targets_after_commit = AsyncMock()
    task = FullScanCommentsTask(
        tid=1,
        pid=100,
        run_id="synthetic-run",
        generation=6,
        parent_generation=4,
        parent_kind="reply_post",
        fid=10,
    )

    for page_number, has_more in ((5, True), (6, False)):
        response = make_research_comment_page(
            current_page=page_number, total_page=6, has_more=has_more
        )
        assert await handler._commit_research_comments_page(
            task,
            response.dto,
            response,
            page_number=page_number,
            cursor={
                "empty_comment_page_total_limit": 6,
                "empty_comment_page_count": 1,
            }
            if has_more
            else {"empty_tail_reason": "server_confirmed_empty_terminal_page"},
            attempts=1,
        )

    checkpoints = datastore.record_research_checkpoint.await_args_list
    assert [item.kwargs["scope_status"] for item in checkpoints] == ["running", "completed"]
    assert [item.kwargs["has_more"] for item in checkpoints] == [True, False]
    assert [item.kwargs["next_page"] for item in checkpoints] == [6, None]
    assert checkpoints[0].kwargs["continuation_json"]["empty_comment_page_total_limit"] == 6
    assert checkpoints[1].kwargs["continuation_json"]["empty_tail_reason"] == (
        "server_confirmed_empty_terminal_page"
    )
    datastore.remove_pending_comment_scan_in_session.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatcher_snapshot_skips_scope_that_became_incomplete_before_lock():
    states = {
        "post_comments:9:100": iter([(6, "pending", 4), (6, "incomplete", 4)]),
        "thread_replies:9": iter([(4, "running", None), (4, "running", None)]),
    }

    async def get_scope_state(_run_id, logical_key):
        return next(states[logical_key])

    datastore = SimpleNamespace(
        get_research_task_scope_state=AsyncMock(side_effect=get_scope_state),
        get_research_resume_state=AsyncMock(return_value=(1, "incomplete", None)),
        claim_research_comments_scope=AsyncMock(return_value=False),
    )
    container = make_research_container(SimpleNamespace())
    handler = FullScanCommentsTaskHandler(0, container, cast("Any", datastore), QueueRouter())
    handler.get_comments = AsyncMock()

    await handler.handle(
        FullScanCommentsTask(
            tid=9,
            pid=100,
            run_id="synthetic-run",
            generation=6,
            parent_generation=4,
            pending_only=True,
        )
    )

    handler.get_comments.assert_not_awaited()
    assert datastore.claim_research_comments_scope.await_args.kwargs["pending_only"] is True
    container.lock_manager.release.assert_awaited_once()


@pytest.mark.asyncio
async def test_explicit_recovery_can_resume_incomplete_comment_scope():
    datastore = SimpleNamespace(
        get_research_task_scope_state=AsyncMock(
            side_effect=[(6, "incomplete", 4), (4, "running", None)]
        ),
        get_research_resume_state=AsyncMock(return_value=(1, "incomplete", None)),
        claim_research_comments_scope=AsyncMock(return_value=True),
    )
    container = make_research_container(SimpleNamespace())
    handler = FullScanCommentsTaskHandler(0, container, cast("Any", datastore), QueueRouter())
    handler.get_comments = AsyncMock(side_effect=asyncio.CancelledError)

    with pytest.raises(asyncio.CancelledError):
        await handler.handle(
            FullScanCommentsTask(tid=9, pid=100, run_id="synthetic-run", generation=6, parent_generation=4)
        )

    handler.get_comments.assert_awaited_once_with(9, 100, 1)
    assert datastore.claim_research_comments_scope.await_args.kwargs["pending_only"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_failed_comment_identity_parent_zero_revalidation_is_explicitly_opt_in(enabled):
    post = make_post(pid=101, tid=9, fid=10, floor=2, reply_num=0)
    evidence = {
        "tid": 9,
        "pid": post.pid,
        "fid": 10,
        "author_user_id": post.author_id,
        "reported_count": 0,
        "count_field_present": True,
        "source_api": "aiotieba.get_posts",
        "evidence_kind": "protobuf_wire_explicit_zero",
        "source_page": 1,
        "count_field_path": "data.post_list.sub_post_number",
        "count_wire_field_number": 13,
    }
    response = ResearchPage(
        raw=SimpleNamespace(),
        dto=make_posts_response([post], current_page=1),
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
        zero_comment_evidence={post.pid: evidence},
    )
    datastore = SimpleNamespace(
        get_research_task_scope_state=AsyncMock(side_effect=[(3, "incomplete", 2), (2, "completed", None)]),
        get_research_resume_state=AsyncMock(return_value=(1, "incomplete", None)),
        claim_research_comments_scope=AsyncMock(return_value=True),
        get_research_comment_parent_page_hint=AsyncMock(return_value=(2, 30)),
        complete_failed_comment_scope_from_parent_zero=AsyncMock(return_value=True),
        record_research_checkpoint=AsyncMock(),
        set_research_run_status_if=AsyncMock(),
    )
    container = make_research_container(SimpleNamespace())
    container.revalidate_failed_comment_page_zero = enabled
    handler = FullScanCommentsTaskHandler(0, container, cast("Any", datastore), QueueRouter())
    handler.get_comments = AsyncMock(
        side_effect=PageIntegrityError(
            "Comment detail empty response is missing or has zero page identity for page 1."
        )
    )
    handler.get_posts = AsyncMock(return_value=FetchedPage(response=response, attempts=1))

    await handler.handle(
        FullScanCommentsTask(
            tid=9,
            pid=101,
            run_id="synthetic-run",
            generation=3,
            parent_generation=2,
            fid=10,
        )
    )

    if enabled:
        handler.get_posts.assert_awaited_once_with(9, 1, rn=30, with_comments=False)
        datastore.get_research_comment_parent_page_hint.assert_awaited_once_with(
            "synthetic-run", "post_comments:9:101:g3", 9, 101, 3, 2
        )
        completion = datastore.complete_failed_comment_scope_from_parent_zero.await_args.kwargs
        assert completion["evidence"] == evidence
        assert completion["current_failure"] == {
            "error_type": "PageIntegrityError",
            "reason": "Comment detail empty response is missing or has zero page identity for page 1.",
        }
        datastore.record_research_checkpoint.assert_not_awaited()
    else:
        handler.get_posts.assert_not_awaited()
        datastore.get_research_comment_parent_page_hint.assert_not_awaited()
        datastore.complete_failed_comment_scope_from_parent_zero.assert_not_awaited()
        assert datastore.record_research_checkpoint.await_args.kwargs["status"] == "failed"


@pytest.mark.asyncio
async def test_failed_comment_non_terminal_empty_page_one_triggers_parent_zero_revalidation():
    post = make_post(pid=101, tid=9, fid=10, floor=2, reply_num=0)
    evidence = {
        "tid": 9,
        "pid": post.pid,
        "fid": 10,
        "author_user_id": post.author_id,
        "reported_count": 0,
        "count_field_present": True,
        "source_api": "aiotieba.get_posts",
        "evidence_kind": "protobuf_wire_explicit_zero",
        "source_page": 1,
        "count_field_path": "data.post_list.sub_post_number",
        "count_wire_field_number": 13,
    }
    post_response = ResearchPage(
        raw=SimpleNamespace(),
        dto=make_posts_response([post], current_page=1),
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
        zero_comment_evidence={post.pid: evidence},
    )
    empty_dto = make_comments_response([], current_page=1, total_page=1, has_more=False)
    empty_dto.page.total_count = 2
    empty_comment_response = ResearchPage(
        raw=SimpleNamespace(
            page=SimpleNamespace(current_page=1, total_page=1, total_count=2, has_more=False),
        ),
        dto=empty_dto,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_comments",
    )
    client = SimpleNamespace(
        get_comments_research=AsyncMock(return_value=empty_comment_response),
        get_posts_research=AsyncMock(return_value=post_response),
    )
    datastore = SimpleNamespace(
        get_research_task_scope_state=AsyncMock(side_effect=[(3, "incomplete", 2), (2, "completed", None)]),
        get_research_resume_state=AsyncMock(return_value=(1, "incomplete", None)),
        claim_research_comments_scope=AsyncMock(return_value=True),
        get_research_comment_parent_page_hint=AsyncMock(return_value=(2, 30)),
        complete_failed_comment_scope_from_parent_zero=AsyncMock(return_value=True),
        record_research_checkpoint=AsyncMock(),
        resume_research_run_if_status=AsyncMock(),
        set_research_run_status_if=AsyncMock(),
    )
    container = make_research_container(client)
    container.revalidate_failed_comment_page_zero = True
    handler = FullScanCommentsTaskHandler(0, container, cast("Any", datastore), QueueRouter())

    await handler.handle(
        FullScanCommentsTask(
            tid=9,
            pid=101,
            run_id="synthetic-run",
            generation=3,
            parent_generation=2,
            fid=10,
        )
    )

    client.get_comments_research.assert_awaited_once_with(9, 101, 1)
    client.get_posts_research.assert_awaited_once_with(9, 1, rn=30, with_comments=False)
    completion = datastore.complete_failed_comment_scope_from_parent_zero.await_args.kwargs
    assert completion["evidence"] == evidence
    assert completion["current_failure"] == {
        "error_type": "PageIntegrityError",
        "reason": "Comment detail empty response is not a confirmed terminal page for page 1.",
    }
    datastore.record_research_checkpoint.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_proof",
    ["missing_target", "missing_wire_presence", "nonzero", "wrong_pid", "wrong_page"],
)
async def test_failed_comment_identity_revalidation_rejects_unproven_parent_page(invalid_proof):
    post = make_post(pid=101, tid=9, fid=10, floor=2, reply_num=5 if invalid_proof == "nonzero" else 0)
    evidence = {
        "tid": 9,
        "pid": 999 if invalid_proof == "wrong_pid" else post.pid,
        "fid": 10,
        "author_user_id": post.author_id,
        "reported_count": 5 if invalid_proof == "nonzero" else 0,
        "count_field_present": invalid_proof != "missing_wire_presence",
        "source_api": "aiotieba.get_posts",
        "evidence_kind": "protobuf_wire_explicit_zero",
        "source_page": 1,
        "count_field_path": "data.post_list.sub_post_number",
        "count_wire_field_number": 13,
    }
    posts = [] if invalid_proof == "missing_target" else [post]
    current_page = 2 if invalid_proof == "wrong_page" else 1
    response = ResearchPage(
        raw=SimpleNamespace(),
        dto=make_posts_response(posts, current_page=current_page),
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
        zero_comment_evidence={post.pid: evidence},
    )
    datastore = SimpleNamespace(
        get_research_comment_parent_page_hint=AsyncMock(return_value=(2, 30)),
        complete_failed_comment_scope_from_parent_zero=AsyncMock(return_value=True),
    )
    container = make_research_container(SimpleNamespace())
    container.revalidate_failed_comment_page_zero = True
    handler = FullScanCommentsTaskHandler(0, container, cast("Any", datastore), QueueRouter())
    handler.get_posts = AsyncMock(return_value=FetchedPage(response=response, attempts=1))
    task = FullScanCommentsTask(
        tid=9,
        pid=101,
        run_id="synthetic-run",
        generation=3,
        parent_generation=2,
        fid=10,
    )

    completed = await handler._revalidate_failed_comment_page_from_parent_zero(
        task,
        PageIntegrityError("Comment detail empty response is missing or has zero page identity for page 1."),
        run_id="synthetic-run",
        task_key="post_comments:9:101:g3",
    )

    assert completed is False
    handler.get_posts.assert_awaited_once_with(9, 1, rn=30, with_comments=False)
    datastore.complete_failed_comment_scope_from_parent_zero.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_comment_revalidation_uses_dense_hint_on_shifted_thread():
    post = make_post(pid=101, tid=9, fid=10, floor=443, reply_num=0)
    evidence = {
        "tid": 9,
        "pid": post.pid,
        "fid": 10,
        "author_user_id": post.author_id,
        "reported_count": 0,
        "count_field_present": True,
        "source_api": "aiotieba.get_posts",
        "evidence_kind": "protobuf_wire_explicit_zero",
        "source_page": 9,
        "count_field_path": "data.post_list.sub_post_number",
        "count_wire_field_number": 13,
    }
    response = ResearchPage(
        raw=SimpleNamespace(),
        dto=make_posts_response([post], current_page=9, total_page=10),
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
        zero_comment_evidence={post.pid: evidence},
    )
    datastore = SimpleNamespace(
        get_research_comment_parent_page_hint=AsyncMock(return_value=(443, 30)),
        get_research_comment_parent_dense_page_hint=AsyncMock(return_value=9),
        complete_failed_comment_scope_from_parent_zero=AsyncMock(return_value=True),
    )
    container = make_research_container(SimpleNamespace())
    container.revalidate_failed_comment_page_zero = True
    handler = FullScanCommentsTaskHandler(0, container, cast("Any", datastore), QueueRouter())
    handler.get_posts = AsyncMock(return_value=FetchedPage(response=response, attempts=1))
    task = FullScanCommentsTask(
        tid=9,
        pid=101,
        run_id="synthetic-run",
        generation=3,
        parent_generation=2,
        fid=10,
    )

    completed = await handler._revalidate_failed_comment_page_from_parent_zero(
        task,
        PageIntegrityError("Comment detail empty response is missing or has zero page identity for page 1."),
        run_id="synthetic-run",
        task_key="post_comments:9:101:g3",
    )

    assert completed is True
    datastore.get_research_comment_parent_dense_page_hint.assert_awaited_once_with(9, 443, 30)
    handler.get_posts.assert_awaited_once_with(9, 9, rn=30, with_comments=False)
    completion = datastore.complete_failed_comment_scope_from_parent_zero.await_args.kwargs
    assert completion["parent_page"] == 9
    assert completion["parent_floor"] == 443
    assert completion["evidence"] == evidence


@pytest.mark.asyncio
async def test_failed_comment_revalidation_binary_searches_shifted_pages_and_stops_when_crossed():
    target_post = make_post(pid=101, tid=9, fid=10, floor=443, reply_num=0)
    evidence = {
        "tid": 9,
        "pid": target_post.pid,
        "fid": 10,
        "author_user_id": target_post.author_id,
        "reported_count": 0,
        "count_field_present": True,
        "source_api": "aiotieba.get_posts",
        "evidence_kind": "protobuf_wire_explicit_zero",
        "source_page": 8,
        "count_field_path": "data.post_list.sub_post_number",
        "count_wire_field_number": 13,
    }
    pages = {
        10: FetchedPage(
            response=ResearchPage(
                raw=SimpleNamespace(),
                dto=make_posts_response([make_post(pid=210, tid=9, fid=10, floor=460)], current_page=10, total_page=12),
                observed_at=datetime.now(UTC),
                source_api="aiotieba.get_posts",
            ),
            attempts=1,
        ),
        5: FetchedPage(
            response=ResearchPage(
                raw=SimpleNamespace(),
                dto=make_posts_response([make_post(pid=205, tid=9, fid=10, floor=300)], current_page=5, total_page=12),
                observed_at=datetime.now(UTC),
                source_api="aiotieba.get_posts",
            ),
            attempts=1,
        ),
        7: FetchedPage(
            response=ResearchPage(
                raw=SimpleNamespace(),
                dto=make_posts_response([make_post(pid=207, tid=9, fid=10, floor=430)], current_page=7, total_page=12),
                observed_at=datetime.now(UTC),
                source_api="aiotieba.get_posts",
            ),
            attempts=1,
        ),
        8: FetchedPage(
            response=ResearchPage(
                raw=SimpleNamespace(),
                dto=make_posts_response([target_post], current_page=8, total_page=12),
                observed_at=datetime.now(UTC),
                source_api="aiotieba.get_posts",
                zero_comment_evidence={target_post.pid: evidence},
            ),
            attempts=1,
        ),
    }

    async def fake_get_posts(_tid, pn, **_kwargs):
        return pages[pn]

    datastore = SimpleNamespace(
        get_research_comment_parent_page_hint=AsyncMock(return_value=(443, 30)),
        get_research_comment_parent_dense_page_hint=AsyncMock(return_value=10),
        complete_failed_comment_scope_from_parent_zero=AsyncMock(return_value=True),
    )
    container = make_research_container(SimpleNamespace())
    container.revalidate_failed_comment_page_zero = True
    handler = FullScanCommentsTaskHandler(0, container, cast("Any", datastore), QueueRouter())
    handler.get_posts = AsyncMock(side_effect=fake_get_posts)
    task = FullScanCommentsTask(
        tid=9,
        pid=101,
        run_id="synthetic-run",
        generation=3,
        parent_generation=2,
        fid=10,
    )

    completed = await handler._revalidate_failed_comment_page_from_parent_zero(
        task,
        PageIntegrityError("Comment detail empty response is not a confirmed terminal page for page 1."),
        run_id="synthetic-run",
        task_key="post_comments:9:101:g3",
    )

    assert completed is True
    assert [call_args.args[1] for call_args in handler.get_posts.await_args_list] == [10, 5, 7, 8]
    assert datastore.complete_failed_comment_scope_from_parent_zero.await_args.kwargs["parent_page"] == 8

    # Stop immediately when the page floor range brackets parent_floor without matching pid.
    crossed_page = FetchedPage(
        response=ResearchPage(
            raw=SimpleNamespace(),
            dto=make_posts_response(
                [
                    make_post(pid=301, tid=9, fid=10, floor=420),
                    make_post(pid=302, tid=9, fid=10, floor=460),
                ],
                current_page=9,
                total_page=10,
            ),
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_posts",
        ),
        attempts=1,
    )
    crossed_store = SimpleNamespace(
        get_research_comment_parent_page_hint=AsyncMock(return_value=(443, 30)),
        get_research_comment_parent_dense_page_hint=AsyncMock(return_value=9),
        complete_failed_comment_scope_from_parent_zero=AsyncMock(return_value=True),
    )
    crossed_handler = FullScanCommentsTaskHandler(0, container, cast("Any", crossed_store), QueueRouter())
    crossed_handler.get_posts = AsyncMock(return_value=crossed_page)

    crossed_completed = await crossed_handler._revalidate_failed_comment_page_from_parent_zero(
        task,
        PageIntegrityError("Comment detail empty response is missing or has zero page identity for page 1."),
        run_id="synthetic-run",
        task_key="post_comments:9:101:g3",
    )

    assert crossed_completed is False
    crossed_handler.get_posts.assert_awaited_once_with(9, 9, rn=30, with_comments=False)
    crossed_store.complete_failed_comment_scope_from_parent_zero.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_comment_revalidation_recovers_from_over_range_page_integrity_error():
    target_post = make_post(pid=101, tid=9, fid=10, floor=443, reply_num=0)
    evidence = {
        "tid": 9,
        "pid": target_post.pid,
        "fid": 10,
        "author_user_id": target_post.author_id,
        "reported_count": 0,
        "count_field_present": True,
        "source_api": "aiotieba.get_posts",
        "evidence_kind": "protobuf_wire_explicit_zero",
        "source_page": 8,
        "count_field_path": "data.post_list.sub_post_number",
        "count_wire_field_number": 13,
    }
    page_seven = FetchedPage(
        response=ResearchPage(
            raw=SimpleNamespace(),
            dto=make_posts_response([make_post(pid=207, tid=9, fid=10, floor=400)], current_page=7, total_page=8),
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_posts",
        ),
        attempts=1,
    )
    valid_page = FetchedPage(
        response=ResearchPage(
            raw=SimpleNamespace(),
            dto=make_posts_response([target_post], current_page=8, total_page=8),
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_posts",
            zero_comment_evidence={target_post.pid: evidence},
        ),
        attempts=1,
    )

    async def fake_get_posts(_tid, pn, **_kwargs):
        if pn == 15:
            raise PageIntegrityError("Post page empty response is not a confirmed terminal page for page 15.")
        if pn == 7:
            return page_seven
        return valid_page

    datastore = SimpleNamespace(
        get_research_comment_parent_page_hint=AsyncMock(return_value=(443, 30)),
        complete_failed_comment_scope_from_parent_zero=AsyncMock(return_value=True),
    )
    container = make_research_container(SimpleNamespace())
    container.revalidate_failed_comment_page_zero = True
    handler = FullScanCommentsTaskHandler(0, container, cast("Any", datastore), QueueRouter())
    handler.get_posts = AsyncMock(side_effect=fake_get_posts)
    task = FullScanCommentsTask(
        tid=9,
        pid=101,
        run_id="synthetic-run",
        generation=3,
        parent_generation=2,
        fid=10,
    )

    completed = await handler._revalidate_failed_comment_page_from_parent_zero(
        task,
        PageIntegrityError("Comment detail empty response is missing or has zero page identity for page 1."),
        run_id="synthetic-run",
        task_key="post_comments:9:101:g3",
    )

    assert completed is True
    assert [call_args.args[1] for call_args in handler.get_posts.await_args_list] == [15, 7, 8]
    assert datastore.complete_failed_comment_scope_from_parent_zero.await_args.kwargs["parent_page"] == 8


@pytest.mark.asyncio
async def test_positive_comment_page_does_not_trigger_parent_zero_revalidation():
    comments = [make_comment(cid=1000 + index, pid=101, tid=9) for index in range(5)]
    response = SimpleNamespace(dto=make_comments_response(comments), source_api=None)
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set()),
        remove_pending_comment_scan=AsyncMock(),
    )
    container = create_mock_container(SimpleNamespace())
    container.config.research_enabled = False
    container.revalidate_failed_comment_page_zero = True
    handler = FullScanCommentsTaskHandler(0, container, cast("Any", datastore), QueueRouter())
    handler.get_comments = AsyncMock(return_value=FetchedPage(response=response, attempts=1))
    handler._revalidate_failed_comment_page_from_parent_zero = AsyncMock(return_value=False)

    await handler.handle(FullScanCommentsTask(tid=9, pid=101))

    handler.get_comments.assert_awaited_once_with(9, 101, 1)
    handler._revalidate_failed_comment_page_from_parent_zero.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("checkpoint_exists", "include_evidence", "expect_skip"),
    [(False, True, True), (True, True, False), (False, False, False)],
)
@pytest.mark.parametrize("enqueue_fails", [False, True])
async def test_posts_page_completes_only_new_child_with_verified_zero_evidence(
    monkeypatch, checkpoint_exists, include_evidence, expect_skip, enqueue_fails
):
    post = make_post(pid=101, tid=9, fid=10, floor=2, reply_num=0)
    evidence = {
        "tid": 9,
        "pid": post.pid,
        "fid": 10,
        "author_user_id": post.author_id,
        "reported_count": 0,
        "count_field_present": True,
        "source_api": "aiotieba.get_posts",
        "evidence_kind": "protobuf_wire_explicit_zero",
        "source_page": 1,
        "count_field_path": "data.post_list.sub_post_number",
        "count_wire_field_number": 13,
    }
    posts_page = make_posts_response([post], current_page=1, total_page=1)
    response = ResearchPage(
        raw=SimpleNamespace(thread=SimpleNamespace(pid=100, tid=9, fid=10, fname="bar")),
        dto=posts_page,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
        zero_comment_evidence={post.pid: evidence} if include_evidence else {},
    )

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def begin(self):
            return self

    session = Session()

    class DataStore:
        def async_sessionmaker(self):
            return session

        def __init__(self):
            self.checkpoints = []
            self.removed_pending = []
            self.added_pending = []
            self.push_object_event = AsyncMock()

        async def get_research_task_scope_for_update(self, _session, _run_id, logical_key):
            if logical_key == "thread_replies:9":
                return SimpleNamespace(generation=4, status="running")
            return SimpleNamespace(
                generation=6,
                status="pending",
                parent_generation=4,
                parent_logical_task_key="thread_replies:9",
            )

        async def save_items_in_session(self, *_args, **_kwargs):
            return None

        async def ensure_research_task_scope(self, **_kwargs):
            return SimpleNamespace(
                generation=6,
                status="pending",
                parent_generation=4,
                parent_logical_task_key="thread_replies:9",
            )

        async def has_research_checkpoint_in_session(self, *_args):
            return checkpoint_exists

        async def record_research_checkpoint(self, **values):
            self.checkpoints.append(values)
            return True

        async def remove_pending_comment_scan_in_session(self, _session, tid, pid, *, task_kind):
            self.removed_pending.append((tid, pid, task_kind))

        async def add_pending_comment_scan_in_session(self, _session, tid, pid, **_kwargs):
            self.added_pending.append((tid, pid))

        async def remove_pending_thread_scan_in_session(self, *_args):
            return None

    async def persist(_maker, _page, *, session, run_id=None):
        assert session is datastore.async_sessionmaker()
        assert run_id == "synthetic-run"

    monkeypatch.setattr("src.research.persistence.persist_research_page", persist)
    datastore = DataStore()
    handler = FullScanPostsTaskHandler(
        0,
        make_research_container(SimpleNamespace()),
        cast("Any", datastore),
        QueueRouter(),
    )
    handler.ensure_users = AsyncMock()
    handler._prepare_research_page = AsyncMock()
    handler._enqueue_missing_web_targets = AsyncMock(
        side_effect=RuntimeError("synthetic postcommit enqueue error") if enqueue_fails else None
    )

    committed, children = await handler._commit_research_posts_page(
        FullScanPostsTask(tid=9, run_id="synthetic-run", generation=4, fid=10, fname="bar"),
        posts_page,
        response,
        page_number=1,
        cursor={},
        attempts=1,
    )

    assert committed is True
    handler._enqueue_missing_web_targets.assert_awaited_once()
    comment_checkpoints = [item for item in datastore.checkpoints if item["scope_type"] == "post_comments"]
    if expect_skip:
        assert children == []
        assert datastore.added_pending == []
        assert datastore.removed_pending == [(9, 101, "full")]
        assert len(comment_checkpoints) == 1
        checkpoint = comment_checkpoints[0]
        assert checkpoint["status"] == checkpoint["scope_status"] == "completed"
        assert checkpoint["attempts"] == 0
        assert checkpoint["reason"] == "parent_api_explicit_zero"
        assert checkpoint["has_more"] is None
        assert checkpoint["next_page"] is None
        assert checkpoint["total_pages"] is None
        assert checkpoint["page_item_count"] is None
        assert checkpoint["continuation_json"]["comment_api_called"] is False
        assert checkpoint["continuation_json"]["zero_comment_evidence"] == evidence
    else:
        assert [(child.pid, child.generation) for child in children] == [(101, 6)]
        assert datastore.added_pending == [(9, 101)]
        assert datastore.removed_pending == []
        assert comment_checkpoints == []


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
async def test_shutdown_after_posts_page_persists_metadata_and_leaves_next_page_pending(monkeypatch):
    post = make_post(pid=101, tid=9, floor=2, reply_num=0)
    dto = make_posts_response([post], has_more=True, current_page=1, total_page=2)
    response = ResearchPage(
        raw=SimpleNamespace(thread=SimpleNamespace(tid=9, pid=0, fid=10, fname="bar")),
        dto=dto,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )
    stop_event = asyncio.Event()
    events: list[str] = []

    async def get_posts(_tid, pn, **_kwargs):
        events.append(f"api_page_{pn}")
        stop_event.set()
        return SimpleNamespace(response=response, attempts=1)

    async def enqueue_missing(_sessions, _run_id, *, thread_ids=(), post_ids=(), comment_ids=()):
        assert post_ids == (post.pid,)
        events.append("web_target_enqueued")

    monkeypatch.setattr("src.research.web_fallback.live.enqueue_missing_web_targets", enqueue_missing)
    datastore = SimpleNamespace(
        async_sessionmaker=object(),
        get_research_resume_state=AsyncMock(return_value=(1, "running", {})),
        record_research_checkpoint=AsyncMock(side_effect=lambda **_values: events.append("checkpoint")),
        filter_new_ids=AsyncMock(return_value={post.pid}),
        save_items=AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("canonical_saved")),
        push_object_event=AsyncMock(),
        add_pending_comment_scan=AsyncMock(),
        remove_pending_thread_scan=AsyncMock(),
    )
    container = make_research_container(SimpleNamespace())
    container.async_sessionmaker = datastore.async_sessionmaker
    handler = FullScanPostsTaskHandler(
        0,
        container,
        cast("Any", datastore),
        QueueRouter(),
        stop_event=stop_event,
    )
    handler.get_posts = get_posts
    handler._persist_research_page = AsyncMock(side_effect=lambda _page: events.append("api_page_persisted"))

    await handler.handle(FullScanPostsTask(tid=9, backfill=True))

    assert events.count("api_page_1") == 1
    assert not any(event.startswith("api_page_2") for event in events)
    assert events.index("api_page_persisted") < events.index("canonical_saved")
    assert events.index("canonical_saved") < events.index("web_target_enqueued")
    checkpoint = datastore.record_research_checkpoint.await_args.kwargs
    assert checkpoint["page_number"] == 1
    assert checkpoint["scope_status"] == "running"
    assert checkpoint["next_page"] == 2
    datastore.remove_pending_thread_scan.assert_not_awaited()


@pytest.mark.asyncio
async def test_shutdown_while_waiting_for_api_rate_slot_does_not_start_request():
    api_called = AsyncMock()
    limiter_started = asyncio.Event()

    async def acquire():
        limiter_started.set()
        await asyncio.Event().wait()

    container = make_research_container(SimpleNamespace(get_posts_research=api_called))
    container.posts_limiter.acquire = acquire
    stop_event = asyncio.Event()
    handler = FullScanPostsTaskHandler(
        0,
        container,
        cast("Any", SimpleNamespace()),
        QueueRouter(),
        stop_event=stop_event,
    )

    request = asyncio.create_task(handler.get_posts(9, 1))
    await limiter_started.wait()
    stop_event.set()
    with pytest.raises(asyncio.CancelledError):
        await request
    api_called.assert_not_awaited()


@pytest.mark.asyncio
async def test_first_post_comments_use_floor_one_pid_when_raw_thread_pid_is_zero():
    first_post = make_post(pid=500, tid=9, floor=1, reply_num=20)
    posts_page = make_posts_response([first_post])
    research_response = ResearchPage(
        raw=SimpleNamespace(thread=SimpleNamespace(pid=0, tid=9, fid=10, fname="bar")),
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
async def test_page_head_reply_uses_stored_canonical_first_pid_for_unmanaged_scope():
    author_b = make_user(PostUserDTO, user_id=20, portrait="author-B")
    reply = make_post(pid=200, tid=9, floor=31, user=author_b)
    posts_page = make_posts_response([reply])
    research_response = ResearchPage(
        raw=SimpleNamespace(
            thread=SimpleNamespace(
                pid=200,
                tid=9,
                fid=10,
                fname="bar",
                author_id=20,
                user=author_b,
            )
        ),
        dto=posts_page,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )
    datastore = SimpleNamespace(
        save_items=AsyncMock(),
        add_pending_comment_scan=AsyncMock(),
        get_research_first_post_pids=AsyncMock(return_value={100}),
    )
    router = QueueRouter()
    handler = FullScanPostsTaskHandler(
        0,
        make_research_container(SimpleNamespace()),
        cast("Any", datastore),
        router,
    )
    handler.ensure_users = AsyncMock()

    await handler._process_posts_on_page(9, posts_page, True, research_response=research_response)

    datastore.get_research_first_post_pids.assert_awaited_once_with(9)
    assert datastore.add_pending_comment_scan.await_args_list == [
        call(tid=9, pid=200, fid=10, fname="bar", backfill=True, task_kind="full"),
        call(tid=9, pid=100, fid=10, fname="bar", backfill=True, task_kind="full"),
    ]
    tasks = [task.content for task in router.comments_queue._q._queue]
    assert {(task.pid, task.parent_kind) for task in tasks} == {
        (200, "reply_post"),
        (100, "first_post"),
    }


@pytest.mark.asyncio
async def test_unknown_floor_zero_does_not_create_first_post_scope():
    author_b = make_user(PostUserDTO, user_id=20, portrait="author-B")
    reply = make_post(pid=200, tid=9, floor=0, user=author_b)
    posts_page = make_posts_response([reply])
    response = ResearchPage(
        raw=SimpleNamespace(
            thread=SimpleNamespace(pid=200, tid=9, fid=10, fname="bar", author_id=20, user=author_b)
        ),
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
    handler.ensure_users = AsyncMock()

    await handler._process_posts_on_page(9, posts_page, True, research_response=response)

    datastore.get_research_first_post_pids.assert_awaited_once_with(9)
    tasks = [task.content for task in router.comments_queue._q._queue]
    assert [(task.pid, task.parent_kind) for task in tasks] == [(200, "reply_post")]


@pytest.mark.asyncio
async def test_unmanaged_stored_first_post_floor_zero_is_filtered_before_reply_enqueue():
    author_a = make_user(PostUserDTO, user_id=10, portrait="author-A")
    preview = make_comment(cid=1700, pid=100, tid=9)
    first_post = make_post(pid=100, tid=9, floor=0, user=author_a, comments=[preview])
    posts_page = make_posts_response([first_post])
    response = ResearchPage(
        raw=SimpleNamespace(
            thread=SimpleNamespace(pid=200, tid=9, fid=10, fname="bar", author_id=20)
        ),
        dto=posts_page,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )
    datastore = SimpleNamespace(
        save_items=AsyncMock(),
        add_pending_comment_scan=AsyncMock(),
        get_research_first_post_pids=AsyncMock(return_value={100}),
    )
    router = QueueRouter()
    handler = FullScanPostsTaskHandler(
        0,
        make_research_container(SimpleNamespace()),
        cast("Any", datastore),
        router,
    )
    handler.ensure_users = AsyncMock()

    await handler._process_posts_on_page(9, posts_page, True, research_response=response)

    saved_items = [item for entry in datastore.save_items.await_args_list for item in entry.args[0]]
    assert not any(isinstance(item, PostModel) and item.pid == 100 for item in saved_items)
    assert [item.cid for item in saved_items if isinstance(item, CommentModel)] == [1700]
    datastore.add_pending_comment_scan.assert_awaited_once_with(
        tid=9,
        pid=100,
        fid=10,
        fname="bar",
        backfill=True,
        task_kind="full",
    )
    tasks = [task.content for task in router.comments_queue._q._queue]
    assert [(task.pid, task.parent_kind) for task in tasks] == [(100, "first_post")]


@pytest.mark.asyncio
async def test_managed_page_head_reply_reuses_canonical_first_post_scope(monkeypatch):
    monkeypatch.setattr(
        "src.research.web_fallback.live.enqueue_missing_web_targets",
        AsyncMock(return_value=()),
    )
    author_a = make_user(PostUserDTO, user_id=10, portrait="author-A")
    author_b = make_user(PostUserDTO, user_id=20, portrait="author-B")
    first_post = make_post(pid=100, tid=9, floor=1, user=author_a)
    first_reply = make_post(pid=150, tid=9, floor=2, user=author_b)
    page1 = SimpleNamespace(
        objs=[first_post, first_reply],
        forum=SimpleNamespace(fid=10, fname="bar"),
        page=SimpleNamespace(has_more=True, total_page=2, total_count=2),
    )
    reply = make_post(pid=200, tid=9, floor=31, user=author_b)
    posts_page = SimpleNamespace(
        objs=[reply],
        forum=SimpleNamespace(fid=10, fname="bar"),
        page=SimpleNamespace(has_more=False, total_page=2, total_count=0),
    )
    first_response = ResearchPage(
        raw=SimpleNamespace(
            thread=SimpleNamespace(
                pid=100,
                tid=9,
                fid=10,
                fname="bar",
                reply_num=20,
                author_id=10,
                user=author_a,
            ),
            objs=[SimpleNamespace(pid=100, tid=9, fid=10, floor=1)],
        ),
        dto=page1,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )
    response = ResearchPage(
        raw=SimpleNamespace(
            thread=SimpleNamespace(
                pid=200,
                tid=9,
                fid=10,
                fname="bar",
                reply_num=20,
                author_id=20,
                user=author_b,
            ),
            objs=[SimpleNamespace(pid=200, tid=9, fid=10, floor=31, author_id=20)],
        ),
        dto=posts_page,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )
    events = []
    persisted_run_ids: list[str | None] = []
    persisted_page_head_pids: list[int] = []

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
            self.saved_post_pids = []
            self.saved_comment_cids = []

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
            return {100}

        async def save_items_in_session(self, session, items):
            assert session is self.session
            self.saved_post_pids.extend(item.pid for item in items if isinstance(item, PostModel))
            self.saved_comment_cids.extend(item.cid for item in items if isinstance(item, CommentModel))

        async def ensure_research_task_scope(self, *, session, logical_task_key, parent_generation, **kwargs):
            assert session is self.session
            assert parent_generation == 4
            events.append(("ensure_child", logical_task_key, kwargs["parent_kind"]))
            return SimpleNamespace(
                generation=0,
                status="pending",
                parent_generation=4,
                parent_logical_task_key="thread_replies:9",
            )

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

    async def persist(_maker, _page, *, session, run_id=None):
        assert session is datastore.session
        persisted_run_ids.append(run_id)
        persisted_page_head_pids.append(_page.raw.thread.pid)
        events.append("research_page_persisted")

    monkeypatch.setattr("src.research.persistence.persist_research_page", persist)
    datastore = ManagedDataStore()
    router = QueueRouter(maxsize=6)
    handler = FullScanPostsTaskHandler(
        0,
        make_research_container(SimpleNamespace()),
        cast("Any", datastore),
        router,
    )
    handler.ensure_users = AsyncMock()
    handler._prepare_research_page = AsyncMock()
    task = FullScanPostsTask(tid=9, run_id="synthetic-run", generation=4, fid=10, fname="bar", backfill=True)

    committed_page1, children_page1 = await handler._commit_research_posts_page(
        task,
        page1,
        first_response,
        page_number=1,
        cursor={},
        attempts=1,
    )
    committed_page2, children_page2 = await handler._commit_research_posts_page(
        task,
        posts_page,
        response,
        page_number=2,
        cursor={"last_pid": 150},
        attempts=1,
    )
    preview = make_comment(cid=1700, pid=100, tid=9)
    first_post_floor_zero = make_post(pid=100, tid=9, floor=0, user=author_a, comments=[preview])
    page3 = SimpleNamespace(
        objs=[first_post_floor_zero],
        forum=SimpleNamespace(fid=10, fname="bar"),
        page=SimpleNamespace(has_more=False, total_page=3, total_count=0),
    )
    page3_response = ResearchPage(
        raw=SimpleNamespace(
            thread=SimpleNamespace(
                pid=200,
                tid=9,
                fid=10,
                fname="bar",
                reply_num=20,
                author_id=20,
                user=author_b,
            ),
            objs=[SimpleNamespace(pid=100, tid=9, fid=10, floor=0, author_id=10)],
        ),
        dto=page3,
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )
    page3_router = QueueRouter(maxsize=1)
    page3_router.put_nowait(
        Task(
            priority=Priority.LOW,
            content=FullScanCommentsTask(tid=99, pid=999, backfill=True),
        )
    )
    handler.router = page3_router
    event_count_before_page3 = len(events)
    committed_page3, children_page3 = await asyncio.wait_for(
        handler._commit_research_posts_page(
            task,
            page3,
            page3_response,
            page_number=3,
            cursor={"last_pid": 200},
            attempts=1,
        ),
        timeout=1,
    )
    page3_events = events[event_count_before_page3:]

    assert committed_page1 is True
    assert committed_page2 is True
    assert committed_page3 is True
    assert persisted_run_ids == ["synthetic-run", "synthetic-run", "synthetic-run"]
    assert persisted_page_head_pids == [100, 200, 200]
    assert datastore.saved_post_pids == [150, 200]
    assert datastore.saved_comment_cids == [1700]
    assert events.count(("ensure_child", "post_comments:9:100", "first_post")) == 3
    assert ("ensure_child", "post_comments:9:150", "reply_post") in events
    assert ("ensure_child", "post_comments:9:200", "reply_post") in events
    assert ("ensure_child", "post_comments:9:200", "first_post") not in events
    assert ("pending_child", 9, 100) in events
    assert ("pending_child", 9, 200) in events
    assert events.index("parent_locked") < events.index("stored_first_pid_lookup")
    checkpoint_pages = [
        entry[1]["page_number"]
        for entry in events
        if isinstance(entry, tuple) and entry[0] == "checkpoint"
    ]
    assert checkpoint_pages == [1, 2, 3]
    assert datastore.checkpoint["page_number"] == 3
    assert ("ensure_child", "post_comments:9:100", "reply_post") not in page3_events
    assert ("ensure_child", "post_comments:9:100", "first_post") in page3_events
    assert datastore.checkpoint["reported_count_missing_reason"] == "not_provided_by_aiotieba_posts_page"
    assert len(children_page3) == 1
    assert page3_router.comments_queue.qsize() == 1
    assert {(child.pid, child.parent_kind) for child in children_page1} == {
        (100, "first_post"),
        (150, "reply_post"),
    }
    assert {(child.pid, child.parent_kind) for child in children_page2} == {
        (100, "first_post"),
        (200, "reply_post"),
    }
    assert {(child.pid, child.parent_kind) for child in children_page3} == {(100, "first_post")}
    assert all(
        child.parent_generation == 4
        for child in [*children_page1, *children_page2, *children_page3]
    )
    assert len(router.comments_queue._q._queue) == 3
    assert [(item.content.pid, item.content.parent_kind) for item in page3_router.comments_queue._q._queue] == [
        (999, None)
    ]


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
    monkeypatch.setattr(
        "src.research.web_fallback.live.enqueue_missing_web_targets",
        AsyncMock(return_value=()),
    )
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
                "post_comments:9:500": {
                    "generation": 0,
                    "status": "running",
                    "parent_generation": 0,
                    "parent_logical_task_key": "thread_replies:9",
                },
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
                {
                    "generation": 0,
                    "status": "pending",
                    "parent_generation": parent_generation,
                    "parent_logical_task_key": parent_logical_task_key,
                },
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
@pytest.mark.parametrize("error_code", [429, 220034])
async def test_exhausted_rate_limit_retries_leave_shared_research_network_gate_open(error_code):
    tb_client = SimpleNamespace(
        get_threads_research=AsyncMock(
            side_effect=[
                RetriableApiError(error_code, "synthetic rate limit"),
                RetriableApiError(error_code, "synthetic rate limit"),
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

    assert error.value.code == error_code
    assert tb_client.get_threads_research.await_count == 2
    assert container.wait_for_rate_limit.await_count == 2
    container.wait_for_rate_limit.assert_awaited_with(5, run_id="synthetic-run")
    container.block_research_network.assert_not_awaited()
    assert datastore.set_research_run_status_if.await_args_list[-1].args[1:3] == (
        {"running", "paused_auth", "paused_rate_limit"},
        "paused_rate_limit",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("error_code", [429, 220034])
async def test_user_stop_interrupts_rate_limit_cooldown_without_retry(error_code):
    cooldown_started = asyncio.Event()

    async def wait_for_cooldown(seconds, *, run_id):
        assert seconds == 5
        assert run_id == "synthetic-run"
        cooldown_started.set()
        await asyncio.Future()

    tb_client = SimpleNamespace(
        get_posts_research=AsyncMock(side_effect=RetriableApiError(error_code, "synthetic rate limit"))
    )
    container = make_research_container(tb_client)
    container.config.research_max_api_retries = 2
    container.wait_for_rate_limit = AsyncMock(side_effect=wait_for_cooldown)
    stop_event = asyncio.Event()
    datastore = SimpleNamespace(
        set_research_run_status_if=AsyncMock(return_value=True),
        resume_research_run_if_status=AsyncMock(return_value=True),
        record_research_checkpoint=AsyncMock(),
    )
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=container,
        datastore=cast("Any", datastore),
        router=QueueRouter(),
        stop_event=stop_event,
    )

    request = asyncio.create_task(handler.get_posts(9, 1))
    await asyncio.wait_for(cooldown_started.wait(), timeout=1)
    stop_event.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(request, timeout=1)

    tb_client.get_posts_research.assert_awaited_once()
    container.wait_for_rate_limit.assert_awaited_once_with(5, run_id="synthetic-run")
    datastore.record_research_checkpoint.assert_not_awaited()
    datastore.resume_research_run_if_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_auth_refresh_frequency_limit_cools_down_without_blocking_network():
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
    container.wait_for_rate_limit.assert_awaited_once_with(5, run_id="synthetic-run")
    container.block_research_network.assert_not_awaited()
    assert datastore.set_research_run_status_if.await_args_list[-1].args[1:3] == (
        {"paused_auth"},
        "paused_rate_limit",
    )


@pytest.mark.asyncio
async def test_empty_terminal_page_resets_shared_suspicious_envelope_streak():
    missing_page = make_threads_response([], current_page=0, total_page=0)
    confirmed_empty = make_threads_response([], current_page=1, total_page=1)
    terminal_response = ResearchPage(
        SimpleNamespace(
            objs=[],
            page=SimpleNamespace(current_page=1, total_page=1, total_count=0, has_more=False),
            forum=SimpleNamespace(fid=10, fname="bar"),
        ),
        confirmed_empty,
        datetime.now(UTC),
        "aiotieba.get_threads",
    )
    tb_client = SimpleNamespace(
        get_threads_research=AsyncMock(
            side_effect=[
                ResearchPage(SimpleNamespace(objs=[]), missing_page, datetime.now(UTC), "aiotieba.get_threads"),
                ResearchPage(SimpleNamespace(objs=[]), missing_page, datetime.now(UTC), "aiotieba.get_threads"),
                terminal_response,
                ResearchPage(SimpleNamespace(objs=[]), missing_page, datetime.now(UTC), "aiotieba.get_threads"),
                ResearchPage(SimpleNamespace(objs=[]), missing_page, datetime.now(UTC), "aiotieba.get_threads"),
                ResearchPage(SimpleNamespace(objs=[]), missing_page, datetime.now(UTC), "aiotieba.get_threads"),
            ]
        )
    )
    container = make_research_container(tb_client)
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=container,
        datastore=cast("Any", SimpleNamespace(resume_research_run_if_status=AsyncMock())),
        router=QueueRouter(),
    )
    handler._persist_research_page = AsyncMock()

    for _ in range(2):
        with pytest.raises(PageIntegrityError, match="missing or has zero page identity"):
            await handler.get_threads("bar", pn=1)
    await handler.handle(ScanThreadsTask(fid=10, fname="bar", pn=1))
    for _ in range(2):
        with pytest.raises(PageIntegrityError, match="missing or has zero page identity"):
            await handler.get_threads("bar", pn=1)

    assert container.wait_for_rate_limit.await_count == 0
    with pytest.raises(PageIntegrityError, match="missing or has zero page identity"):
        await handler.get_threads("bar", pn=1)
    container.wait_for_rate_limit.assert_awaited_once_with(5, run_id="synthetic-run")


@pytest.mark.asyncio
async def test_invalid_reply_page_does_not_reset_shared_suspicious_empty_streak():
    missing_page = make_threads_response([], current_page=0, total_page=0)
    thread = make_thread(tid=123, fname="bar")
    dto_page = make_threads_response([thread], current_page=1, total_page=1)
    invalid_reply_page = ResearchPage(
        SimpleNamespace(
            objs=[SimpleNamespace(tid=124, fid=10)],
            page=SimpleNamespace(current_page=1, total_page=1, has_more=False),
            forum=SimpleNamespace(fid=10),
        ),
        dto_page,
        datetime.now(UTC),
        "aiotieba.get_threads",
    )
    tb_client = SimpleNamespace(
        get_threads_research=AsyncMock(
            side_effect=[
                ResearchPage(SimpleNamespace(objs=[]), missing_page, datetime.now(UTC), "aiotieba.get_threads"),
                ResearchPage(SimpleNamespace(objs=[]), missing_page, datetime.now(UTC), "aiotieba.get_threads"),
                invalid_reply_page,
                ResearchPage(SimpleNamespace(objs=[]), missing_page, datetime.now(UTC), "aiotieba.get_threads"),
            ]
        )
    )
    container = make_research_container(tb_client)
    datastore = SimpleNamespace(resume_research_run_if_status=AsyncMock())
    handler = ThreadsTaskHandler(0, container, cast("Any", datastore), QueueRouter())

    for _ in range(2):
        with pytest.raises(PageIntegrityError):
            await handler.get_threads("bar", pn=1)
    with pytest.raises(PageIntegrityError, match="raw/DTO TID sets differ"):
        await handler.handle(
            ScanThreadsTask(fid=10, fname="bar", pn=1, rn=100, backfill=True, sort_mode="reply")
        )
    with pytest.raises(PageIntegrityError, match="missing or has zero page identity"):
        await handler.get_threads("bar", pn=1)

    container.wait_for_rate_limit.assert_awaited_once_with(5, run_id="synthetic-run")


@pytest.mark.asyncio
async def test_empty_page_missing_requested_forum_identity_counts_as_suspicious():
    empty_identity = SimpleNamespace(
        objs=[],
        page=SimpleNamespace(current_page=1, total_page=1, total_count=0, has_more=False),
        forum=SimpleNamespace(fid=0),
    )
    tb_client = SimpleNamespace(
        get_threads_research=AsyncMock(
            return_value=ResearchPage(
                SimpleNamespace(objs=[]), empty_identity, datetime.now(UTC), "aiotieba.get_threads"
            )
        )
    )
    container = make_research_container(tb_client)
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=container,
        datastore=cast("Any", SimpleNamespace(resume_research_run_if_status=AsyncMock())),
        router=QueueRouter(),
    )

    with pytest.raises(PageIntegrityError, match="forum identity"):
        await handler.get_threads("bar", pn=1, _expected_fid=10)

    assert await container.record_research_api_response(suspicious_empty=True) is False


@pytest.mark.asyncio
async def test_exhausted_detail_rate_limit_failure_is_checkpointed_incomplete():
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
    assert checkpoint["status"] == "failed"
    assert checkpoint["scope_status"] == "incomplete"
    assert checkpoint["next_page"] == 2
    datastore.set_research_run_status_if.assert_not_awaited()


def _make_legacy_zero_comment_page(comments: list[Any]) -> ResearchPage:
    dto_template = make_comments_response(comments, has_more=False, current_page=0, total_page=0)
    page_info = dto_template.page.model_copy(
        update={
            "current_page": 0,
            "total_page": 0,
            "total_count": 0,
            "has_more": False,
        }
    )
    dto_page = dto_template.model_copy(update={"page": page_info})
    raw_page = SimpleNamespace(
        objs=[SimpleNamespace(cid=comment.cid, pid=comment.pid, tid=comment.tid) for comment in comments],
        page=SimpleNamespace(
            current_page=0,
            total_page=0,
            total_count=0,
            has_more=False,
        ),
        forum=SimpleNamespace(fid=10, fname="bar"),
    )
    return ResearchPage(raw_page, dto_page, datetime.now(UTC), "aiotieba.get_comments")


@pytest.mark.asyncio
async def test_legacy_zero_page_comments_single_and_multi_page_complete_without_truncation():
    tid = 3484431880
    pid = 62491655139
    multi_pages = {
        1: _make_legacy_zero_comment_page(
            [make_comment(cid=7001, pid=pid, tid=tid), make_comment(cid=7002, pid=pid, tid=tid)]
        ),
        2: _make_legacy_zero_comment_page([make_comment(cid=7003, pid=pid, tid=tid)]),
        3: _make_legacy_zero_comment_page([]),
    }
    tb_client = SimpleNamespace(
        get_comments_research=AsyncMock(side_effect=lambda _tid, _pid, pn: multi_pages[pn])
    )
    datastore = SimpleNamespace(
        get_research_task_scope_state=AsyncMock(side_effect=[(0, "pending", 0), (0, "completed", None)]),
        claim_research_comments_scope=AsyncMock(return_value=True),
        resume_research_run_if_status=AsyncMock(),
        remove_pending_comment_scan=AsyncMock(),
    )
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=make_research_container(tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler._commit_research_comments_page = AsyncMock(return_value=True)

    await handler.handle(
        FullScanCommentsTask(
            tid=tid,
            pid=pid,
            run_id="synthetic-run",
            generation=0,
            parent_generation=0,
            fid=10,
        )
    )

    assert tb_client.get_comments_research.await_args_list == [
        call(tid, pid, 1),
        call(tid, pid, 2),
        call(tid, pid, 3),
    ]
    commits = handler._commit_research_comments_page.await_args_list
    assert [commit.kwargs["page_number"] for commit in commits] == [1, 2, 3]
    assert commits[0].kwargs["has_more_override"] is True
    assert commits[0].kwargs["cursor"]["legacy_zero_page_continuation"] is True
    assert commits[1].kwargs["has_more_override"] is True
    assert commits[1].kwargs["cursor"]["legacy_zero_page_continuation"] is True
    assert "has_more_override" not in commits[2].kwargs
    assert commits[2].kwargs["cursor"] == {"empty_tail_reason": "legacy_zero_page_empty_tail"}

    # Non-scope-managed mode also probes until the empty tail and completes cleanly.
    single_pages = {
        1: _make_legacy_zero_comment_page([make_comment(cid=8001, pid=pid, tid=tid)]),
        2: _make_legacy_zero_comment_page([]),
    }
    legacy_client = SimpleNamespace(
        get_comments_research=AsyncMock(side_effect=lambda _tid, _pid, pn: single_pages[pn])
    )
    legacy_datastore = SimpleNamespace(
        resume_research_run_if_status=AsyncMock(),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
        remove_pending_comment_scan=AsyncMock(),
        record_research_checkpoint=AsyncMock(return_value=True),
    )
    legacy_handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=make_research_container(legacy_client),
        datastore=cast("Any", legacy_datastore),
        router=QueueRouter(),
    )
    legacy_handler.ensure_users = AsyncMock()
    legacy_handler._persist_research_page = AsyncMock()
    legacy_handler._process_comments_on_page = AsyncMock()
    await legacy_handler.handle(FullScanCommentsTask(tid=tid, pid=pid, backfill=True))

    checkpoints = [c.kwargs for c in legacy_datastore.record_research_checkpoint.await_args_list]
    assert [(c["page_number"], c["has_more"], c["scope_status"]) for c in checkpoints] == [
        (1, True, "running"),
        (2, False, "completed"),
    ]
    assert checkpoints[1]["continuation_json"] == {"empty_tail_reason": "legacy_zero_page_empty_tail"}

    # Page 1 empty all-zero response must still be rejected as missing page identity.
    empty_p1_client = SimpleNamespace(
        get_comments_research=AsyncMock(return_value=_make_legacy_zero_comment_page([]))
    )
    empty_p1_handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=make_research_container(empty_p1_client),
        datastore=cast("Any", SimpleNamespace(resume_research_run_if_status=AsyncMock())),
        router=QueueRouter(),
    )
    with pytest.raises(PageIntegrityError, match="missing or has zero page identity"):
        await empty_p1_handler.get_comments(tid, pid, 1)

    # Non-all-zero page 0 (e.g., total_page > 0) must still fail page-number check.
    broken_dto = make_comments_response([make_comment(cid=9001, pid=pid, tid=tid)], current_page=0, total_page=1)
    broken_raw = SimpleNamespace(
        objs=[SimpleNamespace(cid=9001, pid=pid, tid=tid)],
        page=SimpleNamespace(current_page=0, total_page=1, total_count=1, has_more=False),
        forum=SimpleNamespace(fid=10, fname="bar"),
    )
    broken_client = SimpleNamespace(
        get_comments_research=AsyncMock(
            return_value=ResearchPage(broken_raw, broken_dto, datetime.now(UTC), "aiotieba.get_comments")
        )
    )
    broken_handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=make_research_container(broken_client),
        datastore=cast("Any", SimpleNamespace(resume_research_run_if_status=AsyncMock())),
        router=QueueRouter(),
    )
    with pytest.raises(PageIntegrityError, match="returned page 0 when page 1 was requested"):
        await broken_handler.get_comments(tid, pid, 1)


@pytest.mark.asyncio
async def test_threads_handler_min_thread_create_time_defers_older_details_and_keeps_metadata():
    older_new = make_thread(
        tid=101,
        fid=10,
        reply_num=5,
        create_time=datetime(2020, 12, 31, 23, 59, 59, tzinfo=SHANGHAI_TZ),
    )
    cutoff_new = make_thread(
        tid=102,
        fid=10,
        reply_num=3,
        create_time=datetime(2021, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ),
    )
    newer_new = make_thread(
        tid=103,
        fid=10,
        reply_num=2,
        create_time=datetime(2022, 5, 1, 12, 0, 0, tzinfo=SHANGHAI_TZ),
    )
    older_existing = make_thread(
        tid=104,
        fid=10,
        reply_num=7,
        create_time=datetime(2019, 6, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ),
        last_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ),
    )
    dto_page = make_threads_response([older_new, cutoff_new, newer_new, older_existing], has_more=False)
    raw_page = SimpleNamespace(
        objs=[SimpleNamespace(tid=t.tid, fid=10) for t in dto_page.objs],
        page=SimpleNamespace(current_page=1, total_page=1, has_more=False),
        forum=SimpleNamespace(fid=10, fname="bar"),
    )
    tb_client = SimpleNamespace(
        get_threads_research=AsyncMock(
            return_value=ResearchPage(raw_page, dto_page, datetime.now(UTC), "aiotieba.get_threads")
        )
    )
    saved_batches: list[list[int]] = []

    async def save_scans(threads, **_kwargs):
        saved_batches.append([t.tid for t in threads])
        return {t.tid: 0 for t in threads}

    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={101, 102, 103}),
        save_threads_and_pending_scans=AsyncMock(side_effect=save_scans),
        get_threads_by_tids=AsyncMock(
            return_value=[
                SimpleNamespace(
                    tid=104,
                    reply_num=7,
                    last_time=datetime(2020, 6, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ),
                )
            ]
        ),
        get_pending_thread_scan_tids=AsyncMock(return_value=set()),
        needs_research_thread_detail=AsyncMock(return_value={104}),
        record_research_checkpoint=AsyncMock(return_value=True),
        resume_research_run_if_status=AsyncMock(),
    )
    container = make_research_container(tb_client)
    container.min_thread_create_time = datetime(2021, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ)
    router = QueueRouter()
    handler = ThreadsTaskHandler(0, container, cast("Any", datastore), router)
    handler.ensure_users = AsyncMock()
    handler._persist_research_page = AsyncMock()
    handler._enqueue_missing_web_targets = AsyncMock()

    await handler.handle(ScanThreadsTask(fid=10, fname="bar", pn=1, rn=100, backfill=True, sort_mode="reply"))

    # Metadata & pending scopes are preserved for both eligible and deferred threads.
    assert saved_batches == [[101, 102, 103], [104]]
    enqueued_tids = sorted(task.content.tid for task in router.posts_queue._q._queue)
    assert enqueued_tids == [102, 103]
    handler._enqueue_missing_web_targets.assert_awaited_once_with("synthetic-run", thread_ids=(102, 103))
