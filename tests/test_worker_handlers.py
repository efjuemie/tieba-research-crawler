"""Worker 任务处理器测试。"""

import asyncio
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from aiotieba.enums import ThreadSortType
from conftest import (
    make_comment,
    make_comments_response,
    make_post,
    make_posts_response,
    make_thread,
    make_threads_response,
)
from tiebameow.client.tieba_client import RetriableApiError
from tiebameow.models.orm import Post as PostModel
from tiebameow.models.orm import Thread as ThreadModel

from src.core.lock import MemoryLockManager
from src.research.client import ResearchPage
from src.scraper.router import QueueRouter
from src.scraper.scheduler import Scheduler
from src.scraper.tasks import (
    FullScanCommentsTask,
    FullScanPostsTask,
    IncrementalScanCommentsTask,
    IncrementalScanPostsTask,
    Priority,
    ScanThreadsTask,
    Task,
    ThreadPageResult,
)
from src.scraper.worker import (
    DeepScanTaskHandler,
    FullScanCommentsTaskHandler,
    FullScanPostsTaskHandler,
    IncrementalScanCommentsTaskHandler,
    IncrementalScanPostsTaskHandler,
    PageIntegrityError,
    ThreadsTaskHandler,
    Worker,
)


def _drain_router(router: QueueRouter) -> list[Task]:
    """从路由器的所有通道队列中取出所有任务"""
    items: list[Task] = []
    for q in router.lanes.values():
        while not q.empty():
            items.append(q.get_nowait())
    return items


@pytest.mark.asyncio
@pytest.mark.parametrize("source_api", ["aiotieba.get_posts", "aiotieba.get_comments"])
async def test_research_detail_pages_enrich_unique_author_forum_snapshots(source_api, monkeypatch):
    events: list[tuple[str, object]] = []

    async def persist(_sessionmaker, page, *, session=None, run_id=None):
        events.append(("persist", (page.source_api, run_id)))

    monkeypatch.setattr("src.research.persistence.persist_research_page", persist)

    if source_api == "aiotieba.get_posts":
        thread = SimpleNamespace(
            fid=42,
            author_id=10,
            user=SimpleNamespace(user_id=10, portrait="portrait-10"),
        )
        authors = [
            SimpleNamespace(
                fid=42,
                author_id=10,
                user=SimpleNamespace(user_id=10, portrait="portrait-10"),
            ),
            SimpleNamespace(
                fid=42,
                author_id=11,
                user=SimpleNamespace(user_id=11, portrait="portrait-11"),
            ),
        ]
        raw = SimpleNamespace(thread=thread, objs=authors)
        forum = SimpleNamespace(fid=42)
    else:
        authors = [
            SimpleNamespace(
                fid=42,
                author_id=10,
                user=SimpleNamespace(user_id=10, portrait="portrait-10"),
            ),
            SimpleNamespace(
                fid=42,
                author_id=11,
                user=SimpleNamespace(user_id=11, portrait="portrait-11"),
            ),
            SimpleNamespace(
                fid=42,
                author_id=11,
                user=SimpleNamespace(user_id=11, portrait="portrait-11"),
            ),
        ]
        raw = SimpleNamespace(objs=authors)
        forum = SimpleNamespace(fid=42)

    response = ResearchPage(
        raw=raw,
        dto=SimpleNamespace(forum=forum),
        observed_at=datetime.now(),
        source_api=source_api,
    )
    user_page = SimpleNamespace(
        source_api="aiotieba.get_user_forum_info",
        dto=SimpleNamespace(status="available", error_code=None),
    )

    async def lookup(fid, user_id, portrait):
        events.append(("lookup", (fid, user_id, portrait)))
        return user_page

    container = create_mock_container(
        tb_client=SimpleNamespace(get_user_forum_info_research=AsyncMock(side_effect=lookup)),
        config=SimpleNamespace(
            research_enabled=True,
            fetch_user_forum_info=True,
            research_max_api_retries=1,
            research_retry_wait_seconds=0,
        ),
    )
    container.async_sessionmaker = object()
    container.research_run_id = "synthetic-run"
    handler = FullScanPostsTaskHandler(0, container, cast("Any", SimpleNamespace()), QueueRouter())

    await handler._persist_research_page(response)

    assert events == [
        ("lookup", (42, 10, "portrait-10")),
        ("persist", ("aiotieba.get_user_forum_info", None)),
        ("lookup", (42, 11, "portrait-11")),
        ("persist", ("aiotieba.get_user_forum_info", None)),
        ("persist", (source_api, "synthetic-run")),
    ]


@pytest.mark.asyncio
async def test_user_forum_rejection_blocks_network_without_finishing_page(monkeypatch):
    persisted: list[tuple[str, str | None]] = []

    async def persist(_sessionmaker, page, *, session=None, run_id=None):
        persisted.append((page.source_api, run_id))

    monkeypatch.setattr("src.research.persistence.persist_research_page", persist)
    user_page = SimpleNamespace(
        source_api="aiotieba.get_user_forum_info",
        dto=SimpleNamespace(status="retryable_failure", error_code=220034),
    )
    container = create_mock_container(
        tb_client=SimpleNamespace(get_user_forum_info_research=AsyncMock(return_value=user_page)),
        config=SimpleNamespace(
            research_enabled=True,
            fetch_user_forum_info=True,
            research_max_api_retries=1,
            research_retry_wait_seconds=0,
        ),
    )
    container.async_sessionmaker = object()
    container.research_run_id = "synthetic-run"
    container.block_research_network = AsyncMock()
    datastore = SimpleNamespace(set_research_run_status_if=AsyncMock())
    handler = FullScanPostsTaskHandler(0, container, cast("Any", datastore), QueueRouter())
    raw = SimpleNamespace(
        thread=SimpleNamespace(fid=42, author_id=10, user=SimpleNamespace(user_id=10, portrait="portrait-10")),
        objs=[],
    )
    response = ResearchPage(
        raw=raw,
        dto=SimpleNamespace(forum=SimpleNamespace(fid=42)),
        observed_at=datetime.now(),
        source_api="aiotieba.get_posts",
    )

    with pytest.raises(RetriableApiError) as error:
        await handler._persist_research_page(response)

    assert error.value.code == 220034
    assert persisted == [("aiotieba.get_user_forum_info", None)]
    datastore.set_research_run_status_if.assert_awaited_once()
    assert datastore.set_research_run_status_if.await_args.args[2] == "blocked"
    container.block_research_network.assert_awaited_once()


@pytest.mark.asyncio
async def test_user_forum_auth_rejection_waits_for_refresh_then_persists_snapshot(monkeypatch):
    persisted: list[tuple[str, str | None]] = []

    async def persist(_sessionmaker, page, *, session=None, run_id=None):
        persisted.append((page.source_api, run_id))

    monkeypatch.setattr("src.research.persistence.persist_research_page", persist)
    pages = [
        SimpleNamespace(
            source_api="aiotieba.get_user_forum_info",
            dto=SimpleNamespace(status="retryable_failure", error_code=4011),
        ),
        SimpleNamespace(
            source_api="aiotieba.get_user_forum_info",
            dto=SimpleNamespace(status="available", error_code=None),
        ),
    ]
    lookup = AsyncMock(side_effect=pages)
    auth_ready = asyncio.Event()
    auth_ready.set()
    container = create_mock_container(
        tb_client=SimpleNamespace(get_user_forum_info_research=lookup),
        config=SimpleNamespace(
            research_enabled=True,
            fetch_user_forum_info=True,
            research_max_api_retries=2,
            research_retry_wait_seconds=0,
        ),
    )
    container.async_sessionmaker = object()
    container.research_run_id = "synthetic-run"
    container.research_auth_ready = auth_ready

    async def wait_for_auth_refresh(callback, *, run_id):
        assert run_id == "synthetic-run"
        assert not auth_ready.is_set()
        page = await callback()
        auth_ready.set()
        return page

    container.wait_for_auth_refresh = AsyncMock(side_effect=wait_for_auth_refresh)
    datastore = SimpleNamespace(
        set_research_run_status_if=AsyncMock(),
        resume_research_run_if_status=AsyncMock(),
    )
    handler = FullScanPostsTaskHandler(0, container, cast("Any", datastore), QueueRouter())
    response = ResearchPage(
        raw=SimpleNamespace(
            thread=SimpleNamespace(
                fid=42,
                author_id=10,
                user=SimpleNamespace(user_id=10, portrait="portrait-10"),
            ),
            objs=[],
        ),
        dto=SimpleNamespace(forum=SimpleNamespace(fid=42)),
        observed_at=datetime.now(),
        source_api="aiotieba.get_posts",
    )

    await handler._persist_research_page(response)

    assert lookup.await_count == 2
    assert persisted == [
        ("aiotieba.get_user_forum_info", None),
        ("aiotieba.get_posts", "synthetic-run"),
    ]
    datastore.set_research_run_status_if.assert_awaited_once()
    assert datastore.set_research_run_status_if.await_args.args[2] == "paused_auth"
    datastore.resume_research_run_if_status.assert_awaited_once_with("synthetic-run", "paused_auth")


def create_mock_container(tb_client=None, redis_client=None, config=None):
    lock_manager = MemoryLockManager()
    config = config or SimpleNamespace(
        mode="periodic",
        max_backfill_pages=5,
        backfill_force_scan=False,
        research_enabled=False,
        fetch_user_forum_info=False,
        research_max_api_retries=1,
        research_retry_wait_seconds=0,
    )
    return cast(
        "Any",
        SimpleNamespace(
            tb_client=tb_client,
            redis_client=redis_client,
            config=config,
            tb_clients=None,
            threads_limiter=AsyncMock(),
            posts_limiter=AsyncMock(),
            comments_limiter=AsyncMock(),
            limiter=AsyncMock(),
            lock_manager=lock_manager,
        ),
    )


def _make_reply_history_page(
    threads,
    *,
    pn=1,
    has_more=True,
    total_page=3306,
    source_api="aiotieba.get_threads",
    raw_tid_delta=0,
    raw_fid=10,
    raw_page=None,
):
    dto = make_threads_response(threads, has_more=has_more, current_page=pn, total_page=total_page)
    dto.page.total_count = 330600
    raw_items = [
        SimpleNamespace(
            tid=thread.tid + raw_tid_delta,
            fid=raw_fid,
            pid=thread.pid,
            author_id=thread.author_id,
            user=SimpleNamespace(user_id=thread.author_id),
            is_top=thread.is_top,
            is_good=thread.is_good,
            create_time=int(thread.create_time.timestamp()),
            last_time=int(thread.last_time.timestamp()),
            reply_num=thread.reply_num,
            title=thread.title,
        )
        for thread in threads
    ]
    raw = SimpleNamespace(
        objs=raw_items,
        page=raw_page
        or SimpleNamespace(current_page=pn, total_page=total_page, total_count=330600, has_more=has_more),
        forum=SimpleNamespace(fid=raw_fid),
    )
    return ResearchPage(
        raw=raw,
        dto=dto,
        observed_at=datetime.now(),
        source_api=source_api,
    )


def _make_reply_history_handler(response, *, new_tids=()):
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set(new_tids)),
        record_research_checkpoint=AsyncMock(return_value=True),
    )
    container = create_mock_container(
        config=SimpleNamespace(research_enabled=True, fetch_user_forum_info=False)
    )
    container.research_run_id = "reply-run"
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=container,
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler.get_threads = AsyncMock(return_value=SimpleNamespace(response=response, attempts=1))
    handler._persist_research_page = AsyncMock()
    handler._process_new_threads = AsyncMock()
    handler._process_old_threads = AsyncMock()
    handler._enqueue_missing_web_targets = AsyncMock()
    return handler, datastore


# ==================== ThreadsTaskHandler 测试 ====================


@pytest.mark.asyncio
async def test_threads_handler_new_threads_schedule_and_push():
    """测试 ThreadsTaskHandler 处理新主题帖"""
    thread = make_thread(tid=1, reply_num=5)
    threads_response = make_threads_response([thread])

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={thread.tid}),
        save_items=AsyncMock(),
        save_threads_and_pending_scans=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
    )
    router = QueueRouter()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1))

    tb_client.get_threads_dto.assert_awaited_once()
    assert tb_client.get_threads_dto.await_args.kwargs["sort"] == ThreadSortType.REPLY

    # 新主题帖应该推送到对象流
    datastore.push_object_event.assert_awaited_once_with("thread", thread)

    # 应该原子保存 thread 元数据与 pending_scan
    datastore.save_threads_and_pending_scans.assert_awaited_once()
    atomic_call = datastore.save_threads_and_pending_scans.await_args
    thread_dtos = atomic_call.args[0]
    assert thread_dtos is not None
    assert thread_dtos[0].tid == thread.tid
    assert atomic_call.kwargs.get("backfill") is False
    assert atomic_call.kwargs.get("upsert_threads") is False

    # 应该生成 FullScanPostsTask
    queued_tasks = _drain_router(router)
    assert any(isinstance(item.content, FullScanPostsTask) for item in queued_tasks)


@pytest.mark.asyncio
async def test_threads_handler_new_thread_zero_replies():
    """测试 ThreadsTaskHandler 处理没有回复的新主题帖"""
    thread = make_thread(tid=1, reply_num=0)
    threads_response = make_threads_response([thread])

    async def get_threads_dto(_fname: str, **kwargs):
        return threads_response if not kwargs["is_good"] else make_threads_response([])

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(side_effect=get_threads_dto))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={thread.tid}),
        save_items=AsyncMock(),
        save_threads_and_pending_scans=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
    )
    router = QueueRouter()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1, backfill=True))

    # 没有回复的主题帖也应通过原子方法保存元数据
    datastore.save_threads_and_pending_scans.assert_awaited_once()
    atomic_call = datastore.save_threads_and_pending_scans.await_args
    thread_dtos = atomic_call.args[0]
    assert thread_dtos is not None
    assert thread_dtos[0].tid == thread.tid
    assert atomic_call.kwargs.get("backfill") is True
    assert atomic_call.kwargs.get("upsert_threads") is False

    # 即使主题帖显示零回复，也要获取详情中的首楼完整正文。
    queued_tasks = _drain_router(router)
    assert any(isinstance(item.content, FullScanPostsTask) for item in queued_tasks)


@pytest.mark.asyncio
async def test_threads_handler_old_thread_with_updates():
    """测试 ThreadsTaskHandler 处理有更新的旧主题帖"""
    # 旧主题帖：数据库中的 last_time 是 1，新抓取的是 100
    old_last_time = datetime.fromtimestamp(1)
    new_last_time = datetime.fromtimestamp(100)

    thread = make_thread(tid=1, reply_num=10, last_time=new_last_time)
    threads_response = make_threads_response([thread])

    # 模拟数据库中的旧数据（ThreadModel 没有 fname 字段）
    stored_thread = ThreadModel(tid=1, fid=10, reply_num=5, last_time=old_last_time)

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set()),  # 不是新帖
        save_items=AsyncMock(),
        save_threads_and_pending_scans=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[stored_thread]),
        get_pending_thread_scan_tids=AsyncMock(return_value=set()),
        push_object_event=AsyncMock(),
    )
    router = QueueRouter()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1))

    # 应该生成 IncrementalScanPostsTask
    queued_tasks = _drain_router(router)
    incremental_tasks = [t for t in queued_tasks if isinstance(t.content, IncrementalScanPostsTask)]
    assert len(incremental_tasks) == 1
    assert isinstance(incremental_tasks[0].content, IncrementalScanPostsTask)
    assert incremental_tasks[0].content.tid == thread.tid


@pytest.mark.asyncio
async def test_threads_handler_old_thread_with_updates_skips_incremental_when_full_scan_pending():
    """测试旧主题帖在存在 pending full scan 标记时不会调度增量任务。"""
    old_last_time = datetime.fromtimestamp(1)
    new_last_time = datetime.fromtimestamp(100)

    thread = make_thread(tid=1, reply_num=10, last_time=new_last_time)
    threads_response = make_threads_response([thread])
    stored_thread = ThreadModel(tid=1, fid=10, reply_num=5, last_time=old_last_time)

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set()),
        save_items=AsyncMock(),
        save_threads_and_pending_scans=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[stored_thread]),
        get_pending_thread_scan_tids=AsyncMock(return_value={thread.tid}),
        push_object_event=AsyncMock(),
    )
    router = QueueRouter()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1))

    queued_tasks = _drain_router(router)
    incremental_tasks = [t for t in queued_tasks if isinstance(t.content, IncrementalScanPostsTask)]
    assert len(incremental_tasks) == 0


@pytest.mark.asyncio
async def test_threads_handler_backfill_processes_one_create_page_and_retains_pinned_threads():
    """Backfill page handlers process one CREATE page and report non-pinned boundaries."""
    pinned = make_thread(tid=10, is_top=True, create_time=datetime.fromtimestamp(1))
    recent = make_thread(tid=11, create_time=datetime.fromtimestamp(30))
    older = make_thread(tid=12, create_time=datetime.fromtimestamp(20))
    response = make_threads_response([pinned, recent, older], has_more=True, current_page=2, total_page=3)
    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={10, 11, 12}),
        save_items=AsyncMock(),
        save_threads_and_pending_scans=AsyncMock(),
    )
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    result = await handler.handle(ScanThreadsTask(fid=10, fname="bar", pn=2, backfill=True, max_pages=0))

    assert tb_client.get_threads_dto.await_args.kwargs["sort"] == ThreadSortType.CREATE
    assert result == ThreadPageResult(
        has_more=True,
        current_page=2,
        total_pages=3,
        total_count=3,
        tids=(10, 11, 12),
        non_top_create_times=(datetime.fromtimestamp(30), datetime.fromtimestamp(20)),
        non_top_tids=(11, 12),
        continuation_json={
            "recent_non_top_tids": [11, 12],
            "recent_page_fingerprints": [[10, 11, 12]],
            "page_fingerprint": [10, 11, 12],
            "page_duplicate_tids": [],
            "page_non_top_tids": [11, 12],
            "flow_duplicate_streak": 0,
            "page_duplicate_ratio": 0.0,
            "last_create_time": datetime.fromtimestamp(20).isoformat(),
        },
    )
    saved_threads = datastore.save_threads_and_pending_scans.await_args.args[0]
    assert {thread.tid for thread in saved_threads} == {10, 11, 12}


@pytest.mark.asyncio
async def test_reply_history_requires_verified_api_page_and_ignores_total_page_metadata():
    threads = [make_thread(tid=77), make_thread(tid=78)]
    response = _make_reply_history_page(threads, pn=2, total_page=3306, has_more=True)
    handler, datastore = _make_reply_history_handler(response, new_tids=(77, 78))

    result = await handler.handle(
        ScanThreadsTask(fid=10, fname="bar", pn=2, rn=100, backfill=True, sort_mode="reply")
    )

    call = handler.get_threads.await_args
    assert call.args == ("bar",)
    assert call.kwargs["pn"] == 2
    assert call.kwargs["rn"] == 100
    assert call.kwargs["sort"] is ThreadSortType.REPLY
    assert call.kwargs["is_good"] is False
    assert call.kwargs["_expected_fid"] == 10
    assert result.current_page == 2
    assert result.total_pages == 3306
    assert handler._process_new_threads.await_args.args[1] == {77, 78}
    assert handler._persist_research_page.await_args.args[0] is response

    checkpoint = datastore.record_research_checkpoint.await_args.kwargs
    assert checkpoint["task_key"] == "thread_list:10:reply:rn100:0"
    assert checkpoint["sort_mode"] == "reply"
    assert checkpoint["has_more"] is True
    assert checkpoint["continuation_json"]["source_api"] == "aiotieba.get_threads"
    assert checkpoint["continuation_json"]["rn"] == 100
    assert checkpoint["continuation_json"]["sort_type"] == 6
    assert "last_create_time" not in checkpoint["continuation_json"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["missing_raw", "source", "raw_tid", "raw_fid", "raw_page"])
async def test_reply_history_rejects_missing_or_conflicting_raw_identity(mismatch):
    threads = [make_thread(tid=77)]
    response = _make_reply_history_page(threads, pn=2)
    if mismatch == "missing_raw":
        response = ResearchPage(
            raw=None,
            dto=response.dto,
            observed_at=response.observed_at,
            source_api=response.source_api,
        )
    elif mismatch == "source":
        response = _make_reply_history_page(threads, pn=2, source_api="aiotieba.get_posts")
    elif mismatch == "raw_tid":
        response = _make_reply_history_page(threads, pn=2, raw_tid_delta=1)
    elif mismatch == "raw_fid":
        response = _make_reply_history_page(threads, pn=2, raw_fid=11)
    else:
        response = _make_reply_history_page(
            threads,
            pn=2,
            raw_page=SimpleNamespace(current_page=1, total_page=3306, total_count=330600, has_more=True),
        )
    handler, _datastore = _make_reply_history_handler(response)

    with pytest.raises(PageIntegrityError):
        await handler.handle(
            ScanThreadsTask(fid=10, fname="bar", pn=2, rn=100, backfill=True, sort_mode="reply")
        )

    handler._persist_research_page.assert_not_awaited()
    handler._process_new_threads.assert_not_awaited()


@pytest.mark.asyncio
async def test_reply_history_fails_closed_before_dto_fallback_request():
    dto_fallback = AsyncMock()
    container = create_mock_container(
        tb_client=SimpleNamespace(get_threads_dto=dto_fallback),
        config=SimpleNamespace(
            research_enabled=True,
            fetch_user_forum_info=False,
            research_max_api_retries=1,
            research_retry_wait_seconds=0,
        ),
    )
    datastore = SimpleNamespace()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=container,
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    with pytest.raises(PageIntegrityError, match="required get_threads_research"):
        await handler.handle(
            ScanThreadsTask(fid=10, fname="bar", pn=1, rn=100, backfill=True, sort_mode="reply")
        )

    dto_fallback.assert_not_awaited()
    container.threads_limiter.acquire.assert_not_awaited()


@pytest.mark.asyncio
async def test_reply_page_repeating_home_ids_are_overlap_evidence_not_identity_error():
    response = _make_reply_history_page([make_thread(tid=77)], pn=2)
    handler, datastore = _make_reply_history_handler(response)
    task = ScanThreadsTask(
        fid=10,
        fname="bar",
        pn=2,
        rn=100,
        backfill=True,
        sort_mode="reply",
        continuation_json={"_flow_seen_tids": [77], "_flow_duplicate_streak": 0},
    )

    result = await handler.handle(task)

    assert result.tids == (77,)
    assert result.continuation_json["page_duplicate_ratio"] == 1.0
    assert result.continuation_json["flow_duplicate_streak"] == 1
    assert "high_duplicate_pause_reason" not in result.continuation_json
    assert datastore.record_research_checkpoint.await_args.kwargs["scope_status"] == "running"


@pytest.mark.asyncio
async def test_reply_third_high_overlap_page_commits_new_tid_before_incomplete_pause():
    threads = [make_thread(tid=tid) for tid in range(1, 11)]
    response = _make_reply_history_page(threads, pn=3, has_more=True)
    handler, datastore = _make_reply_history_handler(response, new_tids=(10,))
    task = ScanThreadsTask(
        fid=10,
        fname="bar",
        pn=3,
        rn=100,
        backfill=True,
        sort_mode="reply",
        continuation_json={"_flow_seen_tids": list(range(1, 10)), "_flow_duplicate_streak": 2},
    )

    result = await handler.handle(task)

    assert result.continuation_json["page_duplicate_ratio"] == 0.9
    assert result.continuation_json["flow_duplicate_streak"] == 3
    assert result.continuation_json["high_duplicate_pause_reason"] == "three_consecutive_pages_at_least_90_percent_seen"
    assert 10 in result.continuation_json["page_non_top_tids"]
    assert handler._process_new_threads.await_args.args[1] == {10}
    checkpoint = datastore.record_research_checkpoint.await_args.kwargs
    assert checkpoint["status"] == "completed"
    assert checkpoint["scope_status"] == "incomplete"
    assert 10 in checkpoint["continuation_json"]["page_non_top_tids"]


@pytest.mark.asyncio
async def test_reply_terminal_page_with_high_overlap_is_not_marked_completed():
    threads = [make_thread(tid=tid) for tid in range(1, 11)]
    response = _make_reply_history_page(threads, pn=3, has_more=False)
    handler, datastore = _make_reply_history_handler(response)
    task = ScanThreadsTask(
        fid=10,
        fname="bar",
        pn=3,
        rn=100,
        backfill=True,
        sort_mode="reply",
        continuation_json={"_flow_seen_tids": list(range(1, 10)), "_flow_duplicate_streak": 0},
    )

    await handler.handle(task)

    checkpoint = datastore.record_research_checkpoint.await_args.kwargs
    assert checkpoint["has_more"] is False
    assert checkpoint["scope_status"] == "incomplete"
    assert checkpoint["reason"] == "terminal_page_at_least_90_percent_seen"


@pytest.mark.asyncio
async def test_reply_nonempty_terminal_page_with_only_pinned_threads_is_untrusted():
    pinned = make_thread(tid=99)
    pinned.is_top = True
    response = _make_reply_history_page([pinned], pn=1, has_more=False)
    handler, datastore = _make_reply_history_handler(response, new_tids=(99,))

    result = await handler.handle(
        ScanThreadsTask(fid=10, fname="bar", pn=1, rn=100, backfill=True, sort_mode="reply")
    )

    checkpoint = datastore.record_research_checkpoint.await_args.kwargs
    assert result.tids == (99,)
    assert handler._process_new_threads.await_args.args[1] == {99}
    assert checkpoint["status"] == "completed"
    assert checkpoint["scope_status"] == "incomplete"
    assert checkpoint["reason"] == "terminal_page_without_non_top_threads"


@pytest.mark.asyncio
async def test_reply_replay_unions_drifted_page_ids_without_changing_repeat_streak():
    threads = [make_thread(tid=tid) for tid in (6, 7, 8)]
    response = _make_reply_history_page(threads, pn=4, has_more=True)
    handler, _datastore = _make_reply_history_handler(response, new_tids=(7, 8))
    task = ScanThreadsTask(
        fid=10,
        fname="bar",
        pn=4,
        rn=100,
        backfill=True,
        sort_mode="reply",
        continuation_json={
            "_flow_seen_tids": list(range(1, 11)),
            "_flow_duplicate_streak": 2,
            "_resume_replay_page": 4,
            "_resume_page_seen_tids": [5, 6],
        },
    )

    result = await handler.handle(task)

    assert result.continuation_json["page_non_top_tids"] == [5, 6, 7, 8]
    assert result.continuation_json["flow_duplicate_streak"] == 2
    assert "high_duplicate_pause_reason" not in result.continuation_json


@pytest.mark.asyncio
async def test_threads_handler_dedupes_same_page_tid_before_persistence_and_dispatch():
    listed = make_thread(tid=77)
    listed.is_good = True
    duplicate = make_thread(tid=77)
    duplicate.is_good = False
    dto_response = make_threads_response([listed, duplicate])
    raw_thread = SimpleNamespace(
        tid=77,
        fid=10,
        pid=77,
        author_id=1,
        user=SimpleNamespace(user_id=1),
        is_top=False,
        is_good=True,
    )
    raw_duplicate = SimpleNamespace(
        tid=77,
        fid=10,
        pid=77,
        author_id=1,
        user=SimpleNamespace(user_id=1),
        is_top=False,
        is_good=False,
    )
    research_response = ResearchPage(
        raw=SimpleNamespace(objs=[raw_thread, raw_duplicate]),
        dto=dto_response,
        observed_at=datetime.now(),
        source_api="aiotieba.get_threads",
    )
    datastore = SimpleNamespace(filter_new_ids=AsyncMock(return_value={77}))
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(
            tb_client=SimpleNamespace(get_threads_dto=AsyncMock(return_value=research_response)),
            config=SimpleNamespace(research_enabled=False, fetch_user_forum_info=False),
        ),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler._persist_research_page = AsyncMock()
    handler._process_new_threads = AsyncMock()
    handler._process_old_threads = AsyncMock()

    result = await handler.handle(ScanThreadsTask(fid=10, fname="bar", pn=1, backfill=True))

    assert result.tids == (77,)
    assert datastore.filter_new_ids.await_args.args[1] == [77]
    assert dto_response.objs == [listed]
    assert dto_response.objs[0].is_good is True
    assert research_response.raw.objs == [raw_thread]
    assert handler._persist_research_page.await_args.args[0] is research_response
    assert handler._process_new_threads.await_args.args[0].objs == [listed]


@pytest.mark.asyncio
async def test_threads_handler_rejects_duplicate_tid_with_conflicting_identity():
    conflicting = make_thread(tid=77)
    conflicting.pid = 99
    response = make_threads_response([make_thread(tid=77), conflicting])
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(
            tb_client=SimpleNamespace(get_threads_dto=AsyncMock(return_value=response)),
            config=SimpleNamespace(research_enabled=False, fetch_user_forum_info=False),
        ),
        datastore=cast("Any", SimpleNamespace(filter_new_ids=AsyncMock(return_value=set()))),
        router=QueueRouter(),
    )
    handler._persist_research_page = AsyncMock()

    with pytest.raises(PageIntegrityError, match="conflicting core identity"):
        await handler.handle(ScanThreadsTask(fid=10, fname="bar", pn=1, backfill=True))

    handler._persist_research_page.assert_not_awaited()


@pytest.mark.parametrize("identity_field", ["fid", "pid", "author_id"])
@pytest.mark.parametrize(
    ("values", "top_flags"),
    [
        ((11, 0, 22), (False, True, False)),
        ((0, 11, 22), (False, False, False)),
    ],
)
def test_thread_list_dedupe_accumulates_known_identity_across_representative_changes(
    identity_field, values, top_flags
):
    items = []
    for value, is_top in zip(values, top_flags, strict=True):
        attrs = {
            "tid": 77,
            "fid": 10,
            "pid": 77,
            "author_id": 1,
            "user": SimpleNamespace(user_id=1),
            "is_top": is_top,
        }
        if identity_field == "fid":
            attrs["fid"] = value
        elif identity_field == "pid":
            attrs["pid"] = value
        else:
            attrs["author_id"] = value
            attrs["user"].user_id = value
        items.append(SimpleNamespace(**attrs))

    with pytest.raises(PageIntegrityError, match="conflicting core identity"):
        ThreadsTaskHandler._dedupe_thread_list_items(items, fname="bar", pn=1)


@pytest.mark.asyncio
async def test_thread_list_dedupe_shares_identity_constraints_between_dto_and_raw():
    listed = make_thread(tid=77)
    listed.author_id = 11
    listed.author.user_id = 11
    dto_response = make_threads_response([listed])
    raw_unknown_top = SimpleNamespace(
        tid=77,
        fid=10,
        pid=77,
        author_id=0,
        user=SimpleNamespace(user_id=0),
        is_top=True,
    )
    raw_conflicting_author = SimpleNamespace(
        tid=77,
        fid=10,
        pid=77,
        author_id=22,
        user=SimpleNamespace(user_id=22),
        is_top=False,
    )
    response = ResearchPage(
        raw=SimpleNamespace(objs=[raw_unknown_top, raw_conflicting_author]),
        dto=dto_response,
        observed_at=datetime.now(),
        source_api="aiotieba.get_threads",
    )
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(
            tb_client=SimpleNamespace(get_threads_dto=AsyncMock(return_value=response)),
            config=SimpleNamespace(research_enabled=False, fetch_user_forum_info=False),
        ),
        datastore=cast("Any", SimpleNamespace(filter_new_ids=AsyncMock(return_value=set()))),
        router=QueueRouter(),
    )
    handler._persist_research_page = AsyncMock()

    with pytest.raises(PageIntegrityError, match="conflicting core identity"):
        await handler.handle(ScanThreadsTask(fid=10, fname="bar", pn=1, backfill=True))

    handler._persist_research_page.assert_not_awaited()


@pytest.mark.asyncio
async def test_single_threads_worker_serves_monitor_before_next_backfill_page():
    """A bounded producer lets a single threads worker service hybrid monitor work."""
    first_page_started = asyncio.Event()
    release_first_page = asyncio.Event()
    requested: list[tuple[str, bool, int]] = []

    async def get_threads_dto(fname: str, **kwargs):
        pn = kwargs["pn"]
        is_good = kwargs["is_good"]
        requested.append((fname, is_good, pn))
        if fname == "backfill" and not is_good and pn == 1:
            first_page_started.set()
            await release_first_page.wait()
        if fname == "monitor":
            return make_threads_response([], current_page=1, total_page=1)
        thread = make_thread(
            tid=1000 + int(is_good) * 100 + pn,
            fname="backfill",
            create_time=datetime.fromtimestamp(100 - pn),
        )
        return make_threads_response(
            [thread],
            has_more=pn == 1,
            current_page=pn,
            total_page=2,
        )

    config = SimpleNamespace(
        mode="hybrid",
        max_backfill_pages=0,
        backfill_force_scan=False,
        research_enabled=False,
        research_max_api_retries=1,
        research_retry_wait_seconds=0,
    )
    container = create_mock_container(
        tb_client=SimpleNamespace(get_threads_dto=AsyncMock(side_effect=get_threads_dto)),
        config=config,
    )
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set()),
        get_threads_by_tids=AsyncMock(return_value=[]),
        get_pending_thread_scan_tids=AsyncMock(return_value=set()),
    )
    router = QueueRouter(maxsize=1)
    container.forums = [SimpleNamespace(fid=10, fname="backfill")]
    worker = Worker(0, router.threads_queue, container, cast("Any", datastore), router, lane="threads")
    scheduler = Scheduler(router, container)
    worker_task = asyncio.create_task(worker.run())
    try:
        scheduler_task = asyncio.create_task(scheduler._run_backfill())
        await asyncio.wait_for(first_page_started.wait(), timeout=1)
        await router.put(Task(priority=Priority.REALTIME, content=ScanThreadsTask(fid=99, fname="monitor", pn=1)))
        release_first_page.set()
        await asyncio.wait_for(scheduler_task, timeout=2)
    finally:
        release_first_page.set()
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)

    assert requested[0] == ("backfill", False, 1)
    assert requested[1] == ("monitor", False, 1)
    assert {call[:2] for call in requested if call[0] == "backfill"} == {
        ("backfill", False),
    }
    assert len([call for call in requested if call[0] == "backfill"]) == 2


@pytest.mark.asyncio
async def test_resume_replays_last_list_page_and_continues_after_page_shift():
    """The resumed list stream overlaps one page and tolerates boundary tids."""
    requested: list[tuple[bool, int]] = []

    async def get_threads_dto(_fname, **kwargs):
        pn = kwargs["pn"]
        is_good = kwargs["is_good"]
        requested.append((is_good, pn))
        if pn == 2:
            tids, times, has_more = (100, 101), (20, 10), True
        else:
            tids, times, has_more = (101, 102), (10, 5), False
        threads = [
            make_thread(tid=tid, create_time=datetime.fromtimestamp(created))
            for tid, created in zip(tids, times, strict=True)
        ]
        return make_threads_response(
            threads,
            has_more=has_more,
            current_page=pn,
            total_page=3,
        )

    config = SimpleNamespace(
        mode="backfill",
        max_backfill_pages=0,
        backfill_force_scan=False,
        research_enabled=False,
        fetch_user_forum_info=False,
    )
    container = create_mock_container(
        tb_client=SimpleNamespace(get_threads_dto=AsyncMock(side_effect=get_threads_dto)),
        config=config,
    )
    container.forums = [SimpleNamespace(fid=10, fname="bar")]
    container.research_run_id = "synthetic-run"
    datastore = SimpleNamespace(
        get_research_list_checkpoint_states=AsyncMock(
            return_value={(10, False): (2, "running", {}), (10, True): (None, "completed", {})}
        ),
        set_research_run_status_if=AsyncMock(),
        filter_new_ids=AsyncMock(return_value=set()),
        get_threads_by_tids=AsyncMock(
            side_effect=lambda tids: [
                ThreadModel(tid=tid, fid=10, reply_num=1, last_time=datetime.fromtimestamp(1)) for tid in tids
            ]
        ),
        get_pending_thread_scan_tids=AsyncMock(side_effect=lambda tids: set(tids)),
    )
    router = QueueRouter(maxsize=1)
    worker = Worker(0, router.threads_queue, container, cast("Any", datastore), router, lane="threads")
    scheduler = Scheduler(router, container, cast("Any", datastore))
    worker_task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(scheduler._run_backfill(), timeout=2)
    finally:
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)

    assert requested == [(False, 2), (False, 3)]


@pytest.mark.asyncio
async def test_list_page_overlap_records_duplicate_tids_without_stopping_stream():
    previous_cursor = {
        "_flow_seen_tids": [100, 101],
        "_flow_duplicate_streak": 0,
        "recent_non_top_tids": [100, 101],
        "recent_page_fingerprints": [[100, 101]],
        "last_create_time": datetime.fromtimestamp(10).isoformat(),
    }
    response = make_threads_response(
        [
            make_thread(tid=101, create_time=datetime.fromtimestamp(10)),
            make_thread(tid=102, create_time=datetime.fromtimestamp(5)),
        ],
        has_more=True,
        current_page=3,
        total_page=4,
    )
    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set()),
        get_threads_by_tids=AsyncMock(
            return_value=[
                ThreadModel(tid=101, fid=10, reply_num=1, last_time=datetime.fromtimestamp(1)),
                ThreadModel(tid=102, fid=10, reply_num=1, last_time=datetime.fromtimestamp(1)),
            ]
        ),
        get_pending_thread_scan_tids=AsyncMock(return_value={101, 102}),
    )
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(
            tb_client=tb_client,
            config=SimpleNamespace(research_enabled=False, fetch_user_forum_info=False),
        ),
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )

    result = await handler.handle(
        ScanThreadsTask(fid=10, fname="bar", pn=3, backfill=True, continuation_json=previous_cursor)
    )

    assert result.has_more is True
    assert result.continuation_json is not None
    assert result.continuation_json["page_duplicate_tids"] == [101]


@pytest.mark.asyncio
async def test_replayed_list_page_unions_new_ids_without_incrementing_overlap_streak():
    response = make_threads_response(
        [
            make_thread(tid=102, create_time=datetime.fromtimestamp(10)),
            make_thread(tid=103, create_time=datetime.fromtimestamp(5)),
        ],
        has_more=True,
        current_page=4,
        total_page=6,
    )
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set()),
        record_research_checkpoint=AsyncMock(return_value=True),
    )
    container = create_mock_container(
        tb_client=SimpleNamespace(get_threads_dto=AsyncMock(return_value=response)),
        config=SimpleNamespace(research_enabled=False, fetch_user_forum_info=False),
    )
    container.research_run_id = "synthetic-replay-run"
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=container,
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler._persist_research_page = AsyncMock()
    handler._process_new_threads = AsyncMock()
    handler._process_old_threads = AsyncMock()

    result = await handler.handle(
        ScanThreadsTask(
            fid=10,
            fname="bar",
            pn=4,
            backfill=True,
            continuation_json={
                "_flow_seen_tids": [100, 101, 102],
                "_flow_duplicate_streak": 3,
                "_resume_replay_page": 4,
                "_resume_page_seen_tids": [101, 102],
                "page_non_top_tids": [101, 102],
            },
        )
    )

    cursor = result.continuation_json
    assert cursor["page_non_top_tids"] == [101, 102, 103]
    assert cursor["flow_duplicate_streak"] == 3
    assert "page_duplicate_ratio" not in cursor
    assert "high_duplicate_pause_reason" not in cursor
    checkpoint = datastore.record_research_checkpoint.await_args.kwargs
    assert checkpoint["status"] == "completed"
    assert checkpoint["scope_status"] == "running"


@pytest.mark.asyncio
async def test_thread_list_checkpoint_is_committed_after_all_page_side_effects():
    response = make_threads_response([make_thread(tid=88)])
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={88}),
        record_research_checkpoint=AsyncMock(return_value=True),
    )
    container = create_mock_container(
        tb_client=SimpleNamespace(get_threads_dto=AsyncMock(return_value=response)),
        config=SimpleNamespace(research_enabled=False, fetch_user_forum_info=False),
    )
    container.research_run_id = "synthetic-side-effect-run"
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=container,
        datastore=cast("Any", datastore),
        router=QueueRouter(),
    )
    handler._persist_research_page = AsyncMock()
    handler._process_new_threads = AsyncMock()
    handler._process_old_threads = AsyncMock()
    handler._enqueue_missing_web_targets = AsyncMock(side_effect=RuntimeError("synthetic sidecar failure"))

    with pytest.raises(RuntimeError, match="synthetic sidecar failure"):
        await handler.handle(ScanThreadsTask(fid=10, fname="bar", pn=1, backfill=True))

    datastore.record_research_checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_threads_handler_keeps_livepost():
    """直播帖仍属于回溯总体，不应被列表处理器过滤。"""
    normal_thread = make_thread(tid=1, reply_num=5, is_livepost=False)
    live_thread = make_thread(tid=2, reply_num=5, is_livepost=True)
    threads_response = make_threads_response([normal_thread, live_thread])

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={1, 2}),
        save_items=AsyncMock(),
        save_threads_and_pending_scans=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
    )
    router = QueueRouter()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(ScanThreadsTask(fid=10, fname="bar", pn=1, backfill=True))

    thread_calls = [call for call in datastore.filter_new_ids.await_args_list if call.args[0] == "thread"]
    assert thread_calls[0].args[1] == [1, 2]
    saved = datastore.save_threads_and_pending_scans.await_args.args[0]
    assert {thread.tid for thread in saved} == {1, 2}


@pytest.mark.asyncio
async def test_threads_handler_force_mode_enables_thread_upsert():
    """测试 ThreadsTaskHandler 在 force 模式下会启用 thread 元数据 upsert。"""
    thread = make_thread(tid=1, reply_num=5)
    threads_response = make_threads_response([thread])

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set()),
        save_items=AsyncMock(),
        save_threads_and_pending_scans=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
    )
    router = QueueRouter()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1, force=True))

    datastore.save_threads_and_pending_scans.assert_awaited_once()
    atomic_call = datastore.save_threads_and_pending_scans.await_args
    assert atomic_call.kwargs.get("upsert_threads") is True

    # force 模式不应走 thread 的 filter_new_ids 分流（但 ensure_users 仍会查询 user）
    assert not any(call.args and call.args[0] == "thread" for call in datastore.filter_new_ids.await_args_list)


# ==================== FullScanPostsTaskHandler 测试 ====================


@pytest.mark.asyncio
async def test_full_scan_posts_handler_process_page():
    """测试 FullScanPostsTaskHandler 处理回复页面"""
    post = make_post(pid=100, tid=1, floor=2, reply_num=0)
    posts_response = make_posts_response([post])

    tb_client = SimpleNamespace(get_posts_dto=AsyncMock(return_value=posts_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={post.pid}),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
        remove_pending_scan=AsyncMock(),
        add_pending_comment_scan=AsyncMock(),
    )
    router = QueueRouter()
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(FullScanPostsTask(tid=post.tid))

    # 检查是否保存了回复
    post_save_calls = [
        call.args[0]
        for call in datastore.save_items.await_args_list
        if call.args and isinstance(call.args[0], list) and call.args[0] and isinstance(call.args[0][0], PostModel)
    ]
    assert post_save_calls, "Expected post models to be persisted"
    assert post_save_calls[0][0].pid == post.pid

    # 检查是否推送了事件
    datastore.push_object_event.assert_awaited_once_with("post", post)


@pytest.mark.asyncio
async def test_full_scan_posts_handler_generates_comment_task():
    """测试 FullScanPostsTaskHandler 在楼中楼超过10条时生成扫描任务"""
    # 回复有超过10条楼中楼
    post = make_post(pid=100, tid=1, floor=2, reply_num=15)
    posts_response = make_posts_response([post])

    tb_client = SimpleNamespace(get_posts_dto=AsyncMock(return_value=posts_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={post.pid}),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
        remove_pending_scan=AsyncMock(),
        add_pending_comment_scan=AsyncMock(),
    )
    router = QueueRouter()
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(FullScanPostsTask(tid=post.tid))

    # 应该生成 FullScanCommentsTask
    queued_tasks = _drain_router(router)
    comment_tasks = [t for t in queued_tasks if isinstance(t.content, FullScanCommentsTask)]
    assert len(comment_tasks) == 1
    assert isinstance(comment_tasks[0].content, FullScanCommentsTask)
    assert comment_tasks[0].content.pid == post.pid


@pytest.mark.asyncio
async def test_full_scan_posts_handler_removes_pending_marker():
    """测试 FullScanPostsTaskHandler 在完成后移除 pending_scan 标记"""
    post = make_post(pid=100, tid=1, floor=2)
    posts_response = make_posts_response([post])

    tb_client = SimpleNamespace(get_posts_dto=AsyncMock(return_value=posts_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set()),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
        remove_pending_thread_scan=AsyncMock(),
        add_pending_comment_scan=AsyncMock(),
    )
    router = QueueRouter()
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(FullScanPostsTask(tid=1))

    # _finalize_thread 应该移除 pending_scan 标记
    datastore.remove_pending_thread_scan.assert_awaited_with(1)


# ==================== IncrementalScanPostsTaskHandler 测试 ====================


@pytest.mark.asyncio
async def test_incremental_scan_posts_handler():
    """测试 IncrementalScanPostsTaskHandler 增量扫描"""
    stored_last_time = datetime.fromtimestamp(1)
    new_post_time = datetime.fromtimestamp(100)

    # 新回复（时间戳大于 stored_last_time）
    new_post = make_post(pid=200, tid=1, floor=3, create_time=new_post_time)
    posts_response = make_posts_response([new_post], current_page=1, total_page=1)

    async def mock_get_posts_dto(*args, **kwargs):
        return posts_response

    tb_client = SimpleNamespace(get_posts_dto=AsyncMock(side_effect=mock_get_posts_dto))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={new_post.pid}),
        save_items=AsyncMock(),
        get_posts_by_pids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
        update_thread_metadata=AsyncMock(),
        add_pending_comment_scan=AsyncMock(),
    )

    class MockConfig:
        deep_scan_enabled = False
        deep_scan_depth = 3
        research_enabled = False

    router = QueueRouter()
    handler = IncrementalScanPostsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client, config=MockConfig()),
        datastore=cast("Any", datastore),
        router=router,
    )

    target_last_time = datetime.fromtimestamp(200)
    await handler.handle(
        IncrementalScanPostsTask(
            tid=1,
            stored_last_time=stored_last_time,
            stored_reply_num=5,
            target_last_time=target_last_time,
            target_reply_num=10,
        )
    )

    # 应该更新主题帖元数据
    datastore.update_thread_metadata.assert_awaited()


# ==================== FullScanCommentsTaskHandler 测试 ====================


@pytest.mark.asyncio
async def test_full_scan_comments_handler():
    """测试 FullScanCommentsTaskHandler 全量扫描楼中楼"""
    comment = make_comment(cid=1000, pid=100, tid=1)
    comments_response = make_comments_response([comment])

    tb_client = SimpleNamespace(get_comments_dto=AsyncMock(return_value=comments_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={comment.cid}),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
        remove_pending_comment_scan=AsyncMock(),
    )
    router = QueueRouter()
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(FullScanCommentsTask(tid=1, pid=100))

    # 应该推送楼中楼事件
    datastore.push_object_event.assert_awaited_with("comment", comment)
    datastore.remove_pending_comment_scan.assert_awaited_once_with(1, 100)


@pytest.mark.asyncio
async def test_full_scan_comments_handler_backfill_no_push():
    """测试 FullScanCommentsTaskHandler 回溯模式不推送事件"""
    comment = make_comment(cid=1000, pid=100, tid=1)
    comments_response = make_comments_response([comment])

    tb_client = SimpleNamespace(get_comments_dto=AsyncMock(return_value=comments_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={comment.cid}),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
        remove_pending_comment_scan=AsyncMock(),
    )
    router = QueueRouter()
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(FullScanCommentsTask(tid=1, pid=100, backfill=True))

    # 回溯模式不应该推送事件
    datastore.push_object_event.assert_not_called()
    datastore.remove_pending_comment_scan.assert_awaited_once_with(1, 100)


# ==================== IncrementalScanCommentsTaskHandler 测试 ====================


@pytest.mark.asyncio
async def test_incremental_scan_comments_handler():
    """测试 IncrementalScanCommentsTaskHandler 增量扫描楼中楼"""
    new_comment = make_comment(cid=2000, pid=100, tid=1)
    old_comment = make_comment(cid=1000, pid=100, tid=1)
    comments_response = make_comments_response([new_comment, old_comment], current_page=1, total_page=1)

    tb_client = SimpleNamespace(get_comments_dto=AsyncMock(return_value=comments_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={new_comment.cid}),  # 只有 cid=2000 是新的
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
        remove_pending_comment_scan=AsyncMock(),
    )
    router = QueueRouter()
    handler = IncrementalScanCommentsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    await handler.handle(IncrementalScanCommentsTask(tid=1, pid=100))

    # 应该只推送新楼中楼
    datastore.push_object_event.assert_awaited_with("comment", new_comment)
    # 增量扫描只应清理 incremental 标记，避免误删 full 标记
    datastore.remove_pending_comment_scan.assert_awaited_once_with(1, 100, task_kind="incremental")


@pytest.mark.asyncio
async def test_deep_scan_schedules_incremental_comment_task_with_pending_marker():
    """测试 DeepScan 在生成 IncrementalScanCommentsTask 前会写入 pending_comment_scan。"""
    post = make_post(pid=100, tid=1, floor=2, reply_num=12, comments=[])
    stored_post = SimpleNamespace(pid=100, reply_num=1)

    datastore = SimpleNamespace(
        get_posts_by_pids=AsyncMock(return_value=[stored_post]),
        add_pending_comment_scan=AsyncMock(),
        save_items=AsyncMock(),
    )
    router = QueueRouter()
    handler = DeepScanTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=SimpleNamespace()),
        datastore=cast("Any", datastore),
        router=router,
    )

    updates_found = await handler._check_posts_for_comment_updates([post])

    assert updates_found == 0
    datastore.add_pending_comment_scan.assert_awaited_once_with(
        tid=post.tid,
        pid=post.pid,
        fid=post.fid,
        fname=post.fname,
        backfill=False,
        task_kind="incremental",
    )

    queued_tasks = _drain_router(router)
    incremental_tasks = [t for t in queued_tasks if isinstance(t.content, IncrementalScanCommentsTask)]
    assert len(incremental_tasks) == 1
    assert isinstance(incremental_tasks[0].content, IncrementalScanCommentsTask)
    assert incremental_tasks[0].content.tid == post.tid
    assert incremental_tasks[0].content.pid == post.pid


# ==================== Worker 测试 ====================


@pytest.mark.asyncio
async def test_worker_processes_task():
    """测试 Worker 处理任务"""
    thread = make_thread(tid=1, reply_num=0)
    threads_response = make_threads_response([thread])

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={thread.tid}),
        save_items=AsyncMock(),
        save_threads_and_pending_scans=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
    )
    router = QueueRouter()
    queue = router.threads_queue

    worker = Worker(
        worker_id=0,
        queue=queue,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
        lane="threads",
    )

    # 添加任务到队列
    from src.scraper.tasks import Priority

    task = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1))
    await queue.put(task)

    # 启动 worker 并在处理完一个任务后取消
    async def run_worker_briefly():
        worker_task = asyncio.create_task(worker.run())
        # 等待任务被处理
        await asyncio.sleep(0.1)
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    await run_worker_briefly()

    # 验证任务被处理
    tb_client.get_threads_dto.assert_awaited()


@pytest.mark.asyncio
async def test_worker_memory_lock():
    """测试 LockManager 锁功能"""
    tb_client = SimpleNamespace()
    datastore = SimpleNamespace()
    router = QueueRouter()

    # 创建一个 handler 来测试锁
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        router=router,
    )

    # 第一次获取锁应该成功
    lock_key = "test:lock:1"
    assert await handler.try_acquire_lock(lock_key, ttl=10)

    # 第二次获取同一个锁应该失败
    assert not await handler.try_acquire_lock(lock_key, ttl=10)

    # 释放锁后应该可以重新获取
    await handler.release_lock(lock_key)
    assert await handler.try_acquire_lock(lock_key, ttl=10)

    # 清理
    await handler.release_lock(lock_key)
