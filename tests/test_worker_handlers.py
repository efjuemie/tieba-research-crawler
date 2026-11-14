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
from tiebameow.models.orm import Post as PostModel
from tiebameow.models.orm import Thread as ThreadModel

from src.core.lock import MemoryLockManager
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


def create_mock_container(tb_client=None, redis_client=None, config=None):
    lock_manager = MemoryLockManager()
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
    )
    saved_threads = datastore.save_threads_and_pending_scans.await_args.args[0]
    assert {thread.tid for thread in saved_threads} == {10, 11, 12}


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
        if fname == "backfill" and not is_good and pn == 2:
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
            has_more=pn == 2,
            current_page=pn,
            total_page=3,
        )

    config = SimpleNamespace(mode="hybrid", max_backfill_pages=0, backfill_force_scan=False)
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

    assert requested[0] == ("backfill", False, 2)
    assert requested[1] == ("monitor", False, 1)
    assert {call[:2] for call in requested if call[0] == "backfill"} == {
        ("backfill", False),
        ("backfill", True),
    }
    assert len([call for call in requested if call[0] == "backfill"]) == 4


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
