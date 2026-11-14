"""调度器和任务模块测试。"""

import asyncio
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from conftest import DummyConfig, DummyContainer, DummyForum

from src.scraper.queue import UniquePriorityQueue
from src.scraper.router import QueueRouter
from src.scraper.scheduler import BackfillIncompleteError, Scheduler
from src.scraper.tasks import (
    DeepScanTask,
    FullScanCommentsTask,
    FullScanPostsTask,
    IncrementalScanCommentsTask,
    IncrementalScanPostsTask,
    Priority,
    ScanThreadsTask,
    Task,
    ThreadPageResult,
)

# ==================== Task 优先级测试 ====================


def test_task_priority_ordering():
    """测试任务优先级排序"""
    t_high = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=1))
    t_medium = Task(priority=Priority.MEDIUM, content=FullScanPostsTask(tid=1))
    t_low = Task(priority=Priority.LOW, content=FullScanCommentsTask(tid=1, pid=1))
    t_backfill = Task(
        priority=Priority.BACKFILL_THREADS, content=ScanThreadsTask(fid=1, fname="x", pn=2, backfill=True)
    )

    tasks = sorted([t_backfill, t_low, t_medium, t_high])
    assert [t.priority for t in tasks] == [Priority.HIGH, Priority.MEDIUM, Priority.LOW, Priority.BACKFILL_THREADS]


def test_task_same_priority_fifo():
    """测试相同优先级的任务按创建顺序排序"""
    t1 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=1))
    t2 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=2))
    t3 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=3))

    tasks = sorted([t3, t1, t2])
    # 按序列号排序，t1 应该最先
    assert isinstance(tasks[0].content, ScanThreadsTask)
    assert tasks[0].content.pn == 1
    assert isinstance(tasks[1].content, ScanThreadsTask)
    assert tasks[1].content.pn == 2
    assert isinstance(tasks[2].content, ScanThreadsTask)
    assert tasks[2].content.pn == 3


def test_task_unique_key():
    """测试任务唯一键生成"""
    t1 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=1))
    t2 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=1))  # 相同内容
    t3 = Task(priority=Priority.MEDIUM, content=ScanThreadsTask(fid=1, fname="x", pn=1))  # 不同优先级

    # 相同优先级和内容应该有相同的 unique_key
    assert t1.unique_key == t2.unique_key
    # 不同优先级应该有不同的 unique_key
    assert t1.unique_key != t3.unique_key


# ==================== UniquePriorityQueue 测试 ====================


@pytest.mark.asyncio
async def test_unique_priority_queue_dedup():
    """测试去重优先队列的去重功能"""
    q = UniquePriorityQueue(lane="test")

    t1 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=1))
    t2 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=1))  # 重复

    await q.put(t1)
    await q.put(t2)  # 应该被去重跳过

    assert q.qsize() == 1


@pytest.mark.asyncio
async def test_unique_priority_queue_priority_order():
    """测试去重优先队列的优先级排序"""
    q = UniquePriorityQueue(lane="test")

    t_low = Task(priority=Priority.LOW, content=ScanThreadsTask(fid=1, fname="x", pn=1))
    t_high = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=2, fname="y", pn=1))

    await q.put(t_low)
    await q.put(t_high)

    # 高优先级应该先出队
    first = await q.get()
    assert first.priority == Priority.HIGH
    q.task_done(first)


@pytest.mark.asyncio
async def test_unique_priority_queue_allows_after_get():
    """测试出队后可以重新入队相同任务"""
    q = UniquePriorityQueue(lane="test")

    t1 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=1))

    await q.put(t1)
    got = await q.get()

    # 处理中（未 task_done）期间不应允许重复入队
    t2 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=1))
    await q.put(t2)
    assert q.qsize() == 0

    q.task_done(got)

    # 出队后应该可以重新入队
    t3 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=1))
    await q.put(t3)
    assert q.qsize() == 1


@pytest.mark.asyncio
async def test_unique_priority_queue_cancelled_put_rolls_back_key():
    """被取消的 put 不应污染去重集合"""
    q = UniquePriorityQueue(lane="test", maxsize=1)

    t1 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=1))
    t2 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=2, fname="y", pn=1))

    await q.put(t1)

    blocked_put = asyncio.create_task(q.put(t2))
    await asyncio.sleep(0)
    blocked_put.cancel()

    with pytest.raises(asyncio.CancelledError):
        await blocked_put

    got = await q.get()
    q.task_done(got)

    await q.put(t2)
    assert q.qsize() == 1


# ==================== Scheduler 测试 ====================


async def _consume_backfill_pages(scheduler: Scheduler, on_page):
    producer = asyncio.create_task(scheduler._run_backfill())
    try:
        while not producer.done():
            try:
                task = await asyncio.wait_for(scheduler.router.threads_queue.get(), timeout=0.2)
            except TimeoutError:
                break
            try:
                assert isinstance(task.content, ScanThreadsTask)
                await on_page(task.content)
            finally:
                scheduler.router.threads_queue.task_done(task)
        await producer
    finally:
        if not producer.done():
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)


def _synthetic_page(content: ScanThreadsTask, *, tids: tuple[int, ...], times: tuple[int, ...], has_more: bool):
    continuation = {}
    if times:
        continuation["last_create_time"] = datetime.fromtimestamp(times[-1]).isoformat()
    return ThreadPageResult(
        has_more=has_more,
        current_page=content.pn,
        total_pages=content.pn + int(has_more),
        total_count=len(tids),
        tids=tids,
        non_top_create_times=tuple(datetime.fromtimestamp(value) for value in times),
        continuation_json=continuation,
    )


@pytest.mark.asyncio
async def test_scheduler_round_robins_both_streams_from_create_page_one_in_hybrid():
    """Hybrid mode still starts both CREATE streams at page one."""
    router = QueueRouter(maxsize=1)
    container = DummyContainer(config=DummyConfig(mode="hybrid", max_backfill_pages=0), forums=[DummyForum(1, "bar")])
    scheduler = Scheduler(router, container)  # type: ignore[arg-type]
    requested: list[tuple[bool, int]] = []

    async def acknowledge(content: ScanThreadsTask):
        requested.append((content.is_good, content.pn))
        result = _synthetic_page(
            content,
            tids=(content.pn + int(content.is_good) * 10,),
            times=(100 - content.pn * 10,),
            has_more=content.pn == 1,
        )
        assert content.max_pages == 0
        assert content.completion is not None
        content.completion.set_result(result)

    await _consume_backfill_pages(scheduler, acknowledge)

    assert requested == [(False, 1), (True, 1), (False, 2), (True, 2)]


@pytest.mark.asyncio
async def test_scheduler_stream_failure_does_not_skip_good_stream():
    router = QueueRouter(maxsize=1)
    container = DummyContainer(config=DummyConfig(mode="backfill", max_backfill_pages=0), forums=[DummyForum(1, "bar")])
    scheduler = Scheduler(router, container)  # type: ignore[arg-type]
    requested: list[bool] = []

    async def acknowledge(content: ScanThreadsTask):
        requested.append(content.is_good)
        assert content.completion is not None
        if not content.is_good:
            content.completion.set_exception(RuntimeError("synthetic list error"))
        else:
            content.completion.set_result(_synthetic_page(content, tids=(20,), times=(10,), has_more=False))

    with pytest.raises(BackfillIncompleteError):
        await _consume_backfill_pages(scheduler, acknowledge)

    assert requested == [False, True]


@pytest.mark.asyncio
async def test_scheduler_preserves_rate_limit_pause_when_stream_fails():
    class RateLimitedError(Exception):
        code = 429

    router = QueueRouter(maxsize=1)
    container = DummyContainer(config=DummyConfig(mode="backfill", max_backfill_pages=0), forums=[DummyForum(1, "bar")])
    container.research_run_id = "synthetic-run"
    run_status = {"value": "running"}
    recorded = []
    requested = []

    async def set_status_if(_run_id, expected_statuses, status, _reason=None):
        if run_status["value"] not in expected_statuses:
            return False
        run_status["value"] = status
        return True

    async def scan_page(_forum, pn, _max_pages, is_good, _continuation=None):
        requested.append(is_good)
        if not is_good:
            run_status["value"] = "paused_rate_limit"
            raise RateLimitedError("synthetic 429")
        return ThreadPageResult(
            has_more=False,
            current_page=pn,
            total_pages=pn,
            total_count=0,
            tids=(),
            non_top_create_times=(),
        )

    datastore = SimpleNamespace(
        get_research_list_checkpoint_states=AsyncMock(return_value={}),
        record_research_checkpoint=AsyncMock(side_effect=lambda **kwargs: recorded.append(kwargs)),
        set_research_run_status_if=AsyncMock(side_effect=set_status_if),
    )
    scheduler = Scheduler(router, container, cast("Any", datastore))  # type: ignore[arg-type]
    scheduler._scan_backfill_page = scan_page  # type: ignore[method-assign]

    with pytest.raises(BackfillIncompleteError):
        await scheduler._run_backfill()

    assert recorded[0]["status"] == "paused_rate_limit"
    assert recorded[0]["scope_status"] == "paused_rate_limit"
    assert run_status["value"] == "paused_rate_limit"
    assert requested == [False]


@pytest.mark.asyncio
async def test_scheduler_leaves_run_resumable_after_list_pages_before_detail_drain():
    router = QueueRouter(maxsize=1)
    container = DummyContainer(config=DummyConfig(mode="backfill", max_backfill_pages=0), forums=[DummyForum(1, "bar")])
    container.research_run_id = "synthetic-run"
    set_status = AsyncMock(return_value=True)
    datastore = SimpleNamespace(
        get_research_list_checkpoint_states=AsyncMock(return_value={}),
        set_research_run_status_if=set_status,
    )
    scheduler = Scheduler(router, container, cast("Any", datastore))  # type: ignore[arg-type]

    async def terminal_list_page(_forum, pn, _max_pages, _is_good, _continuation=None):
        return ThreadPageResult(
            has_more=False,
            current_page=pn,
            total_pages=pn,
            total_count=0,
            tids=(),
            non_top_create_times=(),
        )

    scheduler._scan_backfill_page = terminal_list_page  # type: ignore[method-assign]

    await scheduler._run_backfill()

    set_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_scheduler_duplicate_tid_overlap_does_not_stop_page_continuation():
    router = QueueRouter(maxsize=1)
    container = DummyContainer(config=DummyConfig(mode="backfill", max_backfill_pages=0), forums=[DummyForum(1, "bar")])
    scheduler = Scheduler(router, container)  # type: ignore[arg-type]
    requested: list[tuple[bool, int]] = []

    async def acknowledge(content: ScanThreadsTask):
        requested.append((content.is_good, content.pn))
        assert content.completion is not None
        if content.is_good:
            page = _synthetic_page(content, tids=(900,), times=(5,), has_more=False)
        elif content.pn == 1:
            page = _synthetic_page(content, tids=(1, 2), times=(20, 10), has_more=True)
        else:
            page = _synthetic_page(content, tids=(2, 3), times=(10, 5), has_more=False)
        content.completion.set_result(page)

    await _consume_backfill_pages(scheduler, acknowledge)

    assert requested == [(False, 1), (True, 1), (False, 2)]


@pytest.mark.asyncio
async def test_scheduler_cross_page_create_time_reversal_marks_stream_incomplete():
    router = QueueRouter(maxsize=1)
    container = DummyContainer(config=DummyConfig(mode="backfill", max_backfill_pages=0), forums=[DummyForum(1, "bar")])
    scheduler = Scheduler(router, container)  # type: ignore[arg-type]
    requested: list[tuple[bool, int]] = []

    async def acknowledge(content: ScanThreadsTask):
        requested.append((content.is_good, content.pn))
        assert content.completion is not None
        if content.is_good:
            page = _synthetic_page(content, tids=(900,), times=(5,), has_more=False)
        else:
            times = (20,) if content.pn == 1 else (21,)
            page = _synthetic_page(content, tids=(content.pn,), times=times, has_more=True)
        content.completion.set_result(page)

    with pytest.raises(BackfillIncompleteError):
        await _consume_backfill_pages(scheduler, acknowledge)

    assert requested == [(False, 1), (True, 1), (False, 2)]


@pytest.mark.asyncio
async def test_scheduler_finite_page_limit_is_reported_as_paused_limit():
    router = QueueRouter(maxsize=1)
    container = DummyContainer(config=DummyConfig(mode="backfill", max_backfill_pages=1), forums=[DummyForum(1, "bar")])
    scheduler = Scheduler(router, container)  # type: ignore[arg-type]
    requested: list[bool] = []

    async def acknowledge(content: ScanThreadsTask):
        requested.append(content.is_good)
        assert content.completion is not None
        content.completion.set_result(_synthetic_page(content, tids=(content.pn,), times=(10,), has_more=True))

    await _consume_backfill_pages(scheduler, acknowledge)

    assert requested == [False, True]


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_scheduler_homepage_scans():
    """测试首页扫描任务生成"""
    router = QueueRouter()
    forums = [DummyForum(1, "bar"), DummyForum(2, "baz")]
    container = DummyContainer(config=DummyConfig(), forums=forums)
    s = Scheduler(router, container)  # type: ignore[arg-type]

    await s._schedule_homepage_scans(forums, is_good=False)  # type: ignore[arg-type]

    q = router.threads_queue
    assert q.qsize() == 2
    tasks = []
    while not q.empty():
        tasks.append(await q.get())

    assert all(isinstance(t.content, ScanThreadsTask) for t in tasks)
    assert all(t.priority == Priority.REALTIME for t in tasks)
    assert {t.content.fname for t in tasks} == {"bar", "baz"}


@pytest.mark.asyncio
async def test_scheduler_stop_event():
    """测试调度器停止事件"""
    router = QueueRouter()
    container = DummyContainer(config=DummyConfig(scheduler_interval_seconds=10), forums=[DummyForum(1, "bar")])
    s = Scheduler(router, container)  # type: ignore[arg-type]

    assert not s.is_stopped
    s.stop()
    assert s.is_stopped


@pytest.mark.asyncio
async def test_scheduler_backpressure_uses_threads_lane_only():
    """背压应只看 threads 通道深度，不受 comments 通道堆积影响"""
    router = QueueRouter()
    container = DummyContainer(
        config=DummyConfig(
            queue_depth_threshold=1,
            skip_wait_seconds=1,
            scheduler_interval_seconds=1,
            good_page_every_ticks=2,
        ),
        forums=[DummyForum(1, "bar")],
    )
    s = Scheduler(router, container)  # type: ignore[arg-type]

    # 仅填满 comments 通道
    await router.comments_queue.put(Task(priority=Priority.LOW, content=FullScanCommentsTask(tid=1, pid=1)))

    scheduled_once = asyncio.Event()

    async def _mock_schedule(*args, **kwargs):
        scheduled_once.set()
        s.stop()

    s._schedule_homepage_scans = AsyncMock(side_effect=_mock_schedule)  # type: ignore[method-assign]

    await s._run_loop(lambda: cast("Any", container.forums), 1, "Default")

    assert scheduled_once.is_set()


@pytest.mark.asyncio
async def test_scheduler_backpressure_blocks_when_threads_lane_full():
    """threads 通道达到阈值时应触发背压并跳过调度"""
    router = QueueRouter()
    container = DummyContainer(
        config=DummyConfig(
            queue_depth_threshold=1,
            skip_wait_seconds=1,
            scheduler_interval_seconds=1,
            good_page_every_ticks=2,
        ),
        forums=[DummyForum(1, "bar")],
    )
    s = Scheduler(router, container)  # type: ignore[arg-type]

    # 填满 threads 通道
    await router.threads_queue.put(Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="bar", pn=1)))

    s._schedule_homepage_scans = AsyncMock()  # type: ignore[method-assign]

    loop_task = asyncio.create_task(s._run_loop(lambda: cast("Any", container.forums), 1, "Default"))
    await asyncio.sleep(0.05)
    s.stop()
    await loop_task

    s._schedule_homepage_scans.assert_not_awaited()


# ==================== 各种任务类型的 unique_key 测试 ====================


def test_scan_threads_task_unique_key():
    """测试 ScanThreadsTask 的 unique_key"""
    t1 = ScanThreadsTask(fid=1, fname="x", pn=1, is_good=False)
    t2 = ScanThreadsTask(fid=1, fname="x", pn=1, is_good=True)
    t3 = ScanThreadsTask(fid=1, fname="x", pn=2, is_good=False)

    # 相同 fid, pn, is_good 应该相同
    assert t1.unique_key == (1, 1, False)
    assert t2.unique_key == (1, 1, True)
    assert t3.unique_key == (1, 2, False)


def test_full_scan_posts_task_unique_key():
    """测试 FullScanPostsTask 的 unique_key"""
    t1 = FullScanPostsTask(tid=100)
    t2 = FullScanPostsTask(tid=100, backfill=True)
    t3 = FullScanPostsTask(tid=200)

    # unique_key 只基于 tid
    assert t1.unique_key == 100
    assert t2.unique_key == 100
    assert t3.unique_key == 200


def test_incremental_scan_posts_task_unique_key():
    """测试 IncrementalScanPostsTask 的 unique_key"""
    from datetime import datetime

    t1 = IncrementalScanPostsTask(tid=100, stored_last_time=datetime.now(), stored_reply_num=10)
    t2 = IncrementalScanPostsTask(tid=200, stored_last_time=datetime.now(), stored_reply_num=20)

    assert t1.unique_key == 100
    assert t2.unique_key == 200


def test_full_scan_comments_task_unique_key():
    """测试 FullScanCommentsTask 的 unique_key"""
    t1 = FullScanCommentsTask(tid=1, pid=100)
    t2 = FullScanCommentsTask(tid=1, pid=200)
    t3 = FullScanCommentsTask(tid=2, pid=100)

    # unique_key 基于 (tid, pid)
    assert t1.unique_key == (1, 100)
    assert t2.unique_key == (1, 200)
    assert t3.unique_key == (2, 100)


def test_incremental_scan_comments_task_unique_key():
    """测试 IncrementalScanCommentsTask 的 unique_key"""
    t1 = IncrementalScanCommentsTask(tid=1, pid=100)
    t2 = IncrementalScanCommentsTask(tid=1, pid=100, backfill=True)

    # unique_key 基于 (tid, pid)，不包含 backfill
    assert t1.unique_key == (1, 100)
    assert t2.unique_key == (1, 100)


def test_deep_scan_task_unique_key():
    """测试 DeepScanTask 的 unique_key"""
    t1 = DeepScanTask(tid=100, total_pages=10, depth=3)
    t2 = DeepScanTask(tid=100, total_pages=20, depth=5)
    t3 = DeepScanTask(tid=200, total_pages=10, depth=3)

    # unique_key 只基于 tid
    assert t1.unique_key == 100
    assert t2.unique_key == 100
    assert t3.unique_key == 200
