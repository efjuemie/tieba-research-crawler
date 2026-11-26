"""调度器和任务模块测试。"""

import asyncio
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from conftest import DummyConfig, DummyContainer, DummyForum

from src.core.models import research_thread_list_task_key
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
async def test_scheduler_uses_one_unified_create_stream_in_hybrid():
    """Hybrid mode starts one CREATE stream and leaves thread flags to the response."""
    router = QueueRouter(maxsize=1)
    container = DummyContainer(config=DummyConfig(mode="hybrid", max_backfill_pages=0), forums=[DummyForum(1, "bar")])
    scheduler = Scheduler(router, container)  # type: ignore[arg-type]
    requested: list[tuple[bool, int]] = []

    async def acknowledge(content: ScanThreadsTask):
        requested.append((content.is_good, content.pn))
        result = _synthetic_page(content, tids=(content.pn,), times=(100 - content.pn * 10,), has_more=content.pn == 1)
        assert content.max_pages == 0
        assert content.completion is not None
        content.completion.set_result(result)

    await _consume_backfill_pages(scheduler, acknowledge)

    assert requested == [(False, 1), (False, 2)]


@pytest.mark.asyncio
async def test_scheduler_stream_failure_does_not_start_a_separate_good_stream():
    router = QueueRouter(maxsize=1)
    container = DummyContainer(config=DummyConfig(mode="backfill", max_backfill_pages=0), forums=[DummyForum(1, "bar")])
    scheduler = Scheduler(router, container)  # type: ignore[arg-type]
    requested: list[bool] = []

    async def acknowledge(content: ScanThreadsTask):
        requested.append(content.is_good)
        assert content.completion is not None
        content.completion.set_exception(RuntimeError("synthetic list error"))

    with pytest.raises(BackfillIncompleteError):
        await _consume_backfill_pages(scheduler, acknowledge)

    assert requested == [False]


@pytest.mark.asyncio
async def test_scheduler_failure_during_replay_preserves_committed_page_evidence():
    router = QueueRouter(maxsize=1)
    container = DummyContainer(config=DummyConfig(mode="backfill", max_backfill_pages=0), forums=[DummyForum(1, "bar")])
    container.research_run_id = "synthetic-replay-run"
    replay_cursor = {
        "page_non_top_tids": [100, 101, 102],
        "flow_duplicate_streak": 2,
        "_flow_seen_tids": [90, 100, 101, 102],
        "_flow_duplicate_streak": 2,
        "_resume_replay_page": 5,
        "_resume_page_seen_tids": [100, 101, 102],
    }
    checkpoints = []
    datastore = SimpleNamespace(
        get_research_list_checkpoint_states=AsyncMock(
            return_value={(1, False): (5, "running", replay_cursor)}
        ),
        record_research_checkpoint=AsyncMock(side_effect=lambda **kwargs: checkpoints.append(kwargs)),
        set_research_run_status_if=AsyncMock(return_value=True),
    )
    scheduler = Scheduler(router, container, cast("Any", datastore))  # type: ignore[arg-type]

    async def fail_replayed_page(*_args, **_kwargs):
        raise RuntimeError("synthetic replay failure")

    scheduler._scan_backfill_page = fail_replayed_page  # type: ignore[method-assign]
    with pytest.raises(BackfillIncompleteError):
        await scheduler._run_backfill()

    checkpoint = checkpoints[0]
    assert checkpoint["status"] == "failed"
    assert checkpoint["continuation_json"]["replay_page_checkpoint_committed"] == 5
    assert checkpoint["continuation_json"]["page_non_top_tids"] == [100, 101, 102]
    assert checkpoint["continuation_json"]["flow_duplicate_streak"] == 2
    assert not any(key.startswith("_") for key in checkpoint["continuation_json"])


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
@pytest.mark.parametrize(
    ("sort_mode", "raw_code", "expected_error_code", "expected_status", "expected_scope_status"),
    [
        ("create", "rvf5", None, "failed", "incomplete"),
        ("reply", [], None, "failed", "incomplete"),
        ("create", 4011, 4011, "paused_auth", "paused_auth"),
        ("reply", 429, 429, "paused_rate_limit", "paused_rate_limit"),
        ("create", 220034, 220034, "blocked", "blocked"),
        ("reply", 4, 4, "failed", "incomplete"),
    ],
)
async def test_scheduler_normalizes_checkpoint_error_code_without_changing_api_status_mapping(
    sort_mode, raw_code, expected_error_code, expected_status, expected_scope_status
):
    class CodedError(Exception):
        code = raw_code

    router = QueueRouter(maxsize=1)
    container = DummyContainer(
        config=DummyConfig(mode="backfill", max_backfill_pages=0),
        forums=[DummyForum(1, "synthetic")],
    )
    container.config.research_enabled = True
    container.research_run_id = "synthetic-run"
    checkpoints = []
    datastore = SimpleNamespace(
        get_research_list_checkpoint_states=AsyncMock(return_value={}),
        record_research_checkpoint=AsyncMock(side_effect=lambda **kwargs: checkpoints.append(kwargs)),
        set_research_run_status_if=AsyncMock(return_value=True),
    )
    scheduler = Scheduler(router, container, cast("Any", datastore), backfill_sort=sort_mode)  # type: ignore[arg-type]

    async def fail_page(*_args, **_kwargs):
        raise CodedError("synthetic list failure")

    scheduler._scan_backfill_page = fail_page  # type: ignore[method-assign]
    with pytest.raises(BackfillIncompleteError):
        await scheduler._run_backfill()

    checkpoint = checkpoints[0]
    assert checkpoint["error_code"] == expected_error_code
    assert checkpoint["status"] == expected_status
    assert checkpoint["scope_status"] == expected_scope_status
    assert checkpoint["sort_mode"] == sort_mode
    assert checkpoint["task_key"] == research_thread_list_task_key(
        1,
        False,
        sort_mode,
        100 if sort_mode == "reply" else 30,
    )


@pytest.mark.asyncio
async def test_scheduler_reads_exception_code_once_for_classification_and_checkpoint():
    class ChangingCodeError(Exception):
        def __init__(self):
            self.reads = 0

        @property
        def code(self):
            self.reads += 1
            return "rvf5" if self.reads == 1 else 429

    error = ChangingCodeError()
    router = QueueRouter(maxsize=1)
    container = DummyContainer(
        config=DummyConfig(mode="backfill", max_backfill_pages=0),
        forums=[DummyForum(1, "synthetic")],
    )
    container.config.research_enabled = True
    container.research_run_id = "synthetic-run"
    checkpoints = []
    datastore = SimpleNamespace(
        get_research_list_checkpoint_states=AsyncMock(return_value={}),
        record_research_checkpoint=AsyncMock(side_effect=lambda **kwargs: checkpoints.append(kwargs)),
        set_research_run_status_if=AsyncMock(return_value=True),
    )
    scheduler = Scheduler(router, container, cast("Any", datastore))  # type: ignore[arg-type]

    async def fail_page(*_args, **_kwargs):
        raise error

    scheduler._scan_backfill_page = fail_page  # type: ignore[method-assign]
    with pytest.raises(BackfillIncompleteError):
        await scheduler._run_backfill()

    assert error.reads == 1
    assert checkpoints[0]["error_code"] is None
    assert checkpoints[0]["status"] == "failed"


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
        if content.pn == 1:
            page = _synthetic_page(content, tids=(1, 2), times=(20, 10), has_more=True)
        else:
            page = _synthetic_page(content, tids=(2, 3), times=(10, 5), has_more=False)
        content.completion.set_result(page)

    await _consume_backfill_pages(scheduler, acknowledge)

    assert requested == [(False, 1), (False, 2)]


@pytest.mark.asyncio
async def test_scheduler_cross_page_create_time_reversal_warns_and_advances():
    router = QueueRouter(maxsize=1)
    container = DummyContainer(config=DummyConfig(mode="backfill", max_backfill_pages=0), forums=[DummyForum(1, "bar")])
    scheduler = Scheduler(router, container)  # type: ignore[arg-type]
    requested: list[tuple[bool, int]] = []

    async def acknowledge(content: ScanThreadsTask):
        requested.append((content.is_good, content.pn))
        assert content.completion is not None
        times = (20,) if content.pn == 1 else (21,)
        page = _synthetic_page(
            content,
            tids=(content.pn,),
            times=times,
            has_more=content.pn == 1,
        )
        content.completion.set_result(page)

    await _consume_backfill_pages(scheduler, acknowledge)

    assert requested == [(False, 1), (False, 2)]


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

    assert requested == [False]


@pytest.mark.asyncio
async def test_reply_backfill_uses_distinct_rn100_checkpoint_and_ignores_total_page_cap():
    router = QueueRouter(maxsize=1)
    container = DummyContainer(
        config=DummyConfig(mode="backfill", max_backfill_pages=1),
        forums=[DummyForum(1, "bar")],
    )
    container.config.research_enabled = True
    container.research_run_id = "reply-run"
    datastore = SimpleNamespace(
        get_research_list_checkpoint_states=AsyncMock(return_value={}),
        mark_research_checkpoint_scope_status=AsyncMock(),
        set_research_run_status_if=AsyncMock(),
    )
    scheduler = Scheduler(router, container, datastore, backfill_sort="reply")  # type: ignore[arg-type]
    requested = []

    async def page(_forum, pn, max_pages, is_good, _cursor=None, *, sort_mode, rn):
        requested.append((pn, max_pages, is_good, sort_mode, rn))
        return ThreadPageResult(
            has_more=True,
            current_page=pn,
            total_pages=3306,
            total_count=330600,
            tids=(1, 2),
            non_top_create_times=(),
            non_top_tids=(1, 2),
            continuation_json={"list_sort_mode": "reply", "rn": 100},
        )

    scheduler._scan_backfill_page = page  # type: ignore[method-assign]

    await scheduler._run_backfill()

    assert requested == [(1, 1, False, "reply", 100)]
    datastore.get_research_list_checkpoint_states.assert_awaited_once_with(
        "reply-run",
        sort_mode="reply",
        rn=100,
        source_api="aiotieba.get_threads",
    )
    datastore.mark_research_checkpoint_scope_status.assert_awaited_once_with(
        "reply-run",
        research_thread_list_task_key(1, False, "reply", 100),
        1,
        "paused_limit",
    )


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
async def test_periodic_loop_schedules_only_the_unified_thread_list():
    router = QueueRouter()
    container = DummyContainer(
        config=DummyConfig(scheduler_interval_seconds=0, good_page_every_ticks=1),
        forums=[DummyForum(1, "bar")],
    )
    scheduler = Scheduler(router, container)  # type: ignore[arg-type]
    requested = []

    async def capture(_forums, *, is_good=False):
        requested.append(is_good)
        scheduler.stop()

    scheduler._schedule_homepage_scans = capture  # type: ignore[method-assign]
    await scheduler._run_loop(lambda: cast("Any", container.forums), 0, "Default")

    assert requested == [False]


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

    create_task = ScanThreadsTask(fid=1, fname="x", pn=1, backfill=True)
    reply_task = ScanThreadsTask(fid=1, fname="x", pn=1, rn=100, backfill=True, sort_mode="reply")
    assert create_task.unique_key == (1, 1, False)
    assert reply_task.unique_key == (1, 1, False, "reply", 100, True)
    assert reply_task.unique_key != t1.unique_key


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


@pytest.mark.asyncio
@pytest.mark.parametrize("sort_mode", ["create", "reply"])
async def test_backfill_min_thread_create_time_stops_list_after_three_old_pages(sort_mode):
    from tiebameow.utils.time_utils import SHANGHAI_TZ

    router = QueueRouter(maxsize=1)
    container = DummyContainer(
        config=DummyConfig(mode="backfill", max_backfill_pages=0),
        forums=[DummyForum(1, "synthetic")],
    )
    container.config.research_enabled = True
    container.research_run_id = "synthetic-run"
    container.min_thread_create_time = datetime(2021, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ)
    statuses: list[str] = []
    marked_scopes: list[tuple[int, str]] = []
    datastore = SimpleNamespace(
        get_research_list_checkpoint_states=AsyncMock(return_value={}),
        mark_research_checkpoint_scope_status=AsyncMock(
            side_effect=lambda _r, _k, pn, st: marked_scopes.append((pn, st))
        ),
        set_research_run_status_if=AsyncMock(
            side_effect=lambda _r, _e, st, _reason=None: statuses.append(st) or True
        ),
    )
    scheduler = Scheduler(
        router, container, cast("Any", datastore), backfill_sort=sort_mode
    )  # type: ignore[arg-type]

    scanned_pages: list[int] = []
    old_dt = datetime(2020, 12, 31, 23, 0, 0, tzinfo=SHANGHAI_TZ)
    new_dt = datetime(2021, 1, 2, 0, 0, 0, tzinfo=SHANGHAI_TZ)

    async def fake_scan(_forum, pn, _max_pages, _is_good, _continuation=None, **_kwargs):
        scanned_pages.append(pn)
        dt = new_dt if pn == 1 else old_dt
        return ThreadPageResult(
            has_more=True,
            current_page=pn,
            total_pages=100,
            total_count=100,
            tids=(1000 + pn,),
            non_top_create_times=(dt,),
            non_top_tids=(1000 + pn,),
            non_top_last_times=(dt,),
            continuation_json={},
        )

    scheduler._scan_backfill_page = fake_scan  # type: ignore[method-assign]
    await scheduler._run_backfill()

    assert scanned_pages == [1, 2, 3, 4]
    assert marked_scopes == [(4, "paused_limit")]
    assert statuses == ["paused_limit"]


@pytest.mark.asyncio
async def test_reply_resume_saved_repeat_boundary_respects_min_thread_create_time_cutoff():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from tiebameow.utils.time_utils import SHANGHAI_TZ

    from src.core.datastore import DataStore
    from src.core.models import (
        REPLY_BACKFILL_RN,
        ResearchCheckpoint,
        ResearchRun,
    )
    from src.research.models import ResearchThread

    engine = create_engine("sqlite:///:memory:")
    ResearchRun.__table__.create(engine)
    ResearchCheckpoint.__table__.create(engine)
    ResearchThread.__table__.create(engine)

    class _SyncSessionWrapper:
        def __init__(self, session):
            self.session = session

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            self.session.close()

        async def get(self, *args, **kwargs):
            return self.session.get(*args, **kwargs)

        async def execute(self, stmt):
            return self.session.execute(stmt)

        async def scalars(self, stmt):
            return self.session.scalars(stmt)

        async def stream(self, stmt):
            rows = list(self.session.scalars(stmt))

            class _AsyncStream:
                def scalars(self):
                    async def _gen():
                        for row in rows:
                            yield row

                    return _gen()

            return _AsyncStream()

        async def commit(self):
            self.session.commit()

    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _SyncSessionWrapper(Session(engine))

    try:
        cutoff = datetime(2021, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ)
        old_time = datetime(2020, 6, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ)
        new_time = datetime(2026, 11, 26, 0, 0, 0, tzinfo=SHANGHAI_TZ)
        task_key = research_thread_list_task_key(905, False, "reply", REPLY_BACKFILL_RN)
        run_id = "run-cutoff-boundary"
        now = datetime(2026, 11, 26, 12, 0, 0, tzinfo=SHANGHAI_TZ)

        with Session(engine) as session, session.begin():
            session.add(
                ResearchRun(
                    run_id=run_id,
                    mode="reply_backfill",
                    scope_key="k",
                    forums_json=[{"fid": 905, "fname": "bar"}],
                    status="running",
                    started_at=now,
                    last_checked=now,
                )
            )
            for tid, dt in [(101, old_time), (102, old_time), (103, old_time), (104, new_time)]:
                session.add(
                    ResearchThread(
                        tid=tid,
                        fid=905,
                        fname="bar",
                        title=f"t{tid}",
                        create_time=dt,
                        last_time=dt,
                        scraped_at=now,
                        source_api="aiotieba.get_threads",
                        observed_at=now,
                        field_sources_json={},
                        source_payload_json={},
                        missing_reasons_json={},
                    )
                )
            # Simulate live failure: 3 consecutive pre-cutoff pages (pn=1..3), 1 wraparound page (pn=4),
            # then high-duplicate pause at pn=5.
            pages = [
                (1, [101], 0.0, False, "running"),
                (2, [102], 0.0, False, "running"),
                (3, [103], 0.0, False, "running"),
                (4, [104], 0.64, False, "running"),
                (5, [104], 1.0, True, "incomplete"),
            ]
            for pn, tids, ratio, paused, scope_status in pages:
                cont = {
                    "list_contract_version": 1,
                    "list_sort_mode": "reply",
                    "source_api": "aiotieba.get_threads",
                    "rn": REPLY_BACKFILL_RN,
                    "sort_type": 6,
                    "is_good": False,
                    "page_non_top_tids": tids,
                    "page_duplicate_ratio": ratio,
                    "flow_duplicate_streak": 3 if paused else 0,
                }
                if paused:
                    cont["high_duplicate_pause_reason"] = "three_consecutive_pages_at_least_90_percent_seen"
                session.add(
                    ResearchCheckpoint(
                        run_id=run_id,
                        task_key=task_key,
                        page_number=pn,
                        task_type="thread_list",
                        scope_type="thread_list",
                        sort_mode="reply",
                        fid=905,
                        fname="bar",
                        is_good=False,
                        status="completed",
                        scope_status=scope_status,
                        has_more=True,
                        next_page=pn + 1,
                        continuation_json=cont,
                        last_checked=now,
                    )
                )

        assert (
            await store.research_list_crossed_min_create_time(
                run_id,
                905,
                False,
                sort_mode="reply",
                rn=REPLY_BACKFILL_RN,
                min_thread_create_time=cutoff,
            )
            is True
        )
        assert (
            await store.research_list_crossed_min_create_time(
                run_id,
                905,
                False,
                sort_mode="reply",
                rn=REPLY_BACKFILL_RN,
                min_thread_create_time=datetime(2019, 1, 1, tzinfo=SHANGHAI_TZ),
            )
            is False
        )

        # With min_thread_create_time set and crossed, resume pauses cleanly with paused_limit.
        router = QueueRouter(maxsize=1)
        container = DummyContainer(
            config=DummyConfig(mode="backfill", max_backfill_pages=0),
            forums=[DummyForum(905, "bar")],
        )
        container.config.research_enabled = True
        container.research_run_id = run_id
        container.min_thread_create_time = cutoff
        scheduler = Scheduler(router, container, cast("Any", store), backfill_sort="reply")
        await scheduler._run_backfill()
        with Session(engine) as session:
            run_row = session.get(ResearchRun, run_id)
            cp5 = session.get(ResearchCheckpoint, (run_id, task_key, 5))
            assert run_row is not None and run_row.status == "paused_limit"
            assert cp5 is not None and cp5.scope_status == "paused_limit"

        # Without min_thread_create_time, saved repeat boundary still fails closed.
        container.min_thread_create_time = None
        scheduler_unbounded = Scheduler(router, container, cast("Any", store), backfill_sort="reply")
        with pytest.raises(BackfillIncompleteError, match="paused_at_saved_repeat_boundary"):
            await scheduler_unbounded._run_backfill()
    finally:
        engine.dispose()
