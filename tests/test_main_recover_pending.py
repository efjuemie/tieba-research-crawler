from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from main import _recover_pending_tasks
from src.scraper.tasks import FullScanCommentsTask, FullScanPostsTask, IncrementalScanCommentsTask


class DummyRouter:
    def __init__(self) -> None:
        self.tasks: list[Any] = []

    async def put(self, task: Any) -> None:
        self.tasks.append(task)


class DummyDataStore:
    def __init__(self, pending_threads: list[Any], pending_comments: list[Any], threads: list[Any]) -> None:
        self._pending_threads = pending_threads
        self._pending_comments = pending_comments
        self._threads = threads
        self.queried_tids: set[int] | None = None

    async def get_all_pending_thread_scans(self) -> list[Any]:
        return self._pending_threads

    async def get_all_pending_comment_scans(self) -> list[Any]:
        return self._pending_comments

    async def get_threads_by_tids(self, tids: list[int] | set[int]) -> list[Any]:
        self.queried_tids = set(tids)
        return [t for t in self._threads if t.tid in tids]


@pytest.mark.asyncio
async def test_recover_pending_tasks_only_for_responsible_forums() -> None:
    pending_threads = [
        SimpleNamespace(tid=1001, fid=1, backfill=False),
        SimpleNamespace(tid=1002, fid=2, backfill=False),
    ]
    pending_comments = [
        SimpleNamespace(tid=1001, pid=2001, fid=1, backfill=False, task_kind="incremental"),
        SimpleNamespace(tid=1002, pid=2002, fid=2, backfill=False, task_kind="full"),
    ]
    thread_rows = [
        SimpleNamespace(tid=1001, fid=1),
        SimpleNamespace(tid=1002, fid=2),
    ]

    datastore = DummyDataStore(
        pending_threads=pending_threads,
        pending_comments=pending_comments,
        threads=thread_rows,
    )
    router = DummyRouter()

    await _recover_pending_tasks(cast("Any", datastore), cast("Any", router), responsible_fids={1})

    assert datastore.queried_tids is None
    assert len(router.tasks) == 2

    thread_tasks = [t for t in router.tasks if isinstance(t.content, FullScanPostsTask)]
    comment_tasks = [
        t for t in router.tasks if isinstance(t.content, (FullScanCommentsTask, IncrementalScanCommentsTask))
    ]

    assert len(thread_tasks) == 1
    assert thread_tasks[0].content.tid == 1001

    assert len(comment_tasks) == 1
    assert comment_tasks[0].content.tid == 1001
    assert isinstance(comment_tasks[0].content, IncrementalScanCommentsTask)


@pytest.mark.asyncio
async def test_recover_pending_tasks_api_continue_older_phases_and_waits() -> None:
    import asyncio
    from datetime import datetime
    from tiebameow.utils.time_utils import SHANGHAI_TZ

    cutoff = datetime(2021, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ)
    calls: list[dict[str, Any]] = []
    older_enabled = asyncio.Event()

    new_scope = SimpleNamespace(
        scope_type="thread_replies",
        task_type="full_scan_posts",
        status="pending",
        tid=2001,
        fid=1,
        fname="bar",
        rn=100,
        backfill=True,
        generation=0,
        logical_task_key="thread_replies:2001",
    )
    old_scope = SimpleNamespace(
        scope_type="thread_replies",
        task_type="full_scan_posts",
        status="pending",
        tid=1001,
        fid=1,
        fname="bar",
        rn=100,
        backfill=True,
        generation=0,
        logical_task_key="thread_replies:1001",
    )

    class _PhasedDataStore:
        def __init__(self) -> None:
            self.pending_count_calls = 0

        async def adopt_legacy_research_scopes(self, _run_id: str, _fids: set[int]) -> None:
            return None

        async def iter_pending_research_task_scopes(self, _run_id, _fids, **kwargs):
            calls.append(dict(kwargs))
            if kwargs.get("scope_type") != "thread_replies":
                return
            if kwargs.get("min_thread_create_time") == cutoff:
                yield [new_scope]
            elif kwargs.get("older_than_thread_create_time") == cutoff:
                yield [old_scope]

        async def count_pending_research_task_scopes(self, _run_id, _fids, **_kwargs):
            self.pending_count_calls += 1
            return 0

    class _QueueRouter(DummyRouter):
        def get_queue(self, _name: str):
            return SimpleNamespace(empty=lambda: True)

    router = _QueueRouter()
    ds = _PhasedDataStore()
    await _recover_pending_tasks(
        cast("Any", ds),
        cast("Any", router),
        responsible_fids={1},
        research_run_id="run-phased",
        min_thread_create_time=cutoff,
        newest_first=True,
        api_continue_older=True,
        older_api_enabled=older_enabled,
    )

    assert older_enabled.is_set()
    assert ds.pending_count_calls >= 1
    assert [t.content.tid for t in router.tasks] == [2001, 1001]
    assert any(c.get("min_thread_create_time") == cutoff and c.get("newest_first") is True for c in calls)
    assert any(c.get("older_than_thread_create_time") == cutoff and c.get("newest_first") is True for c in calls)
