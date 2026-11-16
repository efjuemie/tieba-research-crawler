"""Synthetic CREATE list integrity checks for the preflight gate."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from conftest import make_thread, make_threads_response
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session
from tiebameow.models.dto import PageInfoDTO

from src.core.datastore import DataStore, _research_scope_key
from src.core.models import (
    REPLY_BACKFILL_MODE,
    REPLY_BACKFILL_RN,
    REPLY_BACKFILL_SORT_TYPE,
    REPLY_BACKFILL_SOURCE_API,
    ResearchCheckpoint,
    ResearchRun,
    research_thread_list_task_key,
)
from src.scraper.router import QueueRouter
from src.scraper.scheduler import BackfillIncompleteError, Scheduler
from src.scraper.tasks import ScanThreadsTask
from src.scraper.worker import Worker


class _AsyncSessionAdapter:
    def __init__(self, engine):
        self.engine = engine
        self.session: Session | None = None

    async def __aenter__(self):
        self.session = Session(self.engine)
        return self

    async def __aexit__(self, *_args):
        assert self.session is not None
        self.session.close()

    async def execute(self, statement):
        assert self.session is not None
        return self.session.execute(statement)

    def add(self, value):
        assert self.session is not None
        self.session.add(value)

    async def commit(self):
        assert self.session is not None
        self.session.commit()


def _thread_page(
    threads,
    *,
    current_page: int = 1,
    total_pages: int = 1,
    total_count: int | None = None,
    has_more: bool = False,
):
    response = make_threads_response(
        threads,
        current_page=current_page,
        total_page=total_pages,
        has_more=has_more,
    )
    response.page = PageInfoDTO(
        page_size=30,
        current_page=current_page,
        total_page=total_pages,
        total_count=len(threads) if total_count is None else total_count,
        has_more=has_more,
        has_prev=current_page > 1,
    )
    return response


async def _exercise_list_scan(page_factory, *, expose_persisted=False):
    checkpoints: list[dict[str, Any]] = []
    persisted_pages: list[tuple[int, ...]] = []
    run = {"status": "running"}

    async def get_threads_dto(_fname, **kwargs):
        assert kwargs["sort"].name == "CREATE"
        return page_factory(kwargs["is_good"], kwargs["pn"])

    async def record_checkpoint(**values):
        checkpoints.append(dict(values))
        return True

    async def set_run_status_if(_run_id, expected, status, _reason=None):
        if run["status"] not in expected:
            return False
        run["status"] = status
        return True

    router = QueueRouter()
    container = SimpleNamespace(
        tb_client=SimpleNamespace(get_threads_dto=AsyncMock(side_effect=get_threads_dto)),
        tb_clients=None,
        threads_limiter=AsyncMock(),
        posts_limiter=AsyncMock(),
        comments_limiter=AsyncMock(),
        config=SimpleNamespace(
            mode="backfill",
            max_backfill_pages=0,
            backfill_force_scan=False,
            research_enabled=False,
            fetch_user_forum_info=False,
        ),
        lock_manager=SimpleNamespace(acquire=AsyncMock(return_value=True), release=AsyncMock()),
        forums=[SimpleNamespace(fid=10, fname="synthetic")],
        research_run_id="synthetic-preflight-run",
    )
    datastore = SimpleNamespace(
        get_research_list_checkpoint_states=AsyncMock(return_value={}),
        record_research_checkpoint=AsyncMock(side_effect=record_checkpoint),
        filter_new_ids=AsyncMock(return_value=set()),
        set_research_run_status_if=AsyncMock(side_effect=set_run_status_if),
    )
    worker = Worker(0, router.threads_queue, container, datastore, router, lane="threads")
    handler = worker.handlers[ScanThreadsTask]

    async def capture_persisted_page(response):
        dto = getattr(response, "dto", response)
        persisted_pages.append(tuple(thread.tid for thread in dto.objs))

    handler._persist_research_page = AsyncMock(side_effect=capture_persisted_page)
    handler._process_new_threads = AsyncMock()
    handler._process_old_threads = AsyncMock()
    scheduler = Scheduler(router, container, datastore)
    worker_task = asyncio.create_task(worker.run())
    error = None
    try:
        try:
            await scheduler._run_backfill()
        except BackfillIncompleteError as exc:
            error = exc
    finally:
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)
    result = (checkpoints, run["status"], error)
    return (*result, persisted_pages) if expose_persisted else result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "normal_page"),
    [
        (
            "wrong returned page",
            lambda: _thread_page([make_thread(tid=101)], current_page=2, total_pages=2, has_more=True),
        ),
        (
            "has_more beyond total_pages",
            lambda: _thread_page([make_thread(tid=102)], total_pages=1, has_more=True),
        ),
        (
            "terminal page before total_pages",
            lambda: _thread_page([make_thread(tid=103)], total_pages=2, has_more=False),
        ),
        (
            "empty page without pagination evidence",
            lambda: _thread_page([], total_pages=0, total_count=0, has_more=False),
        ),
    ],
)
async def test_create_list_integrity_failures_persist_nonterminal_checkpoint(case, normal_page):
    def page_factory(is_good: bool, pn: int):
        assert not is_good
        assert pn == 1
        return normal_page()

    checkpoints, run_status, error = await _exercise_list_scan(page_factory)

    assert isinstance(error, BackfillIncompleteError), case
    bad_stream = [
        checkpoint
        for checkpoint in checkpoints
        if checkpoint["task_key"] == research_thread_list_task_key(10, False)
    ]
    assert len(bad_stream) == 1, case
    assert bad_stream[0]["status"] == "failed", case
    assert bad_stream[0]["scope_status"] == "incomplete", case
    assert "has_more" not in bad_stream[0], case
    assert run_status == "incomplete", case


@pytest.mark.asyncio
async def test_create_list_time_reversal_warns_and_completes():
    def page_factory(is_good: bool, pn: int):
        assert not is_good
        return _thread_page(
            [
                make_thread(tid=104, create_time=datetime.fromtimestamp(20)),
                make_thread(tid=105, create_time=datetime.fromtimestamp(30)),
            ],
            total_pages=1,
            has_more=False,
        )

    checkpoints, run_status, error = await _exercise_list_scan(page_factory)

    assert error is None
    assert run_status == "running"
    assert len(checkpoints) == 1
    assert checkpoints[0]["status"] == "completed"
    assert checkpoints[0]["scope_status"] == "completed"


@pytest.mark.asyncio
async def test_create_list_pauses_after_three_high_overlap_pages_and_commits_page_ids():
    first_page_ids = tuple(range(200, 210))

    def page_factory(is_good: bool, pn: int):
        assert not is_good
        new_tid = 209 + pn
        tids = first_page_ids if pn == 1 else (*first_page_ids, new_tid)
        if pn == 2:
            tids = (*tids, *(new_tid for _ in range(10)))
        threads = [make_thread(tid=tid, create_time=datetime.fromtimestamp(100 - tid)) for tid in tids]
        return _thread_page(threads, current_page=pn, total_pages=6, has_more=True)

    checkpoints, run_status, error, persisted_pages = await _exercise_list_scan(
        page_factory,
        expose_persisted=True,
    )

    assert isinstance(error, BackfillIncompleteError)
    normal_pages = [
        checkpoint
        for checkpoint in checkpoints
        if checkpoint["task_key"] == research_thread_list_task_key(10, False)
    ]
    assert [
        (checkpoint["page_number"], checkpoint["status"], checkpoint["scope_status"])
        for checkpoint in normal_pages
    ] == [(page, "completed", "running") for page in (1, 2, 3)] + [(4, "completed", "incomplete")]
    final_cursor = normal_pages[-1]["continuation_json"]
    assert final_cursor["flow_duplicate_streak"] == 3
    assert final_cursor["page_duplicate_ratio"] >= 0.9
    assert final_cursor["high_duplicate_pause_reason"] == "three_consecutive_pages_at_least_90_percent_seen"
    assert 213 in final_cursor["page_non_top_tids"]
    assert 213 in persisted_pages[-1]
    assert normal_pages[-1]["reason"] == final_cursor["high_duplicate_pause_reason"]
    assert run_status == "incomplete"


@pytest.mark.asyncio
async def test_create_list_repeated_page_fingerprint_continues_until_overlap_threshold():
    page_threads = [
        make_thread(tid=320 + index, create_time=datetime.fromtimestamp(100 - index))
        for index in range(10)
    ]

    def page_factory(is_good: bool, pn: int):
        assert not is_good
        return _thread_page(page_threads, current_page=pn, total_pages=6, has_more=True)

    checkpoints, run_status, error = await _exercise_list_scan(page_factory)

    assert isinstance(error, BackfillIncompleteError)
    normal_pages = [checkpoint for checkpoint in checkpoints if checkpoint["is_good"] is False]
    assert [checkpoint["page_number"] for checkpoint in normal_pages] == [1, 2, 3, 4]
    assert [checkpoint["status"] for checkpoint in normal_pages] == ["completed"] * 4
    assert normal_pages[1]["continuation_json"]["flow_duplicate_streak"] == 1
    assert normal_pages[2]["continuation_json"]["flow_duplicate_streak"] == 2
    assert normal_pages[3]["scope_status"] == "incomplete"
    assert run_status == "incomplete"


@pytest.mark.asyncio
async def test_create_list_uses_has_more_when_total_pages_and_total_count_are_unknown():
    thread = make_thread(tid=300, create_time=datetime.fromtimestamp(20))

    def page_factory(is_good: bool, pn: int):
        assert not is_good
        if pn == 1:
            return _thread_page([thread], total_pages=0, total_count=0, has_more=True)
        return _thread_page(
            [make_thread(tid=301, create_time=datetime.fromtimestamp(10))],
            current_page=pn,
            total_pages=0,
            total_count=0,
        )

    checkpoints, run_status, error = await _exercise_list_scan(page_factory)

    assert error is None
    normal_pages = [
        checkpoint
        for checkpoint in checkpoints
        if checkpoint["task_key"] == research_thread_list_task_key(10, False)
    ]
    assert [
        (checkpoint["page_number"], checkpoint["scope_status"], checkpoint["has_more"])
        for checkpoint in normal_pages
    ] == [
        (1, "running", True),
        (2, "completed", False),
    ]
    assert run_status == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize("resume_mode", ["periodic", "hybrid"])
@pytest.mark.parametrize("explicit_run_id", [False, True])
async def test_resume_rejects_run_from_another_mode_without_mutating_it(resume_mode, explicit_run_id):
    engine = create_engine("sqlite:///:memory:")
    ResearchRun.__table__.create(engine)
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _AsyncSessionAdapter(engine)
    forums = [(77, "synthetic-preflight")]
    run_id = await store.start_research_run(
        mode="backfill",
        forums=forums,
        max_backfill_pages=0,
    )
    await store.set_research_run_status(run_id, "interrupted", "synthetic_preflight")

    try:
        with pytest.raises(ValueError, match="No resumable research run matches"):
            await store.start_research_run(
                mode=resume_mode,
                forums=forums,
                max_backfill_pages=0,
                resume=True,
                run_id=run_id if explicit_run_id else None,
            )

        with Session(engine) as session:
            stored_run = session.get(ResearchRun, run_id)
            run_count = session.query(ResearchRun).count()
            assert stored_run is not None
            assert stored_run.mode == "backfill"
            assert stored_run.status == "interrupted"
            assert run_count == 1
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_run_id", [False, True])
async def test_resume_reopens_matching_backfill_run(explicit_run_id):
    engine = create_engine("sqlite:///:memory:")
    ResearchRun.__table__.create(engine)
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _AsyncSessionAdapter(engine)
    forums = [(78, "synthetic-preflight")]
    run_id = await store.start_research_run(
        mode="backfill",
        forums=forums,
        max_backfill_pages=0,
    )
    await store.set_research_run_status(run_id, "interrupted", "synthetic_preflight")

    try:
        resumed_id = await store.start_research_run(
            mode="backfill",
            forums=forums,
            max_backfill_pages=0,
            resume=True,
            run_id=run_id if explicit_run_id else None,
        )

        with Session(engine) as session:
            stored_run = session.get(ResearchRun, run_id)
            run_count = session.query(ResearchRun).count()
            assert resumed_id == run_id
            assert stored_run is not None
            assert stored_run.mode == "backfill"
            assert stored_run.status == "running"
            assert run_count == 1
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_reply_backfill_resume_scope_is_separate_and_validated_before_mutation():
    engine = create_engine("sqlite:///:memory:")
    ResearchRun.__table__.create(engine)
    ResearchCheckpoint.__table__.create(engine)
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _AsyncSessionAdapter(engine)
    forums = [(87, "synthetic-preflight")]
    create_run_id = await store.start_research_run(
        mode="backfill",
        forums=forums,
        max_backfill_pages=0,
    )
    await store.set_research_run_status(create_run_id, "interrupted", "synthetic_preflight")

    try:
        with pytest.raises(ValueError, match="No resumable research run matches"):
            await store.start_research_run(
                mode=REPLY_BACKFILL_MODE,
                forums=forums,
                max_backfill_pages=0,
                resume=True,
                run_id=create_run_id,
            )

        with Session(engine) as session:
            create_run = session.get(ResearchRun, create_run_id)
            assert create_run is not None
            assert create_run.mode == "backfill"
            assert create_run.status == "interrupted"

        reply_run_id = await store.start_research_run(
            mode=REPLY_BACKFILL_MODE,
            forums=forums,
            max_backfill_pages=0,
        )
        await store.set_research_run_status(reply_run_id, "interrupted", "synthetic_preflight")
        assert await store.load_research_run_for_drain(reply_run_id, forums) == "interrupted"
        with pytest.raises(ValueError, match="mode/list contract"):
            await store.load_research_run_for_drain(reply_run_id, [(88, "other")])

        with Session(engine) as session:
            reply_run = session.get(ResearchRun, reply_run_id)
            create_run = session.get(ResearchRun, create_run_id)
            assert reply_run is not None
            assert create_run is not None
            assert reply_run.mode == REPLY_BACKFILL_MODE
            assert reply_run.scope_key != create_run.scope_key
            assert reply_run.status == "interrupted"
            assert create_run.status == "interrupted"
    finally:
        engine.dispose()


def test_legacy_backfill_scope_fingerprint_keeps_its_original_payload():
    forums = [(2, "second"), (1, "first")]
    legacy_payload = {
        "forums": [(1, "first"), (2, "second")],
        "sort_mode": "create",
        "sampling_scope": "all_accessible_threads_and_replies",
        "checkpoint_version": 1,
    }
    legacy_hash = hashlib.sha256(json.dumps(legacy_payload, separators=(",", ":")).encode()).hexdigest()

    assert _research_scope_key(forums, mode="backfill") == legacy_hash


@pytest.mark.asyncio
async def test_reply_resume_rejects_bad_checkpoint_before_research_run_status_change():
    engine = create_engine("sqlite:///:memory:")
    ResearchRun.__table__.create(engine)
    ResearchCheckpoint.__table__.create(engine)
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _AsyncSessionAdapter(engine)
    forums = [(89, "synthetic-preflight")]
    run_id = await store.start_research_run(
        mode=REPLY_BACKFILL_MODE,
        forums=forums,
        max_backfill_pages=0,
    )
    await store.set_research_run_status(run_id, "interrupted", "synthetic_preflight")
    now = datetime.now()
    with Session(engine) as session:
        session.add(
            ResearchCheckpoint(
                run_id=run_id,
                task_key=research_thread_list_task_key(89, False, "reply", REPLY_BACKFILL_RN),
                page_number=1,
                task_type="thread_list",
                scope_type="thread_list",
                sort_mode="reply",
                fid=89,
                is_good=False,
                status="failed",
                scope_status="incomplete",
                continuation_json={
                    "list_contract_version": 1,
                    "list_sort_mode": "reply",
                    "source_api": REPLY_BACKFILL_SOURCE_API,
                    "rn": 30,
                    "sort_type": REPLY_BACKFILL_SORT_TYPE,
                    "is_good": False,
                },
                last_checked=now,
            )
        )
        session.commit()

    status_changes = []

    def record_status_change(target, value, _old_value, _initiator):
        if target.run_id == run_id:
            status_changes.append(value)

    event.listen(ResearchRun.status, "set", record_status_change)
    try:
        with pytest.raises(ValueError, match="verified API/rn/sort contract"):
            await store.load_research_run_for_drain(run_id, forums)
        with pytest.raises(ValueError, match="verified API/rn/sort contract"):
            await store.start_research_run(
                mode=REPLY_BACKFILL_MODE,
                forums=forums,
                max_backfill_pages=0,
                resume=True,
                run_id=run_id,
            )
        assert status_changes == []
        with Session(engine) as session:
            stored_run = session.get(ResearchRun, run_id)
            assert stored_run is not None
            assert stored_run.status == "interrupted"
    finally:
        event.remove(ResearchRun.status, "set", record_status_change)
        engine.dispose()


@pytest.mark.asyncio
async def test_implicit_resume_selects_older_run_matching_requested_mode():
    engine = create_engine("sqlite:///:memory:")
    ResearchRun.__table__.create(engine)
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _AsyncSessionAdapter(engine)
    forums = [(79, "synthetic-preflight")]
    backfill_run_id = await store.start_research_run(
        mode="backfill",
        forums=forums,
        max_backfill_pages=0,
    )
    await store.set_research_run_status(backfill_run_id, "interrupted", "synthetic_preflight")
    periodic_run_id = await store.start_research_run(
        mode="periodic",
        forums=forums,
        max_backfill_pages=0,
    )
    await store.set_research_run_status(periodic_run_id, "interrupted", "synthetic_preflight")

    try:
        resumed_id = await store.start_research_run(
            mode="backfill",
            forums=forums,
            max_backfill_pages=0,
            resume=True,
        )

        with Session(engine) as session:
            backfill_run = session.get(ResearchRun, backfill_run_id)
            periodic_run = session.get(ResearchRun, periodic_run_id)
            run_count = session.query(ResearchRun).count()
            assert resumed_id == backfill_run_id
            assert backfill_run is not None
            assert backfill_run.mode == "backfill"
            assert backfill_run.status == "running"
            assert periodic_run is not None
            assert periodic_run.mode == "periodic"
            assert periodic_run.status == "interrupted"
            assert run_count == 2
    finally:
        engine.dispose()
