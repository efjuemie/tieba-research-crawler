from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from conftest import make_thread
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from src.core.datastore import DataStore, _first_post_comment_count_expressions
from src.core.models import (
    REPLY_BACKFILL_CONTRACT_VERSION,
    REPLY_BACKFILL_MODE,
    REPLY_BACKFILL_RN,
    REPLY_BACKFILL_SORT_TYPE,
    REPLY_BACKFILL_SOURCE_API,
    ResearchCheckpoint,
    ResearchRun,
    ResearchTaskScope,
    research_thread_list_task_key,
)
from src.research.models import ResearchComment, ResearchPost, ResearchThread


def test_first_post_comment_quality_never_uses_thread_reply_count():
    engine = create_engine("sqlite://")
    ResearchThread.__table__.create(engine)
    ResearchCheckpoint.__table__.create(engine)
    now = datetime.now(UTC)

    with Session(engine) as session:
        session.execute(
            ResearchThread.__table__.insert().values(
                tid=77,
                first_pid=700,
                reply_count=100,
                source_api="aiotieba.get_posts",
                observed_at=now,
                source_payload_json={"first_post": {"subreply_count_reported": 2, "missing_reason": None}},
                missing_reasons_json={},
            )
        )
        session.execute(
            ResearchCheckpoint.__table__.insert().values(
                run_id="run-1",
                task_key="post_comments:77:700",
                page_number=1,
                task_type="full_scan_comments",
                scope_type="post_comments",
                tid=77,
                parent_pid=700,
                parent_kind="first_post",
                status="completed",
                scope_status="completed",
                reported_count=None,
                reported_count_missing_reason="page_count_not_yet_recorded",
            )
        )
        reported, _reason = _first_post_comment_count_expressions(ResearchCheckpoint)
        result = session.scalar(
            select(reported).where(
                ResearchCheckpoint.run_id == "run-1",
                ResearchCheckpoint.task_key == "post_comments:77:700",
            )
        )

    assert result == 2
    engine.dispose()


class _AsyncMappingRows:
    def __init__(self, rows):
        self.rows = iter(rows)

    def mappings(self):
        return self

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.rows)
        except StopIteration:
            raise StopAsyncIteration from None


class _AsyncSessionAdapter:
    def __init__(self, engine):
        self.engine = engine

    async def __aenter__(self):
        self.session = Session(self.engine)
        return self

    async def __aexit__(self, *_exc):
        self.session.close()

    async def stream(self, statement):
        rows = self.session.execute(statement).mappings().all()
        return _AsyncMappingRows(rows)

    async def execute(self, statement):
        return self.session.execute(statement)

    async def get(self, model, key):
        return self.session.get(model, key)

    async def commit(self):
        self.session.commit()


@pytest.mark.asyncio
async def test_comment_quality_uses_current_generation_report_before_parent_metadata():
    engine = create_engine("sqlite://")
    for model in (ResearchThread, ResearchPost, ResearchComment, ResearchTaskScope, ResearchCheckpoint):
        model.__table__.create(engine)

    now = datetime.now(UTC)
    run_id = "quality-run"
    with Session(engine) as session:
        session.add_all(
            [
                ResearchPost(
                    pid=700,
                    tid=77,
                    subreply_count_reported=None,
                    source_api="aiotieba.get_posts",
                    observed_at=now,
                ),
                ResearchPost(
                    pid=701,
                    tid=77,
                    subreply_count_reported=25,
                    source_api="aiotieba.get_posts",
                    observed_at=now,
                ),
                ResearchTaskScope(
                    run_id=run_id,
                    logical_task_key="post_comments:77:700",
                    generation=0,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    fid=5,
                    tid=77,
                    parent_pid=700,
                    parent_kind="reply_post",
                    status="completed",
                ),
                ResearchTaskScope(
                    run_id=run_id,
                    logical_task_key="post_comments:77:701",
                    generation=1,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    fid=5,
                    tid=77,
                    parent_pid=701,
                    parent_kind="reply_post",
                    status="completed",
                ),
            ]
        )
        session.add_all(
            [
                ResearchCheckpoint(
                    run_id=run_id,
                    task_key="post_comments:77:700",
                    page_number=page_number,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    tid=77,
                    parent_pid=700,
                    parent_kind="reply_post",
                    status="completed",
                    reported_count=51,
                    reported_count_missing_reason=None,
                    total_pages=2,
                )
                for page_number in (1, 2)
            ]
            + [
                ResearchCheckpoint(
                    run_id=run_id,
                    task_key="post_comments:77:701",
                    page_number=1,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    tid=77,
                    parent_pid=701,
                    parent_kind="reply_post",
                    status="completed",
                    reported_count=999,
                    reported_count_missing_reason=None,
                ),
                ResearchCheckpoint(
                    run_id=run_id,
                    task_key="post_comments:77:701:g1",
                    page_number=1,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    tid=77,
                    parent_pid=701,
                    parent_kind="reply_post",
                    status="completed",
                    reported_count=40,
                    reported_count_missing_reason=None,
                    total_pages=2,
                ),
            ]
        )
        session.add_all(
            [
                ResearchComment(
                    cid=1000 + index,
                    tid=77,
                    parent_post_pid=700,
                    source_api="aiotieba.get_comments",
                    observed_at=now,
                )
                for index in range(50)
            ]
            + [
                ResearchComment(
                    cid=2000 + index,
                    tid=77,
                    parent_post_pid=701,
                    source_api="aiotieba.get_comments",
                    observed_at=now,
                )
                for index in range(40)
            ]
        )
        session.commit()

    async def summaries():
        async for summary in DataStore._iter_crawl_quality_summaries(
            lambda: _AsyncSessionAdapter(engine), run_id
        ):
            yield summary

    results = {row["parent_pid"]: row async for row in summaries()}
    first_scope = results[700]
    refreshed_scope = results[701]

    assert first_scope["reported_reply_num"] == 51
    assert first_scope["unique_saved_replies"] == 50
    assert first_scope["count_mismatch"] is True
    assert first_scope["success_pages"] == 2
    assert refreshed_scope["reported_reply_num"] == 40
    assert refreshed_scope["unique_saved_replies"] == 40
    assert refreshed_scope["count_mismatch"] is False

    engine.dispose()


@pytest.mark.asyncio
async def test_comment_refresh_retains_old_checkpoint_and_upserts_pending_marker():
    events: list[str] = []
    statements = []

    class Session:
        async def execute(self, statement):
            statements.append(statement)
            events.append("execute")

        async def commit(self):
            events.append("commit")

    session = Session()

    @asynccontextmanager
    async def get_session():
        yield session

    datastore = DataStore.__new__(DataStore)
    datastore.get_session = get_session

    await datastore.add_pending_comment_scan(
        tid=77,
        pid=700,
        fid=5,
        fname="bar",
        backfill=False,
        task_kind="full",
        refresh_research_run_id="run-1",
    )

    assert events == ["execute", "commit"]
    assert statements[0].table.name == "pending_comment_scan"


@pytest.mark.asyncio
async def test_thread_refresh_bumps_scope_before_saving_thread_and_retains_child_pending():
    events: list[str] = []
    statements = []

    class Session:
        async def execute(self, statement):
            statements.append(statement)
            events.append("execute")

        async def commit(self):
            events.append("commit")

    session = Session()

    @asynccontextmanager
    async def get_session():
        yield session

    datastore = DataStore.__new__(DataStore)
    datastore.get_session = get_session
    scope_calls = []

    async def ensure_scope(_session, **kwargs):
        scope_calls.append(kwargs)
        return SimpleNamespace(generation=4)

    datastore._ensure_research_task_scope_in_session = ensure_scope

    await datastore.save_threads_and_pending_scans(
        [make_thread(tid=77, fid=5, fname="bar", reply_num=1)],
        force_pending_scan=True,
        refresh_research_run_id="run-1",
    )

    assert events == ["execute", "execute", "commit"]
    assert [statement.table.name for statement in statements] == ["thread", "pending_thread_scan"]
    assert scope_calls[0]["refresh"] is True
    assert scope_calls[0]["logical_task_key"] == "thread_replies:77"


class _AsyncRows:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def __aiter__(self):
        self._iterator = iter(self.rows)
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration:
            raise StopAsyncIteration from None


class _ScalarRows:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return self.rows

    def __iter__(self):
        return iter(self.rows)


class _CheckpointReadSession:
    def __init__(self, rows):
        self.rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def stream(self, _statement):
        return _AsyncRows(self.rows)

    async def execute(self, _statement):
        return _ScalarRows(self.rows)


class _RunReconciliationSession:
    def __init__(self, *, run_status, counts, scope_statuses=()):
        self.run = SimpleNamespace(status=run_status)
        self.counts = counts
        self.scope_statuses = scope_statuses
        self.execute_count = 0
        self.values = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def get(self, _model, _run_id):
        return self.run

    async def execute(self, statement):
        self.execute_count += 1
        if self.execute_count == 1:
            return SimpleNamespace(one=lambda: self.counts)
        if self.execute_count == 2:
            return _ScalarRows(self.scope_statuses)
        self.values = statement.compile().params
        return SimpleNamespace(rowcount=1)

    async def commit(self):
        return None


def test_list_checkpoint_replays_last_nonterminal_page_once():
    rows = [
        SimpleNamespace(
            task_key="thread_list:10:create:0",
            fid=10,
            is_good=False,
            sort_mode="create",
            page_number=4,
            status="completed",
            has_more=True,
            next_page=5,
            scope_status="running",
            continuation_json={"recent_non_top_tids": [1, 2], "last_create_time": "2026-01-01T00:00:00"},
        ),
        SimpleNamespace(
            task_key="thread_list:10:create:1",
            fid=10,
            is_good=True,
            sort_mode="create",
            page_number=2,
            status="completed",
            has_more=False,
            next_page=None,
            scope_status="completed",
            continuation_json={"page_fingerprint": [3]},
        ),
        SimpleNamespace(
            task_key="thread_list:20:create:0",
            fid=20,
            is_good=False,
            sort_mode="create",
            page_number=3,
            status="failed",
            has_more=None,
            next_page=3,
            scope_status="incomplete",
            continuation_json={"recent_non_top_tids": [8]},
        ),
    ]
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _CheckpointReadSession(rows)

    import asyncio

    states = asyncio.run(store.get_research_list_checkpoint_states("run"))

    replay_cursor = states[(10, False)][2]
    assert states[(10, False)][:2] == (4, "running")
    assert replay_cursor["_flow_seen_tids"] == [1, 2]
    assert replay_cursor["_flow_history_partial"] is True
    assert replay_cursor["_resume_replay_page"] == 4
    assert replay_cursor["_resume_page_seen_tids"] == [1, 2]
    assert states[(10, True)][:2] == (None, "completed")
    assert states[(20, False)][:2] == (3, "incomplete")
    assert states[(20, False)][2]["_flow_seen_tids"] == []


def test_list_checkpoint_rebuilds_run_seen_and_preserves_overlap_streak_for_replay():
    rows = [
        SimpleNamespace(
            task_key="thread_list:10:create:0",
            fid=10,
            is_good=False,
            sort_mode="create",
            page_number=3,
            status="completed",
            has_more=True,
            next_page=4,
            scope_status="running",
            continuation_json={"page_non_top_tids": [1, 2, 3], "flow_duplicate_streak": 1},
        ),
        SimpleNamespace(
            task_key="thread_list:10:create:0",
            fid=10,
            is_good=False,
            sort_mode="create",
            page_number=4,
            status="completed",
            has_more=True,
            next_page=5,
            scope_status="running",
            continuation_json={"page_non_top_tids": [3, 4], "flow_duplicate_streak": 2},
        ),
    ]
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _CheckpointReadSession(rows)

    import asyncio

    states = asyncio.run(store.get_research_list_checkpoint_states("run"))
    cursor = states[(10, False)][2]

    assert states[(10, False)][:2] == (4, "running")
    assert cursor["_flow_seen_tids"] == [1, 2, 3, 4]
    assert cursor["_resume_replay_page"] == 4
    assert cursor["_resume_page_seen_tids"] == [3, 4]
    assert cursor["_flow_duplicate_streak"] == 2


def test_failed_replay_retains_its_previously_committed_page_history():
    rows = [
        SimpleNamespace(
            task_key="thread_list:10:create:0",
            fid=10,
            is_good=False,
            sort_mode="create",
            page_number=3,
            status="completed",
            has_more=True,
            next_page=4,
            scope_status="running",
            continuation_json={"page_non_top_tids": [1, 2], "flow_duplicate_streak": 1},
        ),
        SimpleNamespace(
            task_key="thread_list:10:create:0",
            fid=10,
            is_good=False,
            sort_mode="create",
            page_number=4,
            status="failed",
            has_more=None,
            next_page=4,
            scope_status="incomplete",
            continuation_json={
                "page_non_top_tids": [2, 3],
                "flow_duplicate_streak": 2,
                "replay_page_checkpoint_committed": 4,
            },
        ),
    ]
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _CheckpointReadSession(rows)

    import asyncio

    states = asyncio.run(store.get_research_list_checkpoint_states("run"))
    cursor = states[(10, False)][2]

    assert states[(10, False)][:2] == (4, "incomplete")
    assert cursor["_flow_seen_tids"] == [1, 2, 3]
    assert cursor["_flow_duplicate_streak"] == 2
    assert cursor["_resume_replay_page"] == 4
    assert cursor["_resume_page_seen_tids"] == [2, 3]


@pytest.mark.asyncio
async def test_reply_checkpoint_resume_requires_its_source_and_rn_contract():
    contract = {
        "list_contract_version": REPLY_BACKFILL_CONTRACT_VERSION,
        "list_sort_mode": "reply",
        "source_api": REPLY_BACKFILL_SOURCE_API,
        "rn": REPLY_BACKFILL_RN,
        "sort_type": REPLY_BACKFILL_SORT_TYPE,
        "is_good": False,
    }
    rows = [
        SimpleNamespace(
            task_key=research_thread_list_task_key(10, False, "reply", REPLY_BACKFILL_RN),
            fid=10,
            is_good=False,
            sort_mode="reply",
            page_number=page_number,
            status="completed",
            has_more=True,
            next_page=page_number + 1,
            scope_status="running",
            continuation_json={
                **contract,
                "page_non_top_tids": page_tids,
                "flow_duplicate_streak": min(2, page_number - 1),
            },
        )
        for page_number, page_tids in enumerate(([9], [10], [11], [11, 12]), start=1)
    ]
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _CheckpointReadSession(rows)

    states = await store.get_research_list_checkpoint_states(
        "reply-run",
        sort_mode="reply",
        rn=REPLY_BACKFILL_RN,
        source_api=REPLY_BACKFILL_SOURCE_API,
    )

    next_page, scope_status, cursor = states[(10, False)]
    assert (next_page, scope_status) == (4, "running")
    assert cursor is not None
    assert cursor["_flow_seen_tids"] == [9, 10, 11, 12]
    assert cursor["_resume_replay_page"] == 4
    assert cursor["_flow_duplicate_streak"] == 2
    with pytest.raises(ValueError, match="verified API/rn=100"):
        await store.get_research_list_checkpoint_states(
            "reply-run",
            sort_mode="reply",
            rn=30,
            source_api=REPLY_BACKFILL_SOURCE_API,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("page_numbers", "is_good", "error_match"),
    [
        ([1, 3], False, "continuous prefix"),
        ([1], True, "forum/list identity"),
    ],
)
async def test_reply_checkpoint_resume_rejects_gaps_and_non_normal_stream(page_numbers, is_good, error_match):
    contract = {
        "list_contract_version": REPLY_BACKFILL_CONTRACT_VERSION,
        "list_sort_mode": "reply",
        "source_api": REPLY_BACKFILL_SOURCE_API,
        "rn": REPLY_BACKFILL_RN,
        "sort_type": REPLY_BACKFILL_SORT_TYPE,
        "is_good": False,
    }
    task_key = research_thread_list_task_key(10, False, "reply", REPLY_BACKFILL_RN)
    rows = [
        SimpleNamespace(
            task_key=task_key,
            fid=10,
            is_good=is_good,
            sort_mode="reply",
            page_number=page_number,
            status="completed",
            has_more=True,
            next_page=page_number + 1,
            scope_status="running",
            continuation_json={**contract, "page_non_top_tids": [page_number]},
        )
        for page_number in page_numbers
    ]
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _CheckpointReadSession(rows)

    with pytest.raises(ValueError, match=error_match):
        await store.get_research_list_checkpoint_states(
            "reply-run",
            sort_mode="reply",
            rn=REPLY_BACKFILL_RN,
            source_api=REPLY_BACKFILL_SOURCE_API,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cursor_changes", "expected_status"),
    [
        ({}, "completed"),
        ({"rn": 30}, "incomplete"),
        ({"untrusted_terminal_reason": "terminal_page_at_least_90_percent_seen"}, "incomplete"),
    ],
)
async def test_reply_run_reconciliation_requires_verified_terminal_contract(cursor_changes, expected_status):
    engine = create_engine("sqlite://")
    for model in (ResearchRun, ResearchCheckpoint, ResearchTaskScope):
        model.__table__.create(engine)
    now = datetime.now(UTC)
    cursor = {
        "list_contract_version": REPLY_BACKFILL_CONTRACT_VERSION,
        "list_sort_mode": "reply",
        "source_api": REPLY_BACKFILL_SOURCE_API,
        "rn": REPLY_BACKFILL_RN,
        "sort_type": REPLY_BACKFILL_SORT_TYPE,
        "is_good": False,
        **cursor_changes,
    }
    with Session(engine) as session:
        session.add(
            ResearchRun(
                run_id="reply-terminal-run",
                mode=REPLY_BACKFILL_MODE,
                scope_key="synthetic",
                forums_json=[[10, "bar"]],
                status="running",
                started_at=now,
                last_checked=now,
            )
        )
        session.add(
            ResearchCheckpoint(
                run_id="reply-terminal-run",
                task_key=research_thread_list_task_key(10, False, "reply", REPLY_BACKFILL_RN),
                page_number=4,
                task_type="thread_list",
                scope_type="thread_list",
                sort_mode="reply",
                fid=10,
                is_good=False,
                status="completed",
                scope_status="completed",
                has_more=False,
                continuation_json=cursor,
                last_checked=now,
            )
        )
        session.commit()

    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _AsyncSessionAdapter(engine)
    try:
        assert await store.reconcile_research_run_status_after_drain("reply-terminal-run") == expected_status
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_first_post_detail_is_complete_only_after_content_observation():
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _CheckpointReadSession([77])

    missing = await store.needs_research_thread_detail({77, 88})

    assert missing == {88}


@pytest.mark.asyncio
async def test_run_status_reconciliation_keeps_rate_pause_after_scheduler_completion():
    session = _RunReconciliationSession(
        run_status="completed",
        counts=SimpleNamespace(blocked_count=0, auth_count=0, rate_count=1, incomplete_count=1),
    )
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: session

    status = await store.reconcile_research_run_status_after_drain("run-1")

    assert status == "paused_rate_limit"
    assert session.values is not None


@pytest.mark.asyncio
async def test_run_status_reconciliation_does_not_overwrite_auth_pause():
    session = _RunReconciliationSession(
        run_status="paused_auth",
        counts=SimpleNamespace(blocked_count=1, auth_count=1, rate_count=0, incomplete_count=1),
    )
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: session

    status = await store.reconcile_research_run_status_after_drain("run-1")

    assert status == "paused_auth"
    assert session.execute_count == 0


@pytest.mark.asyncio
async def test_run_status_reconciliation_keeps_no_checkpoint_scope_incomplete():
    session = _RunReconciliationSession(
        run_status="running",
        counts=SimpleNamespace(blocked_count=0, auth_count=0, rate_count=0, incomplete_count=0),
        scope_statuses=["pending"],
    )
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: session

    status = await store.reconcile_research_run_status_after_drain("run-1")

    assert status == "incomplete"


@pytest.mark.asyncio
async def test_backfill_reconciliation_requires_false_create_checkpoint_for_every_forum():
    engine = create_engine("sqlite://")
    for model in (ResearchRun, ResearchCheckpoint, ResearchTaskScope):
        model.__table__.create(engine)
    now = datetime.now(UTC)
    with Session(engine) as session:
        session.add(
            ResearchRun(
                run_id="create-proof-run",
                mode="hybrid",
                scope_key="synthetic",
                forums_json=[{"fid": 10, "fname": "dict"}, [20, "legacy"], 30],
                status="running",
                started_at=now,
                last_checked=now,
            )
        )
        session.add_all(
            [
                ResearchCheckpoint(
                    run_id="create-proof-run",
                    task_key="thread_list:10:create:0",
                    page_number=1,
                    task_type="thread_list",
                    scope_type="thread_list",
                    sort_mode="create",
                    fid=10,
                    is_good=False,
                    status="completed",
                    scope_status="completed",
                    has_more=False,
                    last_checked=now,
                ),
                ResearchCheckpoint(
                    run_id="create-proof-run",
                    task_key="thread_list:20:reply:0",
                    page_number=1,
                    task_type="thread_list",
                    scope_type="thread_list",
                    sort_mode="reply",
                    fid=20,
                    is_good=False,
                    status="completed",
                    scope_status="completed",
                    has_more=False,
                    last_checked=now,
                ),
                ResearchCheckpoint(
                    run_id="create-proof-run",
                    task_key="thread_list:20:create:1",
                    page_number=1,
                    task_type="thread_list",
                    scope_type="thread_list",
                    sort_mode="create",
                    fid=20,
                    is_good=True,
                    status="completed",
                    scope_status="completed",
                    has_more=False,
                    last_checked=now,
                ),
            ]
        )
        session.commit()

    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _AsyncSessionAdapter(engine)
    try:
        status = await store.reconcile_research_run_status_after_drain("create-proof-run")
        assert status == "incomplete"
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_backfill_reconciliation_ignores_legacy_good_failure_but_checks_detail_scopes():
    engine = create_engine("sqlite://")
    for model in (ResearchRun, ResearchCheckpoint, ResearchTaskScope):
        model.__table__.create(engine)
    now = datetime.now(UTC)
    with Session(engine) as session:
        session.add(
            ResearchRun(
                run_id="good-history-run",
                mode="backfill",
                scope_key="synthetic",
                forums_json=[[10, "legacy"]],
                status="running",
                started_at=now,
                last_checked=now,
            )
        )
        session.add_all(
            [
                ResearchCheckpoint(
                    run_id="good-history-run",
                    task_key="thread_list:10:create:0",
                    page_number=1,
                    task_type="thread_list",
                    scope_type="thread_list",
                    sort_mode="create",
                    fid=10,
                    is_good=False,
                    status="completed",
                    scope_status="completed",
                    has_more=False,
                    last_checked=now,
                ),
                ResearchCheckpoint(
                    run_id="good-history-run",
                    task_key="thread_list:10:create:1",
                    page_number=1,
                    task_type="thread_list",
                    scope_type="thread_list",
                    sort_mode="create",
                    fid=10,
                    is_good=True,
                    status="failed",
                    scope_status="incomplete",
                    last_checked=now,
                ),
            ]
        )
        session.commit()

    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _AsyncSessionAdapter(engine)
    try:
        assert await store.reconcile_research_run_status_after_drain("good-history-run") == "completed"
        with Session(engine) as session:
            session.add(
                ResearchTaskScope(
                    run_id="good-history-run",
                    logical_task_key="thread_replies:100",
                    generation=0,
                    task_type="full_scan_posts",
                    scope_type="thread_replies",
                    fid=10,
                    tid=100,
                    backfill=True,
                    rn=30,
                    status="pending",
                    last_checked=now,
                )
            )
            session.commit()
        assert await store.reconcile_research_run_status_after_drain("good-history-run") == "incomplete"
    finally:
        engine.dispose()
