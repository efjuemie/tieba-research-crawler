from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from src.core.datastore import DataStore
from src.core.models import ResearchCheckpoint, ResearchRun, ResearchTaskScope


class _AsyncSessionAdapter:
    def __init__(self, engine):
        self.engine = engine

    async def __aenter__(self):
        self.session = Session(self.engine)
        return self

    async def __aexit__(self, *_exc):
        self.session.close()

    async def get(self, model, key):
        return self.session.get(model, key)

    async def execute(self, statement):
        return self.session.execute(statement)

    async def commit(self):
        self.session.commit()


def _reconciliation_store(
    *,
    scope_status=None,
    scope_reason=None,
    scope_type="post_comments",
    scope_generation=0,
    scope_logical_task_key=None,
    scope_parent_generation=None,
    parent_scope_generation=None,
    checkpoints=(),
    mode="periodic",
):
    engine = create_engine("sqlite://")
    for model in (ResearchRun, ResearchCheckpoint, ResearchTaskScope):
        model.__table__.create(engine)

    now = datetime.now(UTC)
    with Session(engine) as session:
        session.add(
            ResearchRun(
                run_id="finalize-run",
                mode=mode,
                scope_key="synthetic",
                forums_json=[],
                status="running",
                started_at=now,
                last_checked=now,
            )
        )
        if scope_status is not None:
            if parent_scope_generation is not None:
                session.add(
                    ResearchTaskScope(
                        run_id="finalize-run",
                        logical_task_key="thread_replies:77",
                        generation=parent_scope_generation,
                        task_type="full_scan_posts",
                        scope_type="thread_replies",
                        tid=77,
                        fid=10,
                        status="completed",
                        last_checked=now,
                    )
                )
            session.add(
                ResearchTaskScope(
                    run_id="finalize-run",
                    logical_task_key=scope_logical_task_key or f"{scope_type}:77:700",
                    generation=scope_generation,
                    task_type="full_scan_comments" if scope_type == "post_comments" else "full_scan_posts",
                    scope_type=scope_type,
                    tid=77,
                    fid=10 if scope_type == "post_comments" else None,
                    parent_pid=700 if scope_type == "post_comments" else None,
                    parent_logical_task_key="thread_replies:77" if scope_type == "post_comments" else None,
                    parent_generation=scope_parent_generation,
                    status=scope_status,
                    reason=scope_reason,
                    last_checked=now,
                )
            )
        for checkpoint in checkpoints:
            session.add(
                ResearchCheckpoint(
                    run_id="finalize-run",
                    task_key=checkpoint["task_key"],
                    page_number=checkpoint.get("page_number", 1),
                    task_type=checkpoint.get("task_type", "full_scan_posts"),
                    sort_mode=checkpoint.get("sort_mode"),
                    is_good=checkpoint.get("is_good", False),
                    status=checkpoint["status"],
                    scope_status=checkpoint["scope_status"],
                    scope_type=checkpoint["scope_type"],
                    error_code=checkpoint["error_code"],
                    last_checked=now,
                )
            )
        session.commit()

    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = lambda: _AsyncSessionAdapter(engine)
    return store, engine


@pytest.mark.asyncio
async def test_content_unavailable_scope_does_not_report_global_api_220034():
    store, engine = _reconciliation_store(scope_status="blocked", scope_reason="content_unavailable")
    try:
        assert await store.reconcile_research_run_status_after_drain("finalize-run") == "incomplete"
        with Session(engine) as session:
            run = session.scalar(select(ResearchRun).where(ResearchRun.run_id == "finalize-run"))
        assert run.reason == "content_unavailable_scopes"
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_unknown_blocked_scope_does_not_become_completed_or_global_blocked():
    store, engine = _reconciliation_store(scope_status="blocked", scope_reason="unclassified_error")
    try:
        assert await store.reconcile_research_run_status_after_drain("finalize-run") == "incomplete"
        with Session(engine) as session:
            run = session.scalar(select(ResearchRun).where(ResearchRun.run_id == "finalize-run"))
        assert run.reason == "latest_checkpoint_not_terminal"
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope_type", "status", "scope_status"),
    [
        ("thread_replies", "failed", "incomplete"),
        ("thread_replies", "failed", None),
        ("post_comments", "failed", "incomplete"),
        ("thread_list", "blocked", "blocked"),
    ],
)
async def test_api_220034_checkpoint_still_blocks_run(scope_type, status, scope_status):
    has_detail_scope = scope_type in {"thread_replies", "post_comments"}
    store, engine = _reconciliation_store(
        scope_status="incomplete" if has_detail_scope else None,
        scope_reason="RetriableApiError" if has_detail_scope else None,
        scope_type=scope_type,
        scope_logical_task_key=("thread_replies:77" if scope_type == "thread_replies" else "post_comments:77:700"),
        scope_parent_generation=0 if scope_type == "post_comments" else None,
        parent_scope_generation=0 if scope_type == "post_comments" else None,
        checkpoints=[
            {
                "scope_type": scope_type,
                "task_key": (
                    "thread_replies:77"
                    if scope_type == "thread_replies"
                    else "post_comments:77:700"
                    if scope_type == "post_comments"
                    else "thread_list:10:create:0"
                ),
                "status": status,
                "scope_status": scope_status,
                "error_code": 220034,
            }
        ],
    )
    try:
        assert await store.reconcile_research_run_status_after_drain("finalize-run") == "blocked"
        with Session(engine) as session:
            run = session.scalar(select(ResearchRun).where(ResearchRun.run_id == "finalize-run"))
        assert run.reason == "latest_checkpoint_reports_api_220034"
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_old_generation_220034_does_not_block_completed_current_scope():
    store, engine = _reconciliation_store(
        scope_status="completed",
        scope_type="thread_replies",
        scope_generation=1,
        scope_logical_task_key="thread_replies:77",
        checkpoints=[
            {
                "scope_type": "thread_replies",
                "task_key": "thread_replies:77",
                "status": "failed",
                "scope_status": "incomplete",
                "error_code": 220034,
            },
            {
                "scope_type": "thread_replies",
                "task_key": "thread_replies:77:g1",
                "status": "completed",
                "scope_status": "completed",
                "error_code": None,
            },
        ],
    )
    try:
        assert await store.reconcile_research_run_status_after_drain("finalize-run") == "completed"
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_later_success_page_clears_earlier_220034_for_current_scope():
    store, engine = _reconciliation_store(
        scope_status="completed",
        scope_type="thread_replies",
        scope_logical_task_key="thread_replies:77",
        checkpoints=[
            {
                "scope_type": "thread_replies",
                "task_key": "thread_replies:77",
                "page_number": 1,
                "status": "failed",
                "scope_status": "incomplete",
                "error_code": 220034,
            },
            {
                "scope_type": "thread_replies",
                "task_key": "thread_replies:77",
                "page_number": 2,
                "status": "completed",
                "scope_status": "completed",
                "error_code": None,
            },
        ],
    )
    try:
        assert await store.reconcile_research_run_status_after_drain("finalize-run") == "completed"
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(("sort_mode", "is_good"), [("create", True), ("reply", False)])
async def test_inactive_list_220034_does_not_block_active_run(sort_mode, is_good):
    store, engine = _reconciliation_store(
        mode="backfill",
        checkpoints=[
            {
                "scope_type": "thread_list",
                "task_key": f"thread_list:10:{sort_mode}:0",
                "task_type": "thread_list",
                "sort_mode": sort_mode,
                "is_good": is_good,
                "status": "blocked",
                "scope_status": "blocked",
                "error_code": 220034,
            }
        ]
    )
    try:
        assert await store.reconcile_research_run_status_after_drain("finalize-run") == "completed"
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_stale_comment_parent_generation_220034_does_not_block_run():
    store, engine = _reconciliation_store(
        scope_status="blocked",
        scope_reason="RetriableApiError",
        scope_type="post_comments",
        scope_generation=1,
        scope_logical_task_key="post_comments:77:700",
        scope_parent_generation=0,
        parent_scope_generation=1,
        checkpoints=[
            {
                "scope_type": "post_comments",
                "task_key": "post_comments:77:700:g1",
                "status": "failed",
                "scope_status": "incomplete",
                "error_code": 220034,
            }
        ],
    )
    try:
        assert await store.reconcile_research_run_status_after_drain("finalize-run") == "incomplete"
        with Session(engine) as session:
            run = session.scalar(select(ResearchRun).where(ResearchRun.run_id == "finalize-run"))
        assert run.reason == "latest_checkpoint_not_terminal"
    finally:
        engine.dispose()
