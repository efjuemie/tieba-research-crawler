"""Opt-in PostgreSQL row-lock race for refreshed research generations."""

from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import select
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.core.datastore import DataStore
from src.core.models import (
    PendingCommentScan,
    ResearchCheckpoint,
    ResearchRun,
    ResearchTaskScope,
    research_comments_task_key,
    research_posts_task_key,
)
from src.research.models import ResearchComment, ResearchPost
from src.scraper.tasks import FullScanCommentsTask
from src.scraper.worker import FullScanCommentsTaskHandler

_TEST_DATABASE_NAME_OVERRIDE = os.environ.get("TIEBA_RESEARCH_SCOPE_TEST_DATABASE_NAME")
TEST_DATABASE_NAME = _TEST_DATABASE_NAME_OVERRIDE or "tieba_research_scope_test_20261114"
if _TEST_DATABASE_NAME_OVERRIDE and not re.fullmatch(
    r"tieba_research_scope_test_nov23_[0-9a-f]{12}", _TEST_DATABASE_NAME_OVERRIDE
):
    pytest.fail("The opt-in scope database name must be a unique nov23 test database.", pytrace=False)


async def _ensure_test_database(url: URL) -> None:
    try:
        connection = await asyncpg.connect(
            host=url.host,
            port=url.port,
            user=url.username,
            password=url.password,
            database="postgres",
        )
    except Exception:
        pytest.fail("Could not reach the configured dedicated PostgreSQL scope test database.", pytrace=False)

    try:
        exists = await connection.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", TEST_DATABASE_NAME)
        if not exists:
            await connection.execute(f'CREATE DATABASE "{TEST_DATABASE_NAME}"')
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_postgres_refresh_fences_late_old_comment_page_transaction(monkeypatch):
    raw_url = os.environ.get("TIEBA_RESEARCH_SCOPE_TEST_DATABASE_URL")
    if not raw_url:
        pytest.skip("Set TIEBA_RESEARCH_SCOPE_TEST_DATABASE_URL to run this row-lock integration test.")
    try:
        configured_url = make_url(raw_url)
    except Exception:
        pytest.fail("TIEBA_RESEARCH_SCOPE_TEST_DATABASE_URL is not a valid SQLAlchemy URL.", pytrace=False)
    if configured_url.database != TEST_DATABASE_NAME:
        pytest.fail("The row-lock test URL must target its dedicated synthetic database.", pytrace=False)

    await _ensure_test_database(configured_url)
    engine = create_async_engine(configured_url.set(drivername="postgresql+asyncpg"), echo=False)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    run_id = str(uuid4())
    tid = 8_000_000_000_000_000_000 + uuid4().int % 500_000_000_000_000_000
    pid = tid + 1
    parent_key = research_posts_task_key(tid)
    child_key = f"post_comments:{tid}:{pid}"
    refresh_holds_parent = asyncio.Event()
    release_refresh = asyncio.Event()
    old_worker_lock_attempted = asyncio.Event()
    persisted: list[object] = []
    refresh_task = None
    old_worker_task = None

    try:
        async with engine.begin() as connection:
            for table in (
                ResearchRun.__table__,
                ResearchTaskScope.__table__,
                PendingCommentScan.__table__,
            ):
                await connection.run_sync(
                    lambda sync_connection, table=table: table.create(sync_connection, checkfirst=True)
                )

        async with sessions() as session, session.begin():
            session.add(
                ResearchRun(
                    run_id=run_id,
                    mode="backfill",
                    scope_key="synthetic-scope-race",
                    forums_json=[[77, "synthetic-forum"]],
                    status="running",
                )
            )
            session.add_all(
                [
                    ResearchTaskScope(
                        run_id=run_id,
                        logical_task_key=parent_key,
                        generation=0,
                        task_type="full_scan_posts",
                        scope_type="thread_replies",
                        fid=77,
                        fname="synthetic-forum",
                        tid=tid,
                        status="completed",
                    ),
                    ResearchTaskScope(
                        run_id=run_id,
                        logical_task_key=child_key,
                        generation=0,
                        task_type="full_scan_comments",
                        scope_type="post_comments",
                        fid=77,
                        fname="synthetic-forum",
                        tid=tid,
                        parent_pid=pid,
                        parent_kind="reply_post",
                        parent_logical_task_key=parent_key,
                        parent_generation=0,
                        status="completed",
                    ),
                    PendingCommentScan(
                        tid=tid,
                        pid=pid,
                        fid=77,
                        fname="synthetic-forum",
                        backfill=True,
                        task_kind="full",
                    ),
                ]
            )

        store = DataStore.__new__(DataStore)
        store.async_sessionmaker = sessions
        original_lock = store.get_research_task_scope_for_update

        def worker_sessions():
            session = sessions()
            session.info["late_old_worker"] = True
            return session

        store.async_sessionmaker = worker_sessions

        async def announce_worker_lock(session, locked_run_id, logical_task_key):
            if session.info.get("late_old_worker"):
                old_worker_lock_attempted.set()
            return await original_lock(session, locked_run_id, logical_task_key)

        store.get_research_task_scope_for_update = announce_worker_lock

        expected_run_id = run_id

        async def fake_persist(_maker, page, *, session, run_id):
            assert run_id == expected_run_id
            persisted.append(page)

        monkeypatch.setattr("src.research.persistence.persist_research_page", fake_persist)

        async def hold_refresh_transaction():
            async with sessions() as session, session.begin():
                parent = await store.ensure_research_task_scope(
                    run_id=run_id,
                    logical_task_key=parent_key,
                    task_type="full_scan_posts",
                    scope_type="thread_replies",
                    fid=77,
                    fname="synthetic-forum",
                    tid=tid,
                    refresh=True,
                    session=session,
                )
                assert parent is not None
                assert parent.generation == 1
                refresh_holds_parent.set()
                await release_refresh.wait()

        refresh_task = asyncio.create_task(hold_refresh_transaction())
        await asyncio.wait_for(refresh_holds_parent.wait(), timeout=3)

        handler = FullScanCommentsTaskHandler.__new__(FullScanCommentsTaskHandler)
        handler.datastore = store
        handler.container = SimpleNamespace(
            research_run_id=run_id,
            config=SimpleNamespace(research_enabled=True, fetch_user_forum_info=False),
        )
        handler.ensure_users = _noop_async
        handler._prepare_research_page = _noop_async
        page = SimpleNamespace(
            objs=[],
            forum=SimpleNamespace(fid=77, fname="synthetic-forum"),
            page=SimpleNamespace(has_more=False, total_page=1, total_count=0),
        )
        task = FullScanCommentsTask(
            tid=tid,
            pid=pid,
            run_id=run_id,
            generation=0,
            parent_generation=0,
            fid=77,
            fname="synthetic-forum",
            parent_kind="reply_post",
        )
        old_worker_task = asyncio.create_task(
            handler._commit_research_comments_page(
                task,
                page,
                SimpleNamespace(),
                page_number=1,
                cursor={},
                attempts=1,
            )
        )
        await asyncio.wait_for(old_worker_lock_attempted.wait(), timeout=3)
        assert not old_worker_task.done(), "The old page transaction should wait on the refresh row lock."

        release_refresh.set()
        await asyncio.wait_for(refresh_task, timeout=3)
        committed = await asyncio.wait_for(old_worker_task, timeout=3)

        assert committed is False
        assert persisted == []
        async with sessions() as session:
            parent = await session.get(ResearchTaskScope, (run_id, parent_key))
            child = await session.get(ResearchTaskScope, (run_id, child_key))
            pending = await session.get(PendingCommentScan, (tid, pid))
            assert parent is not None
            assert parent.generation == 1
            assert child is not None
            assert child.generation == 1
            assert child.status == "pending"
            assert pending is not None
            assert await session.scalar(
                select(ResearchTaskScope.generation).where(
                    ResearchTaskScope.run_id == run_id,
                    ResearchTaskScope.logical_task_key == child_key,
                )
            ) == 1
    finally:
        release_refresh.set()
        tasks = [task for task in (refresh_task, old_worker_task) if task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        async with sessions() as session, session.begin():
            pending = await session.get(PendingCommentScan, (tid, pid))
            if pending is not None:
                await session.delete(pending)
            await session.execute(
                ResearchTaskScope.__table__.delete().where(ResearchTaskScope.run_id == run_id)
            )
            await session.execute(ResearchRun.__table__.delete().where(ResearchRun.run_id == run_id))
        await engine.dispose()


@pytest.mark.asyncio
async def test_postgres_worker_commit_then_refresh_reopens_child_generation(monkeypatch):
    raw_url = os.environ.get("TIEBA_RESEARCH_SCOPE_TEST_DATABASE_URL")
    if not raw_url:
        pytest.skip("Set TIEBA_RESEARCH_SCOPE_TEST_DATABASE_URL to run this row-lock integration test.")
    try:
        configured_url = make_url(raw_url)
    except Exception:
        pytest.fail("TIEBA_RESEARCH_SCOPE_TEST_DATABASE_URL is not a valid SQLAlchemy URL.", pytrace=False)
    if configured_url.database != TEST_DATABASE_NAME:
        pytest.fail("The row-lock test URL must target its dedicated synthetic database.", pytrace=False)

    await _ensure_test_database(configured_url)
    engine = create_async_engine(configured_url.set(drivername="postgresql+asyncpg"), echo=False)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    run_id = str(uuid4())
    tid = 8_000_000_000_000_000_000 + uuid4().int % 500_000_000_000_000_000
    pid = tid + 1
    parent_key = research_posts_task_key(tid)
    child_key = research_comments_task_key(tid, pid)
    worker_checkpoint_written = asyncio.Event()
    release_worker_commit = asyncio.Event()
    refresh_lock_attempted = asyncio.Event()
    worker_task = None
    refresh_task = None

    try:
        async with engine.begin() as connection:
            for table in (
                ResearchRun.__table__,
                ResearchTaskScope.__table__,
                ResearchCheckpoint.__table__,
                PendingCommentScan.__table__,
            ):
                await connection.run_sync(
                    lambda sync_connection, table=table: table.create(sync_connection, checkfirst=True)
                )

        async with sessions() as session, session.begin():
            session.add(
                ResearchRun(
                    run_id=run_id,
                    mode="backfill",
                    scope_key="synthetic-worker-first-race",
                    forums_json=[[77, "synthetic-forum"]],
                    status="running",
                )
            )
            session.add_all(
                [
                    ResearchTaskScope(
                        run_id=run_id,
                        logical_task_key=parent_key,
                        generation=0,
                        task_type="full_scan_posts",
                        scope_type="thread_replies",
                        fid=77,
                        fname="synthetic-forum",
                        tid=tid,
                        status="running",
                    ),
                    ResearchTaskScope(
                        run_id=run_id,
                        logical_task_key=child_key,
                        generation=0,
                        task_type="full_scan_comments",
                        scope_type="post_comments",
                        fid=77,
                        fname="synthetic-forum",
                        tid=tid,
                        parent_pid=pid,
                        parent_kind="reply_post",
                        parent_logical_task_key=parent_key,
                        parent_generation=0,
                        status="running",
                    ),
                    PendingCommentScan(
                        tid=tid,
                        pid=pid,
                        fid=77,
                        fname="synthetic-forum",
                        backfill=True,
                        task_kind="full",
                    ),
                ]
            )

        store = DataStore.__new__(DataStore)
        original_lock = store.get_research_task_scope_for_update
        original_record = store.record_research_checkpoint

        def worker_sessions():
            session = sessions()
            session.info["worker_tx"] = True
            return session

        store.async_sessionmaker = worker_sessions

        async def observe_lock(session, locked_run_id, logical_task_key):
            if session.info.get("refresh_tx") and logical_task_key == parent_key:
                refresh_lock_attempted.set()
            return await original_lock(session, locked_run_id, logical_task_key)

        async def pause_after_old_checkpoint(**values):
            committed = await original_record(**values)
            if values["task_type"] == "full_scan_comments" and values["generation"] == 0:
                worker_checkpoint_written.set()
                await release_worker_commit.wait()
            return committed

        store.get_research_task_scope_for_update = observe_lock
        store.record_research_checkpoint = pause_after_old_checkpoint
        store.save_items_in_session = _noop_async

        expected_run_id = run_id

        async def fake_persist(_maker, _page, *, session, run_id):
            assert session.info.get("worker_tx")
            assert run_id == expected_run_id

        monkeypatch.setattr("src.research.persistence.persist_research_page", fake_persist)

        handler = FullScanCommentsTaskHandler.__new__(FullScanCommentsTaskHandler)
        handler.datastore = store
        handler.container = SimpleNamespace(
            research_run_id=run_id,
            config=SimpleNamespace(research_enabled=True, fetch_user_forum_info=False),
        )
        handler.ensure_users = _noop_async
        handler._prepare_research_page = _noop_async
        task = FullScanCommentsTask(
            tid=tid,
            pid=pid,
            run_id=run_id,
            generation=0,
            parent_generation=0,
            fid=77,
            fname="synthetic-forum",
            parent_kind="reply_post",
        )
        page = SimpleNamespace(
            objs=[],
            forum=SimpleNamespace(fid=77, fname="synthetic-forum"),
            page=SimpleNamespace(has_more=False, total_page=1, total_count=1),
        )
        worker_task = asyncio.create_task(
            handler._commit_research_comments_page(
                task,
                page,
                SimpleNamespace(),
                page_number=1,
                cursor={},
                attempts=1,
            )
        )
        await asyncio.wait_for(worker_checkpoint_written.wait(), timeout=3)

        async def refresh_parent():
            async with sessions() as session, session.begin():
                session.info["refresh_tx"] = True
                parent = await store.ensure_research_task_scope(
                    run_id=run_id,
                    logical_task_key=parent_key,
                    task_type="full_scan_posts",
                    scope_type="thread_replies",
                    fid=77,
                    fname="synthetic-forum",
                    tid=tid,
                    refresh=True,
                    session=session,
                )
                assert parent is not None
                assert parent.generation == 1
                child = await session.get(ResearchTaskScope, (run_id, child_key))
                assert child is not None
                assert child.generation == 1
                assert child.status == "pending"

        refresh_task = asyncio.create_task(refresh_parent())
        await asyncio.wait_for(refresh_lock_attempted.wait(), timeout=3)
        assert not refresh_task.done(), "Refresh should wait until the older page transaction commits cleanup."

        release_worker_commit.set()
        assert await asyncio.wait_for(worker_task, timeout=3) is True
        await asyncio.wait_for(refresh_task, timeout=3)

        async with sessions() as session:
            parent = await session.get(ResearchTaskScope, (run_id, parent_key))
            child = await session.get(ResearchTaskScope, (run_id, child_key))
            pending = await session.get(PendingCommentScan, (tid, pid))
            old_checkpoint = await session.get(ResearchCheckpoint, (run_id, child_key, 1))
            new_checkpoint = await session.get(ResearchCheckpoint, (run_id, f"{child_key}:g1", 1))
            assert parent is not None
            assert parent.generation == 1
            assert child is not None
            assert child.generation == 1
            assert child.status == "pending"
            assert child.parent_generation == 1
            assert pending is None
            assert old_checkpoint is not None
            assert old_checkpoint.status == "completed"
            assert new_checkpoint is None
    finally:
        release_worker_commit.set()
        tasks = [task for task in (worker_task, refresh_task) if task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        async with sessions() as session, session.begin():
            await session.execute(
                ResearchCheckpoint.__table__.delete().where(ResearchCheckpoint.run_id == run_id)
            )
            pending = await session.get(PendingCommentScan, (tid, pid))
            if pending is not None:
                await session.delete(pending)
            await session.execute(
                ResearchTaskScope.__table__.delete().where(ResearchTaskScope.run_id == run_id)
            )
            await session.execute(ResearchRun.__table__.delete().where(ResearchRun.run_id == run_id))
        await engine.dispose()


@pytest.mark.asyncio
async def test_postgres_fresh_parent_zero_completion_preserves_failure_and_fences_generation():
    raw_url = os.environ.get("TIEBA_RESEARCH_SCOPE_TEST_DATABASE_URL")
    if not raw_url:
        pytest.skip("Set TIEBA_RESEARCH_SCOPE_TEST_DATABASE_URL to run this transaction test.")
    try:
        configured_url = make_url(raw_url)
    except Exception:
        pytest.fail("TIEBA_RESEARCH_SCOPE_TEST_DATABASE_URL is not a valid SQLAlchemy URL.", pytrace=False)
    if configured_url.database != TEST_DATABASE_NAME:
        pytest.fail("The scope test URL must target its dedicated synthetic database.", pytrace=False)

    await _ensure_test_database(configured_url)
    engine = create_async_engine(configured_url.set(drivername="postgresql+asyncpg"), echo=False)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = sessions
    run_ids = [str(uuid4()) for _ in range(5)]
    records: list[tuple[str, int, int, int, int]] = []
    now = datetime.now(UTC)

    async def seed_scope(
        run_id: str,
        *,
        live_parent_generation: int,
        child_status: str = "running",
        checkpoint_status: str = "failed",
        later_page: bool = False,
        existing_comment: bool = False,
    ) -> tuple[str, str, int, int, int]:
        tid = 8_000_000_000_000_000_000 + uuid4().int % 500_000_000_000_000_000
        pid = tid + 1
        parent_key = research_posts_task_key(tid)
        child_key = research_comments_task_key(tid, pid)
        async with sessions() as session, session.begin():
            session.add_all(
                [
                    ResearchRun(
                        run_id=run_id,
                        mode="backfill",
                        scope_key="synthetic-parent-zero",
                        forums_json=[[77, "synthetic-forum"]],
                        status="running",
                    ),
                    ResearchTaskScope(
                        run_id=run_id,
                        logical_task_key=parent_key,
                        generation=live_parent_generation,
                        task_type="full_scan_posts",
                        scope_type="thread_replies",
                        fid=77,
                        fname="synthetic-forum",
                        tid=tid,
                        rn=30,
                        status="completed",
                    ),
                    ResearchTaskScope(
                        run_id=run_id,
                        logical_task_key=child_key,
                        generation=6,
                        task_type="full_scan_comments",
                        scope_type="post_comments",
                        fid=77,
                        fname="synthetic-forum",
                        tid=tid,
                        parent_pid=pid,
                        parent_kind="reply_post",
                        parent_logical_task_key=parent_key,
                        parent_generation=4,
                        status=child_status,
                    ),
                    ResearchCheckpoint(
                        run_id=run_id,
                        task_key=research_comments_task_key(tid, pid, 6),
                        page_number=1,
                        task_type="full_scan_comments",
                        scope_type="post_comments",
                        fid=77,
                        fname="synthetic-forum",
                        tid=tid,
                        parent_pid=pid,
                        parent_kind="reply_post",
                        status=checkpoint_status,
                        scope_status="incomplete" if checkpoint_status == "failed" else "completed",
                        attempts=2,
                        next_page=1,
                        continuation_json={"cursor": "preserved"},
                        error_type="PageIntegrityError",
                        reason="PageIntegrityError",
                    ),
                    ResearchPost(
                        pid=pid,
                        tid=tid,
                        fid=77,
                        floor=2,
                        source_api="aiotieba.get_posts",
                        observed_at=now,
                    ),
                    PendingCommentScan(
                        tid=tid,
                        pid=pid,
                        fid=77,
                        fname="synthetic-forum",
                        backfill=True,
                        task_kind="full",
                    ),
                ]
            )
            if later_page:
                session.add(
                    ResearchCheckpoint(
                        run_id=run_id,
                        task_key=research_comments_task_key(tid, pid, 6),
                        page_number=2,
                        task_type="full_scan_comments",
                        scope_type="post_comments",
                        fid=77,
                        fname="synthetic-forum",
                        tid=tid,
                        parent_pid=pid,
                        status="completed",
                        scope_status="completed",
                        has_more=False,
                    )
                )
            if existing_comment:
                session.add(
                    ResearchComment(
                        cid=pid + 1000,
                        tid=tid,
                        parent_post_pid=pid,
                        fid=77,
                        source_api="aiotieba.get_comments",
                        observed_at=now,
                    )
                )
        records.append((run_id, tid, pid, 6, 4))
        return parent_key, child_key, tid, pid, 77

    async def cleanup() -> None:
        async with sessions() as session, session.begin():
            for run_id, tid, pid, _generation, _parent_generation in records:
                pending = await session.get(PendingCommentScan, (tid, pid))
                if pending is not None:
                    await session.delete(pending)
                await session.execute(
                    ResearchComment.__table__.delete().where(
                        ResearchComment.tid == tid, ResearchComment.parent_post_pid == pid
                    )
                )
                await session.execute(ResearchPost.__table__.delete().where(ResearchPost.tid == tid))
                await session.execute(
                    ResearchCheckpoint.__table__.delete().where(ResearchCheckpoint.run_id == run_id)
                )
                await session.execute(
                    ResearchTaskScope.__table__.delete().where(ResearchTaskScope.run_id == run_id)
                )
                await session.execute(ResearchRun.__table__.delete().where(ResearchRun.run_id == run_id))

    try:
        async with engine.begin() as connection:
            for table in (
                ResearchRun.__table__,
                ResearchTaskScope.__table__,
                ResearchCheckpoint.__table__,
                PendingCommentScan.__table__,
                ResearchPost.__table__,
                ResearchComment.__table__,
            ):
                await connection.run_sync(
                    lambda sync_connection, table=table: table.create(sync_connection, checkfirst=True)
                )

        parent_key, child_key, tid, pid, fid = await seed_scope(run_ids[0], live_parent_generation=4)
        hint = await store.get_research_comment_parent_page_hint(
            run_ids[0], research_comments_task_key(tid, pid, 6), tid, pid, 6, 4
        )
        assert hint == (2, 30)
        evidence = {
            "tid": tid,
            "pid": pid,
            "fid": fid,
            "author_user_id": 55,
            "source_page": 1,
            "reported_count": 0,
            "count_field_present": True,
            "source_api": "aiotieba.get_posts",
            "evidence_kind": "protobuf_wire_explicit_zero",
            "count_field_path": "data.post_list.sub_post_number",
            "count_wire_field_number": 13,
        }
        for invalid_evidence in (
            {**evidence, "count_field_present": False},
            {**evidence, "reported_count": 5},
            {**evidence, "tid": tid + 1},
        ):
            assert (
                await store.complete_failed_comment_scope_from_parent_zero(
                    run_id=run_ids[0],
                    task_key=research_comments_task_key(tid, pid, 6),
                    tid=tid,
                    parent_pid=pid,
                    generation=6,
                    parent_generation=4,
                    fid=fid,
                    parent_floor=2,
                    parent_rn=30,
                    parent_page=1,
                    source_observed_at=now.isoformat(),
                    current_failure={
                        "error_type": "PageIntegrityError",
                        "reason": "Comment detail empty response is missing or has zero page identity for page 1.",
                    },
                    evidence=invalid_evidence,
                )
                is False
            )
        complete = await store.complete_failed_comment_scope_from_parent_zero(
            run_id=run_ids[0],
            task_key=research_comments_task_key(tid, pid, 6),
            tid=tid,
            parent_pid=pid,
            generation=6,
            parent_generation=4,
            fid=fid,
            parent_floor=2,
            parent_rn=30,
            parent_page=1,
            source_observed_at=now.isoformat(),
            current_failure={
                "error_type": "PageIntegrityError",
                "reason": "Comment detail empty response is missing or has zero page identity for page 1.",
            },
            evidence=evidence,
        )
        assert complete is True
        resume = await store.get_research_resume_state(
            run_ids[0], research_comments_task_key(tid, pid, 6)
        )
        assert resume[0] is None
        assert resume[1] == "completed"
        continuation = resume[2]
        assert continuation is not None
        assert continuation["completion_basis"] == "fresh_parent_api_explicit_zero"
        assert continuation["checkpoint_kind"] == "scope_completion"
        assert continuation["comment_api_called"] is True
        assert continuation["previous_failure"]["status"] == "failed"
        assert continuation["previous_failure"]["continuation_json"] == {"cursor": "preserved"}
        assert continuation["current_failure"]["error_type"] == "PageIntegrityError"
        assert continuation["source_observed_at"] == now.isoformat()
        assert json.dumps(continuation)
        async with sessions() as session:
            checkpoint = await session.get(
                ResearchCheckpoint, (run_ids[0], research_comments_task_key(tid, pid, 6), 1)
            )
            scope = await session.get(ResearchTaskScope, (run_ids[0], child_key))
            pending = await session.get(PendingCommentScan, (tid, pid))
            assert checkpoint is not None
            assert checkpoint.status == "completed"
            assert checkpoint.error_type is None
            assert checkpoint.reported_count is None
            assert scope is not None
            assert scope.status == "completed"
            assert pending is None

        _parent_key, _child_key, stale_tid, stale_pid, stale_fid = await seed_scope(
            run_ids[1], live_parent_generation=5
        )
        stale_evidence = {**evidence, "tid": stale_tid, "pid": stale_pid, "fid": stale_fid}
        stale_complete = await store.complete_failed_comment_scope_from_parent_zero(
            run_id=run_ids[1],
            task_key=research_comments_task_key(stale_tid, stale_pid, 6),
            tid=stale_tid,
            parent_pid=stale_pid,
            generation=6,
            parent_generation=4,
            fid=stale_fid,
            parent_floor=2,
            parent_rn=30,
            parent_page=1,
            source_observed_at=now.isoformat(),
            current_failure={
                "error_type": "PageIntegrityError",
                "reason": "Comment detail empty response is missing or has zero page identity for page 1.",
            },
            evidence=stale_evidence,
        )
        assert stale_complete is False
        stale_hint = await store.get_research_comment_parent_page_hint(
            run_ids[1], research_comments_task_key(stale_tid, stale_pid, 6), stale_tid, stale_pid, 6, 4
        )
        assert stale_hint is None
        async with sessions() as session:
            stale_checkpoint = await session.get(
                ResearchCheckpoint,
                (run_ids[1], research_comments_task_key(stale_tid, stale_pid, 6), 1),
            )
            stale_scope_key = research_comments_task_key(stale_tid, stale_pid)
            stale_scope = await session.get(ResearchTaskScope, (run_ids[1], stale_scope_key))
            assert stale_checkpoint is not None
            assert stale_checkpoint.status == "failed"
            assert stale_scope is not None
            assert stale_scope.status == "running"

        guard_cases = (
            (run_ids[2], {"checkpoint_status": "completed", "child_status": "completed"}),
            (run_ids[3], {"later_page": True}),
            (run_ids[4], {"existing_comment": True}),
        )
        for guard_run_id, options in guard_cases:
            _parent_key, guard_child_key, guard_tid, guard_pid, guard_fid = await seed_scope(
                guard_run_id,
                live_parent_generation=4,
                **options,
            )
            guard_task_key = research_comments_task_key(guard_tid, guard_pid, 6)
            assert (
                await store.get_research_comment_parent_page_hint(
                    guard_run_id, guard_task_key, guard_tid, guard_pid, 6, 4
                )
                is None
            )
            guard_evidence = {**evidence, "tid": guard_tid, "pid": guard_pid, "fid": guard_fid}
            assert (
                await store.complete_failed_comment_scope_from_parent_zero(
                    run_id=guard_run_id,
                    task_key=guard_task_key,
                    tid=guard_tid,
                    parent_pid=guard_pid,
                    generation=6,
                    parent_generation=4,
                    fid=guard_fid,
                    parent_floor=2,
                    parent_rn=30,
                    parent_page=1,
                    source_observed_at=now.isoformat(),
                    current_failure={
                        "error_type": "PageIntegrityError",
                        "reason": "Comment detail empty response is missing or has zero page identity for page 1.",
                    },
                    evidence=guard_evidence,
                )
                is False
            )
            async with sessions() as session:
                guard_checkpoint = await session.get(ResearchCheckpoint, (guard_run_id, guard_task_key, 1))
                guard_scope = await session.get(ResearchTaskScope, (guard_run_id, guard_child_key))
                assert guard_checkpoint is not None
                assert guard_checkpoint.status == options.get("checkpoint_status", "failed")
                assert guard_scope is not None
                assert guard_scope.status == options.get("child_status", "running")
    finally:
        if records:
            await cleanup()
        await engine.dispose()


async def _noop_async(*_args, **_kwargs):
    return None
