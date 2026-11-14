"""Opt-in PostgreSQL row-lock race for refreshed research generations."""

from __future__ import annotations

import asyncio
import os
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
from src.scraper.tasks import FullScanCommentsTask
from src.scraper.worker import FullScanCommentsTaskHandler

TEST_DATABASE_NAME = "tieba_research_scope_test_20261114"


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

        async def fake_persist(_maker, page, *, session):
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

        async def fake_persist(_maker, _page, *, session):
            assert session.info.get("worker_tx")

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


async def _noop_async(*_args, **_kwargs):
    return None
