"""Opt-in PostgreSQL integration test for the backfill run/checkpoint path."""

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from conftest import make_thread
from sqlalchemy import delete, select
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from tiebameow.models.orm import Thread

import main as app_main
from src.core.datastore import DataStore
from src.core.models import (
    PendingCommentScan,
    PendingThreadScan,
    ResearchCheckpoint,
    ResearchRun,
    ResearchTaskScope,
    research_posts_task_key,
    research_thread_list_task_key,
)
from src.research.models import ResearchWebEnrichmentTask
from src.scraper.router import QueueRouter
from src.scraper.tasks import FullScanPostsTask

TEST_DATABASE_NAME = "tieba_research_runtime_test_20261114"
TEST_DATABASE_COMMENT = "TiebaScraper synthetic runtime integration test database"
TEST_FID = 9_223_372_036_854_775_807
TEST_TID = 9_223_372_036_854_775_806


async def _fake_web_worker_waits_for_shutdown(_sessions, _run_ids, *, stop_event, **_kwargs):
    await stop_event.wait()


async def _fake_discover_missing_web_targets(*_args, **_kwargs):
    return None


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
        pytest.fail("Could not reach the explicitly configured synthetic PostgreSQL test database.", pytrace=False)

    try:
        existing = await connection.fetchrow(
            "SELECT shobj_description(oid, 'pg_database') AS comment "
            "FROM pg_database WHERE datname = $1",
            TEST_DATABASE_NAME,
        )
        if existing is None:
            await connection.execute(f'CREATE DATABASE "{TEST_DATABASE_NAME}"')
            await connection.execute(
                f"COMMENT ON DATABASE \"{TEST_DATABASE_NAME}\" IS '{TEST_DATABASE_COMMENT}'"
            )
        elif existing["comment"] != TEST_DATABASE_COMMENT:
            pytest.fail("The named PostgreSQL database is not marked as this synthetic test database.", pytrace=False)
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_main_backfill_persists_real_run_and_checkpoints_before_completing(monkeypatch):
    raw_url = os.environ.get("TIEBA_RESEARCH_TEST_DATABASE_URL")
    if not raw_url:
        pytest.skip("Set TIEBA_RESEARCH_TEST_DATABASE_URL to opt into the isolated PostgreSQL integration test.")

    try:
        configured_url = make_url(raw_url)
    except Exception:
        pytest.fail("TIEBA_RESEARCH_TEST_DATABASE_URL is not a valid SQLAlchemy URL.", pytrace=False)
    if configured_url.database != TEST_DATABASE_NAME:
        pytest.fail("The integration test URL must target its dedicated synthetic database.", pytrace=False)

    await _ensure_test_database(configured_url)
    database_url = configured_url.set(drivername="postgresql+asyncpg")
    engine = create_async_engine(database_url, echo=False)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    run_id: str | None = None
    container = None
    events: list[str] = []

    try:
        async with engine.begin() as connection:
            for table in (
                ResearchRun.__table__,
                ResearchWebEnrichmentTask.__table__,
                ResearchTaskScope.__table__,
                ResearchCheckpoint.__table__,
                PendingThreadScan.__table__,
                PendingCommentScan.__table__,
            ):
                await connection.run_sync(
                    lambda sync_connection, table=table: table.create(sync_connection, checkfirst=True)
                )

        store = DataStore.__new__(DataStore)
        store.async_sessionmaker = sessions
        store.close = AsyncMock()
        original_record_checkpoint = store.record_research_checkpoint
        original_reconcile = store.reconcile_research_run_status_after_drain

        async def record_checkpoint(**kwargs):
            await original_record_checkpoint(**kwargs)
            events.append(f"checkpoint_{kwargs['is_good']}")

        async def reconcile_after_drain(received_run_id):
            assert events[-1] == "queue_drained"
            events.append("reconcile_started")
            status = await original_reconcile(received_run_id)
            events.append(f"reconciled_{status}")
            return status

        store.record_research_checkpoint = record_checkpoint
        store.reconcile_research_run_status_after_drain = reconcile_after_drain

        class FakeContainer:
            config = SimpleNamespace(
                metrics_enabled=False,
                worker_threads=0,
                worker_posts=0,
                worker_comments=0,
                queue_maxsize=1,
                research_enabled=True,
                fetch_user_forum_info=False,
                max_backfill_pages=0,
            )
            forums = [SimpleNamespace(fid=TEST_FID, fname="synthetic-runtime-test")]
            research_run_id = None

            def __init__(self):
                self.config.pydantic_config = SimpleNamespace(web_fallback=SimpleNamespace(browser_profile=None))
                self.semaphore = asyncio.Semaphore(1)

            async def teardown(self):
                return None

        container = FakeContainer()
        router = QueueRouter(maxsize=container.config.queue_maxsize)
        original_wait_idle = router.wait_idle

        async def tracked_wait_idle():
            events.append("queue_drain_started")
            await original_wait_idle()
            async with sessions() as session:
                current = await session.get(ResearchRun, container.research_run_id)
                assert current is not None
                assert current.status == "running"
            events.append("queue_drained")

        router.wait_idle = tracked_wait_idle

        class FakeScheduler:
            def __init__(self, *, router, container, datastore):
                assert router is not None
                assert container is not None
                assert datastore is store
                events.append("scheduler_received_real_datastore")

            async def run(self, mode):
                assert mode == "backfill"
                async with sessions() as session:
                    current = await session.get(ResearchRun, container.research_run_id)
                    assert current is not None
                    assert current.status == "running"

                for is_good in (False, True):
                    await store.record_research_checkpoint(
                        run_id=container.research_run_id,
                        task_key=research_thread_list_task_key(TEST_FID, is_good),
                        page_number=1,
                        task_type="thread_list",
                        scope_type="thread_list",
                        sort_mode="create",
                        fid=TEST_FID,
                        fname="synthetic-runtime-test",
                        is_good=is_good,
                        status="completed",
                        scope_status="completed",
                        has_more=False,
                        total_pages=1,
                        reported_count=1,
                        page_item_count=1,
                    )
                events.append("list_scheduler_finished")

            def stop(self):
                return None

        async def initialize(*, mode):
            assert mode == "backfill"
            return container, router

        monkeypatch.setattr(app_main, "initialize_application", initialize)
        monkeypatch.setattr(app_main, "DataStore", lambda _container: store)
        monkeypatch.setattr(app_main, "Scheduler", FakeScheduler)
        # Keep the WEB lifecycle alive through shutdown without launching a browser or scanning unrelated entities.
        monkeypatch.setattr(app_main, "run_live_web_worker", _fake_web_worker_waits_for_shutdown)
        monkeypatch.setattr(app_main, "discover_missing_web_targets", _fake_discover_missing_web_targets)

        await app_main.main("backfill")
        run_id = container.research_run_id
        assert run_id is not None

        async with sessions() as session:
            run = await session.get(ResearchRun, run_id)
            checkpoints = await session.execute(
                select(ResearchCheckpoint).where(ResearchCheckpoint.run_id == run_id)
            )
            assert run is not None
            assert run.status == "completed"
            assert len(checkpoints.scalars().all()) == 2

        assert events.index("scheduler_received_real_datastore") < events.index("checkpoint_False")
        assert events.index("checkpoint_True") < events.index("list_scheduler_finished")
        assert events.index("list_scheduler_finished") < events.index("queue_drain_started")
        assert events.index("queue_drained") < events.index("reconcile_started")
        assert events[-1] == "reconciled_completed"
    finally:
        cleanup_run_id = run_id or getattr(container, "research_run_id", None)
        if cleanup_run_id is not None:
            async with sessions() as session:
                await session.execute(delete(ResearchTaskScope).where(ResearchTaskScope.run_id == cleanup_run_id))
                await session.execute(delete(ResearchCheckpoint).where(ResearchCheckpoint.run_id == cleanup_run_id))
                await session.execute(delete(ResearchRun).where(ResearchRun.run_id == cleanup_run_id))
                await session.commit()
        await engine.dispose()


@pytest.mark.asyncio
async def test_main_recovers_committed_current_generation_without_checkpoint(monkeypatch):
    raw_url = os.environ.get("TIEBA_RESEARCH_TEST_DATABASE_URL")
    if not raw_url:
        pytest.skip("Set TIEBA_RESEARCH_TEST_DATABASE_URL to opt into the isolated PostgreSQL integration test.")

    try:
        configured_url = make_url(raw_url)
    except Exception:
        pytest.fail("TIEBA_RESEARCH_TEST_DATABASE_URL is not a valid SQLAlchemy URL.", pytrace=False)
    if configured_url.database != TEST_DATABASE_NAME:
        pytest.fail("The integration test URL must target its dedicated synthetic database.", pytrace=False)

    await _ensure_test_database(configured_url)
    engine = create_async_engine(configured_url.set(drivername="postgresql+asyncpg"), echo=False)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    store = DataStore.__new__(DataStore)
    store.async_sessionmaker = sessions
    store.close = AsyncMock()
    run_id: str | None = None
    captured: list[object] = []
    container = None

    try:
        async with engine.begin() as connection:
            for table in (
                ResearchRun.__table__,
                ResearchWebEnrichmentTask.__table__,
                ResearchTaskScope.__table__,
                ResearchCheckpoint.__table__,
                PendingThreadScan.__table__,
                PendingCommentScan.__table__,
                Thread.__table__,
            ):
                await connection.run_sync(
                    lambda sync_connection, table=table: table.create(sync_connection, checkfirst=True)
                )

        fid = TEST_FID
        tid = TEST_TID
        forum = (fid, "synthetic-runtime-test")
        run_id = await store.start_research_run(
            mode="backfill",
            forums=[forum],
            max_backfill_pages=0,
        )
        await store.set_research_run_status(run_id, "interrupted", "synthetic_before_restart")
        async with sessions() as session:
            session.add(
                ResearchTaskScope(
                    run_id=run_id,
                    logical_task_key=f"thread_replies:{tid}",
                    generation=6,
                    task_type="full_scan_posts",
                    scope_type="thread_replies",
                    fid=fid,
                    fname=forum[1],
                    tid=tid,
                    backfill=True,
                    rn=30,
                    status="completed",
                )
            )
            session.add(
                ResearchCheckpoint(
                    run_id=run_id,
                    task_key=research_posts_task_key(tid, generation=6),
                    page_number=1,
                    task_type="full_scan_posts",
                    scope_type="thread_replies",
                    fid=fid,
                    fname=forum[1],
                    tid=tid,
                    status="completed",
                    scope_status="completed",
                    has_more=False,
                )
            )
            await session.commit()

        generations = await store.save_threads_and_pending_scans(
            [make_thread(tid=tid, fid=fid, fname=forum[1], reply_num=100)],
            backfill=True,
            upsert_threads=True,
            force_pending_scan=True,
            refresh_research_run_id=run_id,
        )
        assert generations[tid] == 7
        # Simulate a stale legacy cleanup removing the old pending marker after
        # the parent refresh committed, followed by a crash before queueing g7.
        await store.remove_pending_thread_scan(tid)
        async with sessions() as session:
            current_scope = await session.get(ResearchTaskScope, (run_id, f"thread_replies:{tid}"))
            current_checkpoint = await session.get(
                ResearchCheckpoint,
                (run_id, research_posts_task_key(tid, generation=7), 1),
            )
            pending_marker = await session.get(PendingThreadScan, tid)
            assert current_scope is not None
            assert current_scope.generation == 7
            assert current_scope.status == "pending"
            assert current_checkpoint is None
            assert pending_marker is None

        class FakeContainer:
            config = SimpleNamespace(
                metrics_enabled=False,
                worker_threads=0,
                worker_posts=1,
                worker_comments=0,
                queue_maxsize=1,
                research_enabled=True,
                fetch_user_forum_info=False,
                max_backfill_pages=0,
            )
            forums = [SimpleNamespace(fid=fid, fname=forum[1])]
            research_run_id = None

            def __init__(self):
                self.config.pydantic_config = SimpleNamespace(web_fallback=SimpleNamespace(browser_profile=None))
                self.semaphore = asyncio.Semaphore(1)

            async def teardown(self):
                return None

        container = FakeContainer()
        router = QueueRouter(maxsize=container.config.queue_maxsize)

        class FakeWorker:
            def __init__(self, worker_id, queue, *_args, lane, **_kwargs):
                self.queue = queue
                self.worker_id = worker_id
                self.lane = lane

            async def run(self):
                while True:
                    task = await self.queue.get()
                    captured.append(task.content)
                    self.queue.task_done(task)

        class FakeScheduler:
            def __init__(self, *, router, container, datastore):
                assert router is not None
                assert container is not None
                assert datastore is store

            async def run(self, mode):
                assert mode == "backfill"

            def stop(self):
                return None

        async def initialize(*, mode):
            assert mode == "backfill"
            return container, router

        monkeypatch.setattr(app_main, "initialize_application", initialize)
        monkeypatch.setattr(app_main, "DataStore", lambda _container: store)
        monkeypatch.setattr(app_main, "Scheduler", FakeScheduler)
        monkeypatch.setattr(app_main, "Worker", FakeWorker)
        # Keep the WEB lifecycle alive through shutdown without launching a browser or scanning unrelated entities.
        monkeypatch.setattr(app_main, "run_live_web_worker", _fake_web_worker_waits_for_shutdown)
        monkeypatch.setattr(app_main, "discover_missing_web_targets", _fake_discover_missing_web_targets)

        await app_main.main("backfill", resume=True, run_id=run_id)

        assert len(captured) == 1
        task = captured[0]
        assert isinstance(task, FullScanPostsTask)
        assert (task.tid, task.run_id, task.generation, task.fid, task.fname) == (
            tid,
            run_id,
            7,
            fid,
            forum[1],
        )

        async with sessions() as session:
            current_run = await session.get(ResearchRun, run_id)
            current_scope = await session.get(ResearchTaskScope, (run_id, f"thread_replies:{tid}"))
            checkpoints = await session.execute(
                select(ResearchCheckpoint).where(ResearchCheckpoint.run_id == run_id)
            )
            assert current_run is not None
            assert current_run.status == "incomplete"
            assert current_scope is not None
            assert current_scope.generation == 7
            assert current_scope.status == "pending"
            saved_checkpoints = checkpoints.scalars().all()
            assert [checkpoint.task_key for checkpoint in saved_checkpoints] == [
                research_posts_task_key(tid, generation=6)
            ]
    finally:
        cleanup_run_id = run_id or getattr(container, "research_run_id", None)
        if cleanup_run_id is not None:
            async with sessions() as session:
                await session.execute(delete(ResearchTaskScope).where(ResearchTaskScope.run_id == cleanup_run_id))
                await session.execute(delete(ResearchCheckpoint).where(ResearchCheckpoint.run_id == cleanup_run_id))
                await session.execute(delete(ResearchRun).where(ResearchRun.run_id == cleanup_run_id))
                await session.execute(delete(PendingThreadScan).where(PendingThreadScan.tid == TEST_TID))
                await session.execute(delete(Thread).where(Thread.tid == TEST_TID))
                await session.commit()
        await engine.dispose()
