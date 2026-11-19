"""Opt-in PostgreSQL checks for persisted research error codes."""

import os
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import InterfaceError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.core.datastore import DataStore
from src.core.models import ResearchCheckpoint, ResearchRun, ResearchTaskScope

TEST_DATABASE_NAME = "tieba_p0_repro_20261120_deploy"


@pytest_asyncio.fixture
async def pg_datastore():
    database_url = os.environ.get("TIEBA_P0_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("requires the explicitly isolated P0 PostgreSQL database")

    engine = create_async_engine(database_url, echo=False, pool_size=1, max_overflow=0)
    try:
        async with engine.connect() as connection:
            current_database = await connection.scalar(text("SELECT current_database()"))
        if current_database != TEST_DATABASE_NAME or current_database == "tieba_data":
            pytest.fail("P0 PostgreSQL test refused a connection outside its exact isolated database")

        sessions = async_sessionmaker(engine, expire_on_commit=False)
        datastore = object.__new__(DataStore)
        datastore.async_sessionmaker = sessions
        yield datastore, sessions
    finally:
        await engine.dispose()


async def _create_run_and_scope(sessions, run_id: str, logical_task_key: str, generation: int) -> None:
    async with sessions() as session, session.begin():
        session.add(
            ResearchRun(
                run_id=run_id,
                mode="backfill",
                scope_key=f"synthetic-p0:{uuid.uuid4()}",
                forums_json=[],
                status="running",
            )
        )
        session.add(
            ResearchTaskScope(
                run_id=run_id,
                logical_task_key=logical_task_key,
                generation=generation,
                task_type="comment_scan",
                scope_type="comments",
                status="running",
            )
        )


def _page_arguments(run_id: str, task_key: str, generation: int, **overrides):
    values = {
        "run_id": run_id,
        "task_key": task_key,
        "page_number": 3,
        "task_type": "comment_scan",
        "scope_type": "comments",
        "status": "running",
        "scope_status": "running",
        "tid": 73100421,
        "parent_pid": 73100422,
        "next_page": 4,
        "continuation_json": {"cursor": "synthetic-page-4"},
        "generation": generation,
    }
    values.update(overrides)
    return values


class _RollbackCheckpointUpdateError(Exception):
    pass


@pytest.mark.asyncio
async def test_invalid_error_code_update_rolls_back_and_caller_session_recovers(pg_datastore):
    datastore, sessions = pg_datastore
    run_id = str(uuid.uuid4())
    logical_task_key = f"synthetic-p0:{uuid.uuid4()}"
    generation = 7
    task_key = f"{logical_task_key}:g{generation}"
    await _create_run_and_scope(sessions, run_id, logical_task_key, generation)

    async with sessions() as session, session.begin():
        await datastore.record_research_checkpoint(
            **_page_arguments(run_id, task_key, generation),
            session=session,
        )

    sqlalchemy_error = InterfaceError("synthetic", {}, Exception("synthetic"), code="rvf5")
    update_values = _page_arguments(
        run_id,
        task_key,
        generation,
        status="failed",
        scope_status="incomplete",
        error_type=type(sqlalchemy_error).__name__,
        error_code=sqlalchemy_error.code,
        reason="InterfaceError",
        continuation_json={"cursor": "synthetic-page-4"},
    )

    async def attempt_rollback():
        async with sessions() as session, session.begin():
            transaction = session.get_transaction()
            assert await datastore.record_research_checkpoint(**update_values, session=session)
            assert session.get_transaction() is transaction
            row = await session.get(ResearchCheckpoint, (run_id, task_key, 3))
            assert row is not None
            assert row.status == "failed"
            assert row.scope_status == "incomplete"
            assert row.error_type == "InterfaceError"
            assert row.error_code is None
            assert row.next_page == 4
            assert row.continuation_json == {"cursor": "synthetic-page-4"}
            raise _RollbackCheckpointUpdateError

    with pytest.raises(_RollbackCheckpointUpdateError):
        await attempt_rollback()

    async with sessions() as session:
        rolled_back = await session.get(ResearchCheckpoint, (run_id, task_key, 3))
        scope = await session.get(ResearchTaskScope, (run_id, logical_task_key))
        assert rolled_back is not None
        assert scope is not None
        assert rolled_back.status == "running"
        assert rolled_back.error_code is None
        assert rolled_back.next_page == 4
        assert rolled_back.continuation_json == {"cursor": "synthetic-page-4"}
        assert scope.generation == generation
        assert scope.status == "running"

    async with sessions() as session, session.begin():
        assert await datastore.record_research_checkpoint(**update_values, session=session)

    async with sessions() as session:
        persisted = await session.get(ResearchCheckpoint, (run_id, task_key, 3))
        scope = await session.get(ResearchTaskScope, (run_id, logical_task_key))
        assert persisted is not None
        assert scope is not None
        assert persisted.status == "failed"
        assert persisted.scope_status == "incomplete"
        assert persisted.error_type == "InterfaceError"
        assert persisted.error_code is None
        assert persisted.next_page == 4
        assert persisted.continuation_json == {"cursor": "synthetic-page-4"}
        assert scope.generation == generation
        assert scope.status == "incomplete"


@pytest.mark.asyncio
async def test_invalid_error_code_checkpoint_insert_commits_without_adapter_error(pg_datastore):
    datastore, sessions = pg_datastore
    run_id = str(uuid.uuid4())
    logical_task_key = f"synthetic-p0:{uuid.uuid4()}"
    generation = 2
    task_key = f"{logical_task_key}:g{generation}"
    await _create_run_and_scope(sessions, run_id, logical_task_key, generation)

    sqlalchemy_error = InterfaceError("synthetic", {}, Exception("synthetic"), code="rvf5")
    assert await datastore.record_research_checkpoint(
        **_page_arguments(
            run_id,
            task_key,
            generation,
            status="failed",
            scope_status="incomplete",
            error_type=type(sqlalchemy_error).__name__,
            error_code=sqlalchemy_error.code,
            reason="InterfaceError",
        )
    )

    async with sessions() as session:
        inserted = await session.get(ResearchCheckpoint, (run_id, task_key, 3))
        scope = await session.get(ResearchTaskScope, (run_id, logical_task_key))
        assert inserted is not None
        assert scope is not None
        assert inserted.status == "failed"
        assert inserted.scope_status == "incomplete"
        assert inserted.error_type == "InterfaceError"
        assert inserted.error_code is None
        assert inserted.next_page == 4
        assert inserted.continuation_json == {"cursor": "synthetic-page-4"}
        assert scope.generation == generation
        assert scope.status == "incomplete"
