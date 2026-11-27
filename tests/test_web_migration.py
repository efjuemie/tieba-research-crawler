from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, inspect, text

from src.research.migration import (
    _ensure_content_field_sources,
    _ensure_web_enrichment_lease_indexes,
    create_research_tables,
)


class _AsyncConnectionAdapter:
    def __init__(self, connection):
        self.connection = connection

    async def run_sync(self, function, *args, **kwargs):
        return function(self.connection, *args, **kwargs)


class _AsyncBegin:
    def __init__(self, engine):
        self.engine = engine

    async def __aenter__(self):
        self.context = self.engine.begin()
        return _AsyncConnectionAdapter(self.context.__enter__())

    async def __aexit__(self, exc_type, exc, traceback):
        return self.context.__exit__(exc_type, exc, traceback)


class _AsyncEngineAdapter:
    def __init__(self, engine):
        self.engine = engine

    def begin(self):
        return _AsyncBegin(self.engine)


@pytest.mark.asyncio
async def test_web_migration_adds_source_columns_and_new_tables_idempotently(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'web-migration.db'}")
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE research_thread (tid BIGINT PRIMARY KEY, title TEXT)"))
            connection.execute(text("CREATE TABLE research_post (pid BIGINT PRIMARY KEY, text TEXT)"))
            connection.execute(text("CREATE TABLE research_comment (cid BIGINT PRIMARY KEY, text TEXT)"))
            connection.execute(
                text(
                    "CREATE TABLE research_user ("
                    "user_id BIGINT PRIMARY KEY, user_name TEXT, nickname TEXT, portrait TEXT, "
                    "tieba_uid BIGINT, first_observed_at TIMESTAMP NOT NULL, "
                    "last_observed_at TIMESTAMP NOT NULL, source_api TEXT NOT NULL, "
                    "observed_at TIMESTAMP NOT NULL, source_payload_json JSON NOT NULL, "
                    "missing_reasons_json JSON NOT NULL)"
                )
            )
            connection.execute(text("INSERT INTO research_thread (tid, title) VALUES (1001, 'synthetic')"))
            connection.execute(text("INSERT INTO research_post (pid, text) VALUES (2001, 'synthetic')"))
            connection.execute(text("INSERT INTO research_comment (cid, text) VALUES (3001, 'synthetic')"))

        adapter = _AsyncEngineAdapter(engine)
        await create_research_tables(adapter)
        await create_research_tables(adapter)

        inspector = inspect(engine)
        for table_name in ("research_thread", "research_post", "research_comment"):
            columns = {column["name"] for column in inspector.get_columns(table_name)}
            assert "field_sources_json" in columns
        table_names = set(inspector.get_table_names())
        assert "research_web_enrichment_task" in table_names
        assert "research_web_enrichment_attempt" in table_names
        assert "research_field_observation" in table_names
        assert "research_web_enrichment_failure" in table_names
        with engine.connect() as connection:
            assert connection.execute(text("SELECT tid, title FROM research_thread")).all() == [(1001, "synthetic")]
            assert connection.execute(text("SELECT pid, text FROM research_post")).all() == [(2001, "synthetic")]
            assert connection.execute(text("SELECT cid, text FROM research_comment")).all() == [(3001, "synthetic")]
    finally:
        engine.dispose()


def test_postgresql_web_source_migration_uses_add_column_if_not_exists():
    statements = []
    connection = SimpleNamespace(
        dialect=SimpleNamespace(name="postgresql"),
        exec_driver_sql=statements.append,
    )

    _ensure_content_field_sources(connection)

    assert statements == [
        "ALTER TABLE research_thread ADD COLUMN IF NOT EXISTS field_sources_json JSONB",
        "ALTER TABLE research_post ADD COLUMN IF NOT EXISTS field_sources_json JSONB",
        "ALTER TABLE research_comment ADD COLUMN IF NOT EXISTS field_sources_json JSONB",
    ]


def test_postgresql_web_enrichment_lease_indexes_migration_is_idempotent():
    pg_statements = []
    pg_connection = SimpleNamespace(
        dialect=SimpleNamespace(name="postgresql"),
        exec_driver_sql=pg_statements.append,
    )
    _ensure_web_enrichment_lease_indexes(pg_connection)
    assert len(pg_statements) == 2
    assert all("CREATE INDEX IF NOT EXISTS" in stmt for stmt in pg_statements)
    assert any("ix_rwt_active_lease_newest" in stmt for stmt in pg_statements)
    assert any("ix_rwt_active_lease_tid_desc" in stmt for stmt in pg_statements)

    sqlite_statements = []
    sqlite_connection = SimpleNamespace(
        dialect=SimpleNamespace(name="sqlite"),
        exec_driver_sql=sqlite_statements.append,
    )
    _ensure_web_enrichment_lease_indexes(sqlite_connection)
    assert sqlite_statements == []


@pytest.mark.asyncio
async def test_core_create_tables_calls_web_enrichment_lease_indexes(tmp_path):
    from src.core.initialize import create_tables

    engine = create_engine(f"sqlite:///{tmp_path / 'core-init.db'}")
    try:
        called = []

        class _ConnWrapper:
            def __init__(self, sync_conn):
                self._sync_conn = sync_conn

            async def run_sync(self, fn, *args, **kwargs):
                if fn is _ensure_web_enrichment_lease_indexes:
                    called.append(True)
                return fn(self._sync_conn, *args, **kwargs)

        class _BeginWrapper:
            def __init__(self, eng):
                self._eng = eng

            async def __aenter__(self):
                self._ctx = self._eng.begin()
                return _ConnWrapper(self._ctx.__enter__())

            async def __aexit__(self, exc_type, exc, tb):
                return self._ctx.__exit__(exc_type, exc, tb)

        class _EngineWrapper:
            def __init__(self, eng):
                self._eng = eng

            def begin(self):
                return _BeginWrapper(self._eng)

        container = SimpleNamespace(
            tb_client=object(),
            async_sessionmaker=object(),
            db_engine=_EngineWrapper(engine),
            config=SimpleNamespace(partition_enabled=False),
        )
        await create_tables(container)
        assert called == [True]
    finally:
        engine.dispose()
