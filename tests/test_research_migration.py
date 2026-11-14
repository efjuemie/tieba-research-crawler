from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, inspect, text

from src.research.migration import _ensure_research_user_global_growth_level, create_research_tables


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
async def test_migration_adds_global_growth_to_existing_user_table_without_changing_rows(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'research-user-migration.db'}")
    try:
        with engine.begin() as connection:
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
            connection.execute(
                text(
                    "INSERT INTO research_user (user_id, user_name, nickname, portrait, tieba_uid, "
                    "first_observed_at, last_observed_at, source_api, observed_at, "
                    "source_payload_json, missing_reasons_json) VALUES "
                    "(123456789012345678, 'synthetic-user', 'synthetic-name', 'synthetic-portrait', "
                    "987654321012345678, '2025-01-01T00:00:00+00:00', '2025-01-02T00:00:00+00:00', "
                    "'aiotieba.get_threads', '2025-01-02T00:00:00+00:00', '{}', '{}')"
                )
            )

        adapter = _AsyncEngineAdapter(engine)
        await create_research_tables(adapter)
        await create_research_tables(adapter)

        columns = {column["name"] for column in inspect(engine).get_columns("research_user")}
        assert "global_growth_level" in columns
        with engine.connect() as connection:
            rows = connection.execute(
                text("SELECT user_id, user_name, tieba_uid, global_growth_level FROM research_user")
            ).all()
        assert rows == [(123456789012345678, "synthetic-user", 987654321012345678, None)]
    finally:
        engine.dispose()


def test_postgresql_migration_uses_idempotent_add_column_statement():
    statements = []
    connection = SimpleNamespace(
        dialect=SimpleNamespace(name="postgresql"),
        exec_driver_sql=statements.append,
    )

    _ensure_research_user_global_growth_level(connection)

    assert statements == [
        "ALTER TABLE research_user ADD COLUMN IF NOT EXISTS global_growth_level INTEGER"
    ]
