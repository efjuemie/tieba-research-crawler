from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, inspect, text

from src.research.migration import (
    _ensure_content_metadata_columns,
    _ensure_research_user_global_growth_level,
    create_research_tables,
)
from src.research.models import ResearchComment, ResearchPost, ResearchThread


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


def test_content_metadata_columns_are_nullable_on_all_content_models():
    for model in (ResearchThread, ResearchPost, ResearchComment):
        assert model.__table__.c.global_growth_icon_url.nullable is True
    assert ResearchComment.__table__.c.reply_to_display_name.nullable is True


@pytest.mark.asyncio
async def test_migration_adds_nullable_content_metadata_columns_without_changing_rows(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'content-metadata-migration.db'}")
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE research_thread (tid BIGINT PRIMARY KEY, legacy TEXT NOT NULL)"))
            connection.execute(text("CREATE TABLE research_post (pid BIGINT PRIMARY KEY, legacy TEXT NOT NULL)"))
            connection.execute(text("CREATE TABLE research_comment (cid BIGINT PRIMARY KEY, legacy TEXT NOT NULL)"))
            connection.execute(text("INSERT INTO research_thread VALUES (1, 'thread')"))
            connection.execute(text("INSERT INTO research_post VALUES (2, 'post')"))
            connection.execute(text("INSERT INTO research_comment VALUES (3, 'comment')"))

        adapter = _AsyncEngineAdapter(engine)
        await create_research_tables(adapter)
        await create_research_tables(adapter)

        for table_name in ("research_thread", "research_post", "research_comment"):
            columns = {column["name"]: column for column in inspect(engine).get_columns(table_name)}
            assert columns["global_growth_icon_url"]["nullable"] is True
            assert columns["field_sources_json"]["nullable"] is True
        comment_columns = {column["name"]: column for column in inspect(engine).get_columns("research_comment")}
        assert comment_columns["reply_to_display_name"]["nullable"] is True
        with engine.connect() as connection:
            assert connection.execute(text("SELECT tid, legacy FROM research_thread")).all() == [(1, "thread")]
            assert connection.execute(text("SELECT pid, legacy FROM research_post")).all() == [(2, "post")]
            assert connection.execute(text("SELECT cid, legacy FROM research_comment")).all() == [(3, "comment")]
    finally:
        engine.dispose()


def test_postgresql_content_metadata_migration_uses_add_column_if_not_exists():
    statements = []
    connection = SimpleNamespace(
        dialect=SimpleNamespace(name="postgresql"),
        exec_driver_sql=statements.append,
    )

    _ensure_content_metadata_columns(connection)

    assert statements == [
        "ALTER TABLE research_thread ADD COLUMN IF NOT EXISTS global_growth_icon_url TEXT",
        "ALTER TABLE research_post ADD COLUMN IF NOT EXISTS global_growth_icon_url TEXT",
        "ALTER TABLE research_comment ADD COLUMN IF NOT EXISTS global_growth_icon_url TEXT",
        "ALTER TABLE research_comment ADD COLUMN IF NOT EXISTS reply_to_display_name TEXT",
    ]
