"""Idempotent, additive creation of the research extension tables."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import inspect
from tiebameow.models.orm import Base

from src.core.models import ResearchCheckpoint, ResearchRun, ResearchTaskScope

from .models import RESEARCH_TABLES

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


RESEARCH_MIGRATION_TABLES = (
    *RESEARCH_TABLES,
    ResearchRun.__table__,
    ResearchTaskScope.__table__,
    ResearchCheckpoint.__table__,
)


async def create_research_tables(engine: AsyncEngine) -> None:
    """Create missing research tables and apply additive research-column migrations."""
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=RESEARCH_MIGRATION_TABLES)
        await connection.run_sync(_ensure_research_user_global_growth_level)
        await connection.run_sync(_ensure_content_field_sources)
        await connection.run_sync(_ensure_content_metadata_columns)
        await connection.run_sync(_ensure_api_field_observation_nullability)
        await connection.run_sync(_ensure_web_enrichment_lease_indexes)


def _ensure_web_enrichment_lease_indexes(connection) -> None:
    """Ensure PostgreSQL partial indexes exist for tiered web task leasing."""
    if connection.dialect.name != "postgresql":
        return
    connection.exec_driver_sql(
        """
        CREATE INDEX IF NOT EXISTS ix_rwt_active_lease_newest
        ON research_web_enrichment_task (
            run_id,
            target_type,
            attempts ASC,
            priority DESC,
            tid DESC,
            parent_post_pid ASC,
            target_id ASC,
            created_at ASC,
            task_key ASC
        )
        WHERE status IN ('pending_web', 'failed_retryable', 'running')
        """
    )
    connection.exec_driver_sql(
        """
        CREATE INDEX IF NOT EXISTS ix_rwt_active_lease_tid_desc
        ON research_web_enrichment_task (
            run_id,
            attempts ASC,
            priority DESC,
            tid DESC,
            parent_post_pid ASC,
            target_id ASC,
            created_at ASC,
            task_key ASC
        )
        WHERE status IN ('pending_web', 'failed_retryable', 'running')
        """
    )


def _ensure_research_user_global_growth_level(connection) -> None:
    if connection.dialect.name == "postgresql":
        connection.exec_driver_sql(
            "ALTER TABLE research_user ADD COLUMN IF NOT EXISTS global_growth_level INTEGER"
        )
        return

    if connection.dialect.name == "sqlite":
        columns = {column["name"] for column in inspect(connection).get_columns("research_user")}
        if "global_growth_level" not in columns:
            connection.exec_driver_sql("ALTER TABLE research_user ADD COLUMN global_growth_level INTEGER")
        return

    raise ValueError(f"Unsupported database dialect for research migration: {connection.dialect.name}.")


def _ensure_content_field_sources(connection) -> None:
    """Add web/API provenance storage without rewriting existing content rows."""
    table_names = ("research_thread", "research_post", "research_comment")
    if connection.dialect.name == "postgresql":
        for table_name in table_names:
            connection.exec_driver_sql(
                f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS field_sources_json JSONB"
            )
        return

    if connection.dialect.name == "sqlite":
        inspector = inspect(connection)
        for table_name in table_names:
            columns = {column["name"] for column in inspector.get_columns(table_name)}
            if "field_sources_json" not in columns:
                connection.exec_driver_sql(f"ALTER TABLE {table_name} ADD COLUMN field_sources_json JSON")
        return

    raise ValueError(f"Unsupported database dialect for research migration: {connection.dialect.name}.")


def _ensure_content_metadata_columns(connection) -> None:
    columns_by_table = {
        "research_thread": ("global_growth_icon_url",),
        "research_post": ("global_growth_icon_url",),
        "research_comment": ("global_growth_icon_url", "reply_to_display_name"),
    }
    if connection.dialect.name == "postgresql":
        for table_name, columns in columns_by_table.items():
            for column_name in columns:
                connection.exec_driver_sql(
                    f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} TEXT"
                )
        return

    if connection.dialect.name == "sqlite":
        for table_name, expected_columns in columns_by_table.items():
            existing_columns = {column["name"] for column in inspect(connection).get_columns(table_name)}
            for column_name in expected_columns:
                if column_name not in existing_columns:
                    connection.exec_driver_sql(f"ALTER TABLE {table_name} ADD COLUMN {column_name} TEXT")
        return

    raise ValueError(f"Unsupported database dialect for research migration: {connection.dialect.name}.")


def _ensure_api_field_observation_nullability(connection) -> None:
    """Allow API observations without a web task, run, or captured request URL."""
    if connection.dialect.name == "postgresql":
        connection.exec_driver_sql(
            "ALTER TABLE research_field_observation ALTER COLUMN task_key DROP NOT NULL"
        )
        connection.exec_driver_sql(
            "ALTER TABLE research_field_observation ALTER COLUMN run_id DROP NOT NULL"
        )
        connection.exec_driver_sql(
            "ALTER TABLE research_field_observation ALTER COLUMN source_url DROP NOT NULL"
        )
        return

    if connection.dialect.name == "sqlite":
        columns = {column["name"]: column for column in inspect(connection).get_columns("research_field_observation")}
        if any(columns[name]["nullable"] is False for name in ("task_key", "run_id", "source_url")):
            raise ValueError(
                "SQLite research_field_observation has an unsupported pre-release NOT NULL schema; "
                "create a fresh synthetic database or migrate this table explicitly."
            )
        return

    raise ValueError(f"Unsupported database dialect for research migration: {connection.dialect.name}.")
