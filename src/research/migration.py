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
