"""Idempotent, additive creation of the research extension tables."""

from __future__ import annotations

from typing import TYPE_CHECKING

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
    """Create missing research data/checkpoint tables without altering existing tables."""
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=RESEARCH_MIGRATION_TABLES)
