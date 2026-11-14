from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import src.core.initialize as initialize_module


@pytest.mark.asyncio
async def test_research_forum_resolution_failure_tears_down_container(monkeypatch):
    class Session:
        async def execute(self, _statement):
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=list))

        async def rollback(self):
            return None

    @asynccontextmanager
    async def sessionmaker():
        yield Session()

    client = SimpleNamespace(get_fid=AsyncMock(return_value=0))
    config = SimpleNamespace(
        queue_maxsize=1,
        research_enabled=True,
        forums=["synthetic-forum"],
    )
    container = SimpleNamespace(
        config=config,
        tb_client=client,
        async_sessionmaker=sessionmaker,
        setup=AsyncMock(),
        teardown=AsyncMock(),
    )

    monkeypatch.setattr(initialize_module, "Config", lambda mode: config)
    monkeypatch.setattr(initialize_module, "Container", lambda config: container)
    monkeypatch.setattr(initialize_module, "create_tables", AsyncMock())

    with pytest.raises(RuntimeError, match="Could not resolve fid"):
        await initialize_module.initialize_application("backfill")

    client.get_fid.assert_awaited_once_with("synthetic-forum")
    container.setup.assert_awaited_once()
    container.teardown.assert_awaited_once()
