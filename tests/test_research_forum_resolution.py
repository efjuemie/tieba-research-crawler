import asyncio
from types import SimpleNamespace

import pytest
from aiotieba import Client as SDKClient
from aiotieba.exception import StrResponse

from src.core.auth import GateAwareSemaphore
from src.research import client as client_module
from src.research.client import ResearchClient


@pytest.mark.asyncio
async def test_cold_forum_id_resolution_and_list_have_separate_request_admissions(monkeypatch):
    calls = []

    async def ready():
        return True

    async def wait():
        return None

    async def resolve(self, fid):
        calls.append(("resolve", asyncio.get_running_loop().time(), fid))
        return StrResponse("合成贴吧")

    async def threads(self, name, pn=1, **kwargs):
        calls.append(("list", asyncio.get_running_loop().time(), name))
        return SimpleNamespace(err=None, page=SimpleNamespace(total_count=1))

    monkeypatch.setattr(client_module.ForumInfoCache, "get_fname", lambda fid: "")
    monkeypatch.setattr(SDKClient, "get_fname", resolve)
    monkeypatch.setattr(SDKClient, "get_threads", threads)
    monkeypatch.setattr(
        client_module,
        "convert_aiotieba_threads",
        lambda raw: SimpleNamespace(page=SimpleNamespace(total_count=0)),
    )
    budget = GateAwareSemaphore(
        asyncio.Semaphore(1), wait, ready, wait, ready, minimum_interval_seconds=0.03
    )
    async with ResearchClient(semaphore=budget, retry_attempts=1, try_ws=False) as client:
        page = await client.get_threads_research(123)
    assert [call[0] for call in calls] == ["resolve", "list"]
    assert calls[1][1] - calls[0][1] >= 0.025
    assert calls[1][2] == "合成贴吧"
    assert page.dto.page.total_count == 1


@pytest.mark.asyncio
async def test_cached_forum_name_does_not_make_another_resolver_call(monkeypatch):
    async def unexpected(*args, **kwargs):
        raise AssertionError("cached forum unexpectedly resolved")

    async def threads(self, name, pn=1, **kwargs):
        assert name == "合成贴吧"
        return SimpleNamespace(err=None, page=SimpleNamespace(total_count=1))

    monkeypatch.setattr(client_module.ForumInfoCache, "get_fname", lambda fid: "合成贴吧")
    monkeypatch.setattr(SDKClient, "get_fname", unexpected)
    monkeypatch.setattr(SDKClient, "get_threads", threads)
    monkeypatch.setattr(
        client_module,
        "convert_aiotieba_threads",
        lambda raw: SimpleNamespace(page=SimpleNamespace(total_count=0)),
    )
    async with ResearchClient(retry_attempts=1, try_ws=False) as client:
        await client.get_threads_research(123)


@pytest.mark.asyncio
@pytest.mark.parametrize("fid", [True, False, 0, -1, 1 << 63])
async def test_invalid_forum_id_is_rejected_before_any_request(fid):
    async with ResearchClient(retry_attempts=1, try_ws=False) as client:
        with pytest.raises(ValueError, match="Invalid forum ID"):
            await client.get_threads_research(fid)
