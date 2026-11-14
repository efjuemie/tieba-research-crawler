"""Synthetic tests for research auth gates and native startup wiring."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiotieba import Client as AiotiebaClient
from aiotieba.enums import ReqUInfo
from tiebameow.client.tieba_client import RetriableApiError

import main as app_main
from src.core import container as container_module
from src.core.auth import GateAwareSemaphore, ResearchAuthError, account_file_fingerprint, wait_for_account_file_update
from src.core.container import Container
from src.research.client import ResearchClient, ResearchRunCache
from src.research.credentials import save_bduss
from src.scraper.router import QueueRouter


def _runtime_container(tmp_path, *, global_rps=50.0, concurrency_limit=1):
    config = SimpleNamespace(
        research_enabled=True,
        fetch_user_forum_info=True,
        research_account_file=str(tmp_path / "account.json"),
        research_max_api_retries=1,
        research_retry_wait_seconds=0,
        cooldown_seconds_429=0.01,
        concurrency_limit=concurrency_limit,
        global_rps=global_rps,
    )
    container = Container(config)
    raw_semaphore = asyncio.Semaphore(concurrency_limit)
    container._request_semaphore = raw_semaphore
    container.semaphore = GateAwareSemaphore(
        raw_semaphore,
        container.wait_for_research_ready,
        container._research_request_gates_open,
        container.wait_for_research_network_ready,
        container._research_network_gates_open,
        minimum_interval_seconds=1 / global_rps,
    )
    return container, raw_semaphore


def _use_fast_account_poll(monkeypatch):
    original = container_module.wait_for_account_file_update

    async def fast_poll(path, fingerprint):
        return await original(path, fingerprint, poll_interval=0.001)

    monkeypatch.setattr(container_module, "wait_for_account_file_update", fast_poll)


class _AuthProbeClient:
    def __init__(self, results):
        self.results = list(results)
        self.calls = 0
        self.require_values = []

    async def get_self_info(self, *, require):
        self.calls += 1
        self.require_values.append(require)
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


@pytest.mark.asyncio
async def test_account_file_watcher_reads_only_after_file_changes(tmp_path):
    path = tmp_path / "account.json"
    fingerprint = account_file_fingerprint(path)
    task = asyncio.create_task(wait_for_account_file_update(path, fingerprint, poll_interval=0.001))
    credential = "A" * 192
    await asyncio.sleep(0.01)
    save_bduss(credential, path)

    loaded, updated_fingerprint = await asyncio.wait_for(task, timeout=1)

    assert loaded == credential
    assert updated_fingerprint == account_file_fingerprint(path)


@pytest.mark.asyncio
async def test_initial_auth_wait_updates_clients_then_opens_auth_gate(tmp_path, monkeypatch):
    _use_fast_account_poll(monkeypatch)
    container, _ = _runtime_container(tmp_path)
    container._research_account_file_state = account_file_fingerprint(container.config.research_account_file)
    client = _AuthProbeClient([SimpleNamespace(user_id=2468)])
    container.tb_client = client
    container._replace_research_accounts = AsyncMock()
    container.research_auth_ready.clear()
    task = asyncio.create_task(container.wait_for_initial_auth())
    credential = "B" * 192
    await asyncio.sleep(0.01)
    save_bduss(credential, container.config.research_account_file)

    await asyncio.wait_for(task, timeout=1)

    container._replace_research_accounts.assert_awaited_once_with(credential)
    assert client.calls == 1
    assert client.require_values == [ReqUInfo.BASIC]
    assert container.research_auth_ready.is_set()


@pytest.mark.asyncio
async def test_initial_auth_wait_rejects_expired_credential_until_next_file_change(tmp_path, monkeypatch):
    _use_fast_account_poll(monkeypatch)
    container, _ = _runtime_container(tmp_path)
    container._research_account_file_state = account_file_fingerprint(container.config.research_account_file)
    client = _AuthProbeClient([
        RetriableApiError(4011, "synthetic"),
        SimpleNamespace(user_id=2468),
    ])
    container.tb_client = client
    container._replace_research_accounts = AsyncMock()
    container.research_auth_ready.clear()
    task = asyncio.create_task(container.wait_for_initial_auth())
    first_credential = "C" * 192
    second_credential = "D" * 192
    await asyncio.sleep(0.01)
    save_bduss(first_credential, container.config.research_account_file)
    for _ in range(100):
        if client.calls == 1:
            break
        await asyncio.sleep(0.005)
    assert client.calls == 1
    assert not container.research_auth_ready.is_set()
    save_bduss(second_credential, container.config.research_account_file)

    await asyncio.wait_for(task, timeout=1)

    assert client.calls == 2
    assert client.require_values == [ReqUInfo.BASIC, ReqUInfo.BASIC]
    assert container.research_auth_ready.is_set()


@pytest.mark.asyncio
async def test_auth_refresh_retries_original_request_after_account_update(tmp_path, monkeypatch):
    _use_fast_account_poll(monkeypatch)
    container, _ = _runtime_container(tmp_path)
    container._research_account_file_state = account_file_fingerprint(container.config.research_account_file)
    container._research_credentials_loaded = False
    container.tb_client = _AuthProbeClient([SimpleNamespace(user_id=2468)])
    container._replace_research_accounts = AsyncMock()
    container.research_auth_ready.clear()
    result = asyncio.get_running_loop().create_future()
    credential = "E" * 192
    task = asyncio.create_task(
        container.wait_for_auth_refresh(lambda: _set_result(result), run_id="synthetic-run")
    )
    await asyncio.sleep(0.01)
    save_bduss(credential, container.config.research_account_file)

    await asyncio.wait_for(task, timeout=1)

    assert result.result() == "request-result"
    assert container.research_auth_ready.is_set()
    assert container.tb_client.require_values == [ReqUInfo.BASIC]
    container._replace_research_accounts.assert_awaited_once_with(credential)


@pytest.mark.asyncio
async def test_auth_validation_uses_basic_sdk_path_without_profile_request(monkeypatch):
    login_calls = 0
    profile_calls = 0

    async def fake_login(client):
        nonlocal login_calls
        login_calls += 1
        client.account.user.user_id = 2468

    async def fail_profile(_client, *_args, **_kwargs):
        nonlocal profile_calls
        profile_calls += 1
        raise AssertionError("basic account validation must not request profile details")

    monkeypatch.setattr(AiotiebaClient, "_Client__login", fake_login)
    monkeypatch.setattr(AiotiebaClient, "_get_uinfo_profile", fail_profile)
    client = ResearchClient(BDUSS="G" * 192, retry_attempts=1)

    await container_module.verify_research_credentials(
        client,
        None,
        max_attempts=1,
        retry_wait_seconds=0,
    )

    assert login_calls == 1
    assert profile_calls == 0
    assert client.account.user.user_id == 2468


@pytest.mark.asyncio
@pytest.mark.parametrize("paused_gate", ["auth", "network"])
async def test_sdk_requests_are_spaced_after_auth_or_network_pause(tmp_path, monkeypatch, paused_gate):
    container, raw_semaphore = _runtime_container(tmp_path, global_rps=50.0, concurrency_limit=4)
    gate = container.research_auth_ready if paused_gate == "auth" else container.research_network_ready
    gate.clear()
    started_at = []
    loop = asyncio.get_running_loop()

    async def fake_login(client):
        started_at.append(loop.time())
        await asyncio.sleep(0)
        client.account.user.user_id = client.synthetic_user_id

    monkeypatch.setattr(AiotiebaClient, "_Client__login", fake_login)
    clients = [
        ResearchClient(BDUSS="H" * 192, semaphore=container.semaphore, retry_attempts=1)
        for _ in range(4)
    ]
    for index, client in enumerate(clients, start=1):
        client.synthetic_user_id = index

    requests = [
        asyncio.create_task(client.get_self_info(require=ReqUInfo.BASIC))
        for client in clients
    ]
    await asyncio.sleep(0.1)
    assert started_at == []
    assert raw_semaphore._value == 4

    gate.set()
    users = await asyncio.wait_for(asyncio.gather(*requests), timeout=1)

    assert sorted(user.user_id for user in users) == [1, 2, 3, 4]
    assert len(started_at) == 4
    assert all(later - earlier >= 0.018 for earlier, later in zip(started_at, started_at[1:], strict=False))
    assert raw_semaphore._value == 4


@pytest.mark.asyncio
async def test_account_refresh_replaces_cached_account_state_and_clears_run_cache(tmp_path):
    container, _ = _runtime_container(tmp_path)
    cache = ResearchRunCache()
    cache._user_forum[(7, 8)] = object()
    client = ResearchClient(research_run_cache=cache)
    await client.__aenter__()
    try:
        client.account.user.user_id = 2468
        client.account.tbs = "T" * 26
        container.tb_client = client
        container.research_run_cache = cache

        await container._replace_research_accounts("F" * 192)

        assert client.account.BDUSS == "F" * 192
        assert client.account.user.user_id == 0
        assert client.account.tbs is None
        assert not cache._user_forum
    finally:
        await client.__aexit__()


async def _set_result(future):
    future.set_result("request-result")
    return "request-result"


@pytest.mark.asyncio
async def test_rate_cooldown_does_not_open_auth_gate_and_is_shared(tmp_path):
    container, _ = _runtime_container(tmp_path)
    container.research_auth_ready.clear()
    started = asyncio.get_running_loop().time()
    first = asyncio.create_task(container.wait_for_rate_limit(0.04))
    await asyncio.sleep(0.005)
    second = asyncio.create_task(container.wait_for_rate_limit(0.08))
    ready = asyncio.create_task(container.wait_for_research_ready())

    await asyncio.gather(first, second)
    assert not container.research_auth_ready.is_set()
    assert not ready.done()
    container.research_auth_ready.set()
    await asyncio.wait_for(ready, timeout=1)
    assert asyncio.get_running_loop().time() - started >= 0.07


@pytest.mark.asyncio
async def test_gate_aware_semaphore_rechecks_auth_after_queueing(tmp_path):
    container, raw_semaphore = _runtime_container(tmp_path)
    await raw_semaphore.acquire()
    acquire_task = asyncio.create_task(container.semaphore.acquire())
    await asyncio.sleep(0.01)
    container.research_auth_ready.clear()
    raw_semaphore.release()
    await asyncio.sleep(0.02)

    assert not acquire_task.done()
    assert raw_semaphore._value == 1

    container.research_auth_ready.set()
    await asyncio.wait_for(acquire_task, timeout=1)
    container.semaphore.release()
    assert raw_semaphore._value == 1


@pytest.mark.asyncio
async def test_gate_aware_semaphore_releases_capacity_when_admission_is_cancelled():
    checker_entered = asyncio.Event()
    finish_check = asyncio.Event()
    raw_semaphore = asyncio.Semaphore(1)

    async def wait_ready():
        return None

    async def is_ready():
        checker_entered.set()
        await finish_check.wait()
        return True

    semaphore = GateAwareSemaphore(
        raw_semaphore,
        wait_ready,
        is_ready,
        wait_ready,
        is_ready,
        minimum_interval_seconds=0.02,
    )
    acquire_task = asyncio.create_task(semaphore.acquire())
    await asyncio.wait_for(checker_entered.wait(), timeout=1)

    assert raw_semaphore._value == 0
    acquire_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire_task

    assert raw_semaphore._value == 1


@pytest.mark.asyncio
async def test_blocked_network_gate_is_not_reopened_by_cooldown(tmp_path):
    container, _ = _runtime_container(tmp_path)
    await container.block_research_network()
    wait_task = asyncio.create_task(container.wait_for_research_ready())
    await container.wait_for_rate_limit(0.01)

    assert not container.research_network_ready.is_set()
    assert not wait_task.done()
    wait_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await wait_task


@pytest.mark.asyncio
async def test_auth_wait_cancellation_propagates_without_opening_gate(tmp_path, monkeypatch):
    _use_fast_account_poll(monkeypatch)
    container, _ = _runtime_container(tmp_path)
    container._research_account_file_state = account_file_fingerprint(container.config.research_account_file)
    container.research_auth_ready.clear()
    task = asyncio.create_task(container.wait_for_initial_auth())
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert not container.research_auth_ready.is_set()


def _config_for_container(*, consumer_transport: str, cache_backend: str):
    return SimpleNamespace(
        research_enabled=False,
        fetch_user_forum_info=False,
        proxy_urls=[],
        proxy_url=None,
        concurrency_limit=1,
        threads_rps=1.0,
        posts_rps=1.0,
        comments_rps=1.0,
        cooldown_seconds_429=1.0,
        database_url="postgresql+asyncpg://synthetic",
        consumer_transport=consumer_transport,
        cache_backend=cache_backend,
        redis_url="redis://synthetic",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("consumer_transport", "cache_backend", "should_connect"),
    [("none", "memory", False), ("none", "redis", True), ("redis", "memory", True)],
)
async def test_container_connects_redis_only_when_a_configured_feature_needs_it(
    monkeypatch,
    consumer_transport,
    cache_backend,
    should_connect,
):
    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class FakeEngine:
        async def dispose(self):
            return None

    fake_redis = SimpleNamespace(ping=AsyncMock(), close=AsyncMock())
    from_url = AsyncMock(return_value=fake_redis)
    monkeypatch.setattr(container_module, "Client", FakeClient)
    monkeypatch.setattr(container_module, "create_async_engine", lambda *_args, **_kwargs: FakeEngine())
    monkeypatch.setattr(container_module, "async_sessionmaker", lambda **_kwargs: object())
    monkeypatch.setattr(container_module.redis, "from_url", from_url)
    container = Container(
        _config_for_container(consumer_transport=consumer_transport, cache_backend=cache_backend)
    )

    await container.setup()
    assert from_url.await_count == int(should_connect)
    assert container.semaphore._minimum_interval_seconds == 0
    await container.teardown()


@pytest.mark.asyncio
async def test_research_clients_share_gate_for_global_request_spacing(monkeypatch):
    client_kwargs = []

    class FakeResearchClient:
        def __init__(self, *_args, **kwargs):
            client_kwargs.append(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class FakeEngine:
        async def dispose(self):
            return None

    config = _config_for_container(consumer_transport="none", cache_backend="memory")
    config.research_enabled = True
    config.proxy_urls = ["http://127.0.0.1:9991", "http://127.0.0.1:9992"]
    config.global_rps = 0.2
    config.research_max_api_retries = 1
    config.research_retry_wait_seconds = 0

    monkeypatch.setattr(container_module, "ResearchClient", FakeResearchClient)
    monkeypatch.setattr(container_module, "create_async_engine", lambda *_args, **_kwargs: FakeEngine())
    monkeypatch.setattr(container_module, "async_sessionmaker", lambda **_kwargs: object())
    container = Container(config)

    await container.setup()

    assert len(client_kwargs) == 2
    assert container.semaphore._minimum_interval_seconds == pytest.approx(5.0)
    assert all("limiter" not in kwargs for kwargs in client_kwargs)
    assert all(kwargs["semaphore"] is container.semaphore for kwargs in client_kwargs)
    assert all(kwargs["research_run_cache"] is container.research_run_cache for kwargs in client_kwargs)

    await container.teardown()


@pytest.mark.asyncio
async def test_prepare_research_run_persists_auth_pause_and_resumes_same_run():
    class FakeStore:
        def __init__(self):
            self.statuses = []
            self.resumed = []

        async def start_research_run(self, **kwargs):
            self.start_args = kwargs
            return "synthetic-run"

        async def set_research_run_status(self, *args):
            self.statuses.append(args)

        async def resume_research_run_if_status(self, *args):
            self.resumed.append(args)
            return True

    class FakeCache:
        def __init__(self):
            self.clears = 0

        def clear(self):
            self.clears += 1

    class FakeContainer:
        config = SimpleNamespace(
            research_enabled=True,
            fetch_user_forum_info=True,
            research_account_file="secrets/synthetic.json",
            max_backfill_pages=5,
        )
        forums = [SimpleNamespace(fid=7, fname="synthetic")]

        def __init__(self):
            self.research_auth_ready = asyncio.Event()
            self.research_auth_ready.set()
            self.research_run_cache = FakeCache()
            self.research_run_id = None
            self.waited = False

        async def validate_research_auth(self):
            raise ResearchAuthError("paused_auth", "account_file_missing_or_invalid")

        async def wait_for_initial_auth(self):
            self.waited = True
            self.research_auth_ready.set()

    store = FakeStore()
    container = FakeContainer()

    run_id = await app_main._prepare_research_run(
        container,
        store,
        "backfill",
        resume=True,
        run_id="synthetic-run",
    )

    assert run_id == container.research_run_id == "synthetic-run"
    assert store.start_args["resume"] is True
    assert store.start_args["run_id"] == "synthetic-run"
    assert store.statuses == [("synthetic-run", "paused_auth", "account_file_missing_or_invalid")]
    assert store.resumed == [("synthetic-run", "paused_auth")]
    assert container.waited
    assert container.research_auth_ready.is_set()
    assert container.research_run_cache.clears == 1


@pytest.mark.asyncio
async def test_research_run_id_requires_resume():
    config = SimpleNamespace(research_enabled=True)
    container = SimpleNamespace(config=config)
    with pytest.raises(ValueError, match="requires --resume"):
        await app_main._prepare_research_run(
            container,
            object(),
            "backfill",
            resume=False,
            run_id="synthetic-run",
        )


@pytest.mark.asyncio
async def test_workers_start_before_bounded_pending_recovery_and_scheduler():
    worker_started = asyncio.Event()
    task_consumed = asyncio.Event()
    recovery_finished = asyncio.Event()

    class PendingThread:
        fid = 1
        tid = 2
        backfill = True

    class FakeStore:
        def __init__(self, _container):
            pass

        async def iter_pending_thread_scans(self, _fids, *, batch_size):
            assert batch_size == 500
            await asyncio.wait_for(worker_started.wait(), timeout=1)
            yield [PendingThread()]
            recovery_finished.set()

        async def iter_pending_comment_scans(self, _fids, *, batch_size):
            assert batch_size == 500
            if False:
                yield []

        async def close(self):
            return None

    class FakeScheduler:
        def __init__(self, *, router, container, datastore=None):
            self.router = router

        async def run(self, mode):
            assert mode == "backfill"
            await asyncio.wait_for(task_consumed.wait(), timeout=1)
            assert recovery_finished.is_set()

    class FakeWorker:
        def __init__(self, _worker_id, queue, _container, _datastore, _router, *, lane):
            self.queue = queue
            self.lane = lane
            self.worker_id = 0

        async def run(self):
            worker_started.set()
            try:
                task = await self.queue.get()
                assert task.content.tid == 2
                task_consumed.set()
                self.queue.task_done(task)
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return

    class FakeContainer:
        config = SimpleNamespace(
            metrics_enabled=False,
            worker_threads=0,
            worker_posts=1,
            worker_comments=0,
            research_enabled=False,
            queue_maxsize=1,
        )
        forums = [SimpleNamespace(fid=1, fname="synthetic")]

        async def teardown(self):
            return None

    container = FakeContainer()
    router = QueueRouter(maxsize=1)

    async def initialize(*, mode):
        assert mode == "backfill"
        return container, router

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(app_main, "initialize_application", initialize)
    monkeypatch.setattr(app_main, "DataStore", FakeStore)
    monkeypatch.setattr(app_main, "Scheduler", FakeScheduler)
    monkeypatch.setattr(app_main, "Worker", FakeWorker)
    try:
        await asyncio.wait_for(app_main.main("backfill"), timeout=2)
    finally:
        monkeypatch.undo()
