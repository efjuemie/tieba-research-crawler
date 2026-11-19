"""DataStore 核心功能测试。"""

import json
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from conftest import make_thread
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import InterfaceError
from tiebameow.models.dto import BaseThreadDTO, ThreadDTO, ThreadUserDTO

from src.core.config import ConsumerConfig
from src.core.datastore import DataStore

# ==================== Mock 类 ====================


class DummyCache:
    """测试用缓存模拟类"""

    def __init__(self) -> None:
        self._kv: dict[str, object] = {}

    async def get_many(self, *keys: str) -> tuple[object | None, ...]:
        return tuple(self._kv.get(k) for k in keys)

    async def set_many(self, mapping: dict[str, object], expire: int | float | None = None) -> None:
        self._kv.update(mapping)


class DummyRedis:
    """测试用 Redis 模拟类"""

    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.streams: dict[str, list[bytes]] = {}

    async def lpush(self, key: str, value: str) -> int:
        lst = self.lists.setdefault(key, [])
        lst.insert(0, value)
        return len(lst)

    async def ltrim(self, key: str, start: int, stop: int) -> None:
        if key not in self.lists:
            return
        self.lists[key] = self.lists[key][start : stop + 1 if stop >= 0 else None]

    async def xadd(
        self,
        stream: str,
        entry: dict[str, bytes],
        maxlen: int | None = None,
        approximate: bool | None = None,
    ) -> str:
        bucket = self.streams.setdefault(stream, [])
        bucket.append(entry["data"])
        if maxlen is not None and len(bucket) > maxlen:
            del bucket[:-maxlen]
        return f"0-{len(bucket)}"

    async def wait(self, n: int, timeout_ms: int) -> int:
        return 1

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False) -> bool:
        return True

    async def delete(self, key: str) -> int:
        return 1


class DummySession:
    """测试用数据库会话模拟类"""

    def __init__(self, existing_ids: set[int] | None = None) -> None:
        self.existing_ids = existing_ids or set()
        self._tx_committed = False
        self._items_added: list[Any] = []
        self.last_statement: object | None = None
        self.statements: list[object] = []

    async def execute(self, statement: object):
        self.last_statement = statement
        self.statements.append(statement)

        class Result:
            def __init__(self, ids: set[int]) -> None:
                self._ids = ids

            def fetchall(self) -> list[tuple[int]]:
                return [(i,) for i in self._ids]

            def scalars(self) -> "Result":
                return self

            def all(self) -> list[Any]:
                return []

        return Result(self.existing_ids)

    async def commit(self) -> None:
        self._tx_committed = True

    async def rollback(self) -> None:
        pass


class DummySessionMaker:
    """测试用会话工厂模拟类"""

    def __init__(self, session: DummySession) -> None:
        self._session = session

    def __call__(self) -> "DummySessionMaker":
        return self

    async def __aenter__(self) -> DummySession:
        return self._session

    async def __aexit__(self, *exc: object) -> bool:
        return False


class DummyConfig:
    """测试用配置模拟类"""

    def __init__(self, transport: str = "redis") -> None:
        self.cache_backend = "memory"
        self.cache_max_size = 100_000
        self.cache_ttl_seconds = 86400
        self.redis_url = "redis://localhost:6379/0"
        self.websocket_enabled = False
        self.websocket_url = "ws://127.0.0.1:0/ws"

        self._consumer_config = ConsumerConfig(
            transport=transport,  # type: ignore
            max_len=16,
            stream_prefix="test:stream",
        )

    @property
    def consumer_transport(self) -> str:
        return self._consumer_config.transport

    @property
    def consumer_config(self) -> ConsumerConfig:
        return self._consumer_config


class DummyContainer:
    """测试用容器模拟类"""

    def __init__(
        self,
        session: DummySession,
        redis_client: DummyRedis,
        config: DummyConfig | None = None,
    ) -> None:
        self.redis_client = redis_client
        self.async_sessionmaker = DummySessionMaker(session)
        self.config = config or DummyConfig()
        self.ws_server = None


# ==================== 缓存过滤测试 ====================


@pytest.mark.asyncio
async def test_filter_by_cache_and_mark_processed():
    """测试缓存过滤和标记已处理"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    # 第一次过滤，所有 ID 都应该是新的
    new = await ds._filter_by_cache("thread", [1, 2, 3])
    assert new == {1, 2, 3}

    # 标记为已处理
    await ds._mark_as_processed("thread", new)

    # 第二次过滤，应该都被过滤掉
    new_again = await ds._filter_by_cache("thread", [1, 2, 3])
    assert new_again == set()


@pytest.mark.asyncio
async def test_filter_by_database():
    """测试数据库过滤"""
    DataStore._cache = None

    # 模拟数据库中已存在 ID 1 和 2
    dummy_session = DummySession(existing_ids={1, 2})
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    # 只有 ID 3 应该是新的
    new = await ds._filter_by_database("thread", [1, 2, 3])
    assert new == {3}


@pytest.mark.asyncio
async def test_filter_new_ids_full_flow():
    """测试完整的 filter_new_ids 流程"""
    DataStore._cache = None

    # 数据库中已存在 ID 1
    dummy_session = DummySession(existing_ids={1})
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    # 第一次调用：ID 2 和 3 应该是新的
    new = await ds.filter_new_ids("thread", [1, 2, 3])
    assert new == {2, 3}

    # 第二次调用：ID 2 和 3 已被缓存标记，应该返回空
    # 注意：由于缓存标记，即使数据库中不存在，也会被认为已处理
    new_again = await ds.filter_new_ids("thread", [2, 3])
    assert new_again == set()


@pytest.mark.asyncio
async def test_filter_new_ids_with_duplicates():
    """测试 filter_new_ids 处理重复 ID"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    # 包含重复 ID
    new = await ds.filter_new_ids("thread", [1, 1, 2, 2, 3])
    assert new == {1, 2, 3}


# ==================== 消息推送测试 ====================


@pytest.mark.asyncio
async def test_push_object_event():
    """测试推送对象事件到 Redis Stream"""
    DataStore._cache = None

    session = DummySession(existing_ids=set())
    redis = DummyRedis()
    container = DummyContainer(session, redis, DummyConfig(transport="redis"))

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    user = ThreadUserDTO(
        user_id=1,
        portrait="p",
        user_name="u",
        nick_name_new="n",
        level=1,
        glevel=1,
        gender="UNKNOWN",
        icons=[],
        is_bawu=False,
        is_vip=False,
        is_god=False,
        priv_like="PUBLIC",
        priv_reply="ALL",
    )
    obj = ThreadDTO(
        tid=77,
        fid=1,
        fname="f",
        pid=77,
        author_id=1,
        author=user,
        title="t",
        contents=[],
        is_good=False,
        is_top=False,
        is_share=False,
        is_hide=False,
        is_livepost=False,
        is_help=False,
        agree_num=0,
        disagree_num=0,
        reply_num=0,
        view_num=0,
        share_num=0,
        create_time=datetime.fromtimestamp(0),
        last_time=datetime.fromtimestamp(0),
        thread_type=0,
        tab_id=0,
        share_origin=BaseThreadDTO(pid=0, tid=0, fid=0, fname="", author_id=0, title="", contents=[]),
    )

    await ds.push_object_event("thread", obj)

    # 验证消息被推送到 Redis Stream
    stream_key = f"{container.config.consumer_config.stream_prefix}:1"
    assert stream_key in redis.streams
    payload = redis.streams[stream_key][0]
    assert payload.startswith(b"{")

    # 验证 payload 内容
    data = json.loads(payload)
    assert data["object_type"] == "thread"
    assert data["object_id"] == 77
    assert data["fid"] == 1


@pytest.mark.asyncio
async def test_push_object_event_noop_when_transport_none():
    """测试 transport=none 时 push_object_event 是 no-op"""
    DataStore._cache = None

    session = DummySession(existing_ids=set())
    redis = DummyRedis()
    container = DummyContainer(session, redis, DummyConfig(transport="none"))

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    user = ThreadUserDTO(
        user_id=1,
        portrait="p",
        user_name="u",
        nick_name_new="n",
        level=1,
        glevel=1,
        gender="UNKNOWN",
        icons=[],
        is_bawu=False,
        is_vip=False,
        is_god=False,
        priv_like="PUBLIC",
        priv_reply="ALL",
    )
    obj = ThreadDTO(
        tid=77,
        fid=1,
        fname="f",
        pid=77,
        author_id=1,
        author=user,
        title="t",
        contents=[],
        is_good=False,
        is_top=False,
        is_share=False,
        is_hide=False,
        is_livepost=False,
        is_help=False,
        agree_num=0,
        disagree_num=0,
        reply_num=0,
        view_num=0,
        share_num=0,
        create_time=datetime.fromtimestamp(0),
        last_time=datetime.fromtimestamp(0),
        thread_type=0,
        tab_id=0,
        share_origin=BaseThreadDTO(pid=0, tid=0, fid=0, fname="", author_id=0, title="", contents=[]),
    )

    await ds.push_object_event("thread", obj)

    # transport=none 时不应该推送到 Stream
    stream_key = f"{container.config.consumer_config.stream_prefix}:1"
    assert stream_key not in redis.streams


@pytest.mark.asyncio
async def test_get_pending_thread_scan_tids_returns_existing_subset():
    """测试 get_pending_thread_scan_tids 返回待扫描 tid 子集。"""
    DataStore._cache = None

    # DummySession.fetchall 会返回 existing_ids 中的值
    dummy_session = DummySession(existing_ids={1, 3})
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    pending = await ds.get_pending_thread_scan_tids({1, 2, 3, 4})
    assert pending == {1, 3}


# ==================== 边界情况测试 ====================


@pytest.mark.asyncio
async def test_filter_new_ids_empty_list():
    """测试空列表输入"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    new = await ds.filter_new_ids("thread", [])
    assert new == set()


@pytest.mark.asyncio
async def test_mark_as_processed_empty_set():
    """测试空集合标记"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    cache = DummyCache()
    ds.cache = cast("Any", cache)

    # 空集合不应该写入缓存
    await ds._mark_as_processed("thread", set())
    assert len(cache._kv) == 0


@pytest.mark.asyncio
async def test_add_pending_comment_scan_uses_conflict_update_for_task_kind_upgrade():
    """测试 pending_comment_scan 在主键冲突时会更新 task_kind 与 backfill。"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    await ds.add_pending_comment_scan(tid=1, pid=2, fid=9, fname="foo", backfill=False, task_kind="full")

    assert dummy_session.last_statement is not None
    compiled = str(
        dummy_session.last_statement.compile(  # type: ignore[attr-defined]
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "ON CONFLICT" in compiled
    assert "DO UPDATE" in compiled
    assert "task_kind" in compiled
    assert "backfill" in compiled
    assert "fid" in compiled
    assert "fname" in compiled


@pytest.mark.asyncio
async def test_backfill_saves_pending_detail_for_zero_reply_thread():
    """零回复回溯主题帖仍需 pending 详情任务读取完整首楼。"""
    DataStore._cache = None
    dummy_session = DummySession(existing_ids=set())
    container = DummyContainer(dummy_session, DummyRedis())
    datastore = DataStore(cast("Any", container))
    datastore.cache = cast("Any", DummyCache())

    thread = make_thread(tid=321, reply_num=0)
    await datastore.save_threads_and_pending_scans([thread], backfill=True)

    assert len(dummy_session.statements) == 2
    pending_statement = dummy_session.statements[-1]
    compiled = str(
        pending_statement.compile(  # type: ignore[attr-defined]
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "INSERT INTO pending_thread_scan" in compiled
    assert "321" in compiled


@pytest.mark.asyncio
async def test_remove_pending_comment_scan_supports_task_kind_filter():
    """测试 remove_pending_comment_scan 支持按 task_kind 条件删除。"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    await ds.remove_pending_comment_scan(1, 2, task_kind="incremental")

    assert dummy_session.last_statement is not None
    compiled = str(
        dummy_session.last_statement.compile(  # type: ignore[attr-defined]
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "DELETE FROM pending_comment_scan" in compiled
    assert "task_kind" in compiled


@pytest.mark.asyncio
async def test_explicit_zero_comment_checkpoint_resumes_as_completed_scope():
    continuation = {
        "completion_basis": "parent_api_explicit_zero",
        "comment_api_called": False,
    }
    row = SimpleNamespace(
        status="completed",
        scope_status="completed",
        has_more=None,
        continuation_json=continuation,
    )

    class Result:
        def scalar_one_or_none(self):
            return row

    class Session:
        def __call__(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def execute(self, _statement):
            return Result()

    container = DummyContainer(DummySession(), DummyRedis())
    container.async_sessionmaker = Session()
    datastore = DataStore(cast("Any", container))

    assert await datastore.get_research_resume_state("run", "post_comments:1:2:g3") == (
        None,
        "completed",
        continuation,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "pending_only", "expected"),
    [
        ("pending", True, True),
        ("incomplete", True, False),
        ("incomplete", False, True),
        ("paused_auth", False, True),
        ("paused_rate_limit", False, True),
        ("completed", False, False),
        ("blocked", False, False),
    ],
)
async def test_comment_scope_claim_serializes_parent_before_child(monkeypatch, status, pending_only, expected):
    class Session:
        def __call__(self):
            return self

        def begin(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    parent = SimpleNamespace(generation=4)
    scope = SimpleNamespace(
        generation=6,
        scope_type="post_comments",
        task_type="full_scan_comments",
        tid=9,
        parent_pid=100,
        parent_logical_task_key="thread_replies:9",
        parent_generation=4,
        status=status,
    )
    locked_keys: list[str] = []

    async def get_scope_for_update(_session, run_id, logical_key):
        assert run_id == "run"
        locked_keys.append(logical_key)
        return parent if logical_key == "thread_replies:9" else scope

    container = DummyContainer(DummySession(), DummyRedis())
    container.async_sessionmaker = Session()
    datastore = DataStore(cast("Any", container))
    monkeypatch.setattr(datastore, "get_research_task_scope_for_update", get_scope_for_update)

    claimed = await datastore.claim_research_comments_scope(
        run_id="run",
        logical_task_key="post_comments:9:100",
        generation=6,
        parent_logical_task_key="thread_replies:9",
        parent_generation=4,
        tid=9,
        parent_pid=100,
        pending_only=pending_only,
    )

    assert claimed is expected
    assert locked_keys == ["thread_replies:9", "post_comments:9:100"]
    assert scope.status == ("running" if expected else status)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_code",
    [
        "rvf5",
        "429",
        True,
        429.0,
        [],
        {},
        2**31,
        -(2**31) - 1,
    ],
    ids=["sqlalchemy-doc-code", "numeric-string", "bool", "float", "list", "mapping", "overflow", "underflow"],
)
@pytest.mark.parametrize("caller_owned", [False, True], ids=["owned-session", "caller-session"])
async def test_record_checkpoint_normalizes_invalid_error_code_before_persistence(
    monkeypatch, raw_code, caller_owned
):
    datastore = DataStore.__new__(DataStore)
    values_seen = []

    async def record(_session, *, values, **_kwargs):
        values_seen.append(values)
        return True

    monkeypatch.setattr(datastore, "_record_research_checkpoint_in_session", record)

    class Session:
        def __init__(self):
            self.entered = 0
            self.exited = 0
            self.begin_called = 0
            self.commit_called = 0
            self.rollback_called = 0

        def __call__(self):
            return self

        def begin(self):
            self.begin_called += 1
            return self

        async def __aenter__(self):
            self.entered += 1
            return self

        async def __aexit__(self, *_args):
            self.exited += 1
            return False

        async def commit(self):
            self.commit_called += 1

        async def rollback(self):
            self.rollback_called += 1

    owned_session = Session()
    datastore.async_sessionmaker = owned_session
    caller_session = object()
    result = await datastore.record_research_checkpoint(
        run_id="synthetic-run",
        task_key="post_comments:9:100:g2",
        page_number=1,
        task_type="full_scan_comments",
        scope_type="post_comments",
        status="failed",
        scope_status="incomplete",
        error_type="InterfaceError",
        error_code=raw_code,
        generation=2,
        session=caller_session if caller_owned else None,
    )

    assert result is True
    assert values_seen[0]["error_code"] is None
    assert values_seen[0]["error_type"] == "InterfaceError"
    assert owned_session.commit_called == owned_session.rollback_called == 0
    if caller_owned:
        assert owned_session.entered == owned_session.exited == owned_session.begin_called == 0
    else:
        assert owned_session.entered == owned_session.exited == 2
        assert owned_session.begin_called == 1


@pytest.mark.asyncio
async def test_real_sqlalchemy_interface_error_code_is_not_passed_to_checkpoint_as_string(monkeypatch):
    error = InterfaceError("UPDATE research_checkpoint", {}, RuntimeError("synthetic"), code="rvf5")
    assert error.code == "rvf5"

    datastore = DataStore.__new__(DataStore)
    values_seen = []

    async def record(_session, *, values, **_kwargs):
        values_seen.append(values)
        return True

    monkeypatch.setattr(datastore, "_record_research_checkpoint_in_session", record)
    await datastore.record_research_checkpoint(
        run_id="synthetic-run",
        task_key="post_comments:9:100:g2",
        page_number=3,
        task_type="full_scan_comments",
        scope_type="post_comments",
        status="failed",
        scope_status="incomplete",
        error_type=type(error).__name__,
        error_code=error.code,
        session=object(),
    )

    assert values_seen[0]["error_type"] == "InterfaceError"
    assert values_seen[0]["error_code"] is None
