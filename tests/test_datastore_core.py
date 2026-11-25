"""DataStore 核心功能测试。"""

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from conftest import make_post, make_thread
from sqlalchemy import create_engine, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import InterfaceError
from sqlalchemy.orm import Session
from tiebameow.models.dto import BaseThreadDTO, ThreadDTO, ThreadUserDTO
from tiebameow.models.orm import Base
from tiebameow.models.orm import Forum as BaseForum
from tiebameow.models.orm import Post as BasePost
from tiebameow.models.orm import Thread as BaseThread
from tiebameow.utils.time_utils import SHANGHAI_TZ

from src.core.config import ConsumerConfig
from src.core.datastore import DataStore
from src.core.models import (
    PendingCommentScan,
    PendingThreadScan,
    ResearchCheckpoint,
    ResearchRun,
    ResearchTaskScope,
)
from src.research.models import ResearchPost

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
@pytest.mark.parametrize("basis", ["parent_api_explicit_zero", "fresh_parent_api_explicit_zero"])
async def test_explicit_zero_comment_checkpoint_resumes_as_completed_scope(basis):
    continuation = {
        "completion_basis": basis,
        "checkpoint_kind": "scope_completion" if basis.startswith("fresh_") else "page",
        "comment_api_called": basis == "fresh_parent_api_explicit_zero",
        "previous_failure": {"status": "failed", "error_type": "PageIntegrityError"},
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
    assert json.dumps(continuation)


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
async def test_checkpoint_fid_defaults_from_current_scope_and_rejects_conflicts(monkeypatch):
    datastore = DataStore.__new__(DataStore)
    scope = SimpleNamespace(generation=3, fid=905818, status="running", last_checked=None)

    async def get_scope(_session, _run_id, logical_key):
        assert logical_key == "post_comments:1:2"
        return scope

    monkeypatch.setattr(datastore, "get_research_task_scope_for_update", get_scope)

    class Session:
        def __init__(self):
            self.added = None

        async def get(self, model, _key):
            return None

        def add(self, row):
            self.added = row

    session = Session()
    values = {
        "fid": None,
        "status": "failed",
        "scope_status": "incomplete",
        "reason": "PageIntegrityError",
    }

    assert await datastore._record_research_checkpoint_in_session(
        session,
        run_id="run",
        task_key="post_comments:1:2:g3",
        page_number=1,
        values=values,
        generation=3,
    )
    assert session.added.fid == 905818

    conflict_session = Session()
    conflict_values = {**values, "fid": 7}
    assert not await datastore._record_research_checkpoint_in_session(
        conflict_session,
        run_id="run",
        task_key="post_comments:1:2:g3",
        page_number=1,
        values=conflict_values,
        generation=3,
    )
    assert conflict_session.added is None

    for incoming_fid in (None, 905818):
        existing_row = SimpleNamespace(fid=999, status="old")

        class ExistingSession(Session):
            async def get(self, model, _key, existing_row=existing_row):
                return existing_row if model is ResearchCheckpoint else None

        existing_session = ExistingSession()
        conflict_values = {**values, "fid": incoming_fid}
        assert not await datastore._record_research_checkpoint_in_session(
            existing_session,
            run_id="run",
            task_key="post_comments:1:2:g3",
            page_number=1,
            values=conflict_values,
            generation=3,
        )
        assert existing_row.fid == 999
        assert existing_row.status == "old"


@pytest.mark.asyncio
async def test_checkpoint_update_preserves_existing_fid_when_new_value_is_missing():
    datastore = DataStore.__new__(DataStore)
    row = SimpleNamespace(fid=905818)

    class Session:
        async def get(self, model, _key):
            return row if model is ResearchCheckpoint else None

        def add(self, _row):
            raise AssertionError("existing checkpoint should not be inserted")

    assert await datastore._record_research_checkpoint_in_session(
        Session(),
        run_id="run",
        task_key="post_comments:1:2",
        page_number=1,
        values={"fid": None, "status": "failed", "scope_status": "incomplete"},
        generation=None,
    )
    assert row.fid == 905818


@pytest.mark.asyncio
async def test_failed_comment_context_accepts_only_legacy_null_checkpoint_fid():
    from src.core.models import research_comments_task_key, research_posts_task_key

    tid, pid, generation, parent_generation, fid = 101, 202, 6, 4, 905818
    parent_key = research_posts_task_key(tid)
    task_key = research_comments_task_key(tid, pid, generation)
    parent = SimpleNamespace(
        generation=parent_generation,
        scope_type="thread_replies",
        tid=tid,
        rn=30,
        fid=fid,
    )
    scope = SimpleNamespace(
        generation=generation,
        status="incomplete",
        task_type="full_scan_comments",
        scope_type="post_comments",
        tid=tid,
        parent_pid=pid,
        parent_kind="reply_post",
        parent_logical_task_key=parent_key,
        parent_generation=parent_generation,
        fid=fid,
    )
    checkpoint = SimpleNamespace(
        status="failed",
        scope_status="incomplete",
        error_type="PageIntegrityError",
        task_type="full_scan_comments",
        scope_type="post_comments",
        tid=tid,
        parent_pid=pid,
        parent_kind="reply_post",
        fid=None,
    )

    class Session:
        async def get(self, model, key):
            if model is ResearchTaskScope:
                return parent if key == ("run", parent_key) else scope
            if model is ResearchCheckpoint:
                return checkpoint
            return None

        async def scalar(self, _statement):
            return None

    datastore = DataStore.__new__(DataStore)
    assert await datastore._failed_comment_scope_context(
        Session(),
        run_id="run",
        task_key=task_key,
        tid=tid,
        parent_pid=pid,
        generation=generation,
        parent_generation=parent_generation,
    ) == (parent, scope, checkpoint)

    checkpoint.fid = fid + 1
    assert await datastore._failed_comment_scope_context(
        Session(),
        run_id="run",
        task_key=task_key,
        tid=tid,
        parent_pid=pid,
        generation=generation,
        parent_generation=parent_generation,
    ) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("parent_kind", "thread_first_pid", "expected"),
    [("first_post", 202, (1, 30)), ("first_post", 203, None), ("reply_post", 202, None)],
)
async def test_comment_parent_page_hint_resolves_first_post_only_by_thread_identity(
    monkeypatch, parent_kind, thread_first_pid, expected
):
    parent = SimpleNamespace(rn=30)
    scope = SimpleNamespace(parent_kind=parent_kind, fid=905818)
    checkpoint = SimpleNamespace()

    class Session:
        def __init__(self):
            self.statements = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def scalar(self, statement):
            self.statements.append(statement)
            return thread_first_pid if parent_kind == "first_post" else None

    session = Session()

    class SessionMaker:
        def __call__(self):
            return session

    datastore = DataStore.__new__(DataStore)
    datastore.async_sessionmaker = SessionMaker()

    async def get_context(_session, **_kwargs):
        return parent, scope, checkpoint

    monkeypatch.setattr(datastore, "_failed_comment_scope_context", get_context)

    hint = await datastore.get_research_comment_parent_page_hint(
        "run", "post_comments:101:202:g6", 101, 202, 6, 4
    )

    assert hint == expected
    if parent_kind == "first_post":
        assert len(session.statements) == 1
        compiled = str(session.statements[0].compile(dialect=postgresql.dialect()))
        assert "research_thread.first_pid" in compiled
        assert "research_thread.tid" in compiled
        assert "research_thread.fid" in compiled
    else:
        assert len(session.statements) == 2


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        ("Comment detail empty response is missing or has zero page identity for page 1.", True),
        ("Comment detail empty response is not a confirmed terminal page for page 1.", True),
        ("Comment detail empty response is not a confirmed terminal page for page 2.", False),
        ("unrelated_failure_reason", False),
    ],
)
async def test_complete_failed_comment_scope_from_parent_zero_reason_allowlist(
    monkeypatch, reason, expected
):
    datastore = DataStore.__new__(DataStore)
    scope = SimpleNamespace(
        fid=10,
        status="incomplete",
        reason="PageIntegrityError",
        last_checked=None,
    )
    checkpoint = SimpleNamespace(
        status="failed",
        scope_status="incomplete",
        error_type="PageIntegrityError",
        error_code=None,
        reason="PageIntegrityError",
        attempts=1,
        has_more=None,
        next_page=1,
        total_pages=None,
        reported_count=None,
        reported_count_missing_reason=None,
        page_item_count=None,
        continuation_json={},
        fid=None,
        last_checked=None,
    )
    parent = SimpleNamespace(rn=30)

    class _FakeSession:
        def __call__(self):
            return self

        def begin(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args):
            return None

    async def fake_context(*_args, **_kwargs):
        return (parent, scope, checkpoint)

    async def fake_remove(*_args, **_kwargs):
        return None

    datastore.async_sessionmaker = _FakeSession()
    monkeypatch.setattr(datastore, "_failed_comment_scope_context", fake_context)
    monkeypatch.setattr(datastore, "remove_pending_comment_scan_in_session", fake_remove)

    evidence: dict[str, object] = {
        "tid": 9,
        "pid": 101,
        "fid": 10,
        "author_user_id": 555,
        "source_page": 1,
        "reported_count": 0,
        "count_field_present": True,
        "source_api": "aiotieba.get_posts",
        "evidence_kind": "protobuf_wire_explicit_zero",
        "count_field_path": "data.post_list.sub_post_number",
        "count_wire_field_number": 13,
    }
    completed = await datastore.complete_failed_comment_scope_from_parent_zero(
        run_id="synthetic-run",
        task_key="post_comments:9:101:g3",
        tid=9,
        parent_pid=101,
        generation=3,
        parent_generation=2,
        fid=10,
        parent_floor=2,
        parent_rn=30,
        parent_page=1,
        source_observed_at="2026-11-24T00:00:00+00:00",
        current_failure={"error_type": "PageIntegrityError", "reason": reason},
        evidence=evidence,
    )

    assert completed is expected
    if expected:
        assert scope.status == "completed"
        assert scope.reason == "fresh_parent_api_explicit_zero"
        assert checkpoint.status == "completed"
        assert checkpoint.fid == 10


class _LocalSQLiteTx:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._tx = None

    async def __aenter__(self):
        self._tx = self._session.begin()
        self._tx.__enter__()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        assert self._tx is not None
        return self._tx.__exit__(exc_type, exc, traceback)


class _LocalSQLiteAsyncSession:
    def __init__(self, session: Session) -> None:
        self._session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self._session.close()

    def begin(self):
        return _LocalSQLiteTx(self._session)

    def get_bind(self):
        return self._session.get_bind()

    async def execute(self, statement):
        return self._session.execute(statement)

    async def scalar(self, statement):
        return self._session.scalar(statement)

    async def scalars(self, statement):
        return self._session.scalars(statement)

    async def get(self, entity, ident):
        return self._session.get(entity, ident)

    async def flush(self):
        self._session.flush()


class _LocalSQLiteSessionMaker:
    def __init__(self, engine) -> None:
        self._engine = engine

    def __call__(self):
        return _LocalSQLiteAsyncSession(Session(self._engine))


def _make_local_sqlite_sessions(*tables):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=list(tables))
    return engine, _LocalSQLiteSessionMaker(engine)


@pytest.mark.asyncio
@pytest.mark.parametrize("refresh", [False, True])
async def test_ensure_research_task_scope_handles_concurrent_insert_conflict(refresh):
    existing_scope = ResearchTaskScope(
        run_id="run-1",
        logical_task_key="thread_replies:101",
        generation=0,
        task_type="full_scan_posts",
        scope_type="thread_replies",
        fid=10,
        fname="bar",
        tid=101,
        backfill=True,
        rn=30,
        status="pending",
    )
    session = SimpleNamespace(
        add=lambda _obj: (_ for _ in ()).throw(AssertionError("session.add should not be used")),
        flush=lambda: (_ for _ in ()).throw(AssertionError("session.flush should not be used")),
        execute=lambda _stmt: None,
    )
    executed_statements = []

    async def fake_execute(statement):
        executed_statements.append(statement)
        return SimpleNamespace(rowcount=0)

    session.execute = fake_execute
    datastore = DataStore.__new__(DataStore)
    lock_calls = 0

    async def fake_get_for_update(_session, _run_id, _key):
        nonlocal lock_calls
        lock_calls += 1
        return None if lock_calls == 1 else existing_scope

    datastore.get_research_task_scope_for_update = fake_get_for_update  # type: ignore[method-assign]

    scope = await datastore._ensure_research_task_scope_in_session(
        cast("Any", session),
        run_id="run-1",
        logical_task_key="thread_replies:101",
        task_type="full_scan_posts",
        scope_type="thread_replies",
        fid=10,
        fname="bar",
        tid=101,
        parent_pid=None,
        parent_kind=None,
        is_good=None,
        backfill=True,
        rn=30,
        parent_logical_task_key=None,
        parent_generation=None,
        refresh=refresh,
    )

    assert scope is existing_scope
    assert lock_calls == 2
    assert len(executed_statements) == (2 if refresh else 1)
    compiled_insert = str(executed_statements[0].compile(dialect=postgresql.dialect()))
    assert "ON CONFLICT (run_id, logical_task_key) DO NOTHING" in compiled_insert
    assert existing_scope.generation == (1 if refresh else 0)


@pytest.mark.asyncio
async def test_save_threads_and_pending_scans_deduplicates_duplicate_tids():
    DataStore._cache = None
    dummy_session = DummySession(existing_ids=set())
    container = DummyContainer(dummy_session, DummyRedis())
    datastore = DataStore(cast("Any", container))
    datastore.cache = cast("Any", DummyCache())
    scope_calls: list[dict[str, Any]] = []

    async def fake_ensure(_session, **kwargs):
        scope_calls.append(kwargs)
        return SimpleNamespace(generation=0)

    datastore._ensure_research_task_scope_in_session = fake_ensure  # type: ignore[method-assign]

    first = make_thread(tid=321, reply_num=111)
    latest = make_thread(tid=321, reply_num=222)
    generations = await datastore.save_threads_and_pending_scans(
        [first, latest],
        backfill=True,
        upsert_threads=True,
        research_run_id="run-1",
    )

    assert generations == {321: 0}
    assert len(scope_calls) == 1
    assert len(dummy_session.statements) == 2
    compiled = dummy_session.statements[0].compile(dialect=postgresql.dialect())  # type: ignore[attr-defined]
    reply_nums = [
        value
        for key, value in compiled.params.items()
        if key == "reply_num" or key.startswith("reply_num_m")
    ]
    assert reply_nums == [222]


@pytest.mark.asyncio
async def test_adopt_legacy_research_scopes_skips_other_run_managed_rows_and_adopts_unowned():
    engine, sessions = _make_local_sqlite_sessions(
        PendingThreadScan.__table__,
        PendingCommentScan.__table__,
        ResearchTaskScope.__table__,
        ResearchCheckpoint.__table__,
    )
    try:
        with Session(engine) as seed:
            seed.add_all(
                [
                    ResearchTaskScope(
                        run_id="run-old",
                        logical_task_key="thread_replies:101",
                        generation=0,
                        task_type="full_scan_posts",
                        scope_type="thread_replies",
                        fid=10,
                        fname="bar",
                        tid=101,
                        status="completed",
                    ),
                    ResearchTaskScope(
                        run_id="run-old",
                        logical_task_key="post_comments:101:201",
                        generation=0,
                        task_type="full_scan_comments",
                        scope_type="post_comments",
                        fid=10,
                        fname="bar",
                        tid=101,
                        parent_pid=201,
                        parent_logical_task_key="thread_replies:101",
                        parent_generation=0,
                        status="incomplete",
                    ),
                    PendingThreadScan(tid=101, fid=10, fname="bar", backfill=True),
                    PendingThreadScan(tid=102, fid=10, fname="bar", backfill=True),
                    PendingCommentScan(tid=101, pid=201, fid=10, fname="bar", backfill=True, task_kind="full"),
                    PendingCommentScan(tid=103, pid=301, fid=10, fname="bar", backfill=True, task_kind="full"),
                ]
            )
            seed.commit()

        datastore = DataStore.__new__(DataStore)
        datastore.async_sessionmaker = cast("Any", sessions)
        await datastore.adopt_legacy_research_scopes("run-new", {10})

        with Session(engine) as check:
            adopted = {
                (row.logical_task_key, row.status, row.parent_generation)
                for row in check.scalars(
                    select(ResearchTaskScope).where(ResearchTaskScope.run_id == "run-new")
                ).all()
            }
        assert adopted == {
            ("thread_replies:102", "pending", None),
            ("thread_replies:103", "completed", None),
            ("post_comments:103:301", "pending", 0),
        }
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_get_research_comment_parent_dense_page_hint_uses_research_or_canonical_posts():
    engine, sessions = _make_local_sqlite_sessions(
        BasePost.__table__,
        ResearchPost.__table__,
    )
    try:
        observed = datetime.now(UTC)
        with Session(engine) as seed:
            # 12 canonical posts with floors up to 20 (sparse due to deletions)
            for index in range(1, 13):
                post_dto = make_post(pid=1000 + index, tid=9, fid=10, floor=index * 10, reply_num=0)
                seed.add(BasePost.from_dto(post_dto))
            # 7 research posts with floor <= 40 for tid=10 (rank 7 with rn=5 -> page 2)
            for index in range(1, 8):
                seed.add(
                    ResearchPost(
                        pid=2000 + index,
                        tid=10,
                        fid=10,
                        fname="bar",
                        floor=index * 5,
                        source_api="aiotieba.get_posts",
                        observed_at=observed,
                        source_payload_json={},
                        missing_reasons_json={},
                    )
                )
            seed.commit()

        datastore = DataStore.__new__(DataStore)
        datastore.async_sessionmaker = cast("Any", sessions)

        assert await datastore.get_research_comment_parent_dense_page_hint(10, 40, 5) == 2
        assert await datastore.get_research_comment_parent_dense_page_hint(9, 100, 3) == 4
        assert await datastore.get_research_comment_parent_dense_page_hint(999, 50, 30) is None
        assert await datastore.get_research_comment_parent_dense_page_hint(9, 1, 30) == 1
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("parent_page", "expected"),
    [
        (1, True),
        (9, True),
        (15, True),
        (0, False),
        (16, False),
    ],
)
async def test_complete_failed_comment_scope_from_parent_zero_accepts_shifted_pages_within_bounds(
    monkeypatch, parent_page, expected
):
    datastore = DataStore.__new__(DataStore)
    scope = SimpleNamespace(
        fid=10,
        status="incomplete",
        reason="PageIntegrityError",
        last_checked=None,
    )
    checkpoint = SimpleNamespace(
        status="failed",
        scope_status="incomplete",
        error_type="PageIntegrityError",
        error_code=None,
        reason="PageIntegrityError",
        attempts=1,
        has_more=None,
        next_page=1,
        total_pages=None,
        reported_count=None,
        reported_count_missing_reason=None,
        page_item_count=None,
        continuation_json={},
        fid=None,
        last_checked=None,
    )
    parent = SimpleNamespace(rn=30)

    class _FakeSession:
        def __call__(self):
            return self

        def begin(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args):
            return None

    async def fake_context(*_args, **_kwargs):
        return (parent, scope, checkpoint)

    async def fake_remove(*_args, **_kwargs):
        return None

    datastore.async_sessionmaker = _FakeSession()
    monkeypatch.setattr(datastore, "_failed_comment_scope_context", fake_context)
    monkeypatch.setattr(datastore, "remove_pending_comment_scan_in_session", fake_remove)

    evidence: dict[str, object] = {
        "tid": 9,
        "pid": 101,
        "fid": 10,
        "author_user_id": 555,
        "source_page": parent_page,
        "reported_count": 0,
        "count_field_present": True,
        "source_api": "aiotieba.get_posts",
        "evidence_kind": "protobuf_wire_explicit_zero",
        "count_field_path": "data.post_list.sub_post_number",
        "count_wire_field_number": 13,
    }
    completed = await datastore.complete_failed_comment_scope_from_parent_zero(
        run_id="synthetic-run",
        task_key="post_comments:9:101:g3",
        tid=9,
        parent_pid=101,
        generation=3,
        parent_generation=2,
        fid=10,
        parent_floor=443,
        parent_rn=30,
        parent_page=parent_page,
        source_observed_at="2026-11-24T00:00:00+00:00",
        current_failure={
            "error_type": "PageIntegrityError",
            "reason": "Comment detail empty response is missing or has zero page identity for page 1.",
        },
        evidence=evidence,
    )

    assert completed is expected


@pytest.mark.asyncio
async def test_iter_pending_research_task_scopes_filters_by_min_thread_create_time():
    engine, maker = _make_local_sqlite_sessions(
        BaseForum.__table__,
        BaseThread.__table__,
        ResearchRun.__table__,
        ResearchTaskScope.__table__,
    )
    cutoff = datetime(2021, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ)
    with Session(engine) as session, session.begin():
        session.add(BaseForum(fid=10, fname="bar"))
        session.add(
            ResearchRun(
                run_id="run-filter",
                mode="reply_backfill",
                scope_key="10",
                forums_json=[{"fid": 10, "fname": "bar"}],
                status="running",
            )
        )
        session.add_all(
            [
                BaseThread.from_dto(
                    make_thread(
                        tid=101,
                        fid=10,
                        create_time=datetime(2020, 12, 31, 23, 59, 59, tzinfo=SHANGHAI_TZ),
                    )
                ),
                BaseThread.from_dto(
                    make_thread(
                        tid=102,
                        fid=10,
                        create_time=cutoff,
                    )
                ),
                BaseThread.from_dto(
                    make_thread(
                        tid=103,
                        fid=10,
                        create_time=datetime(2022, 3, 15, 8, 0, 0, tzinfo=SHANGHAI_TZ),
                    )
                ),
            ]
        )
        session.add_all(
            [
                ResearchTaskScope(
                    run_id="run-filter",
                    logical_task_key="thread_replies:101",
                    generation=0,
                    task_type="full_scan_posts",
                    scope_type="thread_replies",
                    fid=10,
                    fname="bar",
                    tid=101,
                    backfill=True,
                    rn=30,
                    status="pending",
                ),
                ResearchTaskScope(
                    run_id="run-filter",
                    logical_task_key="thread_replies:102",
                    generation=0,
                    task_type="full_scan_posts",
                    scope_type="thread_replies",
                    fid=10,
                    fname="bar",
                    tid=102,
                    backfill=True,
                    rn=30,
                    status="pending",
                ),
                ResearchTaskScope(
                    run_id="run-filter",
                    logical_task_key="thread_replies:103",
                    generation=0,
                    task_type="full_scan_posts",
                    scope_type="thread_replies",
                    fid=10,
                    fname="bar",
                    tid=103,
                    backfill=True,
                    rn=30,
                    status="pending",
                ),
                ResearchTaskScope(
                    run_id="run-filter",
                    logical_task_key="post_comments:101:1001",
                    generation=0,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    fid=10,
                    fname="bar",
                    tid=101,
                    parent_pid=1001,
                    parent_kind="reply_post",
                    parent_generation=0,
                    backfill=True,
                    rn=30,
                    status="incomplete",
                ),
                ResearchTaskScope(
                    run_id="run-filter",
                    logical_task_key="post_comments:102:2001",
                    generation=0,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    fid=10,
                    fname="bar",
                    tid=102,
                    parent_pid=2001,
                    parent_kind="reply_post",
                    parent_generation=0,
                    backfill=True,
                    rn=30,
                    status="pending",
                ),
                # Thread without a stored Thread row stays eligible (unknown create_time).
                ResearchTaskScope(
                    run_id="run-filter",
                    logical_task_key="post_comments:104:4001",
                    generation=0,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    fid=10,
                    fname="bar",
                    tid=104,
                    parent_pid=4001,
                    parent_kind="reply_post",
                    parent_generation=0,
                    backfill=True,
                    rn=30,
                    status="pending",
                ),
            ]
        )

    datastore = DataStore.__new__(DataStore)
    datastore.async_sessionmaker = cast("Any", maker)

    filtered_keys: list[str] = []
    async for batch in datastore.iter_pending_research_task_scopes(
        "run-filter",
        {10},
        min_thread_create_time=cutoff,
    ):
        filtered_keys.extend(scope.logical_task_key for scope in batch)

    assert sorted(filtered_keys) == [
        "post_comments:102:2001",
        "post_comments:104:4001",
        "thread_replies:102",
        "thread_replies:103",
    ]

    with Session(engine) as session:
        assert session.get(ResearchTaskScope, ("run-filter", "thread_replies:101")).status == "pending"
        assert session.get(ResearchTaskScope, ("run-filter", "post_comments:101:1001")).status == "incomplete"

    unfiltered_keys: list[str] = []
    async for batch in datastore.iter_pending_research_task_scopes("run-filter", {10}):
        unfiltered_keys.extend(scope.logical_task_key for scope in batch)

    assert len(unfiltered_keys) == 6
    assert "thread_replies:101" in unfiltered_keys
    assert "post_comments:101:1001" in unfiltered_keys


@pytest.mark.asyncio
async def test_iter_pending_research_task_scopes_newest_first_keyset_pagination():
    engine, maker = _make_local_sqlite_sessions(
        ResearchRun.__table__,
        ResearchTaskScope.__table__,
    )
    with Session(engine) as session, session.begin():
        session.add(
            ResearchRun(
                run_id="run-newest",
                mode="reply_backfill",
                scope_key="10",
                forums_json=[{"fid": 10, "fname": "bar"}],
                status="running",
            )
        )
        # Deliberately mix varying digit lengths (99, 100, 1000) to confirm numeric tid DESC
        # differs from lexicographic logical_task_key ordering, plus multiple scopes sharing a tid.
        session.add_all(
            [
                ResearchTaskScope(
                    run_id="run-newest",
                    logical_task_key="post_comments:99:10",
                    generation=0,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    fid=10,
                    fname="bar",
                    tid=99,
                    parent_pid=10,
                    parent_kind="reply_post",
                    parent_generation=0,
                    backfill=True,
                    rn=30,
                    status="pending",
                ),
                ResearchTaskScope(
                    run_id="run-newest",
                    logical_task_key="post_comments:100:20",
                    generation=0,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    fid=10,
                    fname="bar",
                    tid=100,
                    parent_pid=20,
                    parent_kind="reply_post",
                    parent_generation=0,
                    backfill=True,
                    rn=30,
                    status="pending",
                ),
                ResearchTaskScope(
                    run_id="run-newest",
                    logical_task_key="post_comments:100:21",
                    generation=0,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    fid=10,
                    fname="bar",
                    tid=100,
                    parent_pid=21,
                    parent_kind="reply_post",
                    parent_generation=0,
                    backfill=True,
                    rn=30,
                    status="pending",
                ),
                ResearchTaskScope(
                    run_id="run-newest",
                    logical_task_key="post_comments:1000:30",
                    generation=0,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    fid=10,
                    fname="bar",
                    tid=1000,
                    parent_pid=30,
                    parent_kind="reply_post",
                    parent_generation=0,
                    backfill=True,
                    rn=30,
                    status="pending",
                ),
                ResearchTaskScope(
                    run_id="run-newest",
                    logical_task_key="post_comments:1000:31",
                    generation=0,
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    fid=10,
                    fname="bar",
                    tid=1000,
                    parent_pid=31,
                    parent_kind="reply_post",
                    parent_generation=0,
                    backfill=True,
                    rn=30,
                    status="pending",
                ),
            ]
        )

    datastore = DataStore.__new__(DataStore)
    datastore.async_sessionmaker = cast("Any", maker)

    batches: list[list[tuple[int | None, str]]] = [
        [(scope.tid, scope.logical_task_key) for scope in batch]
        async for batch in datastore.iter_pending_research_task_scopes(
            "run-newest",
            {10},
            batch_size=2,
            scope_type="post_comments",
            newest_first=True,
        )
    ]

    flattened = [item for batch in batches for item in batch]
    assert [len(batch) for batch in batches] == [2, 2, 1]
    assert flattened == [
        (1000, "post_comments:1000:31"),
        (1000, "post_comments:1000:30"),
        (100, "post_comments:100:21"),
        (100, "post_comments:100:20"),
        (99, "post_comments:99:10"),
    ]
