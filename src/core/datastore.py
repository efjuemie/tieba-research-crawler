"""数据存储层模块。

该模块提供了统一的数据访问接口，封装了与PostgreSQL数据库和Redis缓存的交互逻辑。
主要功能包括数据去重、批量保存、状态追踪和消息队列管理。
"""

from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from cashews import Cache, add_prefix
from sqlalchemy import BigInteger, String, and_, case, cast, delete, func, literal, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import aliased
from tiebameow.models.orm import Comment, Forum, Post, Thread, User
from tiebameow.utils.logger import logger

from .metrics import CACHE_HITS, CACHE_MISSES, DB_OPERATIONS
from .models import (
    PendingCommentScan,
    PendingThreadScan,
    ResearchCheckpoint,
    ResearchRun,
    ResearchTaskScope,
    research_comments_task_key,
    research_posts_task_key,
)
from .publisher import EventEnvelope, NoopPublisher, RedisStreamsPublisher, WebSocketPublisher, build_envelope

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from tiebameow.models.dto import CommentDTO, PostDTO, ThreadDTO

    from .container import Container

    type ItemType = Literal["thread", "post", "comment"]
    type DTOType = ThreadDTO | PostDTO | CommentDTO
    type PendingCommentTaskKind = Literal["full", "incremental"]


PRIMARY_KEY_MAP = {
    Thread: {"tid", "create_time"},
    Post: {"pid", "create_time"},
    Comment: {"cid", "create_time"},
    Forum: {"fid"},
    User: {"user_id"},
}

RESEARCH_RUN_RESUMABLE_STATUSES = (
    "running",
    "interrupted",
    "incomplete",
    "paused_auth",
    "paused_rate_limit",
    "paused_limit",
    "blocked",
)


def _research_forum_ids(forums_json: Any) -> tuple[set[int], bool]:
    """Read current dict and legacy pair/scalar forum scope entries."""
    if not isinstance(forums_json, (list, tuple)):
        return set(), forums_json not in (None, [])
    forum_ids: set[int] = set()
    invalid_entry = False
    for item in forums_json:
        if isinstance(item, dict):
            value = item.get("fid")
        elif isinstance(item, (list, tuple)) and item:
            value = item[0]
        else:
            value = item
        try:
            fid = int(value)
        except (TypeError, ValueError):
            invalid_entry = True
            continue
        if isinstance(value, bool) or fid <= 0:
            invalid_entry = True
            continue
        forum_ids.add(fid)
    return forum_ids, invalid_entry


def _checkpoint_tid_set(values: Any) -> set[int]:
    if not isinstance(values, (list, tuple)):
        return set()
    tids: set[int] = set()
    for value in values:
        if isinstance(value, bool):
            continue
        try:
            tid = int(value)
        except (TypeError, ValueError):
            continue
        if tid > 0:
            tids.add(tid)
    return tids


def _research_scope_key(forums: list[tuple[int, str]]) -> str:
    normalized_forums = sorted({(int(fid), fname) for fid, fname in forums})
    scope_payload = {
        "forums": normalized_forums,
        "sort_mode": "create",
        "sampling_scope": "all_accessible_threads_and_replies",
        "checkpoint_version": 1,
    }
    return hashlib.sha256(json.dumps(scope_payload, separators=(",", ":")).encode()).hexdigest()


async def iter_crawl_quality_summaries(
    async_sessionmaker_: async_sessionmaker,
    run_id: str | None = None,
) -> AsyncIterator[dict[str, object]]:
    """Stream scope-level crawl quality rows without constructing DataStore."""
    async for summary in DataStore._iter_crawl_quality_summaries(async_sessionmaker_, run_id):
        yield summary


def _first_post_comment_count_expressions(checkpoint):
    """Build the trusted first-post comment count and its missing reason."""
    from src.research.models import ResearchThread

    payload = (
        select(ResearchThread.source_payload_json["first_post"])
        .where(ResearchThread.tid == checkpoint.tid, ResearchThread.first_pid == checkpoint.parent_pid)
        .limit(1)
        .scalar_subquery()
    )
    payload_count = cast(payload["subreply_count_reported"].as_integer(), BigInteger)
    payload_reason = payload["missing_reason"].as_string()
    pages = aliased(ResearchCheckpoint)
    reported = (
        select(pages.reported_count)
        .where(
            pages.run_id == checkpoint.run_id,
            pages.task_key == checkpoint.task_key,
            pages.status == "completed",
        )
        .order_by(pages.page_number.desc())
        .limit(1)
        .scalar_subquery()
    )
    missing = (
        select(pages.reported_count_missing_reason)
        .where(pages.run_id == checkpoint.run_id, pages.task_key == checkpoint.task_key)
        .order_by(pages.page_number.desc())
        .limit(1)
        .scalar_subquery()
    )
    trusted_checkpoint_count = case((missing.is_(None), reported), else_=None)
    return func.coalesce(trusted_checkpoint_count, payload_count), func.coalesce(
        missing, payload_reason, "first_post_reply_count_unavailable"
    )


class DataStore:
    """数据存储层，负责与数据库和缓存交互。

    提供统一的数据访问接口，包括：
    - 数据去重检查(基于Redis集合)
    - 批量数据保存(PostgreSQL)
    - 处理状态标记
    - 消息队列推送

    Attributes:
        container (Container): 依赖注入容器
        lock (asyncio.Lock): 异步锁，用于确保数据一致性
        redis: Redis异步客户端实例
        async_sessionmaker: 异步数据库会话工厂
        cache: Cashews Redis缓存实例
    """

    _cache: ClassVar[Cache | None] = None

    def __init__(self, container: Container):
        """初始化数据存储层。

        Args:
            container: 依赖注入容器，提供Redis客户端和数据库会话工厂。
        """
        self.container = container
        self.redis = container.redis_client
        self.async_sessionmaker = container.async_sessionmaker

        if DataStore._cache is None:
            DataStore._cache = Cache()
            if self.container.config.cache_backend == "memory":
                DataStore._cache.setup(f"mem://?size={self.container.config.cache_max_size}")
            else:
                DataStore._cache.setup(
                    self.container.config.redis_url,
                    middlewares=(add_prefix("processed:"),),
                    client_side=True,
                )
        self.cache = DataStore._cache
        self.consumer_transport = self.container.config.consumer_transport
        if self.consumer_transport == "redis":
            self.publisher = RedisStreamsPublisher(
                self.redis,  # type: ignore
                consumer_config=self.container.config.consumer_config,
            )
        elif self.consumer_transport == "websocket":
            self.publisher = WebSocketPublisher(
                self.container.config.websocket_url,
                server=self.container.ws_server,
                consumer_config=self.container.config.consumer_config,
            )
        else:
            self.publisher = NoopPublisher()

    async def close(self) -> None:
        """优雅关闭：释放发布器等资源（幂等）。"""
        try:
            if self.publisher is not None:
                await self.publisher.close()
        except Exception as e:
            logger.warning("Failed to close publisher: {}", e)

    async def start_research_run(
        self,
        *,
        mode: str,
        forums: list[tuple[int, str]],
        max_backfill_pages: int,
        resume: bool = False,
        run_id: str | None = None,
    ) -> str:
        """Create a durable run or reopen the latest compatible incomplete run."""
        normalized_forums = sorted({(int(fid), fname) for fid, fname in forums})
        scope_key = _research_scope_key(normalized_forums)
        now = datetime.now(UTC)
        async with self.async_sessionmaker() as session:
            if resume:
                statement = select(ResearchRun).where(
                        ResearchRun.scope_key == scope_key,
                        ResearchRun.mode == mode,
                        ResearchRun.status.in_(RESEARCH_RUN_RESUMABLE_STATUSES),
                    )
                if run_id is not None:
                    statement = statement.where(ResearchRun.run_id == run_id)
                else:
                    statement = statement.order_by(ResearchRun.last_checked.desc()).limit(1)
                result = await session.execute(statement)
                run = result.scalar_one_or_none()
                if run is None:
                    raise ValueError("No resumable research run matches the current mode and forum scope.")
                run.status = "running"
                run.reason = None
                run.last_checked = now
            else:
                run = ResearchRun(
                    run_id=str(uuid.uuid4()),
                    mode=mode,
                    scope_key=scope_key,
                    forums_json=[{"fid": fid, "fname": fname} for fid, fname in normalized_forums],
                    status="running",
                    started_at=now,
                    last_checked=now,
                )
                session.add(run)
            await session.commit()
            return run.run_id

    async def load_research_run_for_drain(self, run_id: str, forums: list[tuple[int, str]]) -> str:
        """Load an existing run only when its persisted forum scope matches."""
        scope_key = _research_scope_key(forums)
        async with self.async_sessionmaker() as session:
            result = await session.execute(
                select(ResearchRun.status, ResearchRun.scope_key).where(ResearchRun.run_id == run_id)
            )
            run = result.one_or_none()
        if run is None:
            raise ValueError(f"Research run {run_id} does not exist.")
        if run.scope_key != scope_key:
            raise ValueError(f"Research run {run_id} does not match the current forum scope.")
        if run.status != "completed" and run.status not in RESEARCH_RUN_RESUMABLE_STATUSES:
            raise ValueError(f"Research run {run_id} is not resumable from status {run.status!r}.")
        return run.status

    async def resume_research_run_for_drain(self, run_id: str, expected_status: str) -> bool:
        """Activate a validated run for drain, recording completed-run reopenings."""
        if expected_status == "completed":
            reason = "drain_reopened_completed_run"
        elif expected_status in RESEARCH_RUN_RESUMABLE_STATUSES:
            reason = None
        else:
            return False
        async with self.async_sessionmaker() as session:
            result = await session.execute(
                update(ResearchRun)
                .where(ResearchRun.run_id == run_id, ResearchRun.status == expected_status)
                .values(status="running", reason=reason, last_checked=datetime.now(UTC))
            )
            await session.commit()
            return bool(result.rowcount)

    async def set_research_run_status(self, run_id: str, status: str, reason: str | None = None) -> None:
        async with self.async_sessionmaker() as session:
            result = await session.execute(select(ResearchRun).where(ResearchRun.run_id == run_id))
            run = result.scalar_one_or_none()
            if run is None:
                raise ValueError(f"Research run {run_id} does not exist.")
            run.status = status
            run.reason = reason
            run.last_checked = datetime.now(UTC)
            await session.commit()

    async def resume_research_run_if_status(self, run_id: str, expected_status: str) -> bool:
        """Change a paused run to running only if no other pause superseded it."""
        async with self.async_sessionmaker() as session:
            result = await session.execute(
                update(ResearchRun)
                .where(ResearchRun.run_id == run_id, ResearchRun.status == expected_status)
                .values(status="running", reason=None, last_checked=datetime.now(UTC))
            )
            await session.commit()
            return bool(result.rowcount)

    async def set_research_run_status_if(
        self,
        run_id: str,
        expected_statuses: set[str],
        status: str,
        reason: str | None = None,
    ) -> bool:
        """Compare-and-set run state without overwriting a stronger pause."""
        async with self.async_sessionmaker() as session:
            result = await session.execute(
                update(ResearchRun)
                .where(ResearchRun.run_id == run_id, ResearchRun.status.in_(expected_statuses))
                .values(status=status, reason=reason, last_checked=datetime.now(UTC))
            )
            await session.commit()
            return bool(result.rowcount)

    async def reconcile_research_run_status_after_drain(self, run_id: str) -> str:
        """Reconcile scheduler status with the latest page state for every scope."""
        protected_statuses = {"paused_auth", "paused_rate_limit", "blocked", "paused_limit", "interrupted"}
        checkpoint = ResearchCheckpoint
        latest_pages = (
            select(
                checkpoint.task_key.label("task_key"),
                func.max(checkpoint.page_number).label("page_number"),
            )
            .where(checkpoint.run_id == run_id)
            .group_by(checkpoint.task_key)
            .subquery()
        )
        is_blocked_network = and_(checkpoint.status == "blocked", checkpoint.error_code == 220034)
        is_auth_paused = or_(checkpoint.status == "paused_auth", checkpoint.scope_status == "paused_auth")
        is_rate_paused = or_(checkpoint.status == "paused_rate_limit", checkpoint.scope_status == "paused_rate_limit")
        is_incomplete = or_(
            checkpoint.status != "completed",
            checkpoint.scope_status.is_(None),
            checkpoint.scope_status != "completed",
        )
        async with self.async_sessionmaker() as session:
            run = await session.get(ResearchRun, run_id)
            if run is None:
                raise ValueError(f"Research run {run_id} does not exist.")
            if run.status in protected_statuses:
                return run.status

            active_list_filters = [checkpoint.run_id == run_id, checkpoint.scope_type == "thread_list"]
            needs_create_proof = getattr(run, "mode", "periodic") in {"backfill", "hybrid"}
            active_list_filters.append(checkpoint.is_good.is_(False))
            if needs_create_proof:
                active_list_filters.append(checkpoint.sort_mode == "create")

            list_counts_result = await session.execute(
                select(
                    func.coalesce(
                        func.sum(case((is_blocked_network, 1), else_=0)),
                        0,
                    ).label("blocked_count"),
                    func.coalesce(
                        func.sum(case((is_auth_paused, 1), else_=0)),
                        0,
                    ).label("auth_count"),
                    func.coalesce(
                        func.sum(case((is_rate_paused, 1), else_=0)),
                        0,
                    ).label("rate_count"),
                    func.coalesce(
                        func.sum(case((is_incomplete, 1), else_=0)),
                        0,
                    ).label("incomplete_count"),
                )
                .select_from(checkpoint)
                .join(
                    latest_pages,
                    and_(
                        checkpoint.task_key == latest_pages.c.task_key,
                        checkpoint.page_number == latest_pages.c.page_number,
                    ),
                )
                .where(*active_list_filters)
            )
            list_counts = list_counts_result.one()

            missing_create_proof = False
            if needs_create_proof:
                forum_ids, invalid_forums = _research_forum_ids(getattr(run, "forums_json", []))
                create_rows_result = await session.execute(
                    select(
                        checkpoint.fid,
                        checkpoint.status,
                        checkpoint.scope_status,
                        checkpoint.has_more,
                    )
                    .select_from(checkpoint)
                    .join(
                        latest_pages,
                        and_(
                            checkpoint.task_key == latest_pages.c.task_key,
                            checkpoint.page_number == latest_pages.c.page_number,
                        ),
                    )
                    .where(*active_list_filters)
                )
                completed_create_forums = {
                    fid
                    for fid, status_value, scope_status, has_more in create_rows_result.all()
                    if fid is not None
                    and status_value == "completed"
                    and scope_status == "completed"
                    and has_more is False
                }
                missing_create_proof = invalid_forums or not forum_ids.issubset(completed_create_forums)

            scope_status_rows = await session.execute(
                select(ResearchTaskScope.status).where(
                    ResearchTaskScope.run_id == run_id,
                    ResearchTaskScope.scope_type.in_(("thread_replies", "post_comments")),
                )
            )
            scope_statuses = list(scope_status_rows.scalars())
            if list_counts.blocked_count or "blocked" in scope_statuses:
                status, reason = "blocked", "latest_checkpoint_reports_api_220034"
            elif list_counts.auth_count or "paused_auth" in scope_statuses:
                status, reason = "paused_auth", "latest_checkpoint_requires_authentication"
            elif list_counts.rate_count or "paused_rate_limit" in scope_statuses:
                status, reason = "paused_rate_limit", "latest_checkpoint_paused_for_rate_limit"
            elif list_counts.incomplete_count or any(
                scope_status != "completed" for scope_status in scope_statuses
            ) or missing_create_proof:
                status, reason = "incomplete", "latest_checkpoint_not_terminal"
            else:
                status, reason = "completed", None

            updated = await session.execute(
                update(ResearchRun)
                .where(
                    ResearchRun.run_id == run_id,
                    ResearchRun.status.in_({"running", "completed", "incomplete"}),
                )
                .values(status=status, reason=reason, last_checked=datetime.now(UTC))
            )
            await session.commit()
            if updated.rowcount:
                return status
            latest_run = await session.get(ResearchRun, run_id)
            if latest_run is None:
                raise ValueError(f"Research run {run_id} does not exist.")
            return latest_run.status

    async def record_research_checkpoint(
        self,
        *,
        run_id: str,
        task_key: str,
        page_number: int,
        task_type: str,
        scope_type: str,
        status: str,
        scope_status: str | None = None,
        sort_mode: str | None = None,
        fid: int | None = None,
        fname: str | None = None,
        tid: int | None = None,
        parent_pid: int | None = None,
        parent_kind: str | None = None,
        is_good: bool | None = None,
        attempts: int = 0,
        has_more: bool | None = None,
        next_page: int | None = None,
        total_pages: int | None = None,
        reported_count: int | None = None,
        reported_count_missing_reason: str | None = None,
        page_item_count: int | None = None,
        continuation_json: dict[str, object] | None = None,
        error_type: str | None = None,
        error_code: int | None = None,
        reason: str | None = None,
        generation: int | None = None,
        session: AsyncSession | None = None,
    ) -> bool:
        """Idempotently record one page state; all key columns are non-null."""
        if page_number < 1:
            raise ValueError("Checkpoint page_number must be positive.")
        values = {
            "task_type": task_type,
            "scope_type": scope_type,
            "sort_mode": sort_mode,
            "fid": fid,
            "fname": fname,
            "tid": tid,
            "parent_pid": parent_pid,
            "parent_kind": parent_kind,
            "is_good": is_good,
            "status": status,
            "scope_status": scope_status,
            "attempts": attempts,
            "has_more": has_more,
            "next_page": next_page,
            "total_pages": total_pages,
            "reported_count": reported_count,
            "reported_count_missing_reason": reported_count_missing_reason,
            "page_item_count": page_item_count,
            "continuation_json": continuation_json,
            "error_type": error_type,
            "error_code": error_code,
            "reason": reason,
            "last_checked": datetime.now(UTC),
        }
        if session is not None:
            return await self._record_research_checkpoint_in_session(
                session,
                run_id=run_id,
                task_key=task_key,
                page_number=page_number,
                values=values,
                generation=generation,
            )

        async with self.async_sessionmaker() as owned_session, owned_session.begin():
            return await self._record_research_checkpoint_in_session(
                owned_session,
                run_id=run_id,
                task_key=task_key,
                page_number=page_number,
                values=values,
                generation=generation,
            )

    async def _record_research_checkpoint_in_session(
        self,
        session: AsyncSession,
        *,
        run_id: str,
        task_key: str,
        page_number: int,
        values: dict[str, object],
        generation: int | None,
    ) -> bool:
        if generation is not None:
            logical_key = task_key.rsplit(":g", 1)[0] if ":g" in task_key else task_key
            scope = await self.get_research_task_scope_for_update(session, run_id, logical_key)
            if scope is None or scope.generation != generation:
                return False

        row = await session.get(ResearchCheckpoint, (run_id, task_key, page_number))
        if row is None:
            session.add(
                ResearchCheckpoint(
                    run_id=run_id,
                    task_key=task_key,
                    page_number=page_number,
                    **values,
                )
            )
        else:
            for name, value in values.items():
                setattr(row, name, value)
        run = await session.get(ResearchRun, run_id)
        if run is not None:
            run.last_checked = datetime.now(UTC)
        if generation is not None and scope is not None:
            scope.status = values.get("scope_status") or values.get("status") or scope.status
            scope.reason = values.get("reason")
            scope.last_checked = datetime.now(UTC)
        return True

    async def get_research_task_scope_for_update(
        self,
        session: AsyncSession,
        run_id: str,
        logical_task_key: str,
    ) -> ResearchTaskScope | None:
        result = await session.execute(
            select(ResearchTaskScope)
            .where(
                ResearchTaskScope.run_id == run_id,
                ResearchTaskScope.logical_task_key == logical_task_key,
            )
            .with_for_update()
        )
        return result.scalar_one_or_none()

    async def get_research_task_scope_state(
        self,
        run_id: str,
        logical_task_key: str,
    ) -> tuple[int, str, int | None] | None:
        async with self.async_sessionmaker() as session:
            result = await session.execute(
                select(
                    ResearchTaskScope.generation,
                    ResearchTaskScope.status,
                    ResearchTaskScope.parent_generation,
                ).where(
                    ResearchTaskScope.run_id == run_id,
                    ResearchTaskScope.logical_task_key == logical_task_key,
                )
            )
            row = result.one_or_none()
        return None if row is None else (row.generation, row.status, row.parent_generation)

    async def ensure_research_task_scope(
        self,
        *,
        run_id: str,
        logical_task_key: str,
        task_type: str,
        scope_type: str,
        fid: int | None = None,
        fname: str | None = None,
        tid: int | None = None,
        parent_pid: int | None = None,
        parent_kind: str | None = None,
        is_good: bool | None = None,
        backfill: bool = True,
        rn: int = 30,
        parent_logical_task_key: str | None = None,
        parent_generation: int | None = None,
        refresh: bool = False,
        session: AsyncSession | None = None,
    ) -> ResearchTaskScope | None:
        """Create or refresh a scope; an optional session keeps it in a caller transaction."""
        if session is not None:
            return await self._ensure_research_task_scope_in_session(
                session,
                run_id=run_id,
                logical_task_key=logical_task_key,
                task_type=task_type,
                scope_type=scope_type,
                fid=fid,
                fname=fname,
                tid=tid,
                parent_pid=parent_pid,
                parent_kind=parent_kind,
                is_good=is_good,
                backfill=backfill,
                rn=rn,
                parent_logical_task_key=parent_logical_task_key,
                parent_generation=parent_generation,
                refresh=refresh,
            )
        async with self.async_sessionmaker() as owned_session, owned_session.begin():
            return await self._ensure_research_task_scope_in_session(
                owned_session,
                run_id=run_id,
                logical_task_key=logical_task_key,
                task_type=task_type,
                scope_type=scope_type,
                fid=fid,
                fname=fname,
                tid=tid,
                parent_pid=parent_pid,
                parent_kind=parent_kind,
                is_good=is_good,
                backfill=backfill,
                rn=rn,
                parent_logical_task_key=parent_logical_task_key,
                parent_generation=parent_generation,
                refresh=refresh,
            )

    async def _ensure_research_task_scope_in_session(
        self,
        session: AsyncSession,
        *,
        run_id: str,
        logical_task_key: str,
        task_type: str,
        scope_type: str,
        fid: int | None,
        fname: str | None,
        tid: int | None,
        parent_pid: int | None,
        parent_kind: str | None,
        is_good: bool | None,
        backfill: bool,
        rn: int,
        parent_logical_task_key: str | None,
        parent_generation: int | None,
        refresh: bool,
    ) -> ResearchTaskScope | None:
        if parent_logical_task_key is not None:
            parent = await self.get_research_task_scope_for_update(session, run_id, parent_logical_task_key)
            if parent is None or parent.generation != parent_generation:
                return None

        scope = await self.get_research_task_scope_for_update(session, run_id, logical_task_key)
        now = datetime.now(UTC)
        if scope is None:
            scope = ResearchTaskScope(
                run_id=run_id,
                logical_task_key=logical_task_key,
                generation=0,
                task_type=task_type,
                scope_type=scope_type,
                fid=fid,
                fname=fname,
                tid=tid,
                parent_pid=parent_pid,
                parent_kind=parent_kind,
                is_good=is_good,
                backfill=backfill,
                rn=rn,
                parent_logical_task_key=parent_logical_task_key,
                parent_generation=parent_generation,
                status="pending",
                last_checked=now,
            )
            session.add(scope)
            await session.flush()
            return scope

        if parent_logical_task_key is not None and scope.parent_generation not in (None, parent_generation):
            return None
        if refresh:
            scope.generation += 1
            scope.status = "pending"
            scope.reason = None
            if scope_type == "thread_replies":
                await session.execute(
                    update(ResearchTaskScope)
                    .where(
                        ResearchTaskScope.run_id == run_id,
                        ResearchTaskScope.parent_logical_task_key == logical_task_key,
                    )
                    .values(
                        generation=ResearchTaskScope.generation + 1,
                        parent_generation=scope.generation,
                        status="pending",
                        reason=None,
                        last_checked=now,
                    )
                )
        if parent_logical_task_key is not None and scope.parent_generation is None:
            scope.parent_generation = parent_generation
        scope.task_type = task_type
        scope.scope_type = scope_type
        scope.fid = fid if fid is not None else scope.fid
        scope.fname = fname if fname is not None else scope.fname
        scope.tid = tid if tid is not None else scope.tid
        scope.parent_pid = parent_pid if parent_pid is not None else scope.parent_pid
        scope.parent_kind = parent_kind if parent_kind is not None else scope.parent_kind
        scope.is_good = is_good if is_good is not None else scope.is_good
        scope.backfill = backfill
        scope.rn = rn
        scope.parent_logical_task_key = parent_logical_task_key or scope.parent_logical_task_key
        scope.last_checked = now
        return scope

    async def iter_pending_research_task_scopes(
        self,
        run_id: str,
        responsible_fids: set[int],
        *,
        batch_size: int = 500,
        tid: int | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[list[ResearchTaskScope]]:
        """Yield current non-completed detail scopes in bounded, comments-first batches.

        Optional ``tid`` and ``limit`` filters are applied by SQL. ``limit`` is
        shared across both scope types and preserves the existing comments-first
        ordering.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if tid is not None and tid < 1:
            raise ValueError("tid must be positive")
        if limit is not None and limit < 1:
            raise ValueError("limit must be positive")
        if not responsible_fids:
            return
        yielded = 0
        for scope_type in ("post_comments", "thread_replies"):
            if limit is not None and yielded >= limit:
                break
            last_key = ""
            while True:
                query_limit = batch_size if limit is None else min(batch_size, limit - yielded)
                async with self.async_sessionmaker() as session:
                    filters = [
                        ResearchTaskScope.run_id == run_id,
                        ResearchTaskScope.scope_type == scope_type,
                        ResearchTaskScope.status != "completed",
                        ResearchTaskScope.fid.in_(responsible_fids),
                        ResearchTaskScope.logical_task_key > last_key,
                    ]
                    if tid is not None:
                        filters.append(ResearchTaskScope.tid == tid)
                    result = await session.execute(
                        select(ResearchTaskScope)
                        .where(*filters)
                        .order_by(ResearchTaskScope.logical_task_key)
                        .limit(query_limit)
                    )
                    rows = list(result.scalars().all())
                if not rows:
                    break
                last_key = rows[-1].logical_task_key
                yielded += len(rows)
                yield rows

    async def adopt_legacy_research_scopes(self, run_id: str, responsible_fids: set[int]) -> None:
        """Adopt old pending full scans as generation-zero authoritative scopes."""
        if not responsible_fids:
            return
        last_tid = 0
        while True:
            async with self.async_sessionmaker() as session, session.begin():
                thread_rows = await session.execute(
                    select(PendingThreadScan)
                    .where(PendingThreadScan.fid.in_(responsible_fids), PendingThreadScan.tid > last_tid)
                    .order_by(PendingThreadScan.tid)
                    .limit(500)
                )
                batch = list(thread_rows.scalars())
                if not batch:
                    break
                last_tid = batch[-1].tid
                for pending in batch:
                    await self._ensure_research_task_scope_in_session(
                        session,
                        run_id=run_id,
                        logical_task_key=research_posts_task_key(pending.tid),
                        task_type="full_scan_posts",
                        scope_type="thread_replies",
                        fid=pending.fid,
                        fname=pending.fname,
                        tid=pending.tid,
                        parent_pid=None,
                        parent_kind=None,
                        is_good=None,
                        backfill=pending.backfill,
                        rn=30,
                        parent_logical_task_key=None,
                        parent_generation=None,
                        refresh=False,
                    )

        for scope_type in ("thread_replies", "post_comments"):
            last_key = ""
            while True:
                async with self.async_sessionmaker() as session, session.begin():
                    latest_pages = (
                        select(
                            ResearchCheckpoint.task_key.label("task_key"),
                            func.max(ResearchCheckpoint.page_number).label("page_number"),
                        )
                        .where(
                            ResearchCheckpoint.run_id == run_id,
                            ResearchCheckpoint.scope_type == scope_type,
                            ResearchCheckpoint.fid.in_(responsible_fids),
                            ResearchCheckpoint.task_key.not_like("%:g%"),
                        )
                        .group_by(ResearchCheckpoint.task_key)
                        .subquery()
                    )
                    result = await session.execute(
                        select(ResearchCheckpoint)
                        .join(
                            latest_pages,
                            and_(
                                ResearchCheckpoint.task_key == latest_pages.c.task_key,
                                ResearchCheckpoint.page_number == latest_pages.c.page_number,
                            ),
                        )
                        .where(
                            ResearchCheckpoint.run_id == run_id,
                            ResearchCheckpoint.task_key > last_key,
                        )
                        .order_by(ResearchCheckpoint.task_key)
                        .limit(500)
                    )
                    batch = list(result.scalars())
                    if not batch:
                        break
                    last_key = batch[-1].task_key
                    for checkpoint in batch:
                        if checkpoint.tid is None:
                            continue
                        if scope_type == "thread_replies":
                            logical_key = research_posts_task_key(checkpoint.tid)
                            parent_key = None
                            parent_generation = None
                        else:
                            if checkpoint.parent_pid is None:
                                continue
                            logical_key = research_comments_task_key(checkpoint.tid, checkpoint.parent_pid)
                            parent_key = research_posts_task_key(checkpoint.tid)
                            parent = await self.get_research_task_scope_for_update(session, run_id, parent_key)
                            if parent is None:
                                parent = await self._ensure_research_task_scope_in_session(
                                    session,
                                    run_id=run_id,
                                    logical_task_key=parent_key,
                                    task_type="full_scan_posts",
                                    scope_type="thread_replies",
                                    fid=checkpoint.fid,
                                    fname=checkpoint.fname,
                                    tid=checkpoint.tid,
                                    parent_pid=None,
                                    parent_kind=None,
                                    is_good=None,
                                    backfill=True,
                                    rn=30,
                                    parent_logical_task_key=None,
                                    parent_generation=None,
                                    refresh=False,
                                )
                                if parent is not None:
                                    parent.status = "completed"
                            parent_generation = parent.generation if parent is not None else None
                        scope_was_missing = await session.get(ResearchTaskScope, (run_id, logical_key)) is None
                        scope = await self._ensure_research_task_scope_in_session(
                            session,
                            run_id=run_id,
                            logical_task_key=logical_key,
                            task_type="full_scan_posts" if scope_type == "thread_replies" else "full_scan_comments",
                            scope_type=scope_type,
                            fid=checkpoint.fid,
                            fname=checkpoint.fname,
                            tid=checkpoint.tid,
                            parent_pid=checkpoint.parent_pid,
                            parent_kind=checkpoint.parent_kind,
                            is_good=checkpoint.is_good,
                            backfill=True,
                            rn=30,
                            parent_logical_task_key=parent_key,
                            parent_generation=parent_generation,
                            refresh=False,
                        )
                        if scope is not None and scope_was_missing:
                            scope.status = checkpoint.scope_status or (
                                "completed"
                                if checkpoint.status == "completed" and checkpoint.has_more is False
                                else "incomplete"
                            )
                            scope.reason = checkpoint.reason
                            scope.last_checked = checkpoint.last_checked

        last_pair = (0, 0)
        while True:
            async with self.async_sessionmaker() as session, session.begin():
                comment_rows = await session.execute(
                    select(PendingCommentScan)
                    .where(
                        PendingCommentScan.fid.in_(responsible_fids),
                        PendingCommentScan.task_kind == "full",
                        or_(
                            PendingCommentScan.tid > last_pair[0],
                            and_(PendingCommentScan.tid == last_pair[0], PendingCommentScan.pid > last_pair[1]),
                        ),
                    )
                    .order_by(PendingCommentScan.tid, PendingCommentScan.pid)
                    .limit(500)
                )
                batch = list(comment_rows.scalars())
                if not batch:
                    break
                last_pair = (batch[-1].tid, batch[-1].pid)
                for pending in batch:
                    parent_key = research_posts_task_key(pending.tid)
                    parent = await self.get_research_task_scope_for_update(session, run_id, parent_key)
                    if parent is None:
                        parent = await self._ensure_research_task_scope_in_session(
                            session,
                            run_id=run_id,
                            logical_task_key=parent_key,
                            task_type="full_scan_posts",
                            scope_type="thread_replies",
                            fid=pending.fid,
                            fname=pending.fname,
                            tid=pending.tid,
                            parent_pid=None,
                            parent_kind=None,
                            is_good=None,
                            backfill=pending.backfill,
                            rn=30,
                            parent_logical_task_key=None,
                            parent_generation=None,
                            refresh=False,
                        )
                        if parent is not None and not await session.scalar(
                            select(PendingThreadScan.tid).where(PendingThreadScan.tid == pending.tid)
                        ):
                            parent.status = "completed"
                    await self._ensure_research_task_scope_in_session(
                        session,
                        run_id=run_id,
                        logical_task_key=research_comments_task_key(pending.tid, pending.pid),
                        task_type="full_scan_comments",
                        scope_type="post_comments",
                        fid=pending.fid,
                        fname=pending.fname,
                        tid=pending.tid,
                        parent_pid=pending.pid,
                        parent_kind=None,
                        is_good=None,
                        backfill=pending.backfill,
                        rn=30,
                        parent_logical_task_key=parent_key if parent is not None else None,
                        parent_generation=parent.generation if parent is not None else None,
                        refresh=False,
                    )

    async def save_items_in_session(self, session: AsyncSession, items: list[Any], upsert: bool = False) -> None:
        """Write base ORM items inside a caller-owned transaction."""
        if not items:
            return
        items_as_dicts = [item.to_dict() for item in items]
        model_class = type(items[0])
        statement = insert(model_class).values(items_as_dicts)
        if upsert:
            primary_key = PRIMARY_KEY_MAP.get(model_class)
            update_dict = {
                col.name: statement.excluded[col.name]
                for col in model_class.__table__.columns
                if col.name not in primary_key
            }
            statement = statement.on_conflict_do_update(index_elements=list(primary_key), set_=update_dict)  # type: ignore[arg-type]
        else:
            statement = statement.on_conflict_do_nothing()
        await session.execute(statement)

    async def add_pending_comment_scan_in_session(
        self,
        session: AsyncSession,
        tid: int,
        pid: int,
        *,
        fid: int,
        fname: str,
        backfill: bool,
        task_kind: PendingCommentTaskKind = "full",
    ) -> None:
        pending = PendingCommentScan(
            tid=tid,
            pid=pid,
            fid=fid,
            fname=fname,
            backfill=backfill,
            task_kind=task_kind,
        )
        statement = insert(PendingCommentScan).values(pending.to_dict())
        task_kind_expr = case(
            (PendingCommentScan.task_kind == "full", "full"),
            (statement.excluded.task_kind == "full", "full"),
            else_="incremental",
        )
        statement = statement.on_conflict_do_update(
            index_elements=[PendingCommentScan.tid, PendingCommentScan.pid],
            set_={
                "task_kind": task_kind_expr,
                "backfill": statement.excluded.backfill,
                "fid": statement.excluded.fid,
                "fname": statement.excluded.fname,
            },
        )
        await session.execute(statement)

    async def remove_pending_comment_scan_in_session(
        self,
        session: AsyncSession,
        tid: int,
        pid: int,
        *,
        task_kind: PendingCommentTaskKind | None = None,
    ) -> None:
        statement = delete(PendingCommentScan).where(
            PendingCommentScan.tid == tid,
            PendingCommentScan.pid == pid,
        )
        if task_kind is not None:
            statement = statement.where(PendingCommentScan.task_kind == task_kind)
        await session.execute(statement)

    async def remove_pending_thread_scan_in_session(self, session: AsyncSession, tid: int) -> None:
        await session.execute(delete(PendingThreadScan).where(PendingThreadScan.tid == tid))

    async def mark_research_checkpoint_scope_status(
        self, run_id: str, task_key: str, page_number: int, scope_status: str
    ) -> None:
        async with self.async_sessionmaker() as session:
            row = await session.get(ResearchCheckpoint, (run_id, task_key, page_number))
            if row is None:
                raise ValueError("Cannot mark a scope without its committed page checkpoint.")
            row.scope_status = scope_status
            row.last_checked = datetime.now(UTC)
            await session.commit()

    async def get_research_list_checkpoint_states(
        self, run_id: str
    ) -> dict[tuple[int, bool], tuple[int | None, str | None, dict[str, object] | None]]:
        """Return CREATE stream resume state and rebuild successful page history.

        A successful non-terminal page is replayed once after restart to absorb
        page drift caused by deletions or updates at the top of the list. The
        in-memory seen set is reconstructed from successful page checkpoints;
        only page-sized TID sets are stored in each continuation JSON.
        """
        history: dict[str, list[ResearchCheckpoint]] = {}
        async with self.async_sessionmaker() as session:
            result = await session.stream(
                select(ResearchCheckpoint)
                .where(
                    ResearchCheckpoint.run_id == run_id,
                    ResearchCheckpoint.scope_type == "thread_list",
                )
                .order_by(ResearchCheckpoint.task_key, ResearchCheckpoint.page_number)
            )
            async for row in result.scalars():
                history.setdefault(row.task_key, []).append(row)

        states: dict[tuple[int, bool], tuple[int | None, str | None, dict[str, object] | None]] = {}
        for rows in history.values():
            row = rows[-1]
            if row.fid is None or row.is_good is None or getattr(row, "sort_mode", "create") != "create":
                continue

            successful_rows = [
                checkpoint
                for checkpoint in rows
                if checkpoint.status == "completed"
                or (checkpoint.continuation_json or {}).get("replay_page_checkpoint_committed")
                == checkpoint.page_number
            ]
            flow_seen: set[int] = set()
            history_partial = False
            duplicate_streak = 0
            for checkpoint in successful_rows:
                checkpoint_cursor = checkpoint.continuation_json or {}
                page_ids = checkpoint_cursor.get("page_non_top_tids")
                if isinstance(page_ids, (list, tuple)):
                    flow_seen.update(_checkpoint_tid_set(page_ids))
                else:
                    history_partial = True
                    legacy_ids = checkpoint_cursor.get("recent_non_top_tids", [])
                    flow_seen.update(_checkpoint_tid_set(legacy_ids))
                try:
                    duplicate_streak = max(0, int(checkpoint_cursor.get("flow_duplicate_streak", 0)))
                except (TypeError, ValueError):
                    duplicate_streak = 0

            cursor = dict(row.continuation_json or {})
            cursor["_flow_seen_tids"] = sorted(flow_seen)
            cursor["_flow_duplicate_streak"] = duplicate_streak
            if history_partial:
                cursor["_flow_history_partial"] = True

            duplicate_pause = bool(cursor.get("high_duplicate_pause_reason"))
            failed_replay = cursor.get("replay_page_checkpoint_committed") == row.page_number
            if (row.status == "completed" and (row.has_more is True or duplicate_pause)) or failed_replay:
                next_page = row.page_number
                scope_status = row.scope_status or "running"
                page_ids = cursor.get("page_non_top_tids")
                if not isinstance(page_ids, (list, tuple)):
                    page_ids = cursor.get("recent_non_top_tids", [])
                cursor["_resume_replay_page"] = row.page_number
                cursor["_resume_page_seen_tids"] = sorted(_checkpoint_tid_set(page_ids))
            elif row.status == "completed" and row.has_more is False:
                next_page = None
                scope_status = row.scope_status or "completed"
            else:
                next_page = row.page_number
                scope_status = row.scope_status or row.status
            states[(row.fid, row.is_good)] = (next_page, scope_status, cursor)
        return states

    async def get_research_resume_state(
        self, run_id: str, task_key: str
    ) -> tuple[int | None, str | None, dict[str, object] | None]:
        """Return a detail scope's next page, status, and bounded cursor."""
        async with self.async_sessionmaker() as session:
            result = await session.execute(
                select(ResearchCheckpoint)
                .where(
                    ResearchCheckpoint.run_id == run_id,
                    ResearchCheckpoint.task_key == task_key,
                )
                .order_by(ResearchCheckpoint.page_number.desc())
                .limit(1)
            )
            row = result.scalar_one_or_none()
        if row is None:
            return 1, None, None
        if row.status == "completed" and row.has_more is False:
            return None, row.scope_status or "completed", row.continuation_json
        if row.status == "completed":
            return row.next_page or row.page_number + 1, row.scope_status or "running", row.continuation_json
        return row.page_number, row.scope_status or row.status, row.continuation_json

    async def get_research_resume_page(self, run_id: str, task_key: str) -> tuple[int | None, str | None]:
        """Compatibility wrapper returning the detail scope's next page and status."""
        page, status, _ = await self.get_research_resume_state(run_id, task_key)
        return page, status

    async def get_research_first_post_pids(self, tid: int, *, session: AsyncSession | None = None) -> set[int]:
        """Return known first-post IDs for a thread from research detail rows."""
        from src.research.models import ResearchThread

        statement = select(ResearchThread.first_pid).where(
            ResearchThread.tid == tid,
            ResearchThread.first_pid.is_not(None),
        )
        if session is not None:
            result = await session.execute(statement)
            return {int(pid) for pid in result.scalars().all()}

        async with self.get_session() as owned_session:
            result = await owned_session.execute(statement)
            return {int(pid) for pid in result.scalars().all()}

    async def iter_pending_thread_scans(
        self,
        responsible_fids: set[int],
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[list[PendingThreadScan]]:
        """Yield pending thread scans in keyset-paginated bounded batches."""
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if not responsible_fids:
            return
        last_tid = 0
        while True:
            async with self.async_sessionmaker() as session:
                result = await session.execute(
                    select(PendingThreadScan)
                    .where(PendingThreadScan.fid.in_(responsible_fids), PendingThreadScan.tid > last_tid)
                    .order_by(PendingThreadScan.tid)
                    .limit(batch_size)
                )
                rows = list(result.scalars().all())
            if not rows:
                return
            last_tid = rows[-1].tid
            yield rows

    async def iter_pending_comment_scans(
        self,
        responsible_fids: set[int],
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[list[PendingCommentScan]]:
        """Yield pending comment scans in keyset-paginated bounded batches."""
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if not responsible_fids:
            return
        last_tid = 0
        last_pid = 0
        while True:
            async with self.async_sessionmaker() as session:
                result = await session.execute(
                    select(PendingCommentScan)
                    .where(
                        PendingCommentScan.fid.in_(responsible_fids),
                        or_(
                            PendingCommentScan.tid > last_tid,
                            and_(PendingCommentScan.tid == last_tid, PendingCommentScan.pid > last_pid),
                        ),
                    )
                    .order_by(PendingCommentScan.tid, PendingCommentScan.pid)
                    .limit(batch_size)
                )
                rows = list(result.scalars().all())
            if not rows:
                return
            last_tid, last_pid = rows[-1].tid, rows[-1].pid
            yield rows

    def get_crawl_quality_summaries(
        self, run_id: str | None = None
    ) -> AsyncIterator[dict[str, object]]:
        """Stream summaries from this store's session factory."""
        return iter_crawl_quality_summaries(self.async_sessionmaker, run_id)

    @staticmethod
    async def _iter_crawl_quality_summaries(
        async_sessionmaker_: async_sessionmaker,
        run_id: str | None = None,
    ) -> AsyncIterator[dict[str, object]]:
        """Stream one SQL-aggregated quality summary per current task generation."""
        from src.research.models import ResearchComment, ResearchPost, ResearchThread

        scope = ResearchTaskScope
        checkpoint = ResearchCheckpoint
        task_key = case(
            (scope.generation == 0, scope.logical_task_key),
            else_=scope.logical_task_key + literal(":g") + cast(scope.generation, String),
        )
        current_checkpoint = aliased(ResearchCheckpoint)
        latest_reason = (
            select(func.coalesce(current_checkpoint.reason, current_checkpoint.reported_count_missing_reason))
            .where(
                current_checkpoint.run_id == scope.run_id,
                current_checkpoint.task_key == task_key,
            )
            .order_by(current_checkpoint.page_number.desc())
            .limit(1)
            .scalar_subquery()
        )
        first_post_payload = (
            select(ResearchThread.source_payload_json["first_post"])
            .where(ResearchThread.tid == scope.tid, ResearchThread.first_pid == scope.parent_pid)
            .limit(1)
            .scalar_subquery()
        )
        first_post_payload_count = cast(first_post_payload["subreply_count_reported"].as_integer(), BigInteger)
        first_post_payload_reason = first_post_payload["missing_reason"].as_string()
        latest_completed_checkpoint = aliased(ResearchCheckpoint)
        latest_completed_checkpoint_reported = (
            select(latest_completed_checkpoint.reported_count)
            .where(
                latest_completed_checkpoint.run_id == scope.run_id,
                latest_completed_checkpoint.task_key == task_key,
                latest_completed_checkpoint.status == "completed",
            )
            .order_by(latest_completed_checkpoint.page_number.desc())
            .limit(1)
            .scalar_subquery()
        )
        first_post_checkpoint_missing = (
            select(latest_completed_checkpoint.reported_count_missing_reason)
            .where(
                latest_completed_checkpoint.run_id == scope.run_id,
                latest_completed_checkpoint.task_key == task_key,
                latest_completed_checkpoint.status == "completed",
            )
            .order_by(latest_completed_checkpoint.page_number.desc())
            .limit(1)
            .scalar_subquery()
        )
        trusted_checkpoint_count = case(
            (first_post_checkpoint_missing.is_(None), latest_completed_checkpoint_reported),
            else_=None,
        )
        first_post_count = func.coalesce(trusted_checkpoint_count, first_post_payload_count)
        first_post_missing_reason = func.coalesce(
            first_post_checkpoint_missing,
            first_post_payload_reason,
            "first_post_reply_count_unavailable",
        )
        first_post_exists = (
            select(ResearchThread.tid)
            .where(ResearchThread.tid == scope.tid, ResearchThread.first_pid == scope.parent_pid)
            .limit(1)
            .scalar_subquery()
        )
        is_first_post = or_(scope.parent_kind == "first_post", first_post_exists.is_not(None))
        reported_posts = (
            select(ResearchThread.reply_count)
            .where(ResearchThread.tid == scope.tid)
            .limit(1)
            .scalar_subquery()
        )
        parent_reported_reply_comments = (
            select(ResearchPost.subreply_count_reported)
            .where(ResearchPost.pid == scope.parent_pid, ResearchPost.tid == scope.tid)
            .limit(1)
            .scalar_subquery()
        )
        reported_reply_comments = func.coalesce(trusted_checkpoint_count, parent_reported_reply_comments)
        reported = case(
            (scope.scope_type == "thread_replies", reported_posts),
            (is_first_post, first_post_count),
            else_=reported_reply_comments,
        )
        reason = case(
            (
                and_(scope.scope_type == "post_comments", is_first_post, first_post_count.is_(None)),
                first_post_missing_reason,
            ),
            (scope.scope_type == "post_comments", latest_reason),
            else_=latest_reason,
        )
        unique_posts = select(func.count(ResearchPost.pid)).where(ResearchPost.tid == scope.tid).scalar_subquery()
        unique_comments = (
            select(func.count(ResearchComment.cid))
            .where(
                ResearchComment.tid == scope.tid,
                ResearchComment.parent_post_pid == scope.parent_pid,
            )
            .scalar_subquery()
        )
        saved = case((scope.scope_type == "thread_replies", unique_posts), else_=unique_comments)
        mismatch = case((reported.is_(None), None), else_=(saved != reported))
        successful_pages = func.sum(case((checkpoint.status == "completed", 1), else_=0))
        failure_pages = func.sum(
            case((checkpoint.status.in_(("failed", "blocked", "paused_auth", "paused_rate_limit")), 1), else_=0)
        )
        parent_kind = case((is_first_post, "first_post"), else_=scope.parent_kind)
        statement = (
            select(
                scope.run_id.label("run_id"),
                scope.generation.label("generation"),
                scope.scope_type.label("scope_type"),
                scope.fid.label("fid"),
                scope.tid.label("tid"),
                scope.parent_pid.label("parent_pid"),
                parent_kind.label("parent_kind"),
                reported.label("reported_reply_num"),
                saved.label("unique_saved_replies"),
                func.coalesce(func.max(checkpoint.total_pages), func.count(checkpoint.page_number)).label(
                    "total_pages"
                ),
                successful_pages.label("success_pages"),
                failure_pages.label("failure_pages"),
                scope.status.label("status"),
                func.coalesce(func.max(checkpoint.last_checked), scope.last_checked).label("last_checked"),
                mismatch.label("count_mismatch"),
                func.coalesce(scope.reason, reason).label("reason"),
            )
            .select_from(scope)
            .outerjoin(
                checkpoint,
                and_(checkpoint.run_id == scope.run_id, checkpoint.task_key == task_key),
            )
            .where(scope.scope_type.in_(("thread_replies", "post_comments")))
            .group_by(
                scope.run_id,
                scope.logical_task_key,
                scope.generation,
                scope.scope_type,
                scope.fid,
                scope.tid,
                scope.parent_pid,
                scope.parent_kind,
                scope.status,
                scope.reason,
                scope.last_checked,
            )
            .order_by(scope.run_id, scope.scope_type, scope.fid, scope.tid, scope.parent_pid)
        )
        if run_id is not None:
            statement = statement.where(scope.run_id == run_id)

        async with async_sessionmaker_() as session:
            result = await session.stream(statement)
            async for row in result.mappings():
                yield dict(row)  # noqa: ASYNC119 - keep the session open while the caller consumes the stream.

    @asynccontextmanager
    async def get_session(self):
        """用于获取数据库会话的异步上下文管理器。

        提供自动的事务管理，包括异常处理时的回滚操作。

        Yields:
            AsyncSession: 异步数据库会话对象。

        Raises:
            Exception: 如果在会话中发生错误，将回滚事务并重新抛出异常。
        """
        async with self.async_sessionmaker() as session:
            try:
                yield session
            except Exception as e:
                logger.exception("Error in database session: {}", e)
                await session.rollback()
                raise

    async def filter_new_ids(self, item_type: ItemType | Literal["user"], ids: list[int]) -> set[int]:
        """使用客户端缓存过滤新ID。

        过滤策略如下：
        1. 内存缓存 / Redis缓存检查（自动使用客户端本地缓存）
        2. 数据库查重过滤（最终的真实性检查）

        Args:
            item_type: 数据项类型，可选值为'thread'、'post'、'comment'或'user'。
            ids: 需要检查的ID列表。

        Returns:
            set[int]: 不存在于数据库中的新ID集合。
        """
        if not ids:
            return set()

        # cashews 的 redis client-side caching 后端在 get_many 入参包含重复 key 时
        # 可能抛出 KeyError（内部对 missed_keys 做 set.remove）。
        # 这里通过对 ids 先进行保序去重来规避该问题。
        unique_ids = list(dict.fromkeys(ids))

        # 第一层：内存缓存 / Redis缓存(With Client-Side Caching)过滤
        ids_after_cache = await self._filter_by_cache(item_type, unique_ids)
        if not ids_after_cache:
            return set()

        # 第二层：数据库查重过滤（在事务中执行，确保一致性）
        try:
            ids_after_db = await self._filter_by_database(item_type, list(ids_after_cache))
        except Exception as e:
            # 如果数据库查询失败，不标记缓存，避免缓存和数据库状态不一致
            logger.exception("Database filter failed for {}: {}", item_type, e)
            raise

        if ids_after_db is not None:
            await self._mark_as_processed(item_type, ids_after_db)

        return ids_after_db

    async def _filter_by_cache(self, item_type: ItemType | Literal["user"], ids: list[int]) -> set[int]:
        """通过内存缓存 / Redis缓存（With Client-Side Caching）过滤。

        Redis Client-Side Caching会自动：
        1. 首先检查本地缓存
        2. 本地缓存未命中时才查询Redis服务器
        3. 自动维护缓存一致性

        Args:
            item_type: 数据项类型，可选值为'thread'、'post'、'comment'或'user'。
            ids: 需要检查的ID列表。

        Returns:
            set[int]: 未缓存的ID集合。
        """
        cache_keys = [f"{item_type}:{item_id}" for item_id in ids]
        cached_results = await self.cache.get_many(*cache_keys)

        new_ids = {ids[i] for i, val in enumerate(cached_results) if val is None}

        hits = len(ids) - len(new_ids)
        if hits > 0:
            CACHE_HITS.labels(type=item_type).inc(hits)

        logger.debug("Cache filter: {} -> {} for {}", len(ids), len(new_ids), item_type)
        return new_ids

    async def _filter_by_database(self, item_type: ItemType | Literal["user"], ids: list[int]) -> set[int]:
        """数据库查重过滤

        根据item_type从数据库中查询已存在的ID，并返回未存在的ID集合。

        Args:
            item_type: 数据项类型，可选值为'thread'、'post'、'comment'或'user'。
            ids: 需要检查的ID列表。

        Returns:
            set[int]: 不存在于数据库中的新ID集合。
        """
        if not ids:
            return set()

        async with self.get_session() as session:
            model_map = {
                "thread": Thread.tid,
                "post": Post.pid,
                "comment": Comment.cid,
                "user": User.user_id,
            }
            id_column = model_map.get(item_type)
            if not id_column:
                raise ValueError(f"Unknown item_type: {item_type}")

            statement = select(id_column).where(id_column.in_(ids))
            result = await session.execute(statement)
            existing_ids = {row[0] for row in result.fetchall()}

        if existing_ids:
            CACHE_MISSES.labels(type=item_type).inc(len(existing_ids))
            logger.debug("Database filter: Found {} existing IDs for {}", len(existing_ids), item_type)
            # 标记缓存由 filter_new_ids 统一处理

        new_ids = set(ids) - existing_ids
        logger.debug(
            "Database filter: {} -> {} for {} (found {} existing)",
            len(ids),
            len(new_ids),
            item_type,
            len(existing_ids),
        )
        return new_ids

    async def _mark_as_processed(self, item_type: ItemType | Literal["user"], new_ids: set[int]):
        """标记ID为已处理。"""
        if not new_ids:
            return

        updates = {f"{item_type}:{item_id}": True for item_id in new_ids}
        await self.cache.set_many(updates, expire=self.container.config.cache_ttl_seconds)

        logger.debug("Marked {} new {} IDs as processed", len(new_ids), item_type)

    async def save_items[T: (Comment, Forum, Post, Thread, User)](self, items: list[T], upsert: bool = False):
        """将SQLAlchemy对象批量保存到数据库。

        使用PostgreSQL的 "INSERT ... ON CONFLICT DO NOTHING"
        防止极端情况下的主键冲突，
        或使用 "INSERT ... ON CONFLICT DO UPDATE" 实现UPSERT功能。

        Args:
            items: 需要保存的SQLAlchemy模型实例列表。支持Comment、Forum、
                   Post、Thread、User等类型。
            upsert: 是否启用UPSERT功能。如果为True，则在主键冲突时更新现有记录。
                    默认为False，表示仅插入新记录，冲突时忽略。
        """
        if not items:
            return

        async with self.get_session() as session:
            table_name = "unknown"
            op = "upsert" if upsert else "insert"
            try:
                items_as_dicts = [item.to_dict() for item in items]
                model_class = type(items[0])
                table_name = model_class.__tablename__

                statement = insert(model_class).values(items_as_dicts)

                if upsert:
                    primary_key = PRIMARY_KEY_MAP.get(model_class)
                    update_dict = {
                        col.name: statement.excluded[col.name]
                        for col in model_class.__table__.columns
                        if col.name not in primary_key
                    }
                    statement = statement.on_conflict_do_update(index_elements=list(primary_key), set_=update_dict)  # type: ignore
                else:
                    statement = statement.on_conflict_do_nothing()

                await session.execute(statement)
                await session.commit()
                DB_OPERATIONS.labels(operation=op, table=table_name, status="success").inc(len(items))
                logger.debug("Successfully saved/ignored {} items using ON CONFLICT.", len(items))

            except IntegrityError as e:
                DB_OPERATIONS.labels(operation=op, table=table_name, status="integrity_error").inc()
                logger.error("An unexpected integrity error occurred: {}", e)
                raise
            except Exception as e:
                DB_OPERATIONS.labels(operation=op, table=table_name, status="error").inc()
                logger.error("Failed to save items: {}", e)
                raise

    async def get_threads_by_tids(self, tids: list[int] | set[int]) -> list[Thread]:
        """根据一组tid从数据库获取主题贴信息。

        Args:
            tids: 主题贴tid列表

        Returns:
            list[Thread]: 主题贴对象列表，如果不存在则返回空列表。
        """
        async with self.get_session() as session:
            statement = select(Thread).where(Thread.tid.in_(tids))
            result = await session.execute(statement)
            return list(result.scalars().all())

    async def needs_research_thread_detail(self, tids: list[int] | set[int]) -> set[int]:
        """Return tids without an authoritative first-post content observation."""
        if not tids:
            return set()
        from src.research.models import ResearchContentObservation

        requested = set(tids)
        async with self.get_session() as session:
            result = await session.execute(
                select(ResearchContentObservation.owner_id).where(
                    ResearchContentObservation.owner_type == "thread_first_post",
                    ResearchContentObservation.owner_id.in_(requested),
                )
            )
            complete = set(result.scalars().all())
        return requested - complete

    async def get_posts_by_pids(self, pids: list[int] | set[int]) -> list[Post]:
        """根据一组pid从数据库获取回复信息。

        Args:
            pids: 回复pid列表

        Returns:
            list[Post]: 回复对象列表，如果不存在则返回空列表。
        """
        async with self.get_session() as session:
            statement = select(Post).where(Post.pid.in_(pids))
            result = await session.execute(statement)
            return list(result.scalars().all())

    async def push_object_event(
        self,
        item_type: ItemType,
        obj: DTOType,
        *,
        backfill: bool = False,
        event_type: str = "upsert",
    ) -> None:
        """将完整对象以事件形式推送到对象消费者（Redis Streams 或 WebSocket）。"""
        envelope: EventEnvelope = build_envelope(item_type, obj, event_type=event_type, backfill=backfill)
        await self.publisher.publish_object(envelope)

    async def update_thread_metadata(self, tid: int, last_time: datetime, reply_num: int) -> None:
        """更新主题贴的元数据（最后回复时间和回复数）。"""
        from sqlalchemy import update

        async with self.get_session() as session:
            stmt = update(Thread).where(Thread.tid == tid).values(last_time=last_time, reply_num=reply_num)
            await session.execute(stmt)
            await session.commit()

    async def save_threads_and_pending_scans(
        self,
        thread_dtos: list[ThreadDTO],
        backfill: bool = False,
        *,
        upsert_threads: bool = False,
        force_pending_scan: bool = False,
        research_run_id: str | None = None,
        refresh_research_run_id: str | None = None,
    ) -> dict[int, int]:
        """原子保存 thread 元数据与 pending_scan 标记。

        Args:
            thread_dtos: 待保存的 thread DTO 列表。
            backfill: 是否为回溯任务。
            upsert_threads: 是否在 thread 主键冲突时更新元数据。
                仅在需要强制刷新 thread 元数据（如 force 全量扫描）时启用。
        """
        if not thread_dtos:
            return {}
        thread_models = [Thread.from_dto(dto) for dto in thread_dtos]
        pending_scans = [
            PendingThreadScan(tid=dto.tid, fid=dto.fid, fname=dto.fname, backfill=backfill)
            for dto in thread_dtos
            if dto.reply_num > 0 or backfill or force_pending_scan
        ]

        generations: dict[int, int] = {}
        async with self.get_session() as session:
            for dto in thread_dtos:
                scope_run_id = refresh_research_run_id or research_run_id
                if scope_run_id is None:
                    continue
                scope = await self._ensure_research_task_scope_in_session(
                    session,
                    run_id=scope_run_id,
                    logical_task_key=research_posts_task_key(dto.tid),
                    task_type="full_scan_posts",
                    scope_type="thread_replies",
                    fid=dto.fid,
                    fname=dto.fname,
                    tid=dto.tid,
                    parent_pid=None,
                    parent_kind=None,
                    is_good=None,
                    backfill=backfill,
                    rn=30,
                    parent_logical_task_key=None,
                    parent_generation=None,
                    refresh=refresh_research_run_id is not None,
                )
                if scope is not None:
                    generations[dto.tid] = scope.generation

            if thread_models:
                thread_values = [item.to_dict() for item in thread_models]
                thread_stmt = insert(Thread).values(thread_values)
                if upsert_threads:
                    primary_key = PRIMARY_KEY_MAP[Thread]
                    update_dict = {
                        col.name: thread_stmt.excluded[col.name]
                        for col in Thread.__table__.columns
                        if col.name not in primary_key
                    }
                    thread_stmt = thread_stmt.on_conflict_do_update(
                        index_elements=list(primary_key),
                        set_=update_dict,
                    )
                else:
                    thread_stmt = thread_stmt.on_conflict_do_nothing()
                await session.execute(thread_stmt)

            if pending_scans:
                pending_values = [item.to_dict() for item in pending_scans]
                pending_stmt = insert(PendingThreadScan).values(pending_values).on_conflict_do_nothing()
                await session.execute(pending_stmt)
            await session.commit()

        return generations

    async def remove_pending_thread_scan(
        self,
        tid: int,
        *,
        run_id: str | None = None,
        generation: int | None = None,
    ) -> bool:
        """移除一条待扫描的主题贴记录。"""
        async with self.get_session() as session:
            if run_id is not None and generation is not None:
                scope = await self.get_research_task_scope_for_update(session, run_id, research_posts_task_key(tid))
                if scope is None or scope.generation != generation:
                    return False
            stmt = delete(PendingThreadScan).where(PendingThreadScan.tid == tid)
            await session.execute(stmt)
            await session.commit()
            return True

    async def get_all_pending_thread_scans(self) -> list[PendingThreadScan]:
        """获取所有待扫描的主题贴记录（用于重启恢复）。"""
        async with self.get_session() as session:
            result = await session.execute(select(PendingThreadScan))
            return list(result.scalars().all())

    async def get_pending_thread_scan_tids(self, tids: set[int] | list[int]) -> set[int]:
        """查询给定 tid 中仍处于 pending_thread_scan 的集合。"""
        if not tids:
            return set()

        async with self.get_session() as session:
            stmt = select(PendingThreadScan.tid).where(PendingThreadScan.tid.in_(tids))
            result = await session.execute(stmt)
            return {row[0] for row in result.fetchall()}

    async def add_pending_comment_scan(
        self,
        tid: int,
        pid: int,
        *,
        fid: int | None = None,
        fname: str | None = None,
        backfill: bool,
        task_kind: PendingCommentTaskKind,
        refresh_research_run_id: str | None = None,
    ) -> None:
        """添加一条楼中楼待扫描记录。"""
        async with self.get_session() as session:
            pending = PendingCommentScan(
                tid=tid,
                pid=pid,
                fid=fid,
                fname=fname,
                backfill=backfill,
                task_kind=task_kind,
            )
            stmt = insert(PendingCommentScan).values(pending.to_dict())
            # 冲突时采用 "full" 优先策略：
            # - 已有记录是 full，则保持 full；
            # - 新写入是 full，则将 incremental 升级为 full。
            # 这样可避免增量任务覆盖全量语义，导致重启恢复时丢失全量扫描机会。
            task_kind_expr = case(
                (PendingCommentScan.task_kind == "full", "full"),
                (stmt.excluded.task_kind == "full", "full"),
                else_="incremental",
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[PendingCommentScan.tid, PendingCommentScan.pid],
                set_={
                    "task_kind": task_kind_expr,
                    "backfill": stmt.excluded.backfill,
                    "fid": stmt.excluded.fid,
                    "fname": stmt.excluded.fname,
                },
            )
            await session.execute(stmt)
            await session.commit()

    async def remove_pending_comment_scan(
        self,
        tid: int,
        pid: int,
        *,
        task_kind: PendingCommentTaskKind | None = None,
        run_id: str | None = None,
        generation: int | None = None,
    ) -> bool:
        """移除一条楼中楼待扫描记录。"""
        async with self.get_session() as session:
            if run_id is not None and generation is not None:
                scope = await self.get_research_task_scope_for_update(
                    session, run_id, f"post_comments:{tid}:{pid}"
                )
                if scope is None or scope.generation != generation:
                    return False
            stmt = delete(PendingCommentScan).where((PendingCommentScan.tid == tid) & (PendingCommentScan.pid == pid))
            if task_kind is not None:
                stmt = stmt.where(PendingCommentScan.task_kind == task_kind)
            await session.execute(stmt)
            await session.commit()
            return True

    async def get_all_pending_comment_scans(self) -> list[PendingCommentScan]:
        """获取所有楼中楼待扫描记录（用于重启恢复）。"""
        async with self.get_session() as session:
            result = await session.execute(select(PendingCommentScan))
            return list(result.scalars().all())
