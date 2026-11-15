"""Durable, scope-gated web enrichment task lifecycle."""

from __future__ import annotations

import hashlib
import json
import operator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import and_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.core.models import (
    ResearchRun,
    ResearchTaskScope,
    research_comments_task_key,
)
from src.research.models import (
    ResearchComment,
    ResearchPost,
    ResearchThread,
    ResearchWebEnrichmentAttempt,
    ResearchWebEnrichmentFailure,
    ResearchWebEnrichmentTask,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import async_sessionmaker


WEB_FIELD_TYPES: dict[str, type] = {
    "author_user_name": str,
    "author_nickname": str,
    "author_portrait": str,
    "forum_level": int,
    "global_growth_level": int,
    "global_growth_icon_url": str,
    "is_bawu": bool,
    "ip_location_text": str,
    "agree_count": int,
    "reply_count": int,
    "collect_count": int,
    "view_count": int,
    "reply_to_user_id": int,
    "reply_to_display_name": str,
}

# Existing tasks may still contain these fields. Keep their types for reconciliation,
# while excluding them from new task requests.
LEGACY_WEB_FIELD_TYPES: dict[str, type] = {
    "forum_level_name": str,
    "role_type": str,
    "role_name": str,
    "role_badge_id": str,
    "role_badge_url": str,
    "disagree_count": int,
    "share_count": int,
    "forum_level_icon_ref": str,
}

_TARGET_FIELDS: dict[str, frozenset[str]] = {
    "thread": frozenset(
        {
            "author_user_name",
            "author_nickname",
            "author_portrait",
            "forum_level",
            "global_growth_level",
            "global_growth_icon_url",
            "is_bawu",
            "ip_location_text",
            "agree_count",
            "reply_count",
            "collect_count",
            "view_count",
        }
    ),
    "post": frozenset(
        {
            "author_user_name",
            "author_nickname",
            "author_portrait",
            "forum_level",
            "global_growth_level",
            "global_growth_icon_url",
            "is_bawu",
            "ip_location_text",
            "agree_count",
        }
    ),
    "comment": frozenset(
        {
            "author_user_name",
            "author_nickname",
            "author_portrait",
            "forum_level",
            "global_growth_level",
            "global_growth_icon_url",
            "is_bawu",
            "ip_location_text",
            "agree_count",
            "reply_to_user_id",
            "reply_to_display_name",
        }
    ),
}


@dataclass(frozen=True, slots=True)
class EnqueueSummary:
    run_id: str
    planned_count: int
    created_count: int
    deduplicated_count: int
    skipped: dict[str, int]
    task_keys: tuple[str, ...]
    planned_targets: tuple[WebEnrichmentTargetPlan, ...] = ()


@dataclass(frozen=True, slots=True)
class WebEnrichmentTargetPlan:
    task_key: str
    target_type: str
    target_id: int
    tid: int
    pid: int | None
    cid: int | None
    parent_post_pid: int | None
    author_user_id: int
    author_portrait: str | None
    fid: int
    requested_fields: tuple[str, ...]
    target_observed_at: datetime
    scope_generation: int
    parent_scope_generation: int | None
    eligibility_status: str
    field_statuses: dict[str, dict[str, str]]


@dataclass(frozen=True, slots=True)
class WebEnrichmentTaskLease:
    task_key: str
    lease_token: str
    lease_generation: int
    run_id: str
    target_type: str
    target_id: int
    tid: int
    pid: int | None
    cid: int | None
    parent_post_pid: int | None
    author_user_id: int
    author_portrait: str | None
    fid: int | None
    requested_fields: tuple[str, ...]
    target_observed_at: datetime
    scope_generation: int
    parent_scope_generation: int | None


def _target_fields(target_type: str) -> frozenset[str]:
    try:
        return _TARGET_FIELDS[target_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported web target type: {target_type}.") from exc


def _normalize_requested_fields(fields: Sequence[str], target_type: str | None = None) -> tuple[str, ...]:
    if isinstance(fields, (str, bytes)):
        raise ValueError("requested_fields must be a sequence of field names.")
    normalized = tuple(sorted(set(fields)))
    if not normalized:
        raise ValueError("At least one requested field is required.")
    unknown = set(normalized) - WEB_FIELD_TYPES.keys()
    if unknown:
        raise ValueError(f"Unsupported web fields: {', '.join(sorted(unknown))}.")
    if target_type is not None:
        unsupported = set(normalized) - _target_fields(target_type)
        if unsupported:
            raise ValueError(
                f"Fields unsupported for {target_type}: {', '.join(sorted(unsupported))}."
            )
    return normalized


def _now(value: datetime | None) -> datetime:
    result = value or datetime.now(UTC)
    if result.tzinfo is None:
        return result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def _task_key(
    *,
    run_id: str,
    target_type: str,
    target_id: int,
    tid: int,
    pid: int | None,
    cid: int | None,
    parent_post_pid: int | None,
    author_user_id: int,
    author_portrait: str | None,
    fid: int | None,
    requested_fields: tuple[str, ...],
    target_observed_at: datetime,
    scope_generation: int,
    parent_scope_generation: int | None,
) -> str:
    identity = {
        "run_id": run_id,
        "target_type": target_type,
        "target_id": target_id,
        "tid": tid,
        "pid": pid,
        "cid": cid,
        "parent_post_pid": parent_post_pid,
        "author_user_id": author_user_id,
        "author_portrait": author_portrait,
        "fid": fid,
        "requested_fields": requested_fields,
        "target_observed_at": _now(target_observed_at).isoformat(),
        "scope_generation": scope_generation,
        "parent_scope_generation": parent_scope_generation,
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _insert_factory(dialect: str) -> Any:
    if dialect == "postgresql":
        return pg_insert
    if dialect == "sqlite":
        return sqlite_insert
    raise ValueError(f"Unsupported database dialect for web task persistence: {dialect}.")


async def enqueue_web_enrichment_tasks(
    async_sessionmaker_: async_sessionmaker,
    run_id: str,
    requested_fields: Sequence[str],
    *,
    target_type: str | None = None,
    target_ids: Sequence[int] | None = None,
    tid: int | None = None,
    fid: int | None = None,
    parent_pid: int | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    priority: int = 0,
    max_attempts: int = 3,
) -> EnqueueSummary:
    """Create deduplicated field tasks only for content represented in this run.

    Detail scopes that are not complete remain durable as ``pending_api_detail``;
    they become leaseable after a later enqueue sees the same proof complete.
    """
    if target_type is not None and target_type not in _TARGET_FIELDS:
        raise ValueError(f"Unsupported web target type: {target_type}.")
    if parent_pid is not None and target_type not in {None, "comment"}:
        raise ValueError("parent_pid can only be used with comment targets.")
    requested = _normalize_requested_fields(requested_fields, target_type)
    if limit is not None and limit < 1:
        raise ValueError("limit must be a positive integer.")
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive.")
    if isinstance(priority, bool) or not isinstance(priority, int):
        raise ValueError("priority must be an integer.")
    ids = None if target_ids is None else {_positive_target_id(value) for value in target_ids}
    tid_filter = None if tid is None else _positive_target_id(tid)
    fid = None if fid is None else _positive_target_id(fid)
    parent_pid_filter = None if parent_pid is None else _positive_target_id(parent_pid)
    now = datetime.now(UTC)
    skipped: dict[str, int] = {}
    planned: list[dict[str, Any]] = []

    async with async_sessionmaker_() as session, session.begin():
        run = await session.get(ResearchRun, run_id)
        if run is None:
            raise ValueError(f"Unknown research run: {run_id}.")

        scope_rows = (
            await session.scalars(
                select(ResearchTaskScope)
                .where(
                    ResearchTaskScope.run_id == run_id,
                    ResearchTaskScope.scope_type.in_(("thread_replies", "post_comments")),
                    *([ResearchTaskScope.tid == tid_filter] if tid_filter is not None else []),
                )
                .order_by(ResearchTaskScope.logical_task_key)
            )
        ).all()
        thread_scopes: dict[int, ResearchTaskScope] = {}
        comment_scopes: dict[tuple[int, int], ResearchTaskScope] = {}
        for scope in scope_rows:
            if scope.tid is None:
                continue
            if scope.scope_type == "thread_replies":
                if scope.logical_task_key == f"thread_replies:{scope.tid}":
                    thread_scopes[scope.tid] = scope
            elif scope.scope_type == "post_comments" and scope.parent_pid is not None:
                key = research_comments_task_key(scope.tid, scope.parent_pid)
                if scope.logical_task_key == key:
                    comment_scopes[(scope.tid, scope.parent_pid)] = scope

        type_order = (target_type,) if target_type else ("thread", "post", "comment")
        for current_type in type_order:
            applicable_fields = tuple(field for field in requested if field in _target_fields(current_type))
            if not applicable_fields:
                continue
            if target_type is not None and applicable_fields != requested:
                raise ValueError(f"Some requested fields are unsupported for {current_type}.")
            valid_tids = sorted(thread_scopes)
            stmt = None
            if current_type == "thread":
                stmt = select(ResearchThread).where(ResearchThread.tid.in_(valid_tids))
                if ids is not None:
                    stmt = stmt.where(ResearchThread.tid.in_(ids))
                if tid_filter is not None:
                    stmt = stmt.where(ResearchThread.tid == tid_filter)
                if fid is not None:
                    stmt = stmt.where(ResearchThread.fid == fid)
                stmt = stmt.order_by(ResearchThread.tid)
            elif current_type == "post":
                stmt = select(ResearchPost).where(ResearchPost.tid.in_(valid_tids))
                if ids is not None:
                    stmt = stmt.where(ResearchPost.pid.in_(ids))
                if tid_filter is not None:
                    stmt = stmt.where(ResearchPost.tid == tid_filter)
                if fid is not None:
                    stmt = stmt.where(ResearchPost.fid == fid)
                stmt = stmt.order_by(ResearchPost.tid, ResearchPost.pid)
            else:
                possible_parent_scopes = [
                    scope
                    for scope in comment_scopes.values()
                    if scope.tid in thread_scopes
                    and scope.parent_generation == thread_scopes[scope.tid].generation
                ]
                tids = sorted({scope.tid for scope in possible_parent_scopes})
                pids = sorted({scope.parent_pid for scope in possible_parent_scopes if scope.parent_pid is not None})
                stmt = select(ResearchComment).where(
                    ResearchComment.tid.in_(tids),
                    ResearchComment.parent_post_pid.in_(pids),
                )
                if ids is not None:
                    stmt = stmt.where(ResearchComment.cid.in_(ids))
                if tid_filter is not None:
                    stmt = stmt.where(ResearchComment.tid == tid_filter)
                if fid is not None:
                    stmt = stmt.where(ResearchComment.fid == fid)
                if parent_pid_filter is not None:
                    stmt = stmt.where(ResearchComment.parent_post_pid == parent_pid_filter)
                stmt = stmt.order_by(ResearchComment.tid, ResearchComment.parent_post_pid, ResearchComment.cid)
            if limit is not None:
                stmt = stmt.limit(limit)
            entities = (await session.scalars(stmt)).all()

            for row in entities:
                target_tid = row.tid
                thread_scope = thread_scopes.get(target_tid)
                if thread_scope is None:
                    skipped["thread_scope_missing"] = skipped.get("thread_scope_missing", 0) + 1
                    continue
                if thread_scope.fid is not None and thread_scope.fid != row.fid:
                    skipped["thread_scope_fid_mismatch"] = skipped.get("thread_scope_fid_mismatch", 0) + 1
                    continue
                parent_scope: ResearchTaskScope | None = None
                if current_type == "comment":
                    parent_scope = comment_scopes.get((target_tid, row.parent_post_pid))
                    if (
                        parent_scope is None
                        or parent_scope.parent_generation != thread_scope.generation
                    ):
                        skipped["parent_scope_generation_mismatch"] = skipped.get(
                            "parent_scope_generation_mismatch", 0
                        ) + 1
                        continue
                    if parent_scope.fid is not None and parent_scope.fid != row.fid:
                        skipped["comment_scope_fid_mismatch"] = skipped.get(
                            "comment_scope_fid_mismatch", 0
                        ) + 1
                        continue
                author_user_id = getattr(row, "author_user_id", None)
                if author_user_id is None or author_user_id <= 0:
                    skipped["author_identity_missing"] = skipped.get("author_identity_missing", 0) + 1
                    continue
                if row.fid is None or row.fid <= 0:
                    skipped["forum_identity_missing"] = skipped.get("forum_identity_missing", 0) + 1
                    continue
                if row.observed_at is None:
                    skipped["target_api_watermark_missing"] = skipped.get("target_api_watermark_missing", 0) + 1
                    continue
                if current_type == "thread":
                    target_id, pid, cid, target_parent_pid = row.tid, None, None, None
                elif current_type == "post":
                    target_id, pid, cid, target_parent_pid = row.pid, row.pid, None, None
                else:
                    target_id, pid, cid, target_parent_pid = row.cid, None, row.cid, row.parent_post_pid
                field_names = tuple(applicable_fields)
                key = _task_key(
                    run_id=run_id,
                    target_type=current_type,
                    target_id=target_id,
                    tid=target_tid,
                    pid=pid,
                    cid=cid,
                    parent_post_pid=target_parent_pid,
                    author_user_id=author_user_id,
                    author_portrait=row.author_portrait,
                    fid=row.fid,
                    requested_fields=field_names,
                    target_observed_at=row.observed_at,
                    scope_generation=parent_scope.generation if parent_scope else thread_scope.generation,
                    parent_scope_generation=thread_scope.generation if parent_scope else None,
                )
                ready = thread_scope.status == "completed" and (
                    parent_scope is None or parent_scope.status == "completed"
                )
                planned.append(
                    {
                        "task_key": key,
                        "run_id": run_id,
                        "target_type": current_type,
                        "target_id": target_id,
                        "tid": target_tid,
                        "pid": pid,
                        "cid": cid,
                        "parent_post_pid": target_parent_pid,
                        "author_user_id": author_user_id,
                        "author_portrait": row.author_portrait,
                        "fid": row.fid,
                        "requested_fields_json": list(field_names),
                        "field_statuses_json": {
                            name: {"status": "pending", "reason": None} for name in field_names
                        },
                        "target_observed_at": row.observed_at,
                        "scope_generation": parent_scope.generation if parent_scope else thread_scope.generation,
                        "parent_scope_generation": thread_scope.generation if parent_scope else None,
                        "priority": priority,
                        "status": "pending_web" if ready else "pending_api_detail",
                        "attempts": 0,
                        "max_attempts": max_attempts,
                        "last_error_type": None,
                        "last_error": None,
                        "next_retry_at": None,
                        "lease_generation": 0,
                        "identity_verified": None,
                        "identity_reason": None,
                        "created_at": now,
                        "updated_at": now,
                    }
                )
        planned.sort(key=operator.itemgetter("target_type", "target_id", "task_key"))
        if limit is not None:
            planned = planned[:limit]

        plans = tuple(
            WebEnrichmentTargetPlan(
                task_key=task["task_key"],
                target_type=task["target_type"],
                target_id=task["target_id"],
                tid=task["tid"],
                pid=task["pid"],
                cid=task["cid"],
                parent_post_pid=task["parent_post_pid"],
                author_user_id=task["author_user_id"],
                author_portrait=task["author_portrait"],
                fid=task["fid"],
                requested_fields=tuple(task["requested_fields_json"]),
                target_observed_at=task["target_observed_at"],
                scope_generation=task["scope_generation"],
                parent_scope_generation=task["parent_scope_generation"],
                eligibility_status=task["status"],
                field_statuses=task["field_statuses_json"],
            )
            for task in planned
        )

        if dry_run or not planned:
            return EnqueueSummary(
                run_id,
                len(planned),
                0,
                0,
                skipped,
                tuple(task["task_key"] for task in planned),
                plans,
            )

        insert_factory = _insert_factory(session.get_bind().dialect.name)
        created = 0
        deduplicated = 0
        task_keys: list[str] = []
        for task in planned:
            key = task["task_key"]
            task_keys.append(key)
            statement = insert_factory(ResearchWebEnrichmentTask).values(**task).on_conflict_do_nothing(
                index_elements=[ResearchWebEnrichmentTask.task_key]
            )
            result = await session.execute(statement)
            if result.rowcount == 1:
                created += 1
                continue
            deduplicated += 1
            existing = await session.scalar(
                select(ResearchWebEnrichmentTask)
                .where(ResearchWebEnrichmentTask.task_key == key)
                .with_for_update()
            )
            if existing is not None and existing.status == "pending_api_detail" and task["status"] == "pending_web":
                existing.status = "pending_web"
                existing.updated_at = now
                existing.last_error_type = None
                existing.last_error = None

    return EnqueueSummary(run_id, len(planned), created, deduplicated, skipped, tuple(task_keys), plans)


async def plan_web_enrichment_targets(
    async_sessionmaker_: async_sessionmaker,
    run_id: str,
    requested_fields: Sequence[str],
    *,
    target_type: str | None = None,
    target_ids: Sequence[int] | None = None,
    tid: int | None = None,
    fid: int | None = None,
    parent_pid: int | None = None,
    limit: int | None = None,
) -> tuple[WebEnrichmentTargetPlan, ...]:
    """Return precise task identities and scope readiness without writing anything."""
    summary = await enqueue_web_enrichment_tasks(
        async_sessionmaker_,
        run_id,
        requested_fields,
        target_type=target_type,
        target_ids=target_ids,
        tid=tid,
        fid=fid,
        parent_pid=parent_pid,
        limit=limit,
        dry_run=True,
    )
    return summary.planned_targets


def _positive_target_id(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("target IDs must be positive integers.")
    return value


async def lease_web_enrichment_tasks(
    async_sessionmaker_: async_sessionmaker,
    run_id: str,
    worker_id: str,
    *,
    limit: int = 1,
    lease_seconds: int = 120,
    task_keys: Sequence[str] | None = None,
    now: datetime | None = None,
) -> list[WebEnrichmentTaskLease]:
    """Lease due tasks in a short transaction; caller performs HTTP after return."""
    if not worker_id or len(worker_id) > 128:
        raise ValueError("worker_id must contain 1..128 characters.")
    if limit < 1 or lease_seconds < 1:
        raise ValueError("limit and lease_seconds must be positive.")
    if task_keys is not None and not task_keys:
        return []
    current_time = _now(now)
    lease_until = current_time + timedelta(seconds=lease_seconds)
    leases: list[WebEnrichmentTaskLease] = []
    async with async_sessionmaker_() as session, session.begin():
        run = await session.get(ResearchRun, run_id)
        if run is None or run.status in {"cancelled", "canceled"}:
            return []
        eligible = and_(
            ResearchWebEnrichmentTask.run_id == run_id,
            ResearchWebEnrichmentTask.attempts < ResearchWebEnrichmentTask.max_attempts,
            (
                (ResearchWebEnrichmentTask.status == "pending_web")
                | (
                    (ResearchWebEnrichmentTask.status == "failed_retryable")
                    & (
                        ResearchWebEnrichmentTask.next_retry_at.is_(None)
                        | (ResearchWebEnrichmentTask.next_retry_at <= current_time)
                    )
                )
                | (
                    (ResearchWebEnrichmentTask.status == "running")
                    & (ResearchWebEnrichmentTask.lease_until <= current_time)
                )
            ),
        )
        if task_keys is not None:
            eligible = and_(eligible, ResearchWebEnrichmentTask.task_key.in_(set(task_keys)))
        result = await session.execute(
            select(ResearchWebEnrichmentTask)
            .where(eligible)
            .order_by(
                ResearchWebEnrichmentTask.priority.desc(),
                ResearchWebEnrichmentTask.created_at,
                ResearchWebEnrichmentTask.task_key,
            )
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = result.scalars().all()
        for task in rows:
            token = str(uuid4())
            task.status = "running"
            task.attempts += 1
            task.lease_generation += 1
            task.lease_token = token
            task.lease_owner = worker_id
            task.lease_until = lease_until
            task.leased_at = current_time
            task.next_retry_at = None
            task.updated_at = current_time
            task.identity_verified = None
            task.identity_reason = None
            task.field_statuses_json = {
                name: {"status": "pending", "reason": None} for name in task.requested_fields_json
            }
            session.add(
                ResearchWebEnrichmentAttempt(
                    task_key=task.task_key,
                    lease_generation=task.lease_generation,
                    started_at=current_time,
                    status="running",
                )
            )
            leases.append(
                WebEnrichmentTaskLease(
                    task_key=task.task_key,
                    lease_token=token,
                    lease_generation=task.lease_generation,
                    run_id=task.run_id,
                    target_type=task.target_type,
                    target_id=task.target_id,
                    tid=task.tid,
                    pid=task.pid,
                    cid=task.cid,
                    parent_post_pid=task.parent_post_pid,
                    author_user_id=task.author_user_id,
                    author_portrait=task.author_portrait,
                    fid=task.fid,
                    requested_fields=tuple(task.requested_fields_json),
                    target_observed_at=task.target_observed_at,
                    scope_generation=task.scope_generation,
                    parent_scope_generation=task.parent_scope_generation,
                )
            )
        # Expired jobs at the retry bound cannot be leased again.
        exhausted = await session.scalars(
            select(ResearchWebEnrichmentTask).where(
                ResearchWebEnrichmentTask.run_id == run_id,
                ResearchWebEnrichmentTask.attempts >= ResearchWebEnrichmentTask.max_attempts,
                ResearchWebEnrichmentTask.status.in_(("pending_web", "failed_retryable", "running")),
                (ResearchWebEnrichmentTask.lease_until.is_(None)
                 | (ResearchWebEnrichmentTask.lease_until <= current_time)),
                *(
                    [ResearchWebEnrichmentTask.task_key.in_(set(task_keys))]
                    if task_keys is not None
                    else []
                ),
            )
        )
        for task in exhausted:
            task.status = "failed_final"
            task.lease_token = None
            task.lease_owner = None
            task.lease_until = None
            task.leased_at = None
            task.last_error_type = task.last_error_type or "retry_limit_reached"
            task.updated_at = current_time
            task.completed_at = current_time
    return leases


async def cancel_web_enrichment_task(
    async_sessionmaker_: async_sessionmaker,
    task_key: str,
    *,
    reason: str = "cancelled_by_operator",
    now: datetime | None = None,
) -> bool:
    """Invalidate a lease so a response in flight cannot be committed."""
    current_time = _now(now)
    async with async_sessionmaker_() as session, session.begin():
        task = await session.scalar(
            select(ResearchWebEnrichmentTask)
            .where(ResearchWebEnrichmentTask.task_key == task_key)
            .with_for_update()
        )
        if task is None or task.status in {"completed", "cancelled"}:
            return False
        task.status = "cancelled"
        task.last_error_type = "cancelled"
        task.last_error = _safe_reason(reason)
        task.lease_token = None
        task.lease_owner = None
        task.lease_until = None
        task.leased_at = None
        task.updated_at = current_time
        task.completed_at = current_time
        return True


async def resume_web_enrichment_tasks(
    async_sessionmaker_: async_sessionmaker,
    run_id: str,
    *,
    statuses: Sequence[str] = ("blocked", "needs_login", "failed_final"),
    target_ids: Sequence[int] | None = None,
    task_keys: Sequence[str] | None = None,
    now: datetime | None = None,
) -> int:
    """Explicitly release operator-resolved tasks back to the bounded retry queue."""
    allowed = {"blocked", "needs_login", "failed_final"}
    if not statuses or set(statuses) - allowed:
        raise ValueError(f"Only these statuses can be explicitly resumed: {', '.join(sorted(allowed))}.")
    current_time = _now(now)
    if task_keys is not None and not task_keys:
        return 0
    ids = None if target_ids is None else {_positive_target_id(value) for value in target_ids}
    async with async_sessionmaker_() as session, session.begin():
        statement = select(ResearchWebEnrichmentTask).where(
            ResearchWebEnrichmentTask.run_id == run_id,
            ResearchWebEnrichmentTask.status.in_(tuple(statuses)),
        )
        if ids is not None:
            statement = statement.where(ResearchWebEnrichmentTask.target_id.in_(ids))
        if task_keys is not None:
            statement = statement.where(ResearchWebEnrichmentTask.task_key.in_(set(task_keys)))
        rows = (await session.scalars(statement.with_for_update())).all()
        for task in rows:
            task.status = "pending_web"
            task.attempts = 0
            task.next_retry_at = None
            task.last_error_type = None
            task.last_error = None
            task.lease_token = None
            task.lease_owner = None
            task.lease_until = None
            task.leased_at = None
            task.completed_at = None
            task.updated_at = current_time
    return len(rows)


async def _record_failure_audit(
    async_sessionmaker_: async_sessionmaker,
    task_key: str,
    lease_generation: int,
    status: str,
    reason_code: str,
    occurred_at: datetime,
    identity_evidence_json: dict[str, Any] | None = None,
) -> None:
    """Persist privacy-safe audit data after the task transaction rolled back."""
    async with async_sessionmaker_() as session, session.begin():
        session.add(
            ResearchWebEnrichmentFailure(
                failure_id=str(uuid4()),
                task_key=task_key,
                lease_generation=lease_generation,
                status=status,
                reason_code=_safe_reason(reason_code),
                occurred_at=occurred_at,
                identity_evidence_json=identity_evidence_json,
            )
        )


def _safe_reason(value: str) -> str:
    if not isinstance(value, str):
        return "unspecified"
    safe = "".join(char for char in value.strip() if char.isalnum() or char in "_-.:")[:128]
    return safe or "unspecified"


__all__ = [
    "EnqueueSummary",
    "LEGACY_WEB_FIELD_TYPES",
    "WEB_FIELD_TYPES",
    "WebEnrichmentTaskLease",
    "cancel_web_enrichment_task",
    "enqueue_web_enrichment_tasks",
    "lease_web_enrichment_tasks",
    "plan_web_enrichment_targets",
    "resume_web_enrichment_tasks",
]
