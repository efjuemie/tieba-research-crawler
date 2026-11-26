"""Durable, scope-gated web enrichment task lifecycle."""

from __future__ import annotations

import asyncio
import hashlib
import json
import operator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import and_, any_, bindparam, case, exists, select, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from tiebameow.models.orm import Thread
from tiebameow.utils.time_utils import SHANGHAI_TZ

from src.core.models import (
    ResearchRun,
    ResearchTaskScope,
    research_comments_task_key,
)
from src.research.models import (
    ResearchComment,
    ResearchContentObservation,
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

_ZERO_UNKNOWN_REASONS = frozenset(
    {
        "zero_presence_unverified",
        "zero_presence_unverified_in_parsed_object",
    }
)
_GROWTH_FIELDS = frozenset({"global_growth_level", "global_growth_icon_url"})
TIEBA_IP_LOCATION_LAUNCH_DATE = "2022-04-28"
TIEBA_IP_LOCATION_LAUNCH_TIME = datetime(2022, 4, 28, 0, 0, 0, tzinfo=SHANGHAI_TZ)
IP_BEFORE_LAUNCH_REASON = "before_ip_location_launch"
_RESOLVED_FIELD_STATUSES = frozenset(
    {"applied", "confirmed", "absent_on_page", "unsupported_by_source", "not_applicable"}
)
_RESOLVED_NEEDS_EVIDENCE_REASONS = frozenset(
    {"icon_semantics_unverified", "numeric_level_not_exposed_in_page"}
)


def _field_outcome_resolved(field_name: str, outcome: Any) -> bool:
    if not isinstance(outcome, dict):
        return False
    status = outcome.get("status")
    if status in _RESOLVED_FIELD_STATUSES:
        return True
    return (
        field_name in _GROWTH_FIELDS
        and status == "needs_evidence"
        and outcome.get("reason") in _RESOLVED_NEEDS_EVIDENCE_REASONS
    )


def _field_outcome_inheritable_without_value(field_name: str, outcome: Any) -> bool:
    if not isinstance(outcome, dict):
        return False
    if outcome.get("status") in {
        "absent_on_page",
        "unsupported_by_source",
        "not_applicable",
    }:
        return True
    return (
        field_name in _GROWTH_FIELDS
        and outcome.get("status") == "needs_evidence"
        and outcome.get("reason") in _RESOLVED_NEEDS_EVIDENCE_REASONS
    )


def _resolved_web_task_status(
    requested_fields: Sequence[str],
    field_statuses: Any,
    *,
    identity_verified: bool | None,
) -> str | None:
    if identity_verified is not True or not requested_fields or not isinstance(field_statuses, dict):
        return None
    outcomes = {name: field_statuses.get(name) for name in requested_fields}
    if any(not isinstance(outcome, dict) for outcome in outcomes.values()):
        return None
    growth_fields = set(requested_fields) & _GROWTH_FIELDS
    growth_positive = any(
        isinstance(outcomes[name], dict)
        and outcomes[name].get("status") in {"applied", "confirmed"}
        for name in growth_fields
    )
    if any(
        not _field_outcome_resolved(name, outcomes[name])
        for name in requested_fields
        if name not in _GROWTH_FIELDS
    ):
        return None
    if growth_fields and not (
        growth_fields == _GROWTH_FIELDS and growth_positive
    ) and any(not _field_outcome_resolved(name, outcomes[name]) for name in growth_fields):
        return None
    non_growth_positive = all(
        isinstance(outcomes[name], dict)
        and outcomes[name].get("status") in {"applied", "confirmed"}
        for name in requested_fields
        if name not in _GROWTH_FIELDS
    )
    if non_growth_positive and (not growth_fields or growth_positive):
        return "completed"
    statuses = [outcomes[name].get("status") for name in requested_fields]
    if all(status == "unsupported_by_source" for status in statuses):
        return "unsupported_by_source"
    if all(status == "not_applicable" for status in statuses):
        return "not_applicable"
    return "completed_with_absent_fields"


_CONTENT_MARKERS = {
    "thread": ("thread_first_post", "aiotieba.get_posts"),
    "post": ("post", "aiotieba.get_posts"),
    "comment": ("comment", "aiotieba.get_comments"),
}
_THREAD_LIST_METADATA_FIELDS = frozenset(
    {
        "author_user_name",
        "author_nickname",
        "author_portrait",
        "forum_level",
        "global_growth_level",
        "global_growth_icon_url",
        "is_bawu",
        "ip_location_text",
    }
)


def _membership_predicate(column: Any, values: Any, *, use_postgres_array: bool) -> Any:
    if use_postgres_array:
        return column == any_(bindparam(None, value=list(values), type_=ARRAY(column.type)))
    return column.in_(values)


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
    max_attempts: int


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


def _unknown_canonical_value(current: Any, field_name: str, missing_reasons: dict[str, Any]) -> bool:
    if current is None:
        return True
    if field_name == "global_growth_level" and isinstance(current, int) and not isinstance(current, bool):
        return current <= 0
    if field_name in {"global_growth_icon_url", "ip_location_text"} and isinstance(current, str):
        if not current.strip():
            return True
    if missing_reasons.get(field_name) not in _ZERO_UNKNOWN_REASONS:
        return False
    if isinstance(current, bool):
        return field_name == "is_bawu" and current is False
    return isinstance(current, int) and current == 0


def is_entity_before_ip_location_cutoff(
    row: Any, min_ip_create_time: datetime | None
) -> bool:
    """Return True when row.create_time is strictly earlier than ``min_ip_create_time``."""
    if min_ip_create_time is None:
        return False
    create_time = getattr(row, "create_time", None)
    if create_time is None:
        return False
    norm_create = (
        create_time.replace(tzinfo=SHANGHAI_TZ) if create_time.tzinfo is None else create_time
    )
    norm_cutoff = (
        min_ip_create_time.replace(tzinfo=SHANGHAI_TZ)
        if min_ip_create_time.tzinfo is None
        else min_ip_create_time
    )
    return norm_create.astimezone(UTC) < norm_cutoff.astimezone(UTC)


def _unknown_requested_fields(
    row: Any,
    fields: Sequence[str],
    *,
    pair_growth_fields: bool,
    min_ip_create_time: datetime | None = None,
) -> tuple[str, ...]:
    missing_reasons = dict(getattr(row, "missing_reasons_json", None) or {})
    output = [
        name
        for name in fields
        if name not in _GROWTH_FIELDS
        and _unknown_canonical_value(getattr(row, name, None), name, missing_reasons)
    ]
    if (
        min_ip_create_time is not None
        and "ip_location_text" in output
        and is_entity_before_ip_location_cutoff(row, min_ip_create_time)
    ):
        output = [name for name in output if name != "ip_location_text"]
    requested_growth = set(fields) & _GROWTH_FIELDS
    if requested_growth:
        growth_unknown = {
            name
            for name in (_GROWTH_FIELDS if pair_growth_fields else requested_growth)
            if _unknown_canonical_value(getattr(row, name, None), name, missing_reasons)
        }
        if pair_growth_fields and growth_unknown == _GROWTH_FIELDS:
            output.extend(sorted(_GROWTH_FIELDS))
        elif not pair_growth_fields:
            output.extend(sorted(growth_unknown))
    return tuple(sorted(set(output)))


def _content_marker_matches(
    row: Any,
    target_type: str,
    target_id: int,
    marker: ResearchContentObservation | None,
) -> bool:
    if marker is None:
        return False
    owner_type, expected_source = _CONTENT_MARKERS[target_type]
    return (
        marker.owner_type == owner_type
        and marker.owner_id == target_id
        and marker.tid == row.tid
        and marker.source_api == expected_source
        and getattr(row, "source_api", None) == expected_source
        and _now(marker.observed_at) == _now(row.observed_at)
    )


def _thread_list_metadata_matches(row: Any, tid: int) -> bool:
    return (
        row.tid == tid
        and getattr(row, "source_api", None) == "aiotieba.get_threads"
        and getattr(row, "author_user_id", None) is not None
        and row.author_user_id > 0
        and getattr(row, "fid", None) is not None
        and row.fid > 0
        and getattr(row, "observed_at", None) is not None
    )


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


def _enqueue_identity_lock_key(task: dict[str, Any]) -> int:
    identity = (
        task["run_id"],
        task["target_type"],
        task["target_id"],
        task["scope_generation"],
        task["parent_scope_generation"],
    )
    encoded = json.dumps(identity, separators=(",", ":"), ensure_ascii=True)
    return int.from_bytes(hashlib.sha256(encoded.encode("utf-8")).digest()[:8], "big", signed=True)


def _record_enqueue_failure(
    session,
    task: ResearchWebEnrichmentTask,
    *,
    status: str,
    reason: str,
    occurred_at: datetime,
) -> None:
    session.add(
        ResearchWebEnrichmentFailure(
            failure_id=str(uuid4()),
            task_key=task.task_key,
            lease_generation=task.lease_generation,
            status=status,
            reason_code=reason,
            occurred_at=occurred_at,
            identity_evidence_json=None,
        )
    )


async def _persist_enqueued_task(
    async_sessionmaker_: async_sessionmaker,
    task: dict[str, Any],
    *,
    dialect: str,
    now: datetime,
    only_missing_fields: bool,
) -> tuple[bool, bool, str]:
    """Persist one target under a short identity-scoped transaction."""
    insert_factory = _insert_factory(dialect)
    async with async_sessionmaker_() as session, session.begin():
        if dialect == "postgresql":
            await session.execute(
                text("SELECT pg_advisory_xact_lock(:lock_key)"),
                {"lock_key": _enqueue_identity_lock_key(task)},
            )

        existing_rows = (
            await session.scalars(
                select(ResearchWebEnrichmentTask)
                .where(
                    ResearchWebEnrichmentTask.run_id == task["run_id"],
                    ResearchWebEnrichmentTask.target_type == task["target_type"],
                    ResearchWebEnrichmentTask.target_id == task["target_id"],
                    ResearchWebEnrichmentTask.tid == task["tid"],
                    ResearchWebEnrichmentTask.author_user_id == task["author_user_id"],
                    ResearchWebEnrichmentTask.author_portrait.is_(None)
                    if task["author_portrait"] is None
                    else ResearchWebEnrichmentTask.author_portrait == task["author_portrait"],
                    ResearchWebEnrichmentTask.fid.is_(None)
                    if task["fid"] is None
                    else ResearchWebEnrichmentTask.fid == task["fid"],
                    ResearchWebEnrichmentTask.parent_post_pid.is_(None)
                    if task["parent_post_pid"] is None
                    else ResearchWebEnrichmentTask.parent_post_pid == task["parent_post_pid"],
                    ResearchWebEnrichmentTask.scope_generation == task["scope_generation"],
                    ResearchWebEnrichmentTask.parent_scope_generation.is_(None)
                    if task["parent_scope_generation"] is None
                    else ResearchWebEnrichmentTask.parent_scope_generation == task["parent_scope_generation"],
                    ResearchWebEnrichmentTask.target_observed_at == task["target_observed_at"],
                )
                .order_by(ResearchWebEnrichmentTask.created_at.desc(), ResearchWebEnrichmentTask.task_key)
                .with_for_update()
            )
        ).all()
        requested = set(task["requested_fields_json"])
        existing = next(
            (row for row in existing_rows if requested.issubset(set(row.requested_fields_json))),
            None,
        )
        if existing is None and existing_rows:
            inherited = dict(task["field_statuses_json"])
            for field_name in requested:
                if _field_outcome_resolved(field_name, inherited.get(field_name)):
                    continue
                for row in existing_rows:
                    if row.identity_verified is not True or field_name not in row.requested_fields_json:
                        continue
                    outcome = dict(row.field_statuses_json or {}).get(field_name)
                    if _field_outcome_inheritable_without_value(field_name, outcome):
                        inherited[field_name] = dict(outcome)
                        break
            if any(
                _field_outcome_inheritable_without_value(field_name, inherited.get(field_name))
                for field_name in requested
            ):
                task["field_statuses_json"] = inherited
                task["identity_verified"] = True
                terminal_status = _resolved_web_task_status(
                    task["requested_fields_json"],
                    inherited,
                    identity_verified=True,
                )
                if terminal_status is not None:
                    task["status"] = terminal_status
                    task["completed_at"] = now
        if existing is not None:
            planned_status = task["status"]
            if existing.task_key != task["task_key"]:
                task["task_key"] = existing.task_key
                task["requested_fields_json"] = list(existing.requested_fields_json)
            if planned_status == "pending_web" and existing.status == "pending_api_detail":
                existing.status = "pending_web"
            elif planned_status == "pending_web" and existing.status == "partially_completed":
                terminal_status = _resolved_web_task_status(
                    existing.requested_fields_json,
                    existing.field_statuses_json,
                    identity_verified=existing.identity_verified,
                )
                if terminal_status is not None:
                    existing.status = terminal_status
                    existing.completed_at = now
                    existing.next_retry_at = None
                    existing.updated_at = now
                elif existing.attempts < existing.max_attempts:
                    existing.status = "failed_retryable"
                    existing.next_retry_at = now + timedelta(seconds=3 if existing.attempts == 1 else 5)
                    existing.last_error_type = "failed_retryable"
                    existing.last_error = "partial_fields_unresolved"
                    existing.completed_at = None
                    _record_enqueue_failure(
                        session,
                        existing,
                        status="failed_retryable",
                        reason="partial_fields_unresolved",
                        occurred_at=now,
                    )
                else:
                    existing.status = "failed_final"
                    existing.last_error_type = "failed_final"
                    existing.last_error = "max_attempts_exhausted_after_partial"
                    existing.completed_at = now
                    existing.updated_at = now
                    _record_enqueue_failure(
                        session,
                        existing,
                        status="failed_final",
                        reason="max_attempts_exhausted_after_partial",
                        occurred_at=now,
                    )
            elif (
                planned_status == "pending_web"
                and existing.status == "completed"
                and only_missing_fields
            ):
                if existing.attempts < existing.max_attempts:
                    existing.status = "pending_web"
                    existing.completed_at = None
                else:
                    existing.status = "failed_final"
                    existing.last_error_type = "failed_final"
                    existing.last_error = "web_attempt_limit_exhausted_after_api_refresh"
                    existing.completed_at = now
                    existing.updated_at = now
                    _record_enqueue_failure(
                        session,
                        existing,
                        status="failed_final",
                        reason="web_attempt_limit_exhausted_after_api_refresh",
                        occurred_at=now,
                    )
            if existing.status == "pending_web":
                existing.updated_at = now
                existing.last_error_type = None
                existing.last_error = None
            task["status"] = existing.status
            return False, True, existing.task_key

        statement = insert_factory(ResearchWebEnrichmentTask).values(**task).on_conflict_do_nothing(
            index_elements=[ResearchWebEnrichmentTask.task_key]
        )
        result = await session.execute(statement)
        if result.rowcount == 1:
            return True, False, task["task_key"]

        existing = await session.scalar(
            select(ResearchWebEnrichmentTask)
            .where(ResearchWebEnrichmentTask.task_key == task["task_key"])
            .with_for_update()
        )
        if existing is not None and task["status"] == "pending_web":
            if existing.status == "pending_api_detail":
                existing.status = "pending_web"
            elif existing.status == "partially_completed":
                terminal_status = _resolved_web_task_status(
                    existing.requested_fields_json,
                    existing.field_statuses_json,
                    identity_verified=existing.identity_verified,
                )
                if terminal_status is not None:
                    existing.status = terminal_status
                    existing.completed_at = now
                    existing.next_retry_at = None
                    existing.updated_at = now
                elif existing.attempts < existing.max_attempts:
                    existing.status = "failed_retryable"
                    existing.next_retry_at = now + timedelta(seconds=3 if existing.attempts == 1 else 5)
                    existing.last_error_type = "failed_retryable"
                    existing.last_error = "partial_fields_unresolved"
                    existing.completed_at = None
                    _record_enqueue_failure(
                        session,
                        existing,
                        status="failed_retryable",
                        reason="partial_fields_unresolved",
                        occurred_at=now,
                    )
                else:
                    existing.status = "failed_final"
                    existing.last_error_type = "failed_final"
                    existing.last_error = "max_attempts_exhausted_after_partial"
                    existing.completed_at = now
                    existing.updated_at = now
                    _record_enqueue_failure(
                        session,
                        existing,
                        status="failed_final",
                        reason="max_attempts_exhausted_after_partial",
                        occurred_at=now,
                    )
            if existing.status == "pending_web":
                existing.updated_at = now
                existing.last_error_type = None
                existing.last_error = None
        if existing is not None:
            task["status"] = existing.status
        return False, True, task["task_key"]


def _is_noop_existing_web_task(
    existing_status: str,
    existing_error: str | None,
    existing_error_type: str | None,
    *,
    planned_status: str,
    only_missing_fields: bool,
) -> bool:
    if existing_status == "pending_web":
        return existing_error is None and existing_error_type is None
    if existing_status in {"pending_api_detail", "partially_completed"}:
        return planned_status != "pending_web"
    if existing_status == "completed":
        return not (planned_status == "pending_web" and only_missing_fields)
    return True


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
    only_missing_fields: bool = False,
    pair_growth_fields: bool = False,
    min_thread_create_time: datetime | None = None,
    min_ip_create_time: datetime | None = None,
) -> EnqueueSummary:
    """Create deduplicated field tasks only for content represented in this run.

    Incomplete detail scopes can be leased only when the exact stored target has
    a complete content snapshot marker or a verified list row requests author
    metadata; the run scope and its generation still fence the task. Optionally
    restrict requests to fields that remain unknown.
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
    if not isinstance(only_missing_fields, bool) or not isinstance(pair_growth_fields, bool):
        raise ValueError("only_missing_fields and pair_growth_fields must be booleans.")
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
        if ids is not None and not ids:
            return EnqueueSummary(run_id, 0, 0, 0, skipped, (), ())
        use_postgres_array = session.get_bind().dialect.name == "postgresql"

        required_scope_types = (
            ("thread_replies",) if target_type in {"thread", "post"} else ("thread_replies", "post_comments")
        )
        scoped_tids: set[int] | None = {tid_filter} if tid_filter is not None else None
        scoped_parent_pids: set[int] | None = None
        if scoped_tids is None and ids is not None and target_type is not None:
            if target_type == "thread":
                scoped_tids = set(ids)
            elif target_type == "post":
                post_tids_stmt = select(ResearchPost.tid).distinct().where(
                    _membership_predicate(ResearchPost.pid, ids, use_postgres_array=use_postgres_array)
                )
                if fid is not None:
                    post_tids_stmt = post_tids_stmt.where(ResearchPost.fid == fid)
                scoped_tids = {
                    int(value) for value in (await session.scalars(post_tids_stmt)).all() if value is not None
                }
            else:
                comment_pairs_stmt = select(ResearchComment.tid, ResearchComment.parent_post_pid).distinct().where(
                    _membership_predicate(ResearchComment.cid, ids, use_postgres_array=use_postgres_array)
                )
                if fid is not None:
                    comment_pairs_stmt = comment_pairs_stmt.where(ResearchComment.fid == fid)
                if parent_pid_filter is not None:
                    comment_pairs_stmt = comment_pairs_stmt.where(
                        ResearchComment.parent_post_pid == parent_pid_filter
                    )
                comment_pairs = (await session.execute(comment_pairs_stmt)).all()
                scoped_tids = {int(pair[0]) for pair in comment_pairs if pair[0] is not None}
                scoped_parent_pids = {int(pair[1]) for pair in comment_pairs if pair[1] is not None}

        if scoped_tids is not None and not scoped_tids:
            return EnqueueSummary(run_id, 0, 0, 0, skipped, (), ())
        if parent_pid_filter is not None:
            scoped_parent_pids = (
                {parent_pid_filter}
                if scoped_parent_pids is None
                else scoped_parent_pids & {parent_pid_filter}
            )
            if not scoped_parent_pids:
                return EnqueueSummary(run_id, 0, 0, 0, skipped, (), ())

        scope_filters: list[Any] = [ResearchTaskScope.run_id == run_id]
        if scoped_tids is not None:
            scope_filters.append(
                _membership_predicate(
                    ResearchTaskScope.tid,
                    sorted(scoped_tids),
                    use_postgres_array=use_postgres_array,
                )
            )
        if min_thread_create_time is not None:
            scope_filters.append(
                ~exists(
                    select(1).where(
                        Thread.tid == ResearchTaskScope.tid,
                        Thread.create_time.is_not(None),
                        Thread.create_time < min_thread_create_time,
                    )
                )
            )
        if required_scope_types == ("thread_replies",):
            scope_filters.append(ResearchTaskScope.scope_type == "thread_replies")
        elif scoped_parent_pids is not None:
            scope_filters.append(
                (ResearchTaskScope.scope_type == "thread_replies")
                | (
                    (ResearchTaskScope.scope_type == "post_comments")
                    & _membership_predicate(
                        ResearchTaskScope.parent_pid,
                        sorted(scoped_parent_pids),
                        use_postgres_array=use_postgres_array,
                    )
                )
            )
        else:
            scope_filters.append(ResearchTaskScope.scope_type.in_(required_scope_types))

        scope_rows = (
            await session.scalars(
                select(ResearchTaskScope)
                .where(*scope_filters)
                .order_by(ResearchTaskScope.logical_task_key)
            )
        ).all()
        thread_scopes: dict[int, ResearchTaskScope] = {}
        comment_scopes: dict[tuple[int, int], ResearchTaskScope] = {}
        for idx, scope in enumerate(scope_rows):
            if idx and idx % 500 == 0:
                await asyncio.sleep(0)
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
            if pair_growth_fields and set(applicable_fields) & _GROWTH_FIELDS:
                applicable_fields = tuple(sorted(set(applicable_fields) | _GROWTH_FIELDS))
            if not applicable_fields:
                continue
            if target_type is not None and set(requested) - _target_fields(current_type):
                raise ValueError(f"Some requested fields are unsupported for {current_type}.")
            valid_tids = sorted(thread_scopes)
            if not valid_tids:
                continue
            stmt = None
            if current_type == "thread":
                stmt = select(ResearchThread).where(
                    _membership_predicate(ResearchThread.tid, valid_tids, use_postgres_array=use_postgres_array)
                )
                if ids is not None:
                    stmt = stmt.where(
                        _membership_predicate(ResearchThread.tid, ids, use_postgres_array=use_postgres_array)
                    )
                if tid_filter is not None:
                    stmt = stmt.where(ResearchThread.tid == tid_filter)
                if fid is not None:
                    stmt = stmt.where(ResearchThread.fid == fid)
                stmt = stmt.order_by(ResearchThread.tid)
            elif current_type == "post":
                stmt = select(ResearchPost).where(
                    _membership_predicate(ResearchPost.tid, valid_tids, use_postgres_array=use_postgres_array)
                )
                if ids is not None:
                    stmt = stmt.where(
                        _membership_predicate(ResearchPost.pid, ids, use_postgres_array=use_postgres_array)
                    )
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
                if not possible_parent_scopes:
                    continue
                tids = sorted({scope.tid for scope in possible_parent_scopes})
                pids = sorted({scope.parent_pid for scope in possible_parent_scopes if scope.parent_pid is not None})
                stmt = select(ResearchComment).where(
                    _membership_predicate(ResearchComment.tid, tids, use_postgres_array=use_postgres_array),
                    _membership_predicate(
                        ResearchComment.parent_post_pid,
                        pids,
                        use_postgres_array=use_postgres_array,
                    ),
                )
                if ids is not None:
                    stmt = stmt.where(
                        _membership_predicate(ResearchComment.cid, ids, use_postgres_array=use_postgres_array)
                    )
                if tid_filter is not None:
                    stmt = stmt.where(ResearchComment.tid == tid_filter)
                if fid is not None:
                    stmt = stmt.where(ResearchComment.fid == fid)
                if parent_pid_filter is not None:
                    stmt = stmt.where(ResearchComment.parent_post_pid == parent_pid_filter)
                stmt = stmt.order_by(ResearchComment.tid, ResearchComment.parent_post_pid, ResearchComment.cid)
            entities = (await session.scalars(stmt)).all()
            marker_owner_type = _CONTENT_MARKERS[current_type][0]
            entity_ids = [
                row.tid if current_type == "thread" else row.pid if current_type == "post" else row.cid
                for row in entities
            ]
            content_markers: dict[int, ResearchContentObservation] = {}
            if entity_ids:
                marker_rows = (
                    await session.scalars(
                        select(ResearchContentObservation).where(
                            ResearchContentObservation.owner_type == marker_owner_type,
                            _membership_predicate(
                                ResearchContentObservation.owner_id,
                                entity_ids,
                                use_postgres_array=use_postgres_array,
                            ),
                        )
                    )
                ).all()
                content_markers = {marker.owner_id: marker for marker in marker_rows}

            for idx, row in enumerate(entities):
                if idx and idx % 500 == 0:
                    await asyncio.sleep(0)
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
                field_names = (
                    _unknown_requested_fields(
                        row,
                        applicable_fields,
                        pair_growth_fields=pair_growth_fields,
                        min_ip_create_time=min_ip_create_time,
                    )
                    if only_missing_fields
                    else tuple(applicable_fields)
                )
                if not field_names:
                    if (
                        only_missing_fields
                        and min_ip_create_time is not None
                        and "ip_location_text" in applicable_fields
                        and is_entity_before_ip_location_cutoff(row, min_ip_create_time)
                        and _unknown_requested_fields(
                            row,
                            applicable_fields,
                            pair_growth_fields=pair_growth_fields,
                        )
                        == ("ip_location_text",)
                    ):
                        skipped["ip_location_before_mechanism_launch"] = (
                            skipped.get("ip_location_before_mechanism_launch", 0) + 1
                        )
                    else:
                        skipped["requested_fields_already_known"] = skipped.get(
                            "requested_fields_already_known", 0
                        ) + 1
                    continue
                marker = content_markers.get(target_id)
                marker_proves_target = _content_marker_matches(row, current_type, target_id, marker)
                list_thread_proves_target = (
                    current_type == "thread" and _thread_list_metadata_matches(row, target_tid)
                )
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
                ready = (
                    thread_scope.status == "completed"
                    and (parent_scope is None or parent_scope.status == "completed")
                ) or marker_proves_target or (
                    list_thread_proves_target
                    and current_type == "thread"
                    and set(field_names).issubset(_THREAD_LIST_METADATA_FIELDS)
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
        planned.sort(
            key=operator.itemgetter("target_type", "tid", "target_id", "task_key")
        )
        if limit is not None:
            planned = planned[:limit]

        if dry_run or not planned:
            plans = _planned_targets(planned)
            return EnqueueSummary(
                run_id,
                len(planned),
                0,
                0,
                skipped,
                tuple(task["task_key"] for task in planned),
                plans,
            )

        dialect = session.get_bind().dialect.name
    existing_by_key: dict[str, tuple[str, str | None, str | None]] = {}
    planned_keys = [task["task_key"] for task in planned]
    for chunk_start in range(0, len(planned_keys), 500):
        key_chunk = planned_keys[chunk_start : chunk_start + 500]
        async with async_sessionmaker_() as session:
            existing_rows = (
                await session.execute(
                    select(
                        ResearchWebEnrichmentTask.task_key,
                        ResearchWebEnrichmentTask.status,
                        ResearchWebEnrichmentTask.last_error,
                        ResearchWebEnrichmentTask.last_error_type,
                    ).where(
                        _membership_predicate(
                            ResearchWebEnrichmentTask.task_key,
                            key_chunk,
                            use_postgres_array=use_postgres_array,
                        )
                    )
                )
            ).all()
        for existing_key, existing_status, existing_error, existing_error_type in existing_rows:
            existing_by_key[existing_key] = (existing_status, existing_error, existing_error_type)
        if chunk_start + 500 < len(planned_keys):
            await asyncio.sleep(0)

    created = 0
    deduplicated = 0
    task_keys: list[str] = []
    for idx, task in enumerate(planned):
        existing_info = existing_by_key.get(task["task_key"])
        if existing_info is not None and _is_noop_existing_web_task(
            existing_info[0],
            existing_info[1],
            existing_info[2],
            planned_status=task["status"],
            only_missing_fields=only_missing_fields,
        ):
            task["status"] = existing_info[0]
            deduplicated += 1
            task_keys.append(task["task_key"])
            if (idx + 1) % 500 == 0:
                await asyncio.sleep(0)
            continue
        was_created, was_deduplicated, task_key = await _persist_enqueued_task(
            async_sessionmaker_,
            task,
            dialect=dialect,
            now=now,
            only_missing_fields=only_missing_fields,
        )
        created += was_created
        deduplicated += was_deduplicated
        task_keys.append(task_key)
        if (idx + 1) % 500 == 0:
            await asyncio.sleep(0)

    plans = _planned_targets(planned)

    return EnqueueSummary(run_id, len(planned), created, deduplicated, skipped, tuple(task_keys), plans)


def _planned_targets(planned: Sequence[dict[str, Any]]) -> tuple[WebEnrichmentTargetPlan, ...]:
    return tuple(
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
    lease_seconds: int = 240,
    task_keys: Sequence[str] | None = None,
    now: datetime | None = None,
    min_thread_create_time: datetime | None = None,
    newest_first: bool = False,
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
        if min_thread_create_time is not None:
            eligible = and_(
                eligible,
                ~exists(
                    select(1).where(
                        Thread.tid == ResearchWebEnrichmentTask.tid,
                        Thread.create_time.is_not(None),
                        Thread.create_time < min_thread_create_time,
                    )
                ),
            )
        if newest_first:
            target_type_rank = case(
                (ResearchWebEnrichmentTask.target_type == "thread", 0),
                (ResearchWebEnrichmentTask.target_type == "post", 1),
                (ResearchWebEnrichmentTask.target_type == "comment", 2),
                else_=3,
            )
            order_clauses: tuple[Any, ...] = (
                ResearchWebEnrichmentTask.attempts.asc(),
                ResearchWebEnrichmentTask.priority.desc(),
                ResearchWebEnrichmentTask.tid.desc(),
                target_type_rank.asc(),
                ResearchWebEnrichmentTask.parent_post_pid.asc(),
                ResearchWebEnrichmentTask.target_id.asc(),
                ResearchWebEnrichmentTask.created_at.asc(),
                ResearchWebEnrichmentTask.task_key.asc(),
            )
        else:
            order_clauses = (
                ResearchWebEnrichmentTask.attempts.asc(),
                ResearchWebEnrichmentTask.priority.desc(),
                ResearchWebEnrichmentTask.target_type,
                ResearchWebEnrichmentTask.tid,
                ResearchWebEnrichmentTask.parent_post_pid,
                ResearchWebEnrichmentTask.target_id,
                ResearchWebEnrichmentTask.created_at,
                ResearchWebEnrichmentTask.task_key,
            )
        result = await session.execute(
            select(ResearchWebEnrichmentTask)
            .where(eligible)
            .order_by(*order_clauses)
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
                    max_attempts=task.max_attempts,
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


async def prune_pre_launch_ip_web_tasks(
    async_sessionmaker_: async_sessionmaker,
    run_ids: Sequence[str],
    *,
    min_ip_create_time: datetime | None,
    min_thread_create_time: datetime | None = None,
) -> dict[str, int]:
    """Close pre-launch IP-only WEB tasks as ``not_applicable`` and annotate mixed ones."""
    counts = {"marked_not_applicable": 0, "annotated_mixed": 0}
    if min_ip_create_time is None or not run_ids:
        return counts
    stable_run_ids = tuple(dict.fromkeys(run_ids))
    cutoff_utc = _now(min_ip_create_time)
    now = datetime.now(UTC)
    models = (
        ("thread", ResearchThread, ResearchThread.tid),
        ("post", ResearchPost, ResearchPost.pid),
        ("comment", ResearchComment, ResearchComment.cid),
    )

    for target_type, model, id_col in models:
        async with async_sessionmaker_() as session:
            dialect = session.get_bind().dialect.name
            stmt = (
                select(ResearchWebEnrichmentTask.task_key, model.create_time)
                .join(model, id_col == ResearchWebEnrichmentTask.target_id)
                .where(
                    ResearchWebEnrichmentTask.run_id.in_(stable_run_ids),
                    ResearchWebEnrichmentTask.target_type == target_type,
                    ResearchWebEnrichmentTask.status.in_(
                        ("pending_web", "failed_retryable", "pending_api_detail")
                    ),
                    model.create_time.is_not(None),
                )
            )
            if dialect == "postgresql":
                stmt = stmt.where(model.create_time < cutoff_utc)
            if min_thread_create_time is not None:
                stmt = stmt.where(
                    ~exists(
                        select(1).where(
                            Thread.tid == ResearchWebEnrichmentTask.tid,
                            Thread.create_time.is_not(None),
                            Thread.create_time < min_thread_create_time,
                        )
                    )
                )
            rows = (await session.execute(stmt)).all()

        matching_keys = [
            task_key
            for task_key, create_time in rows
            if is_entity_before_ip_location_cutoff(
                type("_Row", (), {"create_time": create_time})(), cutoff_utc
            )
        ]
        for chunk_start in range(0, len(matching_keys), 500):
            chunk = matching_keys[chunk_start : chunk_start + 500]
            async with async_sessionmaker_() as session, session.begin():
                tasks = (
                    await session.scalars(
                        select(ResearchWebEnrichmentTask)
                        .where(ResearchWebEnrichmentTask.task_key.in_(chunk))
                        .with_for_update(skip_locked=True)
                    )
                ).all()
                for task in tasks:
                    requested = list(task.requested_fields_json or ())
                    if "ip_location_text" not in requested:
                        continue
                    if set(requested) == {"ip_location_text"}:
                        task.field_statuses_json = {
                            "ip_location_text": {
                                "status": "not_applicable",
                                "reason": IP_BEFORE_LAUNCH_REASON,
                            }
                        }
                        task.status = "not_applicable"
                        task.identity_verified = True
                        task.identity_reason = None
                        task.last_error = None
                        task.last_error_type = None
                        task.next_retry_at = None
                        task.completed_at = now
                        task.updated_at = now
                        counts["marked_not_applicable"] += 1
                    else:
                        statuses = dict(task.field_statuses_json or {})
                        current_ip = statuses.get("ip_location_text")
                        if not isinstance(current_ip, dict) or current_ip.get("status") != "not_applicable":
                            statuses["ip_location_text"] = {
                                "status": "not_applicable",
                                "reason": IP_BEFORE_LAUNCH_REASON,
                            }
                            task.field_statuses_json = statuses
                            task.updated_at = now
                            counts["annotated_mixed"] += 1
            if chunk_start + 500 < len(matching_keys):
                await asyncio.sleep(0)

    return counts


__all__ = [
    "EnqueueSummary",
    "IP_BEFORE_LAUNCH_REASON",
    "LEGACY_WEB_FIELD_TYPES",
    "TIEBA_IP_LOCATION_LAUNCH_DATE",
    "TIEBA_IP_LOCATION_LAUNCH_TIME",
    "WEB_FIELD_TYPES",
    "WebEnrichmentTaskLease",
    "cancel_web_enrichment_task",
    "enqueue_web_enrichment_tasks",
    "is_entity_before_ip_location_cutoff",
    "lease_web_enrichment_tasks",
    "plan_web_enrichment_targets",
    "prune_pre_launch_ip_web_tasks",
    "resume_web_enrichment_tasks",
]
