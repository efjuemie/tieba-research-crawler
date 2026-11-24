"""Preview or apply the one-time migration of resolved web enrichment tasks."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import operator
import os
import re
import sys
import uuid
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from sqlalchemy import func, or_, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import load_only

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.config import Config  # noqa: E402
from src.core.models import ResearchCheckpoint, ResearchRun, ResearchTaskScope  # noqa: E402
from src.research.models import (  # noqa: E402
    ResearchComment,
    ResearchContentObservation,
    ResearchFieldObservation,
    ResearchPost,
    ResearchThread,
    ResearchWebEnrichmentAttempt,
    ResearchWebEnrichmentTask,
)
from src.research.web_fallback.service import _web_task_matches_current_scope  # noqa: E402
from src.research.web_fallback.tasks import (  # noqa: E402
    _field_outcome_resolved,
    _normalize_requested_fields,
    _resolved_web_task_status,
    _unknown_canonical_value,
)

RUN_ID = "0e8e4ba2-6e13-443d-a2a5-19c6a2507fd9"
SOURCE_STATUS = "failed_retryable"
SOURCE_ERROR = "partial_fields_unresolved"
TARGET_STATUS = "completed_with_absent_fields"
GROWTH_FIELDS = frozenset({"global_growth_icon_url", "global_growth_level"})
GROWTH_NEEDS_EVIDENCE_REASONS = frozenset(
    {"icon_semantics_unverified", "numeric_level_not_exposed_in_page"}
)
ABSENT_REASONS = {
    "ip_location_text": frozenset({"ip_location_not_present", "ip_location_empty_in_response"}),
    "global_growth_icon_url": frozenset({"global_growth_icon_not_present"}),
    "global_growth_level": frozenset(
        {"global_growth_icon_not_present", "global_growth_level_zero_is_unknown"}
    ),
}
TARGET_MODELS = {
    "thread": (ResearchThread, ResearchThread.tid),
    "post": (ResearchPost, ResearchPost.pid),
    "comment": (ResearchComment, ResearchComment.cid),
}
BASE_TASK_COLUMNS = (
    ResearchWebEnrichmentTask.task_key,
    ResearchWebEnrichmentTask.run_id,
    ResearchWebEnrichmentTask.target_type,
    ResearchWebEnrichmentTask.target_id,
    ResearchWebEnrichmentTask.tid,
    ResearchWebEnrichmentTask.pid,
    ResearchWebEnrichmentTask.cid,
    ResearchWebEnrichmentTask.parent_post_pid,
    ResearchWebEnrichmentTask.author_user_id,
    ResearchWebEnrichmentTask.author_portrait,
    ResearchWebEnrichmentTask.fid,
    ResearchWebEnrichmentTask.requested_fields_json,
    ResearchWebEnrichmentTask.field_statuses_json,
    ResearchWebEnrichmentTask.target_observed_at,
    ResearchWebEnrichmentTask.scope_generation,
    ResearchWebEnrichmentTask.parent_scope_generation,
    ResearchWebEnrichmentTask.status,
    ResearchWebEnrichmentTask.attempts,
    ResearchWebEnrichmentTask.last_error_type,
    ResearchWebEnrichmentTask.last_error,
    ResearchWebEnrichmentTask.next_retry_at,
    ResearchWebEnrichmentTask.lease_token,
    ResearchWebEnrichmentTask.lease_owner,
    ResearchWebEnrichmentTask.lease_until,
    ResearchWebEnrichmentTask.leased_at,
    ResearchWebEnrichmentTask.lease_generation,
    ResearchWebEnrichmentTask.identity_verified,
    ResearchWebEnrichmentTask.identity_reason,
    ResearchWebEnrichmentTask.updated_at,
    ResearchWebEnrichmentTask.completed_at,
)
SCOPE_COLUMNS = (
    ResearchTaskScope.run_id,
    ResearchTaskScope.logical_task_key,
    ResearchTaskScope.scope_type,
    ResearchTaskScope.fid,
    ResearchTaskScope.tid,
    ResearchTaskScope.parent_pid,
    ResearchTaskScope.parent_logical_task_key,
    ResearchTaskScope.generation,
    ResearchTaskScope.parent_generation,
)
ATTEMPT_COLUMNS = (
    ResearchWebEnrichmentAttempt.task_key,
    ResearchWebEnrichmentAttempt.lease_generation,
    ResearchWebEnrichmentAttempt.started_at,
    ResearchWebEnrichmentAttempt.finished_at,
    ResearchWebEnrichmentAttempt.status,
)


class MigrationRejectedError(RuntimeError):
    pass


@dataclass(frozen=True)
class Evaluation:
    group: str | None
    reason: str | None
    category: str | None
    payload: dict[str, Any] | None
    sample: dict[str, Any]


def _utc(value: Any) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        return None
    return value.astimezone(UTC)


def _iso(value: Any) -> str | None:
    parsed = _utc(value)
    return parsed.isoformat() if parsed is not None else None


def _reason_code(value: Any) -> str:
    return value if isinstance(value, str) and re.fullmatch(r"[a-z0-9_]{0,128}", value) else "invalid_reason_code"


def _json_hash(value: Any) -> str:
    serialized = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _identity(task: Any, row: Any | None) -> dict[str, Any]:
    output = {
        "target_type": task.target_type,
        "target_id": task.target_id,
        "tid": task.tid,
        "pid": task.pid,
        "cid": task.cid,
        "parent_post_pid": task.parent_post_pid,
        "author_user_id": task.author_user_id,
        "fid": task.fid,
        "scope_generation": task.scope_generation,
        "parent_scope_generation": task.parent_scope_generation,
        "task_observed_at": _iso(task.target_observed_at),
    }
    if row is not None:
        canonical_target_id = {
            "thread": getattr(row, "tid", None),
            "post": getattr(row, "pid", None),
            "comment": getattr(row, "cid", None),
        }.get(task.target_type)
        output["canonical_identity"] = {
            "tid": getattr(row, "tid", None),
            "target_id": canonical_target_id,
            "pid": getattr(row, "pid", None),
            "cid": getattr(row, "cid", None),
            "parent_post_pid": getattr(row, "parent_post_pid", None),
            "author_user_id": getattr(row, "author_user_id", None),
            "fid": getattr(row, "fid", None),
            "observed_at": _iso(getattr(row, "observed_at", None)),
            "author_portrait_sha256": _json_hash(getattr(row, "author_portrait", None)),
        }
    return output


def _same_target_identity(task: Any, row: Any | None) -> bool:
    positive_ids = (task.target_id, task.tid, task.author_user_id, task.fid)
    if any(type(value) is not int or value <= 0 for value in positive_ids):
        return False
    if row is None or type(row.fid) is not int or row.fid <= 0 or task.fid != row.fid:
        return False
    if type(row.author_user_id) is not int or row.author_user_id <= 0 or task.author_user_id != row.author_user_id:
        return False
    if task.author_portrait != row.author_portrait or _utc(task.target_observed_at) is None:
        return False
    if _utc(task.target_observed_at) != _utc(row.observed_at):
        return False
    if task.target_type == "thread":
        return (
            type(row.tid) is int
            and row.tid > 0
            and task.target_id == task.tid == row.tid
            and task.pid is None
            and task.cid is None
            and task.parent_post_pid is None
        )
    if task.target_type == "post":
        return (
            type(task.pid) is int
            and task.pid > 0
            and type(row.pid) is int
            and row.pid > 0
            and task.target_id == task.pid == row.pid
            and task.tid == row.tid
            and task.cid is None
            and task.parent_post_pid is None
        )
    if task.target_type == "comment":
        return (
            type(task.cid) is int
            and task.cid > 0
            and type(task.parent_post_pid) is int
            and task.parent_post_pid > 0
            and type(row.cid) is int
            and row.cid > 0
            and type(row.parent_post_pid) is int
            and row.parent_post_pid > 0
            and task.target_id == task.cid == row.cid
            and task.tid == row.tid
            and task.pid is None
            and task.parent_post_pid == row.parent_post_pid
        )
    return False


def _scope_snapshot(task: Any, scopes: dict[tuple[str, str], Any]) -> dict[str, Any]:
    thread_key = f"thread_replies:{task.tid}"
    thread = scopes.get((task.run_id, thread_key))
    result: dict[str, Any] = {
        "thread": None
        if thread is None
        else {
            "tid": thread.tid,
            "fid": thread.fid,
            "generation": thread.generation,
        },
        "comment": None,
    }
    if task.target_type == "comment":
        comment_key = f"post_comments:{task.tid}:{task.parent_post_pid}"
        comment = scopes.get((task.run_id, comment_key))
        result["comment"] = (
            None
            if comment is None
            else {
                "tid": comment.tid,
                "fid": comment.fid,
                "parent_pid": comment.parent_pid,
                "generation": comment.generation,
                "parent_generation": comment.parent_generation,
            }
        )
    return result


def _scope_matches(task: Any, scopes: dict[tuple[str, str], Any]) -> bool:
    if (
        type(task.scope_generation) is not int
        or task.scope_generation < 0
        or type(task.fid) is not int
        or task.fid <= 0
    ):
        return False
    if task.target_type == "comment" and (
        type(task.parent_scope_generation) is not int or task.parent_scope_generation < 0
    ):
        return False
    if task.target_type != "comment" and task.parent_scope_generation is not None:
        return False
    if not _web_task_matches_current_scope(task, scopes):
        return False
    thread = scopes.get((task.run_id, f"thread_replies:{task.tid}"))
    if thread is None or thread.tid != task.tid or thread.fid != task.fid:
        return False
    if task.target_type == "comment":
        comment = scopes.get((task.run_id, f"post_comments:{task.tid}:{task.parent_post_pid}"))
        return bool(
            comment is not None
            and comment.tid == task.tid
            and comment.fid == task.fid
            and comment.parent_pid == task.parent_post_pid
            and comment.parent_logical_task_key == f"thread_replies:{task.tid}"
        )
    return True


def _field_evidence(
    task: Any,
    row: Any,
    attempt: Any,
) -> tuple[str | None, dict[str, Any], dict[str, str], str | None]:
    try:
        requested = _normalize_requested_fields(task.requested_fields_json, task.target_type)
    except (TypeError, ValueError):
        return "requested_fields_invalid", {}, {}, None
    if not isinstance(task.requested_fields_json, (list, tuple)) or len(requested) != len(task.requested_fields_json):
        return "requested_fields_invalid", {}, {}, None
    outcomes = task.field_statuses_json
    sources = row.field_sources_json if isinstance(row.field_sources_json, dict) else {}
    missing = row.missing_reasons_json if isinstance(row.missing_reasons_json, dict) else {}
    started = _utc(attempt.started_at)
    finished = _utc(attempt.finished_at)
    evidence: dict[str, Any] = {}
    canonical_hashes: dict[str, str] = {}
    has_resolved_absence = False
    if not isinstance(outcomes, dict) or started is None or finished is None or finished < started:
        return "latest_attempt_or_field_status_invalid", evidence, canonical_hashes, None

    for field_name in requested:
        outcome = outcomes.get(field_name)
        if not isinstance(outcome, Mapping):
            return "field_evidence_not_terminal", evidence, canonical_hashes, None
        status = outcome.get("status")
        reason = outcome.get("reason")
        if not isinstance(status, str) or not isinstance(reason, str):
            return "field_evidence_not_terminal", evidence, canonical_hashes, None
        source = sources.get(field_name)
        web_check = source.get("web_check") if isinstance(source, Mapping) else None
        checked_at = _utc(web_check.get("checked_at")) if isinstance(web_check, Mapping) else None
        in_attempt = checked_at is not None and started <= checked_at <= finished
        web_matches = bool(
            isinstance(web_check, Mapping)
            and web_check.get("task_key") == task.task_key
            and web_check.get("status") == status
            and web_check.get("reason") == reason
        )
        canonical_value = getattr(row, field_name, None)
        canonical_known = not _unknown_canonical_value(canonical_value, field_name, missing)
        canonical_hashes[field_name] = _json_hash(
            {"type": type(canonical_value).__name__, "value": canonical_value}
        )
        evidence[field_name] = {
            "status": status,
            "reason": _reason_code(reason),
            "web_check_task_key_matches": bool(
                isinstance(web_check, Mapping) and web_check.get("task_key") == task.task_key
            ),
            "web_check_status_matches": bool(
                isinstance(web_check, Mapping) and web_check.get("status") == status
            ),
            "web_check_reason_matches": bool(
                isinstance(web_check, Mapping) and web_check.get("reason") == reason
            ),
            "web_check_checked_at": checked_at.isoformat() if checked_at is not None else None,
            "checked_at_in_attempt": in_attempt,
            "canonical_known": canonical_known,
        }

        if not web_matches:
            return "web_check_mismatch", evidence, canonical_hashes, None
        if not in_attempt:
            return "web_check_outside_latest_attempt", evidence, canonical_hashes, None
        if not _field_outcome_resolved(field_name, dict(outcome)):
            return "field_evidence_not_terminal", evidence, canonical_hashes, None
        if status == "absent_on_page":
            if reason not in ABSENT_REASONS.get(field_name, frozenset()):
                return "absent_reason_untrusted", evidence, canonical_hashes, None
            has_resolved_absence = True
        elif status in {"applied", "confirmed"}:
            if not canonical_known:
                return "applied_canonical_value_unknown", evidence, canonical_hashes, None
        elif field_name in GROWTH_FIELDS and status == "needs_evidence":
            if reason not in GROWTH_NEEDS_EVIDENCE_REASONS:
                return "growth_evidence_ambiguous", evidence, canonical_hashes, None
            has_resolved_absence = True
        else:
            return "field_evidence_not_terminal", evidence, canonical_hashes, None

    terminal_status = _resolved_web_task_status(requested, outcomes, identity_verified=True)
    if not has_resolved_absence or terminal_status != TARGET_STATUS:
        return "terminal_status_not_completed_with_absent_fields", evidence, canonical_hashes, None
    category = (
        "growth_needs_evidence"
        if any(
            outcomes[name].get("status") == "needs_evidence" for name in requested
        )
        else "absent_only"
        if all(outcomes[name].get("status") == "absent_on_page" for name in requested)
        else "mixed_resolved"
    )
    return None, evidence, canonical_hashes, category


def _old_values(task: Any) -> dict[str, Any]:
    return {
        "task_key": task.task_key,
        "run_id": task.run_id,
        "status": task.status,
        "last_error_type": task.last_error_type,
        "last_error": task.last_error,
        "next_retry_at": _iso(task.next_retry_at),
        "completed_at": _iso(task.completed_at),
        "updated_at": _iso(task.updated_at),
        "attempts": task.attempts,
        "field_statuses_sha256": _json_hash(task.field_statuses_json),
    }


def _evaluate_task(
    task: Any,
    row: Any | None,
    scopes: dict[tuple[str, str], Any],
    attempts: list[Any],
) -> Evaluation:
    identity = _identity(task, row)
    scope_data = _scope_snapshot(task, scopes)
    base_sample = {
        "task_key": task.task_key,
        "target_type": task.target_type,
        "identity": identity,
        "scope": scope_data,
        "identity_verified": task.identity_verified is True,
        "lease_generation": task.lease_generation,
        "requested_fields": list(task.requested_fields_json or ()),
    }

    def reject(group: str, reason: str, field_evidence: dict[str, Any] | None = None) -> Evaluation:
        sample = dict(base_sample)
        sample["exclusion_reason"] = reason
        if field_evidence is not None:
            sample["fields"] = field_evidence
        return Evaluation(group, reason, None, None, sample)

    if task.identity_verified is not True or task.identity_reason is not None:
        return reject("identity_scope_snapshot_mismatch", "task_identity_not_verified")
    if not _same_target_identity(task, row):
        return reject("identity_scope_snapshot_mismatch", "canonical_target_identity_or_snapshot_mismatch")
    if not _scope_matches(task, scopes):
        return reject("identity_scope_snapshot_mismatch", "current_scope_or_generation_mismatch")
    if any(
        value is not None
        for value in (task.lease_token, task.lease_owner, task.lease_until, task.leased_at)
    ):
        return reject("field_evidence_or_attempt_insufficient", "task_lease_fields_not_clear")
    current_attempts = [item for item in attempts if item.lease_generation == task.lease_generation]
    latest = max(attempts, key=lambda item: item.lease_generation, default=None)
    if (
        latest is None
        or latest.lease_generation != task.lease_generation
        or len(current_attempts) != 1
        or latest.finished_at is None
        or latest.status == "running"
    ):
        return reject("field_evidence_or_attempt_insufficient", "latest_attempt_not_current_and_finished")

    reason, evidence, canonical_hashes, category = _field_evidence(task, row, latest)
    if reason is not None:
        return reject("field_evidence_or_attempt_insufficient", reason, evidence)

    old_values = _old_values(task)
    payload = {
        "task_key": task.task_key,
        "run_id": task.run_id,
        "target_type": task.target_type,
        "identity": identity,
        "scope": scope_data,
        "lease_generation": task.lease_generation,
        "attempt": {
            "lease_generation": latest.lease_generation,
            "status": latest.status,
            "started_at": _iso(latest.started_at),
            "finished_at": _iso(latest.finished_at),
        },
        "field_evidence": evidence,
        "canonical_field_hashes": canonical_hashes,
        "field_statuses_sha256": _json_hash(task.field_statuses_json),
        "old_values": old_values,
    }
    sample = dict(base_sample)
    sample.update(
        {
            "candidate_category": category,
            "attempt": payload["attempt"],
            "fields": evidence,
        }
    )
    return Evaluation(None, None, category, payload, sample)


def _candidate_digest(candidates: list[dict[str, Any]]) -> str:
    records = sorted(candidates, key=operator.itemgetter("task_key"))
    return _json_hash(records)


def _target_select_columns(model: Any, tasks: list[Any]) -> tuple[Any, ...]:
    names = [
        "tid",
        "fid",
        "author_user_id",
        "author_portrait",
        "observed_at",
        "field_sources_json",
        "missing_reasons_json",
    ]
    if model is ResearchThread:
        names.append("tid")
    elif model is ResearchPost:
        names.append("pid")
    else:
        names.extend(("cid", "parent_post_pid"))
    for task in tasks:
        requested = task.requested_fields_json
        if isinstance(requested, list):
            names.extend(name for name in requested if isinstance(name, str) and hasattr(model, name))
    unique = list(dict.fromkeys(names))
    return tuple(getattr(model, name) for name in unique)


async def _load_snapshot(session: Any, *, lock: bool = False) -> dict[str, Any]:
    task_query = (
        select(ResearchWebEnrichmentTask)
        .options(load_only(*BASE_TASK_COLUMNS))
        .where(
            ResearchWebEnrichmentTask.run_id == RUN_ID,
            ResearchWebEnrichmentTask.status == SOURCE_STATUS,
            ResearchWebEnrichmentTask.last_error == SOURCE_ERROR,
        )
        .order_by(ResearchWebEnrichmentTask.task_key)
    )
    if lock:
        task_query = task_query.with_for_update(of=ResearchWebEnrichmentTask)
    task_rows = list((await session.scalars(task_query)).all())
    now = datetime.now(UTC)
    active_lease_count = await session.scalar(
        select(func.count())
        .select_from(ResearchWebEnrichmentTask)
        .where(
            ResearchWebEnrichmentTask.run_id == RUN_ID,
            ResearchWebEnrichmentTask.status == "running",
            or_(
                ResearchWebEnrichmentTask.lease_until.is_(None),
                ResearchWebEnrichmentTask.lease_until > now,
            ),
        )
    )

    scope_keys: set[str] = set()
    for task in task_rows:
        scope_keys.add(f"thread_replies:{task.tid}")
        if task.target_type == "comment" and task.parent_post_pid is not None:
            scope_keys.add(f"post_comments:{task.tid}:{task.parent_post_pid}")
    scopes_by_key: dict[tuple[str, str], Any] = {}
    if scope_keys:
        scope_query = select(ResearchTaskScope).options(load_only(*SCOPE_COLUMNS)).where(
            ResearchTaskScope.run_id == RUN_ID,
            ResearchTaskScope.logical_task_key.in_(scope_keys),
        )
        if lock:
            scope_query = scope_query.with_for_update(of=ResearchTaskScope)
        scope_rows = list((await session.scalars(scope_query)).all())
        scopes_by_key = {(row.run_id, row.logical_task_key): row for row in scope_rows}

    task_keys = [task.task_key for task in task_rows]
    attempts_by_task: dict[str, list[Any]] = defaultdict(list)
    if task_keys:
        attempt_query = select(ResearchWebEnrichmentAttempt).options(load_only(*ATTEMPT_COLUMNS)).where(
            ResearchWebEnrichmentAttempt.task_key.in_(task_keys)
        )
        if lock:
            attempt_query = attempt_query.with_for_update(of=ResearchWebEnrichmentAttempt)
        for attempt in (await session.scalars(attempt_query)).all():
            attempts_by_task[attempt.task_key].append(attempt)

    targets_by_type: dict[str, dict[int, Any]] = {name: {} for name in TARGET_MODELS}
    for target_type, (model, id_column) in TARGET_MODELS.items():
        group = [task for task in task_rows if task.target_type == target_type]
        target_ids = sorted({task.target_id for task in group})
        if not target_ids:
            continue
        target_query = select(*_target_select_columns(model, group)).where(id_column.in_(target_ids))
        if lock:
            target_query = target_query.with_for_update(of=model)
        result = await session.execute(target_query)
        targets_by_type[target_type] = {
            getattr(record, id_column.key): SimpleNamespace(**dict(record._mapping))
            for record in result.all()
        }

    candidates: list[dict[str, Any]] = []
    decisions: list[Evaluation] = []
    for task in task_rows:
        row = targets_by_type.get(task.target_type, {}).get(task.target_id)
        decision = _evaluate_task(task, row, scopes_by_key, attempts_by_task.get(task.task_key, []))
        decisions.append(decision)
        if decision.payload is not None:
            candidates.append(decision.payload)
        candidates.sort(key=operator.itemgetter("task_key"))
    return {
        "base_count": len(task_rows),
        "active_lease_count": int(active_lease_count or 0),
        "candidates": candidates,
        "decisions": decisions,
        "candidate_digest": _candidate_digest(candidates),
        "tasks": task_rows,
    }


def _report(snapshot: dict[str, Any]) -> dict[str, Any]:
    exclusions: Counter[str] = Counter()
    details: Counter[str] = Counter()
    candidate_categories: Counter[str] = Counter()
    samples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for decision in snapshot["decisions"]:
        if decision.payload is not None:
            category = decision.category or "unknown"
            candidate_categories[category] += 1
            if len(samples[category]) < 3:
                samples[category].append(decision.sample)
        else:
            group = decision.group or "unknown_exclusion"
            reason = decision.reason or "unknown"
            exclusions[group] += 1
            details[reason] += 1
            key = f"{group}:{reason}"
            if len(samples[key]) < 3:
                samples[key].append(decision.sample)
    return {
        "run_id": RUN_ID,
        "source_status": SOURCE_STATUS,
        "source_last_error": SOURCE_ERROR,
        "target_status": TARGET_STATUS,
        "base_count": snapshot["base_count"],
        "eligible_count": len(snapshot["candidates"]),
        "excluded_count": snapshot["base_count"] - len(snapshot["candidates"]),
        "candidate_categories": dict(sorted(candidate_categories.items())),
        "exclusion_groups": dict(sorted(exclusions.items())),
        "exclusion_reasons": dict(sorted(details.items())),
        "active_run_lease_count": snapshot["active_lease_count"],
        "candidate_digest": snapshot["candidate_digest"],
        "samples": dict(sorted(samples.items())),
    }


def _private_output_dir() -> Path:
    local_root = (ROOT / ".local").resolve()
    backup_root = (local_root / "backups").resolve()
    backup_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output = (backup_root / f"resolved_web_tasks_{stamp}_{uuid.uuid4().hex[:8]}").resolve()
    output.relative_to(backup_root)
    output.mkdir(parents=False, exist_ok=False)
    return output


def _write_preview(snapshot: dict[str, Any], report: dict[str, Any]) -> tuple[Path, Path]:
    output_dir = _private_output_dir()
    old_values_path = output_dir / "candidate_old_values.jsonl"
    header = {
        "kind": "manifest",
        "run_id": RUN_ID,
        "candidate_digest": snapshot["candidate_digest"],
        "candidate_count": len(snapshot["candidates"]),
    }
    with old_values_path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(header, sort_keys=True) + "\n")
        for payload in snapshot["candidates"]:
            stream.write(
                json.dumps({"kind": "candidate", "payload": payload}, sort_keys=True, ensure_ascii=False) + "\n"
            )
    report = dict(report)
    report["old_values_backup"] = str(old_values_path)
    report_path = output_dir / "dry_run_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return old_values_path, report_path


def _read_old_values_backup(path: Path, expected_digest: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    backup_root = (ROOT / ".local" / "backups").resolve()
    resolved = path.resolve()
    try:
        resolved.relative_to(backup_root)
    except ValueError as exc:
        raise MigrationRejectedError("old-values backup must be under .local/backups") from exc
    lines = resolved.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise MigrationRejectedError("old-values backup is empty")
    header = json.loads(lines[0])
    records = [json.loads(line) for line in lines[1:] if line.strip()]
    payloads = [record.get("payload") for record in records]
    if (
        header.get("kind") != "manifest"
        or header.get("run_id") != RUN_ID
        or header.get("candidate_digest") != expected_digest
        or header.get("candidate_count") != len(payloads)
        or any(not isinstance(payload, dict) for payload in payloads)
        or len({payload.get("task_key") for payload in payloads}) != len(payloads)
        or _candidate_digest(payloads) != expected_digest
    ):
        raise MigrationRejectedError("old-values backup does not match expected run/digest")
    return header, payloads


async def _active_lease_count(session: Any) -> int:
    now = datetime.now(UTC)
    result = await session.scalar(
        select(func.count())
        .select_from(ResearchWebEnrichmentTask)
        .where(
            ResearchWebEnrichmentTask.run_id == RUN_ID,
            ResearchWebEnrichmentTask.status == "running",
            or_(
                ResearchWebEnrichmentTask.lease_until.is_(None),
                ResearchWebEnrichmentTask.lease_until > now,
            ),
        )
    )
    return int(result or 0)


async def _table_counts(session: Any) -> dict[str, int]:
    models = {
        "research_run": ResearchRun,
        "research_task_scope": ResearchTaskScope,
        "research_web_enrichment_task": ResearchWebEnrichmentTask,
        "research_web_enrichment_attempt": ResearchWebEnrichmentAttempt,
        "research_thread": ResearchThread,
        "research_post": ResearchPost,
        "research_comment": ResearchComment,
        "research_checkpoint": ResearchCheckpoint,
        "research_content_observation": ResearchContentObservation,
        "research_field_observation": ResearchFieldObservation,
    }
    expressions = [
        select(func.count()).select_from(model).scalar_subquery().label(name)
        for name, model in models.items()
    ]
    row = (await session.execute(select(*expressions))).one()
    return {name: int(getattr(row, name)) for name in models}


async def _run_dry_run(sessions: Any) -> tuple[dict[str, Any], dict[str, Any], Path, Path]:
    async with sessions() as session:
        async with session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            snapshot = await _load_snapshot(session)
    report = _report(snapshot)
    backup_path, report_path = _write_preview(snapshot, report)
    return snapshot, report, backup_path, report_path


async def _apply(sessions: Any, *, expected_digest: str, backup_path: Path) -> dict[str, Any]:
    _header, backup_payloads = _read_old_values_backup(backup_path, expected_digest)
    now = datetime.now(UTC)
    async with sessions() as session:
        async with session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
            if await _active_lease_count(session):
                raise MigrationRejectedError("run has an active web-enrichment lease")
            snapshot = await _load_snapshot(session, lock=True)
            if snapshot["candidate_digest"] != expected_digest:
                raise MigrationRejectedError("current candidates differ from the audited digest")
            if len(snapshot["candidates"]) != len(backup_payloads):
                raise MigrationRejectedError("current candidate count differs from the old-values backup")
            if [item["task_key"] for item in snapshot["candidates"]] != [
                item["task_key"] for item in backup_payloads
            ]:
                raise MigrationRejectedError("current candidate task keys differ from the old-values backup")
            counts_before = await _table_counts(session)
            result = await session.execute(
                update(ResearchWebEnrichmentTask)
                .where(
                    ResearchWebEnrichmentTask.run_id == RUN_ID,
                    ResearchWebEnrichmentTask.task_key.in_([item["task_key"] for item in backup_payloads]),
                    ResearchWebEnrichmentTask.status == SOURCE_STATUS,
                    ResearchWebEnrichmentTask.last_error == SOURCE_ERROR,
                )
                .values(
                    status=TARGET_STATUS,
                    next_retry_at=None,
                    completed_at=now,
                    updated_at=now,
                )
                .execution_options(synchronize_session=False)
            )
            if result.rowcount != len(backup_payloads):
                raise MigrationRejectedError("updated row count differs from the audited candidate count")
            counts_after = await _table_counts(session)
            if counts_before != counts_after:
                raise MigrationRejectedError("non-task table counts changed during the migration transaction")

    async with sessions() as session:
        async with session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            post_snapshot = await _load_snapshot(session)
            post_counts = await _table_counts(session)
    if post_snapshot["candidates"]:
        raise MigrationRejectedError("post-apply preview still contains eligible candidates")
    return {
        "run_id": RUN_ID,
        "candidate_digest": expected_digest,
        "updated_count": len(backup_payloads),
        "target_status": TARGET_STATUS,
        "post_apply_eligible_count": len(post_snapshot["candidates"]),
        "post_apply_exclusion_groups": _report(post_snapshot)["exclusion_groups"],
        "old_values_backup": str(backup_path),
        "table_counts_before": counts_before,
        "table_counts_after_transaction": counts_after,
        "table_counts_after_commit": post_counts,
        "table_counts_unchanged": counts_before == counts_after == post_counts,
        "applied_at_utc": now.isoformat(),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-time strict migration for resolved web task rows.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply only with the audited digest and old-values backup.",
    )
    parser.add_argument("--expected-run-id")
    parser.add_argument("--expected-digest")
    parser.add_argument("--old-values-backup", type=Path)
    args = parser.parse_args()
    if args.apply:
        if args.expected_run_id != RUN_ID or not args.expected_digest or args.old_values_backup is None:
            parser.error("--apply requires the fixed --expected-run-id, --expected-digest, and --old-values-backup")
    elif args.expected_run_id or args.expected_digest or args.old_values_backup:
        parser.error("apply arguments require --apply")
    return args


async def _main(args: argparse.Namespace) -> int:
    os.chdir(ROOT)
    engine = create_async_engine(Config().database_url, echo=False)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        if args.apply:
            result = await _apply(
                sessions,
                expected_digest=args.expected_digest,
                backup_path=args.old_values_backup,
            )
            output_dir = args.old_values_backup.parent
            (output_dir / "apply_report.json").write_text(
                json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            print(json.dumps({"mode": "apply", **result}, sort_keys=True))
            return 0
        snapshot, report, backup_path, report_path = await _run_dry_run(sessions)
        summary = {
            "mode": "dry_run",
            "run_id": RUN_ID,
            "base_count": snapshot["base_count"],
            "eligible_count": len(snapshot["candidates"]),
            "excluded_count": snapshot["base_count"] - len(snapshot["candidates"]),
            "exclusion_groups": report["exclusion_groups"],
            "candidate_categories": report["candidate_categories"],
            "active_run_lease_count": snapshot["active_lease_count"],
            "candidate_digest": snapshot["candidate_digest"],
            "old_values_backup": str(backup_path),
            "report_path": str(report_path),
        }
        print(json.dumps(summary, sort_keys=True))
        return 0
    except MigrationRejectedError as exc:
        print(f"migration_rejected={str(exc)}")
        return 3
    except Exception as exc:
        print(f"migration_error_type={type(exc).__name__}")
        return 4
    finally:
        await engine.dispose()


def main() -> int:
    return asyncio.run(_main(_parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
