"""Atomic identity-checked application of web field candidates."""

from __future__ import annotations

import hashlib
import ipaddress
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.core.models import ResearchRun, ResearchTaskScope
from src.research.models import (
    ResearchComment,
    ResearchFieldObservation,
    ResearchPost,
    ResearchThread,
    ResearchWebEnrichmentAttempt,
    ResearchWebEnrichmentFailure,
    ResearchWebEnrichmentTask,
)

from .diagnostics import with_elapsed_seconds
from .identity import (
    IdentityError,
    ObservationCandidate,
    canonical_growth_icon_url,
    parse_positive_id,
)
from .tasks import LEGACY_WEB_FIELD_TYPES, WEB_FIELD_TYPES, _record_failure_audit, _safe_reason


class _ReconcileRejectedError(Exception):
    def __init__(self, status: str, reason_code: str, *, identity: dict[str, Any] | None = None):
        super().__init__(reason_code)
        self.status = status
        self.reason_code = reason_code
        self.identity = identity


@dataclass(frozen=True, slots=True)
class WebReconcileResult:
    task_key: str
    status: str
    applied_fields: tuple[str, ...] = ()
    confirmed_fields: tuple[str, ...] = ()
    conflict_fields: tuple[str, ...] = ()
    absent_fields: tuple[str, ...] = ()
    reason_code: str | None = None


_STATUS_BY_FIELD = frozenset(
    {
        "absent_on_page",
        "needs_evidence",
        "not_tested",
        "not_observed_in_bounded_page",
        "unsupported_by_source",
        "not_applicable",
        "blocked",
        "needs_login",
        "identity_unverified",
        "applied",
        "confirmed",
        "conflict",
        "approximate_not_applied",
        "audit_only",
        "pending",
        "failed_retryable",
        "failed_final",
    }
)
_ZERO_UNKNOWN_REASONS = frozenset(
    {
        "zero_presence_unverified",
        "zero_presence_unverified_in_parsed_object",
    }
)
_FID_PROOF_METHODS = frozenset(
    {
        "body_fid",
        "request_fid",
        "response_fid",
        "response_forum_id",
        "request_response_fid",
        "known_api_tid_fid_binding",
    }
)
_SAFE_QUERY_KEYS = frozenset({"tid", "thread_id", "kz", "fid", "pid", "post_id", "cid", "offset"})
_IDENTITY_MISMATCH_SUFFIXES = ("_mismatch", "_disagreement")


def _now(value: datetime | None) -> datetime:
    result = value or datetime.now(UTC)
    if result.tzinfo is None:
        return result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _insert_factory(dialect: str) -> Any:
    if dialect == "postgresql":
        return pg_insert
    if dialect == "sqlite":
        return sqlite_insert
    raise ValueError(f"Unsupported database dialect for web reconciliation: {dialect}.")


def _table_for_target(target_type: str):
    try:
        return {"thread": ResearchThread, "post": ResearchPost, "comment": ResearchComment}[target_type]
    except KeyError as exc:
        raise _ReconcileRejectedError("identity_unverified", "invalid_target_type") from exc


def _id_equal(actual: Any, expected: int | None) -> bool:
    if expected is None:
        return actual is None
    try:
        return parse_positive_id(actual) == str(expected)
    except IdentityError:
        return False


def _evidence_identity(evidence: Any) -> dict[str, Any]:
    return {
        "target_type": getattr(evidence, "target_type", None),
        "target_id": getattr(evidence, "target_id", None),
        "tid": getattr(evidence, "tid", None),
        "pid": getattr(evidence, "pid", None),
        "cid": getattr(evidence, "cid", None),
        "parent_post_pid": getattr(evidence, "parent_post_pid", None),
        "author_user_id": getattr(evidence, "author_user_id", None),
        "fid": getattr(evidence, "fid", None),
        "match_methods": list(getattr(evidence, "match_methods", ()) or ()),
        "context_id": getattr(evidence, "context_id", None),
        "request_offset": getattr(evidence, "request_offset", None),
        "identity_verified": bool(getattr(evidence, "identity_verified", False)),
    }


def _validate_candidate_identity(task: ResearchWebEnrichmentTask, candidate: ObservationCandidate) -> dict[str, Any]:
    evidence = candidate.identity
    identity = _evidence_identity(evidence)
    if not identity["identity_verified"]:
        raise _ReconcileRejectedError("identity_unverified", "candidate_identity_unverified", identity=identity)
    expected = {
        "target_type": task.target_type,
        "target_id": task.target_id,
        "tid": task.tid,
        "pid": task.pid,
        "cid": task.cid,
        "parent_post_pid": task.parent_post_pid,
        "author_user_id": task.author_user_id,
    }
    if identity["target_type"] != expected["target_type"]:
        raise _ReconcileRejectedError("identity_unverified", "candidate_target_type_mismatch", identity=identity)
    for name in ("target_id", "tid", "pid", "cid", "parent_post_pid", "author_user_id"):
        if not _id_equal(identity[name], expected[name]):
            raise _ReconcileRejectedError("identity_unverified", f"candidate_{name}_mismatch", identity=identity)
    if task.fid is not None:
        if not _id_equal(identity["fid"], task.fid):
            raise _ReconcileRejectedError("identity_unverified", "candidate_fid_mismatch", identity=identity)
        if not (_FID_PROOF_METHODS & set(identity["match_methods"])):
            raise _ReconcileRejectedError("identity_unverified", "candidate_fid_proof_missing", identity=identity)
    if not identity["match_methods"] or any(not isinstance(method, str) for method in identity["match_methods"]):
        raise _ReconcileRejectedError("identity_unverified", "candidate_identity_methods_missing", identity=identity)
    return identity


def _sanitize_source_url(task: ResearchWebEnrichmentTask, candidate: ObservationCandidate) -> str:
    try:
        parsed = urlsplit(candidate.source_url)
        port = parsed.port
    except (TypeError, ValueError):
        raise _ReconcileRejectedError("identity_unverified", "candidate_source_url_invalid") from None
    if (
        parsed.scheme.lower() != "https"
        or (parsed.hostname or "").lower() not in {"tieba.baidu.com", "www.tieba.baidu.com"}
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
    ):
        raise _ReconcileRejectedError("identity_unverified", "candidate_source_url_not_allowlisted")
    path = parsed.path.rstrip("/") or "/"
    if candidate.source_kind == "web_dom":
        if path != f"/p/{task.tid}":
            raise _ReconcileRejectedError("identity_unverified", "candidate_page_tid_mismatch")
    elif candidate.source_kind == "web_network":
        if path not in {"/c/f/pb/nestedFloor", "/c/f/pb/page_pc", "/c/u/pc/userCard"}:
            raise _ReconcileRejectedError("identity_unverified", "candidate_network_route_not_allowlisted")
    else:
        raise _ReconcileRejectedError("identity_unverified", "candidate_source_kind_invalid")

    filtered: list[tuple[str, str]] = []
    seen_ids: dict[str, set[str]] = defaultdict(set)
    expected_parent = task.pid if task.target_type == "post" else task.parent_post_pid
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered not in _SAFE_QUERY_KEYS:
            continue
        if lowered in {"tid", "thread_id", "kz", "fid", "pid", "post_id", "cid"}:
            try:
                canonical = parse_positive_id(value)
            except IdentityError:
                raise _ReconcileRejectedError("identity_unverified", f"candidate_query_{lowered}_invalid") from None
            aliases = {
                "tid": "tid",
                "thread_id": "tid",
                "kz": "tid",
                "fid": "fid",
                "pid": "pid",
                "post_id": "pid",
                "cid": "cid",
            }
            identity_key = aliases[lowered]
            seen_ids[identity_key].add(canonical)
            expected_value = {
                "tid": task.tid,
                "fid": task.fid,
                "pid": expected_parent,
                "cid": task.cid,
            }[identity_key]
            if expected_value is not None and canonical != str(expected_value):
                raise _ReconcileRejectedError("identity_unverified", f"candidate_query_{identity_key}_mismatch")
            filtered.append((lowered, canonical))
        elif lowered == "offset" and value.isdecimal() and len(value) <= 12:
            filtered.append((lowered, value))
    if any(len(values) > 1 for values in seen_ids.values()):
        raise _ReconcileRejectedError("identity_unverified", "candidate_source_url_duplicate_identity")
    return urlunsplit(("https", "tieba.baidu.com", path, urlencode(filtered), ""))


def _json_value(value: Any) -> Any:
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        return json.loads(encoded)
    except (TypeError, ValueError, OverflowError):
        raise _ReconcileRejectedError("failed_final", "candidate_value_not_json_serializable") from None


def _validate_candidate(task: ResearchWebEnrichmentTask, candidate: ObservationCandidate) -> dict[str, Any]:
    if not isinstance(candidate, ObservationCandidate):
        raise _ReconcileRejectedError("failed_final", "candidate_contract_invalid")
    if candidate.field_name not in tuple(task.requested_fields_json):
        raise _ReconcileRejectedError("failed_final", "candidate_field_not_requested")
    expected_type = WEB_FIELD_TYPES.get(candidate.field_name) or LEGACY_WEB_FIELD_TYPES.get(
        candidate.field_name
    )
    if expected_type is None:
        raise _ReconcileRejectedError("failed_final", "candidate_field_not_allowlisted")
    value = candidate.normalized_value
    if expected_type is int:
        valid_type = isinstance(value, int) and not isinstance(value, bool) and value >= 0
    elif expected_type is bool:
        valid_type = isinstance(value, bool)
    else:
        valid_type = isinstance(value, str) and bool(value.strip())
    if not valid_type:
        raise _ReconcileRejectedError("failed_final", "candidate_normalized_type_invalid")
    if candidate.field_name == "reply_to_user_id" and value <= 0:
        raise _ReconcileRejectedError("failed_final", "candidate_reply_to_user_id_invalid")
    if candidate.field_name == "global_growth_icon_url":
        if canonical_growth_icon_url(value) != value:
            raise _ReconcileRejectedError("failed_final", "candidate_growth_icon_url_invalid")
    if candidate.field_name == "reply_to_display_name":
        raw_evidence = candidate.raw_value
        if (
            candidate.source_kind != "web_network"
            or not isinstance(raw_evidence, Mapping)
            or set(raw_evidence) != {"mention_type", "mention_uid"}
            or raw_evidence.get("mention_type") != 4
        ):
            raise _ReconcileRejectedError("failed_final", "candidate_reply_to_mention_invalid")
        mention_uid = raw_evidence.get("mention_uid")
        if mention_uid is not None:
            try:
                parse_positive_id(mention_uid)
            except IdentityError:
                raise _ReconcileRejectedError("failed_final", "candidate_reply_to_mention_invalid") from None
    if candidate.field_name == "ip_location_text":
        try:
            ipaddress.ip_address(value.strip())
        except ValueError:
            pass
        else:
            raise _ReconcileRejectedError("failed_final", "candidate_ip_value_is_literal_address")
    if candidate.precision not in {"exact", "approximate"}:
        raise _ReconcileRejectedError("failed_final", "candidate_precision_invalid")
    if not isinstance(candidate.source_path, str) or not candidate.source_path or len(candidate.source_path) > 1024:
        raise _ReconcileRejectedError("failed_final", "candidate_source_path_invalid")
    if any(ord(char) < 32 for char in candidate.source_path):
        raise _ReconcileRejectedError("failed_final", "candidate_source_path_invalid")
    if (
        not isinstance(candidate.parser_version, str)
        or not candidate.parser_version
        or len(candidate.parser_version) > 128
    ):
        raise _ReconcileRejectedError("failed_final", "candidate_parser_version_invalid")
    identity = _validate_candidate_identity(task, candidate)
    sanitized_url = _sanitize_source_url(task, candidate)
    return {
        "candidate": candidate,
        "identity": identity,
        "source_url": sanitized_url,
        "raw_value": _json_value(candidate.raw_value),
        "normalized_value": _json_value(candidate.normalized_value),
        "observed_at": _now(candidate.observed_at),
    }


def _conditions(task: ResearchWebEnrichmentTask):
    model = _table_for_target(task.target_type)
    fid_condition = model.fid.is_(None) if task.fid is None else model.fid == task.fid
    conditions = [model.tid == task.tid, model.author_user_id == task.author_user_id, fid_condition]
    if task.target_type == "thread":
        conditions.append(model.tid == task.target_id)
    elif task.target_type == "post":
        conditions.append(model.pid == task.target_id)
    else:
        conditions.extend(
            (
                model.cid == task.target_id,
                model.parent_post_pid.is_(None)
                if task.parent_post_pid is None
                else model.parent_post_pid == task.parent_post_pid,
            )
        )
    conditions.append(model.observed_at == task.target_observed_at)
    return model, conditions


async def _lock_and_validate_scopes(session, task: ResearchWebEnrichmentTask) -> None:
    thread_scope = await session.scalar(
        select(ResearchTaskScope)
        .where(
            ResearchTaskScope.run_id == task.run_id,
            ResearchTaskScope.logical_task_key == f"thread_replies:{task.tid}",
            ResearchTaskScope.scope_type == "thread_replies",
        )
        .with_for_update()
    )
    if thread_scope is None or thread_scope.generation != (
        task.parent_scope_generation if task.target_type == "comment" else task.scope_generation
    ):
        raise _ReconcileRejectedError("failed_final", "scope_generation_changed")
    if thread_scope.status != "completed":
        raise _ReconcileRejectedError("failed_final", "thread_replies_not_completed")
    if task.fid is not None and thread_scope.fid is not None and thread_scope.fid != task.fid:
        raise _ReconcileRejectedError("identity_unverified", "thread_scope_fid_mismatch")
    if task.target_type == "comment":
        comment_scope = await session.scalar(
            select(ResearchTaskScope)
            .where(
                ResearchTaskScope.run_id == task.run_id,
                ResearchTaskScope.logical_task_key == f"post_comments:{task.tid}:{task.parent_post_pid}",
                ResearchTaskScope.scope_type == "post_comments",
                ResearchTaskScope.parent_pid == task.parent_post_pid,
            )
            .with_for_update()
        )
        if (
            comment_scope is None
            or comment_scope.generation != task.scope_generation
            or comment_scope.parent_generation != thread_scope.generation
        ):
            raise _ReconcileRejectedError("failed_final", "comment_scope_generation_changed")
        if comment_scope.status != "completed":
            raise _ReconcileRejectedError("failed_final", "post_comments_not_completed")
        if task.fid is not None and comment_scope.fid is not None and comment_scope.fid != task.fid:
            raise _ReconcileRejectedError("identity_unverified", "comment_scope_fid_mismatch")


async def _lock_target(session, task: ResearchWebEnrichmentTask):
    model, conditions = _conditions(task)
    pk = model.tid if task.target_type == "thread" else model.pid if task.target_type == "post" else model.cid
    row = await session.scalar(select(model).where(pk == task.target_id).with_for_update())
    if row is None:
        raise _ReconcileRejectedError("failed_final", "target_row_missing")
    if _utc(row.observed_at) != _utc(task.target_observed_at):
        raise _ReconcileRejectedError("failed_final", "target_api_watermark_changed")
    if row.tid != task.tid or row.author_user_id != task.author_user_id or row.fid != task.fid:
        raise _ReconcileRejectedError("identity_unverified", "target_database_identity_changed")
    if task.target_type == "comment" and row.parent_post_pid != task.parent_post_pid:
        raise _ReconcileRejectedError("identity_unverified", "target_database_parent_changed")
    return model, conditions, row


def _unknown_canonical_value(current: Any, field_name: str, missing_reasons: Mapping[str, Any]) -> bool:
    if current is None:
        return True
    reason = missing_reasons.get(field_name)
    if reason not in _ZERO_UNKNOWN_REASONS:
        return False
    if isinstance(current, bool):
        return field_name == "is_bawu" and current is False
    return isinstance(current, int) and current == 0


def _outcomes_by_field(
    requested_fields: Sequence[str],
    supplied: Mapping[str, Any] | None,
) -> dict[str, dict[str, str]]:
    supplied = supplied or {}
    unknown = set(supplied) - set(requested_fields)
    if unknown:
        raise _ReconcileRejectedError("failed_final", "field_outcome_not_requested")
    output: dict[str, dict[str, str]] = {}
    for field_name, value in supplied.items():
        if isinstance(value, str):
            status, reason = value, ""
        elif isinstance(value, Mapping):
            status, reason = value.get("status"), value.get("reason", "")
        else:
            raise _ReconcileRejectedError("failed_final", "field_outcome_invalid")
        if status not in _STATUS_BY_FIELD:
            raise _ReconcileRejectedError("failed_final", "field_outcome_status_invalid")
        output[field_name] = {"status": status, "reason": _safe_reason(reason)}
    return output


def _identity_failure_outcome(outcomes: Mapping[str, Mapping[str, str]]) -> str | None:
    for outcome in outcomes.values():
        reason = outcome["reason"]
        if outcome["status"] == "identity_unverified" or reason.endswith(_IDENTITY_MISMATCH_SUFFIXES):
            return reason or "field_identity_unverified"
    return None


def _observation_key(task_key: str, evidence: dict[str, Any]) -> str:
    stable = {
        "task_key": task_key,
        "field_name": evidence["candidate"].field_name,
        "source_kind": evidence["candidate"].source_kind,
        "source_path": evidence["candidate"].source_path,
        "source_url": evidence["source_url"],
        "observed_at": evidence["observed_at"].isoformat(),
        "raw": evidence["raw_value"],
        "normalized": evidence["normalized_value"],
    }
    encoded = json.dumps(stable, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _failure_identity(candidates: Sequence[ObservationCandidate]) -> dict[str, Any] | None:
    for candidate in candidates:
        evidence = getattr(candidate, "identity", None)
        if evidence is not None:
            return _evidence_identity(evidence)
    return None


async def _record_rejected_response(
    async_sessionmaker_,
    task_key: str,
    lease_token: str,
    lease_generation: int | None,
    status: str,
    reason_code: str,
    occurred_at: datetime,
    requested_fields: Sequence[str] | None = None,
    identity: dict[str, Any] | None = None,
    diagnostics: Mapping[str, Any] | None = None,
) -> None:
    safe_reason = _safe_reason(reason_code)
    async with async_sessionmaker_() as session, session.begin():
        task = await session.scalar(
            select(ResearchWebEnrichmentTask)
            .where(ResearchWebEnrichmentTask.task_key == task_key)
            .with_for_update()
        )
        lease_matches = (
            task is not None
            and task.status == "running"
            and task.lease_token == lease_token
            and (lease_generation is None or task.lease_generation == lease_generation)
        )
        if (
            safe_reason != "lease_expired"
            and lease_matches
        ):
            task.status = status
            task.last_error_type = status
            task.last_error = safe_reason
            task.identity_verified = status != "identity_unverified"
            task.identity_reason = safe_reason if status == "identity_unverified" else None
            task.lease_token = None
            task.lease_owner = None
            task.lease_until = None
            task.completed_at = occurred_at
            task.updated_at = occurred_at
            task.last_diagnostics_json = with_elapsed_seconds(diagnostics, task.leased_at, occurred_at)
            if requested_fields is not None:
                task.field_statuses_json = {
                    name: {"status": status, "reason": safe_reason} for name in requested_fields
                }
            task.leased_at = None
        if lease_generation is not None:
            attempt = await session.get(ResearchWebEnrichmentAttempt, (task_key, lease_generation))
            if lease_matches and attempt is not None and attempt.finished_at is None:
                attempt.status = status if safe_reason != "lease_expired" else "lease_expired"
                attempt.error_type = status
                attempt.reason_code = safe_reason
                attempt.finished_at = occurred_at
                attempt.diagnostics_json = with_elapsed_seconds(diagnostics, attempt.started_at, occurred_at)
        if lease_generation is not None:
            session.add(
                ResearchWebEnrichmentFailure(
                    failure_id=str(uuid4()),
                    task_key=task_key,
                    lease_generation=lease_generation,
                    status=status,
                    reason_code=safe_reason,
                    occurred_at=occurred_at,
                    identity_evidence_json=identity,
                )
            )


async def complete_web_enrichment_task(
    async_sessionmaker_,
    task_key: str,
    lease_token: str,
    candidates: Sequence[ObservationCandidate],
    *,
    field_outcomes: Mapping[str, Any] | None = None,
    diagnostics: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> WebReconcileResult:
    """Validate a whole candidate batch, then merge it with one target in a short transaction."""
    current_time = _now(now)
    candidates = tuple(candidates)
    failure_identity = _failure_identity(candidates)
    observed_lease_generation: int | None = None
    requested_fields: tuple[str, ...] | None = None
    try:
        async with async_sessionmaker_() as session, session.begin():
            task = await session.scalar(
                select(ResearchWebEnrichmentTask)
                .where(ResearchWebEnrichmentTask.task_key == task_key)
                .with_for_update()
            )
            if task is None:
                raise _ReconcileRejectedError("failed_final", "task_not_found", identity=failure_identity)
            observed_lease_generation = task.lease_generation
            requested_fields = tuple(task.requested_fields_json)
            if task.status != "running" or task.lease_token != lease_token:
                raise _ReconcileRejectedError("failed_final", "lease_token_stale", identity=failure_identity)
            if task.lease_until is None or _utc(task.lease_until) <= current_time:
                raise _ReconcileRejectedError("failed_final", "lease_expired", identity=failure_identity)
            run = await session.get(ResearchRun, task.run_id)
            if run is None or run.status in {"cancelled", "canceled"}:
                raise _ReconcileRejectedError("failed_final", "run_cancelled", identity=failure_identity)
            requested_fields = tuple(task.requested_fields_json)
            # This check happens while the task is locked and before a content row is locked.
            validated = [_validate_candidate(task, candidate) for candidate in candidates]
            outcomes = _outcomes_by_field(requested_fields, field_outcomes)
            if any(item["candidate"].field_name not in requested_fields for item in validated):
                raise _ReconcileRejectedError("failed_final", "candidate_field_not_requested")
            identity_failure = _identity_failure_outcome(outcomes)
            if identity_failure is not None:
                raise _ReconcileRejectedError("identity_unverified", identity_failure)

            await _lock_and_validate_scopes(session, task)
            model, conditions, row = await _lock_target(session, task)
            missing_reasons = dict(row.missing_reasons_json or {})
            sources = dict(row.field_sources_json or {})
            values_to_set: dict[str, Any] = {}
            observation_statuses: dict[int, str] = {}
            grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in validated:
                grouped[item["candidate"].field_name].append(item)
            reply_uid_candidates = {
                item["normalized_value"]
                for item in grouped.get("reply_to_user_id", ())
                if item["candidate"].precision == "exact"
            }
            reply_name_uid_candidates: list[tuple[str, str]] = []
            reply_name_only_candidates: list[str] = []
            for item in grouped.get("reply_to_display_name", ()):
                if item["candidate"].precision != "exact":
                    continue
                mention_uid = item["raw_value"].get("mention_uid")
                if mention_uid is None:
                    reply_name_only_candidates.append(item["normalized_value"])
                    continue
                try:
                    reply_name_uid_candidates.append(
                        (parse_positive_id(mention_uid), item["normalized_value"])
                    )
                except IdentityError:
                    continue
            reply_name_sources = sources.get("reply_to_display_name")
            reply_name_bindings: list[str | None] = []
            unbound_reply_names: list[str] = []
            bound_reply_names: list[tuple[str, str | None]] = []
            if isinstance(reply_name_sources, Mapping):
                for provenance in reply_name_sources.values():
                    if not isinstance(provenance, Mapping):
                        continue
                    binding = provenance.get("reply_target_user_id")
                    value = provenance.get("value")
                    if binding is not None:
                        try:
                            normalized_binding = parse_positive_id(binding)
                            reply_name_bindings.append(normalized_binding)
                        except IdentityError:
                            normalized_binding = None
                            reply_name_bindings.append(None)
                        if isinstance(value, str):
                            bound_reply_names.append((value, normalized_binding))
                    elif provenance.get("confidence") == "display_name_only":
                        if isinstance(value, str) and value.strip():
                            unbound_reply_names.append(value)
            unbound_reply_names = [
                name
                for name in unbound_reply_names
                if not any(bound_name == name for bound_name, _uid in bound_reply_names)
            ]
            known_reply_uids: list[str | None] = []
            if task.target_type == "comment" and row.reply_to_user_id is not None:
                try:
                    known_reply_uids.append(parse_positive_id(row.reply_to_user_id))
                except IdentityError:
                    known_reply_uids.append(None)
            reply_sources = sources.get("reply_to_user_id")
            verified_reply = reply_sources.get("verified_web") if isinstance(reply_sources, Mapping) else None
            verified_reply_uid = verified_reply.get("value") if isinstance(verified_reply, Mapping) else None
            if verified_reply_uid is not None:
                try:
                    known_reply_uids.append(parse_positive_id(verified_reply_uid))
                except IdentityError:
                    known_reply_uids.append(None)

            applied_fields: list[str] = []
            confirmed_fields: list[str] = []
            conflict_fields: list[str] = []
            candidate_status_by_field: dict[str, str] = {}
            for field_name, group in grouped.items():
                exact_group = [item for item in group if item["candidate"].precision == "exact"]
                for item in group:
                    if item["candidate"].precision != "exact":
                        observation_statuses[id(item)] = "approximate_not_applied"
                if not exact_group:
                    candidate_status_by_field[field_name] = "approximate_not_applied"
                    continue
                if field_name == "forum_level_icon_ref":
                    for item in exact_group:
                        observation_statuses[id(item)] = "audit_only"
                    candidate_status_by_field[field_name] = "audit_only"
                    continue
                distinct = {
                    json.dumps(item["normalized_value"], sort_keys=True, ensure_ascii=False)
                    for item in exact_group
                }
                if len(distinct) > 1:
                    conflict_fields.append(field_name)
                    candidate_status_by_field[field_name] = "conflict"
                    for item in exact_group:
                        observation_statuses[id(item)] = "conflict"
                    continue

                candidate_value = exact_group[0]["normalized_value"]
                if (
                    field_name == "reply_to_user_id"
                    and task.target_type == "comment"
                ):
                    candidate_uid = parse_positive_id(candidate_value)
                    bound_uids = [
                        *reply_name_bindings,
                        *(uid for uid, _name in reply_name_uid_candidates),
                    ]
                    bound_uids.extend(known_reply_uids)
                    same_label_upgrade = any(
                        uid == candidate_uid
                        and name in unbound_reply_names
                        and all(existing_name == name for existing_name in unbound_reply_names)
                        for uid, name in reply_name_uid_candidates
                    )
                    if (
                        row.reply_to_user_id is None
                        and not known_reply_uids
                        and (unbound_reply_names or reply_name_only_candidates)
                        and (reply_name_only_candidates or not same_label_upgrade)
                    ):
                        missing_reasons["reply_to_user_id"] = "reply_to_uid_unbound_display_name"
                        conflict_fields.append(field_name)
                        candidate_status_by_field[field_name] = "conflict"
                        for item in exact_group:
                            observation_statuses[id(item)] = "conflict"
                        continue
                    if any(uid is None or uid != candidate_uid for uid in bound_uids):
                        if bound_uids:
                            missing_reasons["reply_to_user_id"] = "reply_to_uid_binding_conflict"
                        conflict_fields.append(field_name)
                        candidate_status_by_field[field_name] = "conflict"
                        for item in exact_group:
                            observation_statuses[id(item)] = "conflict"
                        continue
                if field_name == "reply_to_display_name":
                    mention_uids = {
                        item["raw_value"].get("mention_uid")
                        for item in exact_group
                    }
                    if len(mention_uids) != 1:
                        conflict_fields.append(field_name)
                        candidate_status_by_field[field_name] = "conflict"
                        for item in exact_group:
                            observation_statuses[id(item)] = "conflict"
                        continue
                    mention_uid = next(iter(mention_uids))
                    if mention_uid is None:
                        if known_reply_uids or reply_name_bindings:
                            reason = "reply_to_mention_uid_missing"
                            missing_reasons[field_name] = reason
                            outcomes[field_name] = {"status": "needs_evidence", "reason": reason}
                            for item in exact_group:
                                observation_statuses[id(item)] = "audit_only"
                            continue
                    else:
                        try:
                            mention_uid = parse_positive_id(mention_uid)
                        except IdentityError:
                            conflict_fields.append(field_name)
                            candidate_status_by_field[field_name] = "conflict"
                            for item in exact_group:
                                observation_statuses[id(item)] = "conflict"
                            continue
                        if unbound_reply_names and any(
                            name != candidate_value for name in unbound_reply_names
                        ):
                            conflict_fields.append(field_name)
                            candidate_status_by_field[field_name] = "conflict"
                            for item in exact_group:
                                observation_statuses[id(item)] = "conflict"
                            continue
                    uid_matches = True
                    if mention_uid is not None and known_reply_uids:
                        try:
                            uid_matches = all(
                                known_uid is not None and mention_uid == known_uid
                                for known_uid in known_reply_uids
                            )
                        except IdentityError:
                            uid_matches = False
                        if not uid_matches:
                            conflict_fields.append(field_name)
                            candidate_status_by_field[field_name] = "conflict"
                            for item in exact_group:
                                observation_statuses[id(item)] = "conflict"
                            continue
                    if uid_matches and mention_uid is not None and reply_name_bindings:
                        uid_matches = all(
                            bound_uid is not None and mention_uid == bound_uid
                            for bound_uid in reply_name_bindings
                        )
                    if uid_matches and mention_uid is not None and reply_uid_candidates:
                        try:
                            uid_matches = (
                                len(reply_uid_candidates) == 1
                                and mention_uid == parse_positive_id(next(iter(reply_uid_candidates)))
                            )
                        except IdentityError:
                            uid_matches = False
                    if not uid_matches:
                        conflict_fields.append(field_name)
                        candidate_status_by_field[field_name] = "conflict"
                        for item in exact_group:
                            observation_statuses[id(item)] = "conflict"
                        continue
                current_value = getattr(row, field_name)
                unknown = _unknown_canonical_value(current_value, field_name, missing_reasons)
                if current_value == candidate_value:
                    confirmed_fields.append(field_name)
                    candidate_status_by_field[field_name] = "confirmed"
                    for item in exact_group:
                        observation_statuses[id(item)] = "confirmed"
                    if field_name == "reply_to_display_name":
                        first = exact_group[0]
                        sources[field_name] = {
                            **(
                                sources.get(field_name)
                                if isinstance(sources.get(field_name), dict)
                                else {}
                            ),
                            "verified_web": {
                                "observation_time": first["observed_at"].isoformat(),
                                "source_kind": first["candidate"].source_kind,
                                "parser_version": first["candidate"].parser_version,
                                "value": candidate_value,
                                "confidence": (
                                    "display_name_only"
                                    if mention_uid is None
                                    else "explicit_type4_mention"
                                ),
                                "reply_target_user_id": mention_uid,
                            },
                        }
                    continue
                if not unknown:
                    conflict_fields.append(field_name)
                    candidate_status_by_field[field_name] = "conflict"
                    for item in exact_group:
                        observation_statuses[id(item)] = "conflict"
                    continue

                values_to_set[field_name] = candidate_value
                missing_reasons.pop(field_name, None)
                applied_fields.append(field_name)
                candidate_status_by_field[field_name] = "applied"
                for item in exact_group:
                    observation_statuses[id(item)] = "applied"
                first = exact_group[0]
                sources[field_name] = {
                    **(sources.get(field_name) if isinstance(sources.get(field_name), dict) else {}),
                    "verified_web": {
                        "observation_time": first["observed_at"].isoformat(),
                        "source_kind": first["candidate"].source_kind,
                        "parser_version": first["candidate"].parser_version,
                        "value": candidate_value,
                        **(
                            {
                                "confidence": (
                                    "display_name_only"
                                    if mention_uid is None
                                    else "explicit_type4_mention"
                                ),
                                "reply_target_user_id": mention_uid,
                            }
                            if field_name == "reply_to_display_name"
                            else {}
                        ),
                    },
                }
                if field_name == "ip_location_text":
                    values_to_set["ip_location_source"] = first["candidate"].source_kind
                    values_to_set["ip_location_observed_at"] = first["observed_at"]

            for field_name, outcome in outcomes.items():
                if field_name not in candidate_status_by_field:
                    candidate_status_by_field[field_name] = outcome["status"]
            for field_name in requested_fields:
                if field_name not in candidate_status_by_field:
                    candidate_status_by_field[field_name] = "absent_on_page"
                    outcomes.setdefault(field_name, {"status": "absent_on_page", "reason": "no_candidate_observed"})

            for item in validated:
                status = observation_statuses.get(id(item), "audit_only")
                candidate = item["candidate"]
                observation_key = _observation_key(task_key, item)
                observation_values = {
                    "observation_id": str(uuid4()),
                    "observation_key": observation_key,
                    "task_key": task_key,
                    "run_id": task.run_id,
                    "target_type": task.target_type,
                    "target_id": task.target_id,
                    "tid": task.tid,
                    "pid": task.pid,
                    "cid": task.cid,
                    "parent_post_pid": task.parent_post_pid,
                    "author_user_id": task.author_user_id,
                    "fid": task.fid,
                    "field_name": candidate.field_name,
                    "raw_value_json": item["raw_value"],
                    "normalized_value_json": item["normalized_value"],
                    "source_url": item["source_url"],
                    "source_path": candidate.source_path,
                    "source_kind": candidate.source_kind,
                    "observed_at": item["observed_at"],
                    # This records when the webpage showed the field, not the field's historical value at post time.
                    "temporal_scope": "web_observation_at_observed_time",
                    "parser_version": candidate.parser_version,
                    "identity_evidence_json": item["identity"],
                    "validation_status": "verified",
                    "applied_status": status,
                    "missing_reason": (
                        outcomes.get(candidate.field_name, {}).get("reason")
                        if status == "audit_only"
                        else None
                    ),
                    "created_at": current_time,
                    "applied_at": current_time if status in {"applied", "confirmed"} else None,
                }
                insert_factory = _insert_factory(session.get_bind().dialect.name)
                statement = insert_factory(ResearchFieldObservation).values(**observation_values)
                await session.execute(
                    statement.on_conflict_do_nothing(index_elements=[ResearchFieldObservation.observation_key])
                )

            # Even audit-only/conflict-only batches execute a strict no-op update,
            # so every accepted batch proves exactly one target row still matched.
            values_to_set["field_sources_json"] = sources or None
            values_to_set["missing_reasons_json"] = missing_reasons
            result = await session.execute(
                update(model).where(*conditions).values(**values_to_set)
            )
            if result.rowcount != 1:
                raise _ReconcileRejectedError("failed_final", "strict_target_update_rowcount_not_one")

            for field_name, status in candidate_status_by_field.items():
                if status in {"applied", "confirmed", "conflict", "audit_only", "approximate_not_applied"}:
                    outcomes[field_name] = {"status": status, "reason": ""}
            absent_fields = tuple(
                name
                for name, outcome in outcomes.items()
                if outcome["status"]
                in {"absent_on_page", "needs_evidence", "not_tested", "not_observed_in_bounded_page"}
            )
            requested_statuses = [candidate_status_by_field[name] for name in requested_fields]
            if all(status in {"applied", "confirmed"} for status in requested_statuses):
                task_status = "completed"
            elif all(status == "unsupported_by_source" for status in requested_statuses):
                task_status = "unsupported_by_source"
            elif all(status == "not_applicable" for status in requested_statuses):
                task_status = "not_applicable"
            else:
                task_status = "partially_completed"
            task.field_statuses_json = {
                name: {
                    "status": outcomes.get(name, {"status": candidate_status_by_field[name]}).get("status"),
                    "reason": outcomes.get(name, {}).get("reason", ""),
                }
                for name in requested_fields
            }
            task.status = task_status
            task.identity_verified = True
            task.identity_reason = None
            task.last_error_type = None
            task.last_error = None
            task.lease_token = None
            task.lease_owner = None
            task.lease_until = None
            task.last_diagnostics_json = with_elapsed_seconds(diagnostics, task.leased_at, current_time)
            task.leased_at = None
            task.completed_at = current_time
            task.updated_at = current_time
            attempt = await session.get(ResearchWebEnrichmentAttempt, (task_key, task.lease_generation))
            if attempt is not None:
                attempt.status = task_status
                attempt.finished_at = current_time
                attempt.diagnostics_json = with_elapsed_seconds(diagnostics, attempt.started_at, current_time)
            return WebReconcileResult(
                task_key=task_key,
                status=task_status,
                applied_fields=tuple(sorted(applied_fields)),
                confirmed_fields=tuple(sorted(confirmed_fields)),
                conflict_fields=tuple(sorted(set(conflict_fields))),
                absent_fields=tuple(sorted(absent_fields)),
            )
    except _ReconcileRejectedError as rejected:
        await _record_rejected_response(
            async_sessionmaker_,
            task_key,
            lease_token,
            observed_lease_generation,
            rejected.status,
            rejected.reason_code,
            current_time,
            requested_fields,
            rejected.identity or failure_identity,
            diagnostics,
        )
        return WebReconcileResult(task_key=task_key, status=rejected.status, reason_code=rejected.reason_code)
    except Exception:
        # The evidence audit uses a fresh transaction so it survives a rolled-back merge.
        if observed_lease_generation is not None:
            await _record_failure_audit(
                async_sessionmaker_,
                task_key,
                observed_lease_generation,
                "failed_final",
                "reconcile_transaction_failed",
                current_time,
                failure_identity,
            )
            await _finish_attempt_after_error(
                async_sessionmaker_,
                task_key,
                observed_lease_generation,
                lease_token,
                "reconcile_transaction_failed",
                current_time,
            )
        raise


async def fail_web_enrichment_task(
    async_sessionmaker_,
    task_key: str,
    lease_token: str,
    *,
    status: str,
    reason_code: str,
    field_outcomes: Mapping[str, Any] | None = None,
    diagnostics: Mapping[str, Any] | None = None,
    retry_after: int | None = None,
    now: datetime | None = None,
) -> str:
    """Finish a failed attempt with bounded retry timing and a separate audit row."""
    current_time = _now(now)
    allowed = {
        "failed_retryable",
        "failed_final",
        "needs_login",
        "blocked",
        "identity_unverified",
        "unsupported_by_source",
        "not_applicable",
    }
    if status not in allowed:
        raise ValueError(f"Unsupported web task failure status: {status}.")
    if retry_after is not None and not 0 <= retry_after <= 1800:
        raise ValueError("retry_after must be between 0 and 1800 seconds.")
    generation: int | None = None
    reason = _safe_reason(reason_code)
    diagnostic_values = dict(diagnostics or {})
    if reason in {"http_429", "http_auth_or_verification", "needs_login", "auth_required"}:
        diagnostic_values.setdefault("blocked_reason", reason)
    async with async_sessionmaker_() as session, session.begin():
        task = await session.scalar(
            select(ResearchWebEnrichmentTask)
            .where(ResearchWebEnrichmentTask.task_key == task_key)
            .with_for_update()
        )
        if task is None:
            return "not_found"
        generation = task.lease_generation
        if task.status != "running" or task.lease_token != lease_token:
            return "stale_lease"
        if task.lease_until is None or _utc(task.lease_until) <= current_time:
            return "expired_lease"
        requested_fields = tuple(task.requested_fields_json)
        outcomes = _outcomes_by_field(requested_fields, field_outcomes)
        task_status = status
        next_retry: datetime | None = None
        if status == "failed_retryable":
            if task.attempts >= task.max_attempts:
                task_status = "failed_final"
            else:
                backoff = retry_after if retry_after is not None else min(30 * (2 ** max(0, task.attempts - 1)), 1800)
                next_retry = current_time + timedelta(seconds=backoff)
        for field_name in requested_fields:
            outcomes.setdefault(field_name, {"status": status, "reason": reason})
        task.field_statuses_json = outcomes
        task.status = task_status
        task.last_error_type = task_status
        task.last_error = reason
        task.next_retry_at = next_retry
        task.identity_verified = False if task_status == "identity_unverified" else None
        task.identity_reason = reason if task_status == "identity_unverified" else None
        task.lease_token = None
        task.lease_owner = None
        task.lease_until = None
        task.last_diagnostics_json = with_elapsed_seconds(diagnostic_values, task.leased_at, current_time)
        task.leased_at = None
        task.updated_at = current_time
        task.completed_at = None if task_status == "failed_retryable" else current_time
        attempt = await session.get(ResearchWebEnrichmentAttempt, (task_key, task.lease_generation))
        if attempt is not None:
            attempt.status = task_status
            attempt.error_type = task_status
            attempt.reason_code = reason
            attempt.finished_at = current_time
            attempt.diagnostics_json = with_elapsed_seconds(diagnostic_values, attempt.started_at, current_time)
    if generation is not None:
        await _record_failure_audit(
            async_sessionmaker_,
            task_key,
            generation,
            task_status,
            reason,
            current_time,
        )
    return task_status


async def _finish_attempt_after_error(
    async_sessionmaker_,
    task_key: str,
    lease_generation: int,
    lease_token: str,
    reason_code: str,
    finished_at: datetime,
) -> None:
    async with async_sessionmaker_() as session, session.begin():
        task = await session.scalar(
            select(ResearchWebEnrichmentTask)
            .where(ResearchWebEnrichmentTask.task_key == task_key)
            .with_for_update()
        )
        if (
            task is None
            or task.status != "running"
            or task.lease_generation != lease_generation
            or task.lease_token != lease_token
        ):
            return
        attempt = await session.get(ResearchWebEnrichmentAttempt, (task_key, lease_generation))
        if attempt is not None and attempt.finished_at is None:
            attempt.status = "failed_final"
            attempt.error_type = "failed_final"
            attempt.reason_code = _safe_reason(reason_code)
            attempt.finished_at = finished_at
            attempt.diagnostics_json = with_elapsed_seconds(None, attempt.started_at, finished_at)


__all__ = ["WebReconcileResult", "complete_web_enrichment_task", "fail_web_enrichment_task"]
