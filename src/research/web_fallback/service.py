"""Bounded orchestration for explicitly requested web field backfills."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import exists, func, select
from tiebameow.models.orm import Thread

from src.core.models import ResearchTaskScope
from src.research.models import (
    ResearchComment,
    ResearchPost,
    ResearchThread,
    ResearchWebEnrichmentTask,
)

from .diagnostics import sanitize_page_readiness_diagnostics
from .identity import Target
from .tasks import (
    _GROWTH_FIELDS,
    WebEnrichmentTargetPlan,
    _resolved_web_task_status,
    _unknown_canonical_value,
    _unknown_requested_fields,
    enqueue_web_enrichment_tasks,
    lease_web_enrichment_tasks,
    plan_web_enrichment_targets,
    resume_web_enrichment_tasks,
)

_TRANSPORT_ERRORS = (ConnectionResetError, BrokenPipeError, EOFError, asyncio.IncompleteReadError)
_QUEUE_WAIT_LOG_INTERVAL_SECONDS = 30.0
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


@dataclass(frozen=True, slots=True)
class TargetFilters:
    target_type: str
    target_ids: tuple[int, ...] | None = None
    tid: int | None = None
    fid: int | None = None
    parent_pid: int | None = None


@dataclass(frozen=True, slots=True)
class FieldBackfillSummary:
    run_id: str
    planned_count: int
    eligible_count: int
    pending_api_detail_count: int
    created_count: int
    deduplicated_count: int
    collected_count: int
    completed_count: int
    partial_count: int
    stopped_reason: str | None
    skipped: dict[str, int]
    diagnostics: dict[str, int | float]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class WebWorkerSummary:
    run_ids: tuple[str, ...]
    collected_count: int
    completed_count: int
    partial_count: int
    unresolved_count: int
    stopped_reason: str | None
    diagnostics: dict[str, int | float]
    blocked_count: int = 0
    retryable_count: int = 0
    stopped_run_id: str | None = None
    stopped_target_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_target_filters(
    target_type: str,
    *,
    tid: int | None = None,
    pid: int | None = None,
    cid: int | None = None,
    fid: int | None = None,
) -> TargetFilters:
    """Map CLI IDs to the requested entity's own key without cross-type reuse."""
    if target_type not in {"thread", "post", "comment"}:
        raise ValueError("target_type must be thread, post, or comment")
    for name, value in (("tid", tid), ("pid", pid), ("cid", cid), ("fid", fid)):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= (1 << 63) - 1
        ):
            raise ValueError(f"{name} must be a positive integer")

    if target_type == "thread":
        if pid is not None or cid is not None:
            raise ValueError("thread targets accept --tid and --fid only")
        return TargetFilters("thread", (tid,) if tid is not None else None, tid=tid, fid=fid)
    if target_type == "post":
        if cid is not None:
            raise ValueError("post targets do not accept --cid")
        return TargetFilters("post", (pid,) if pid is not None else None, tid=tid, fid=fid)
    if cid is not None and pid is not None:
        # For comments, --pid is the parent-post filter, never a comment ID.
        return TargetFilters("comment", (cid,), tid=tid, fid=fid, parent_pid=pid)
    return TargetFilters(
        "comment",
        (cid,) if cid is not None else None,
        tid=tid,
        fid=fid,
        parent_pid=pid,
    )


def _target_from_plan(plan: WebEnrichmentTargetPlan) -> Target:
    return Target(
        target_type=plan.target_type,
        target_id=plan.target_id,
        tid=plan.tid,
        fid=plan.fid,
        pid=plan.pid,
        cid=plan.cid,
        parent_post_pid=plan.parent_post_pid,
        author_user_id=plan.author_user_id,
        author_portrait=plan.author_portrait,
        requested_fields=tuple(plan.requested_fields),
        scope_generation=getattr(plan, "scope_generation", None),
        parent_scope_generation=getattr(plan, "parent_scope_generation", None),
    )


def _target_from_lease(lease: Any) -> Target:
    return Target(
        target_type=lease.target_type,
        target_id=lease.target_id,
        tid=lease.tid,
        fid=lease.fid,
        pid=lease.pid,
        cid=lease.cid,
        parent_post_pid=lease.parent_post_pid,
        author_user_id=lease.author_user_id,
        author_portrait=lease.author_portrait,
        requested_fields=tuple(lease.requested_fields),
        scope_generation=getattr(lease, "scope_generation", None),
        parent_scope_generation=getattr(lease, "parent_scope_generation", None),
    )


async def run_field_backfill(
    sessions: Any,
    run_id: str,
    requested_fields: tuple[str, ...] | list[str],
    filters: TargetFilters,
    *,
    limit: int = 5,
    max_seconds: int = 120,
    dry_run: bool = False,
    browser_profile: str | Path | None = None,
    headed: bool = False,
    wait_for_login: bool = False,
    max_attempts: int = 3,
    request_budget: Any | None = None,
    browser_config: Any | None = None,
    provider_factory: Any | None = None,
    login_confirmation: Any | None = None,
) -> FieldBackfillSummary:
    """Plan or execute a bounded web-only enrichment pass for existing rows."""
    _validate_limits(limit, max_seconds)
    if wait_for_login and not headed:
        raise ValueError("--wait-for-login requires --headed")
    if not dry_run and browser_profile is None:
        raise ValueError("an explicit isolated --browser-profile is required for collection")

    options = _filter_options(filters)
    if dry_run:
        summary = await enqueue_web_enrichment_tasks(
            sessions,
            run_id,
            requested_fields,
            **options,
            limit=limit,
            dry_run=True,
            max_attempts=max_attempts,
            only_missing_fields=True,
            pair_growth_fields=True,
        )
    else:
        summary = await enqueue_web_enrichment_tasks(
            sessions,
            run_id,
            requested_fields,
            **options,
            limit=limit,
            dry_run=False,
            max_attempts=max_attempts,
            only_missing_fields=True,
            pair_growth_fields=True,
        )
    plans = tuple(summary.planned_targets)
    eligible = tuple(
        plan for plan in plans if plan.eligibility_status in {"pending_web", "failed_retryable"}
    )
    pending_api = sum(plan.eligibility_status == "pending_api_detail" for plan in plans)
    if dry_run or not eligible:
        return FieldBackfillSummary(
            run_id=run_id,
            planned_count=summary.planned_count,
            eligible_count=len(eligible),
            pending_api_detail_count=pending_api,
            created_count=0 if dry_run else summary.created_count,
            deduplicated_count=0 if dry_run else summary.deduplicated_count,
            collected_count=0,
            completed_count=0,
            partial_count=0,
            stopped_reason=None,
            skipped=dict(summary.skipped),
            diagnostics={},
        )

    if browser_config is None:
        browser_config = _configured_web_settings()
    if request_budget is None:
        request_budget = make_request_budget(float(browser_config.rps))
    factory = provider_factory or _make_browser_provider
    provider = factory(
        budget=request_budget,
        profile_dir=browser_profile,
        settings=browser_config,
        headed=headed,
        keep_login_page=wait_for_login,
    )
    if asyncio.iscoroutine(provider):
        provider = await provider

    completed = 0
    partial = 0
    collected = 0
    stopped_reason: str | None = None
    diagnostic_counts: Counter[str] = Counter()
    plan_by_key = {plan.task_key: plan for plan in eligible}
    remaining_keys = [plan.task_key for plan in eligible]
    login_resumed: set[str] = set()
    deadline = asyncio.get_running_loop().time() + max_seconds

    try:
        while remaining_keys and asyncio.get_running_loop().time() < deadline:
            leases = await lease_web_enrichment_tasks(
                sessions,
                run_id,
                "field-backfill",
                limit=1,
                task_keys=tuple(remaining_keys),
            )
            if not leases:
                retry_delay = await _next_retry_delay(
                    sessions,
                    (run_id,),
                    task_keys=tuple(remaining_keys),
                )
                if retry_delay is not None:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0 or retry_delay >= remaining:
                        stopped_reason = "max_seconds_exhausted"
                        break
                    await asyncio.sleep(max(0.05, retry_delay))
                    continue
                break
            lease = leases[0]
            plan = plan_by_key.get(lease.task_key)
            if plan is None:
                remaining_keys.remove(lease.task_key)
                continue
            remaining_keys.remove(lease.task_key)
            preflight = await _preflight_web_task(sessions, lease)
            if preflight.status in {
                "completed",
                "completed_with_absent_fields",
                "unsupported_by_source",
                "not_applicable",
            }:
                completed += 1
                continue
            if preflight.status != "ready":
                partial += 1
                continue
            target = replace(_target_from_plan(plan), requested_fields=preflight.missing_fields)
            _set_provider_run_scope(provider, run_id)
            result = await _collect_with_deadline(provider, target, deadline)
            collected += 1
            _add_diagnostics(diagnostic_counts, getattr(result, "diagnostics", {}))
            status = getattr(result, "status", "failed_retryable")
            reason = getattr(result, "reason", None)
            if status == "ok":
                candidates = tuple(getattr(result, "candidates", ()))
                outcomes = _not_observed_outcomes(
                    preflight.missing_fields,
                    candidates,
                    getattr(result, "field_outcomes", None),
                )
                outcomes.update(_preflight_satisfied_outcomes(preflight))
                reconciled = await _complete_task(
                    sessions,
                    lease.task_key,
                    lease.lease_token,
                    candidates,
                    field_outcomes=outcomes,
                    diagnostics=getattr(result, "diagnostics", {}),
                    identity_verified=getattr(result, "identity_verified", None),
                    identity_methods=getattr(result, "identity_methods", ()),
                )
                if getattr(reconciled, "status", None) in {
                    "completed",
                    "completed_with_absent_fields",
                    "unsupported_by_source",
                    "not_applicable",
                }:
                    completed += 1
                else:
                    partial += 1
                    retry_summary = await _enqueue_remaining_fields(sessions, lease)
                    for retry_plan in retry_summary.planned_targets:
                        if retry_plan.eligibility_status in {"pending_web", "failed_retryable"}:
                            plan_by_key[retry_plan.task_key] = retry_plan
                            if retry_plan.task_key not in remaining_keys:
                                remaining_keys.append(retry_plan.task_key)
                continue

            task_status = _task_failure_status(status, reason)
            outcomes = _failure_outcomes(preflight.missing_fields, status, reason)
            outcomes.update(_preflight_satisfied_outcomes(preflight))
            await _fail_task(
                sessions,
                lease.task_key,
                lease.lease_token,
                status=task_status,
                reason_code=_safe_reason(reason or status),
                field_outcomes=outcomes,
                diagnostics=getattr(result, "diagnostics", {}),
            )
            if task_status == "failed_retryable" and lease.task_key not in remaining_keys:
                remaining_keys.append(lease.task_key)
            if task_status in {"blocked", "needs_login"}:
                stopped_reason = _safe_reason(reason or "blocked")
                if _is_rate_limited(reason):
                    break
                if wait_for_login and _is_manual_gate(result, provider):
                    if lease.task_key in login_resumed or not getattr(provider, "has_manual_login_page", False):
                        stopped_reason = "manual_login_page_unavailable"
                        break
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0 or not await _confirm_manual_login(
                        provider,
                        remaining,
                        confirmation=login_confirmation,
                    ):
                        stopped_reason = "manual_login_not_confirmed"
                        break
                    await _close_manual_login_page(provider)
                    resumed = await resume_web_enrichment_tasks(
                        sessions,
                        run_id,
                        statuses=("blocked", "needs_login"),
                        task_keys=(lease.task_key,),
                    )
                    if resumed:
                        login_resumed.add(lease.task_key)
                        remaining_keys.insert(0, lease.task_key)
                        stopped_reason = None
                    else:
                        stopped_reason = "manual_resume_not_applied"
                        break
                    continue
                break
            if task_status == "failed_retryable" and reason == "transport_reset":
                stopped_reason = "transport_reset"
                break
    except asyncio.CancelledError:
        raise
    finally:
        await _close_provider(provider)

    if remaining_keys and asyncio.get_running_loop().time() >= deadline and stopped_reason is None:
        stopped_reason = "max_seconds_exhausted"

    return FieldBackfillSummary(
        run_id=run_id,
        planned_count=summary.planned_count,
        eligible_count=len(eligible),
        pending_api_detail_count=pending_api,
        created_count=summary.created_count,
        deduplicated_count=summary.deduplicated_count,
        collected_count=collected,
        completed_count=completed,
        partial_count=partial,
        stopped_reason=stopped_reason,
        skipped=dict(summary.skipped),
        diagnostics=dict(diagnostic_counts),
    )


async def _enqueue_remaining_fields(
    sessions: Any,
    lease: Any,
    *,
    min_ip_create_time: datetime | None = None,
):
    options: dict[str, Any] = {
        "target_type": lease.target_type,
        "target_ids": (lease.target_id,),
        "tid": lease.tid,
        "fid": lease.fid,
        "limit": 1,
        "max_attempts": lease.max_attempts,
        "only_missing_fields": True,
        "pair_growth_fields": True,
    }
    if min_ip_create_time is not None:
        options["min_ip_create_time"] = min_ip_create_time
    if lease.target_type == "comment":
        options["parent_pid"] = lease.parent_post_pid
    return await enqueue_web_enrichment_tasks(
        sessions,
        lease.run_id,
        lease.requested_fields,
        **options,
    )


async def _process_durable_web_lease(
    sessions: Any,
    provider: Any,
    lease: Any,
    *,
    stop_event: Any | None = None,
    min_ip_create_time: datetime | None = None,
):
    preflight = (
        await _preflight_web_task(
            sessions,
            lease,
            min_ip_create_time=min_ip_create_time,
        )
        if min_ip_create_time is not None
        else await _preflight_web_task(sessions, lease)
    )
    if preflight.status != "ready":
        return preflight.status, preflight
    if stop_event is not None and stop_event.is_set():
        return "stop_requested", replace(
            preflight,
            status="stop_requested",
            reason="stop_requested",
            preflight_skipped=True,
        )
    target = replace(_target_from_lease(lease), requested_fields=preflight.missing_fields)
    run_id = getattr(lease, "run_id", None)
    if isinstance(run_id, str):
        _set_provider_run_scope(provider, run_id)
    try:
        result = await provider.collect(target)
    except _TRANSPORT_ERRORS as error:
        result = _transport_reset_result(error, "web_provider_collect")
    status = getattr(result, "status", "failed_retryable")
    reason = getattr(result, "reason", None)
    diagnostics = getattr(result, "diagnostics", {})
    if status == "ok":
        candidates = tuple(getattr(result, "candidates", ()))
        outcomes = _not_observed_outcomes(
            preflight.missing_fields,
            candidates,
            getattr(result, "field_outcomes", None),
        )
        outcomes.update(_preflight_satisfied_outcomes(preflight))
        reconciled = await _complete_task(
            sessions,
            lease.task_key,
            lease.lease_token,
            candidates,
            field_outcomes=outcomes,
            diagnostics=diagnostics,
            identity_verified=getattr(result, "identity_verified", None),
            identity_methods=getattr(result, "identity_methods", ()),
        )
        return getattr(reconciled, "status", "partially_completed"), result

    task_status = _task_failure_status(status, reason)
    await _fail_task(
        sessions,
        lease.task_key,
        lease.lease_token,
        status=task_status,
        reason_code=_safe_reason(reason or status),
        field_outcomes={
            **_failure_outcomes(preflight.missing_fields, status, reason),
            **_preflight_satisfied_outcomes(preflight),
        },
        diagnostics=diagnostics,
    )
    return task_status, result


def _web_lease_seconds(browser_config: Any | None) -> int:
    timeout_seconds = float(getattr(browser_config, "timeout_seconds", 90.0) or 90.0)
    return max(240, int(timeout_seconds) + 120)


async def _lease_one_web_task(
    sessions: Any,
    run_ids: Sequence[str],
    worker_id: str,
    *,
    min_thread_create_time: datetime | None = None,
    priority_min_thread_create_time: datetime | None = None,
    prioritize_comments: bool = False,
    lease_seconds: int | None = None,
    newest_first: bool = False,
):
    extra_kwargs: dict[str, Any] = {}
    if min_thread_create_time is not None:
        extra_kwargs["min_thread_create_time"] = min_thread_create_time
    if priority_min_thread_create_time is not None:
        extra_kwargs["priority_min_thread_create_time"] = priority_min_thread_create_time
    if prioritize_comments:
        extra_kwargs["prioritize_comments"] = True
    if lease_seconds is not None:
        extra_kwargs["lease_seconds"] = lease_seconds
    if newest_first:
        extra_kwargs["newest_first"] = True
    for run_id in run_ids:
        leases = await lease_web_enrichment_tasks(
            sessions,
            run_id,
            worker_id,
            limit=1,
            **extra_kwargs,
        )
        if leases:
            return leases[0]
    return None


async def _snapshot_run_ids(run_ids: Sequence[str] | Callable[[], Sequence[str]]) -> tuple[str, ...]:
    values = run_ids() if callable(run_ids) else run_ids
    if asyncio.iscoroutine(values):
        values = await values
    if isinstance(values, str):
        values = (values,)
    return tuple(dict.fromkeys(value for value in values if isinstance(value, str) and value))


async def _count_unresolved_web_tasks(sessions: Any, run_ids: Sequence[str]) -> int:
    if not run_ids:
        return 0
    async with sessions() as session:
        tasks = (
            await session.scalars(
                select(ResearchWebEnrichmentTask).where(
                    ResearchWebEnrichmentTask.run_id.in_(tuple(run_ids))
                )
            )
        ).all()
        scopes = (
            await session.scalars(
                select(ResearchTaskScope).where(
                    ResearchTaskScope.run_id.in_(tuple(run_ids))
                )
            )
        ).all()
        scopes_by_key = {
            (scope.run_id, scope.logical_task_key): scope for scope in scopes
        }
        models = {
            "thread": (ResearchThread, ResearchThread.tid),
            "post": (ResearchPost, ResearchPost.pid),
            "comment": (ResearchComment, ResearchComment.cid),
        }
        rows_by_type: dict[str, dict[int, Any]] = {}
        for target_type, (model, id_column) in models.items():
            target_ids = {task.target_id for task in tasks if task.target_type == target_type}
            if not target_ids:
                rows_by_type[target_type] = {}
                continue
            rows = (
                await session.scalars(select(model).where(id_column.in_(target_ids)))
            ).all()
            rows_by_type[target_type] = {
                getattr(row, id_column.key): row for row in rows
            }

    unresolved = 0
    tasks_by_identity: dict[tuple[Any, ...], list[Any]] = {}
    for task in tasks:
        tasks_by_identity.setdefault(_web_task_identity_key(task), []).append(task)
    for task in tasks:
        if task.status in {
            "completed",
            "completed_with_absent_fields",
            "unsupported_by_source",
            "not_applicable",
        }:
            continue
        if task.status == "partially_completed" and _partial_task_fields_satisfied(
            task, rows_by_type.get(task.target_type, {}).get(task.target_id)
        ):
            continue
        if _stale_web_task_is_superseded(
            task,
            rows_by_type.get(task.target_type, {}).get(task.target_id),
            tasks_by_identity.get(_web_task_identity_key(task), ()),
            scopes_by_key,
        ):
            continue
        unresolved += 1
    return unresolved


async def _web_task_status_counts(sessions: Any, run_ids: Sequence[str]) -> dict[str, int]:
    if not run_ids:
        return {}
    async with sessions() as session:
        rows = await session.execute(
            select(ResearchWebEnrichmentTask.status, func.count())
            .where(ResearchWebEnrichmentTask.run_id.in_(tuple(run_ids)))
            .group_by(ResearchWebEnrichmentTask.status)
        )
    return {str(status): int(count) for status, count in rows}


def _partial_task_fields_satisfied(task: Any, row: Any | None) -> bool:
    if (
        row is None
        or row.tid != task.tid
        or row.author_user_id != task.author_user_id
        or row.fid != task.fid
        or _utc_datetime(row.observed_at) != _utc_datetime(task.target_observed_at)
        or (
            task.target_type == "comment"
            and row.parent_post_pid != task.parent_post_pid
        )
    ):
        return False
    requested = tuple(task.requested_fields_json)
    if not requested:
        return False
    field_statuses = dict(task.field_statuses_json or {})
    if _resolved_web_task_status(
        requested, field_statuses, identity_verified=task.identity_verified
    ) is None:
        return False
    missing_reasons = dict(row.missing_reasons_json or {})
    return all(
        not _unknown_canonical_value(getattr(row, name, None), name, missing_reasons)
        for name in requested
        if isinstance(field_statuses.get(name), dict)
        and field_statuses[name].get("status") in {"applied", "confirmed"}
    )


def _web_task_identity_key(task: Any) -> tuple[Any, ...]:
    return (
        task.run_id,
        task.target_type,
        task.target_id,
        task.tid,
        task.fid,
        task.author_user_id,
        task.author_portrait,
        task.parent_post_pid,
    )


def _stale_web_task_is_superseded(
    task: Any,
    row: Any | None,
    candidates: Sequence[Any],
    scopes_by_key: dict[tuple[str, str], Any],
) -> bool:
    expected_id = {
        "thread": getattr(row, "tid", None),
        "post": getattr(row, "pid", None),
        "comment": getattr(row, "cid", None),
    }.get(task.target_type)
    if (
        row is None
        or expected_id != task.target_id
        or row.tid != task.tid
        or row.fid != task.fid
        or row.author_user_id != task.author_user_id
        or getattr(row, "author_portrait", None) != task.author_portrait
        or (task.target_type == "comment" and row.parent_post_pid != task.parent_post_pid)
    ):
        return False
    requested = tuple(task.requested_fields_json)
    if not requested:
        return False
    pair_growth = _GROWTH_FIELDS.issubset(set(requested))
    missing = set(_unknown_requested_fields(row, requested, pair_growth_fields=pair_growth))
    if not missing:
        return True
    current_snapshot = _utc_datetime(row.observed_at) == _utc_datetime(task.target_observed_at)
    current_scope = _web_task_matches_current_scope(task, scopes_by_key)
    if current_snapshot and current_scope:
        return False
    return any(
        _web_task_identity_key(candidate) == _web_task_identity_key(task)
        and _utc_datetime(candidate.target_observed_at) == _utc_datetime(row.observed_at)
        and _web_task_matches_current_scope(candidate, scopes_by_key)
        and missing.issubset(set(candidate.requested_fields_json))
        for candidate in candidates
    )


def _web_task_matches_current_scope(
    task: Any, scopes_by_key: dict[tuple[str, str], Any]
) -> bool:
    thread_scope = scopes_by_key.get((task.run_id, f"thread_replies:{task.tid}"))
    if thread_scope is None or (task.fid is not None and thread_scope.fid != task.fid):
        return False
    if task.target_type == "comment":
        comment_scope = scopes_by_key.get(
            (task.run_id, f"post_comments:{task.tid}:{task.parent_post_pid}")
        )
        return bool(
            comment_scope is not None
            and thread_scope.generation == task.parent_scope_generation
            and comment_scope.generation == task.scope_generation
            and comment_scope.parent_generation == thread_scope.generation
            and (task.fid is None or comment_scope.fid == task.fid)
        )
    return thread_scope.generation == task.scope_generation


async def _next_retry_delay(
    sessions: Any,
    run_ids: Sequence[str],
    *,
    task_keys: Sequence[str] | None = None,
    min_thread_create_time: datetime | None = None,
) -> float | None:
    if not run_ids:
        return None
    now = datetime.now(UTC)
    task_filter = (
        ResearchWebEnrichmentTask.task_key.in_(tuple(task_keys)) if task_keys is not None else True
    )
    thread_time_filter = (
        ~exists(
            select(1).where(
                Thread.tid == ResearchWebEnrichmentTask.tid,
                Thread.create_time.is_not(None),
                Thread.create_time < min_thread_create_time,
            )
        )
        if min_thread_create_time is not None
        else True
    )
    async with sessions() as session:
        retry_at = await session.scalar(
            select(func.min(ResearchWebEnrichmentTask.next_retry_at)).where(
                ResearchWebEnrichmentTask.run_id.in_(tuple(run_ids)),
                ResearchWebEnrichmentTask.status == "failed_retryable",
                ResearchWebEnrichmentTask.next_retry_at.is_not(None),
                task_filter,
                thread_time_filter,
            )
        )
        lease_until = await session.scalar(
            select(func.min(ResearchWebEnrichmentTask.lease_until)).where(
                ResearchWebEnrichmentTask.run_id.in_(tuple(run_ids)),
                ResearchWebEnrichmentTask.status == "running",
                ResearchWebEnrichmentTask.lease_until.is_not(None),
                task_filter,
                thread_time_filter,
            )
        )
    future = [
        value
        for value in (retry_at, lease_until)
        if value is not None and _utc_datetime(value) is not None and _utc_datetime(value) > now
    ]
    if not future:
        return None
    return max(0.0, min((_utc_datetime(value) - now).total_seconds() for value in future))


async def _confirm_login_or_stop(
    provider: Any,
    stop_event: Any | None,
    *,
    confirmation: Any | None = None,
) -> bool:
    confirm = asyncio.create_task(
        _confirm_manual_login(provider, 24 * 60 * 60, confirmation=confirmation)
    )
    stopped = asyncio.create_task(stop_event.wait()) if stop_event is not None else None
    children = tuple(task for task in (confirm, stopped) if task is not None)
    try:
        if stopped is None:
            return bool(await confirm)
        done, _pending = await asyncio.wait(
            children,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stopped in done:
            return False
        return bool(await confirm)
    finally:
        for task in children:
            if not task.done():
                task.cancel()
        await asyncio.gather(*children, return_exceptions=True)


async def drain_pending_web_enrichment(
    sessions: Any,
    run_ids: Sequence[str],
    *,
    provider: Any,
    stop_event: Any | None = None,
    wait_for_login: bool = True,
    login_confirmation: Any | None = None,
    min_thread_create_time: datetime | None = None,
    priority_min_thread_create_time: datetime | None = None,
    prioritize_comments: bool = False,
    newest_first: bool = False,
    min_ip_create_time: datetime | None = None,
) -> WebWorkerSummary:
    """Drain durable web tasks across runs using one caller-owned provider."""
    normalized_run_ids = await _snapshot_run_ids(run_ids)
    collected = 0
    completed = 0
    partial = 0
    diagnostics: Counter[str] = Counter()
    stopped_reason = None
    login_resumed: set[str] = set()
    blocked_count = 0
    retryable_count = 0
    stopped_run_id = None
    stopped_target_type = None
    last_queue_wait_log = 0.0
    extra_filter_kwargs: dict[str, Any] = (
        {"min_thread_create_time": min_thread_create_time} if min_thread_create_time is not None else {}
    )
    lease_kwargs = dict(extra_filter_kwargs)
    if priority_min_thread_create_time is not None:
        lease_kwargs["priority_min_thread_create_time"] = priority_min_thread_create_time
    if prioritize_comments:
        lease_kwargs["prioritize_comments"] = True
    if newest_first:
        lease_kwargs["newest_first"] = True

    while normalized_run_ids and not (stop_event is not None and stop_event.is_set()):
        lease = await _lease_one_web_task(
            sessions,
            normalized_run_ids,
            "live-web-fallback",
            **lease_kwargs,
        )
        if lease is None:
            if stop_event is not None and stop_event.is_set():
                break
            retry_delay = await _next_retry_delay(
                sessions,
                normalized_run_ids,
                **extra_filter_kwargs,
            )
            now = asyncio.get_running_loop().time()
            if now - last_queue_wait_log >= _QUEUE_WAIT_LOG_INTERVAL_SECONDS:
                counts = await _web_task_status_counts(sessions, normalized_run_ids)
                logger.info(
                    "WEB queue waiting: run_ids=%s reason=no_eligible_lease statuses=%s "
                    "next_due_seconds=%s browser_context_started=%s",
                    normalized_run_ids,
                    counts,
                    None if retry_delay is None else round(retry_delay, 1),
                    bool(getattr(provider, "browser_started", False)),
                )
                last_queue_wait_log = now
            if retry_delay is None:
                break
            if stop_event is None:
                await asyncio.sleep(max(0.05, retry_delay))
            else:
                await _wait_for_stop_or_timeout(stop_event, retry_delay)
            continue
        if stop_event is not None and stop_event.is_set():
            break

        process_kwargs: dict[str, Any] = {"stop_event": stop_event}
        enqueue_kwargs: dict[str, Any] = {}
        if min_ip_create_time is not None:
            process_kwargs["min_ip_create_time"] = min_ip_create_time
            enqueue_kwargs["min_ip_create_time"] = min_ip_create_time

        try:
            status, result = await _process_durable_web_lease(
                sessions,
                provider,
                lease,
                **process_kwargs,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            result = _status_result("timeout", "web_worker_target_failed")
            status = "failed_retryable"
            await _fail_task(
                sessions,
                lease.task_key,
                lease.lease_token,
                status=status,
                reason_code="web_worker_target_failed",
                field_outcomes=_failure_outcomes(lease.requested_fields, "timeout", "web_worker_target_failed"),
                diagnostics={},
            )
        if not getattr(result, "preflight_skipped", False):
            collected += 1
        _add_diagnostics(diagnostics, getattr(result, "diagnostics", {}))

        if status == "failed_retryable":
            retryable_count += 1
        elif status in {"blocked", "needs_login"}:
            blocked_count += 1
            stopped_run_id = lease.run_id
            stopped_target_type = lease.target_type

        if status in {
            "completed",
            "completed_with_absent_fields",
            "unsupported_by_source",
            "not_applicable",
        }:
            completed += 1
        elif status == "partially_completed":
            partial += 1
            await _enqueue_remaining_fields(sessions, lease, **enqueue_kwargs)

        reason = getattr(result, "reason", None)
        if status == "failed_retryable" and reason == "transport_reset":
            stopped_reason = "transport_reset"
            break
        if status == "blocked" and _is_rate_limited(reason):
            stopped_reason = "http_429"
            break
        if status in {"blocked", "needs_login"}:
            if (
                wait_for_login
                and _is_manual_gate(result, provider)
                and lease.task_key not in login_resumed
                and getattr(provider, "has_manual_login_page", False)
            ):
                if not await _confirm_login_or_stop(
                    provider, stop_event, confirmation=login_confirmation
                ):
                    stopped_reason = "manual_login_not_confirmed"
                    break
                await _close_manual_login_page(provider)
                resumed = await resume_web_enrichment_tasks(
                    sessions,
                    lease.run_id,
                    statuses=("blocked", "needs_login"),
                    task_keys=(lease.task_key,),
                )
                if resumed:
                    login_resumed.add(lease.task_key)
                    continue
                stopped_reason = "manual_resume_not_applied"
                break
            stopped_reason = _safe_reason(reason or status)
            break

    unresolved = await _count_unresolved_web_tasks(sessions, normalized_run_ids)
    if stopped_reason is None:
        if stop_event is not None and stop_event.is_set():
            stopped_reason = "stop_requested"
        else:
            stopped_reason = "queue_drained_with_unresolved_tasks" if unresolved else "queue_drained"
    return WebWorkerSummary(
        run_ids=normalized_run_ids,
        collected_count=collected,
        completed_count=completed,
        partial_count=partial,
        unresolved_count=unresolved,
        stopped_reason=stopped_reason,
        diagnostics=dict(diagnostics),
        blocked_count=blocked_count,
        retryable_count=retryable_count,
        stopped_run_id=stopped_run_id,
        stopped_target_type=stopped_target_type,
    )


async def run_live_web_fallback_worker(
    sessions: Any,
    run_ids: Callable[[], Sequence[str]],
    *,
    stop_event: Any,
    api_done_event: Any | None = None,
    browser_profile: str | Path,
    request_budget: Any | None = None,
    browser_config: Any | None = None,
    provider_factory: Any | None = None,
    headed: bool = True,
    wait_for_login: bool = True,
    login_confirmation: Any | None = None,
    min_thread_create_time: datetime | None = None,
    priority_min_thread_create_time: datetime | None = None,
    prioritize_comments: bool = False,
    newest_first: bool = False,
    min_ip_create_time: datetime | None = None,
) -> WebWorkerSummary:
    """Run WEB work independently, draining only after normal API completion."""
    if browser_profile is None:
        raise ValueError("an explicit isolated browser_profile is required")
    if wait_for_login and not headed:
        raise ValueError("wait_for_login requires headed=True")
    if stop_event is None or not hasattr(stop_event, "is_set") or not hasattr(stop_event, "wait"):
        raise ValueError("an asyncio stop_event is required")

    if browser_config is None:
        browser_config = _configured_web_settings()
    if request_budget is None:
        request_budget = make_request_budget(float(browser_config.rps))
    factory = provider_factory or _make_browser_provider
    provider = factory(
        budget=request_budget,
        profile_dir=browser_profile,
        settings=browser_config,
        headed=headed,
        keep_login_page=wait_for_login,
    )
    if asyncio.iscoroutine(provider):
        provider = await provider

    collected = 0
    completed = 0
    partial = 0
    diagnostics: Counter[str] = Counter()
    stopped_reason = None
    login_resumed: set[str] = set()
    blocked_count = 0
    retryable_count = 0
    stopped_run_id = None
    stopped_target_type = None
    all_run_ids: set[str] = set()
    last_queue_wait_log = 0.0
    extra_filter_kwargs: dict[str, Any] = (
        {"min_thread_create_time": min_thread_create_time} if min_thread_create_time is not None else {}
    )
    lease_kwargs = dict(extra_filter_kwargs)
    if priority_min_thread_create_time is not None:
        lease_kwargs["priority_min_thread_create_time"] = priority_min_thread_create_time
    if prioritize_comments:
        lease_kwargs["prioritize_comments"] = True
    if newest_first:
        lease_kwargs["newest_first"] = True
    drain_kwargs = dict(lease_kwargs)
    if min_ip_create_time is not None:
        drain_kwargs["min_ip_create_time"] = min_ip_create_time
    lease_seconds = _web_lease_seconds(browser_config)
    if lease_seconds > 240:
        lease_kwargs["lease_seconds"] = lease_seconds
    try:
        while not stop_event.is_set() and not (
            api_done_event is not None and api_done_event.is_set()
        ):
            current_run_ids = await _snapshot_run_ids(run_ids)
            all_run_ids.update(current_run_ids)
            lease = await _lease_one_web_task(
                sessions,
                current_run_ids,
                "live-web-fallback",
                **lease_kwargs,
            )
            if lease is None:
                if api_done_event is not None and api_done_event.is_set():
                    break
                retry_delay = await _next_retry_delay(
                    sessions,
                    current_run_ids,
                    **extra_filter_kwargs,
                )
                now = asyncio.get_running_loop().time()
                if now - last_queue_wait_log >= _QUEUE_WAIT_LOG_INTERVAL_SECONDS:
                    counts = await _web_task_status_counts(sessions, current_run_ids)
                    logger.info(
                        "WEB queue waiting: run_ids=%s reason=no_eligible_lease statuses=%s "
                        "next_due_seconds=%s browser_context_started=%s",
                        current_run_ids,
                        counts,
                        None if retry_delay is None else round(retry_delay, 1),
                        bool(getattr(provider, "browser_started", False)),
                    )
                    last_queue_wait_log = now
                await _wait_for_stop_or_timeout(stop_event, min(retry_delay or 0.5, 1.0))
                continue
            if stop_event.is_set():
                break

            process_kwargs: dict[str, Any] = {"stop_event": stop_event}
            enqueue_kwargs: dict[str, Any] = {}
            if min_ip_create_time is not None:
                process_kwargs["min_ip_create_time"] = min_ip_create_time
                enqueue_kwargs["min_ip_create_time"] = min_ip_create_time

            try:
                status, result = await _process_durable_web_lease(
                    sessions,
                    provider,
                    lease,
                    **process_kwargs,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                result = _status_result("timeout", "web_worker_target_failed")
                status = "failed_retryable"
                await _fail_task(
                    sessions,
                    lease.task_key,
                    lease.lease_token,
                    status=status,
                    reason_code="web_worker_target_failed",
                    field_outcomes=_failure_outcomes(
                        lease.requested_fields, "timeout", "web_worker_target_failed"
                    ),
                    diagnostics={},
                )
            if not getattr(result, "preflight_skipped", False):
                collected += 1
            _add_diagnostics(diagnostics, getattr(result, "diagnostics", {}))
            if status == "failed_retryable":
                retryable_count += 1
            elif status in {"blocked", "needs_login"}:
                blocked_count += 1
                stopped_run_id = lease.run_id
                stopped_target_type = lease.target_type
            if status in {
                "completed",
                "completed_with_absent_fields",
                "unsupported_by_source",
                "not_applicable",
            }:
                completed += 1
            elif status == "partially_completed":
                partial += 1
                await _enqueue_remaining_fields(sessions, lease, **enqueue_kwargs)

            reason = getattr(result, "reason", None)
            if status == "failed_retryable" and reason == "transport_reset":
                stopped_reason = "transport_reset"
                break
            if status == "blocked" and _is_rate_limited(reason):
                stopped_reason = "http_429"
                break
            if status in {"blocked", "needs_login"}:
                if (
                    wait_for_login
                    and _is_manual_gate(result, provider)
                    and lease.task_key not in login_resumed
                    and getattr(provider, "has_manual_login_page", False)
                ):
                    if not await _confirm_login_or_stop(
                        provider, stop_event, confirmation=login_confirmation
                    ):
                        stopped_reason = (
                            "stop_requested_during_login"
                            if stop_event.is_set()
                            else "manual_login_not_confirmed"
                        )
                        break
                    await _close_manual_login_page(provider)
                    resumed = await resume_web_enrichment_tasks(
                        sessions,
                        lease.run_id,
                        statuses=("blocked", "needs_login"),
                        task_keys=(lease.task_key,),
                    )
                    if resumed:
                        login_resumed.add(lease.task_key)
                        continue
                    stopped_reason = "manual_resume_not_applied"
                    break
                stopped_reason = _safe_reason(reason or status)
                break

        if (
            api_done_event is not None
            and api_done_event.is_set()
            and not stop_event.is_set()
            and stopped_reason is None
        ):
            current_run_ids = await _snapshot_run_ids(run_ids)
            all_run_ids.update(current_run_ids)
            drained = await drain_pending_web_enrichment(
                sessions,
                tuple(sorted(all_run_ids)),
                provider=provider,
                stop_event=stop_event,
                wait_for_login=wait_for_login,
                login_confirmation=login_confirmation,
                **drain_kwargs,
            )
            collected += drained.collected_count
            completed += drained.completed_count
            partial += drained.partial_count
            blocked_count += getattr(drained, "blocked_count", 0)
            retryable_count += getattr(drained, "retryable_count", 0)
            stopped_run_id = getattr(drained, "stopped_run_id", None) or stopped_run_id
            stopped_target_type = getattr(drained, "stopped_target_type", None) or stopped_target_type
            _add_diagnostics(diagnostics, drained.diagnostics)
            stopped_reason = (
                "stop_requested_drained"
                if drained.unresolved_count == 0 and not stop_event.is_set()
                else drained.stopped_reason
            )
        elif stop_event.is_set() and stopped_reason is None:
            stopped_reason = "stop_requested"
    finally:
        await _close_provider(provider)

    all_run_ids.update(await _snapshot_run_ids(run_ids))
    unresolved = await _count_unresolved_web_tasks(sessions, tuple(sorted(all_run_ids)))
    return WebWorkerSummary(
        run_ids=tuple(sorted(all_run_ids)),
        collected_count=collected,
        completed_count=completed,
        partial_count=partial,
        unresolved_count=unresolved,
        stopped_reason=stopped_reason
        or ("queue_drained" if unresolved == 0 else "queue_drained_with_unresolved_tasks"),
        diagnostics=dict(diagnostics),
        blocked_count=blocked_count,
        retryable_count=retryable_count,
        stopped_run_id=stopped_run_id,
        stopped_target_type=stopped_target_type,
    )


async def run_web_audit(
    sessions: Any,
    run_id: str,
    requested_fields: tuple[str, ...] | list[str],
    filters: TargetFilters,
    *,
    output_dir: str | Path,
    limit: int = 1,
    max_seconds: int = 120,
    browser_profile: str | Path,
    headed: bool = False,
    wait_for_login: bool = False,
    request_budget: Any | None = None,
    browser_config: Any | None = None,
    provider_factory: Any | None = None,
    login_confirmation: Any | None = None,
) -> Path:
    """Compare a bounded sample without creating tasks or updating canonical rows."""
    _validate_limits(limit, max_seconds)
    if limit > 5:
        raise ValueError("web-audit is limited to at most five targets")
    if browser_profile is None:
        raise ValueError("an explicit isolated --browser-profile is required for web-audit")
    if wait_for_login and not headed:
        raise ValueError("--wait-for-login requires --headed")
    plans = await plan_web_enrichment_targets(
        sessions,
        run_id,
        requested_fields,
        **_filter_options(filters),
        limit=limit,
    )
    if browser_config is None:
        browser_config = _configured_web_settings()
    if request_budget is None:
        request_budget = make_request_budget(float(browser_config.rps))
    deadline = asyncio.get_running_loop().time() + max_seconds
    targets: list[dict[str, Any]] = []
    aggregate: Counter[str] = Counter()
    sole_plan = plans[0] if len(plans) == 1 else None
    precise_target = sole_plan is not None and (
        (
            filters.target_type == "thread"
            and filters.tid == sole_plan.tid
            and sole_plan.target_type == "thread"
        )
        or (
            filters.target_type == sole_plan.target_type
            and filters.target_ids == (sole_plan.target_id,)
        )
    )
    if precise_target and sole_plan is not None:
        precise_target = (
            (filters.tid is None or filters.tid == sole_plan.tid)
            and (filters.fid is None or filters.fid == sole_plan.fid)
            and (filters.parent_pid is None or filters.parent_pid == sole_plan.parent_post_pid)
        )
    allow_blocked_audit = headed and wait_for_login and precise_target
    eligible_plans = [
        plan
        for plan in plans
        if plan.eligibility_status == "pending_web"
        or (allow_blocked_audit and plan.eligibility_status == "blocked")
    ]
    provider = None
    if eligible_plans:
        factory = provider_factory or _make_browser_provider
        provider = factory(
            budget=request_budget,
            profile_dir=browser_profile,
            settings=browser_config,
            headed=headed,
            keep_login_page=wait_for_login,
        )
        if asyncio.iscoroutine(provider):
            provider = await provider
    try:
        for plan in plans:
            entry = _base_audit_target(plan)
            if plan.eligibility_status != "pending_web" and not (
                allow_blocked_audit and plan.eligibility_status == "blocked"
            ):
                entry["status"] = plan.eligibility_status
                entry["field_statuses"] = plan.field_statuses
                targets.append(entry)
                continue
            if asyncio.get_running_loop().time() >= deadline:
                entry["status"] = "deadline_exhausted"
                targets.append(entry)
                break
            entry["database"] = await _read_canonical_snapshot(sessions, plan)
            if entry["database"].get("status") != "found":
                entry["status"] = entry["database"].get("status", "identity_snapshot_mismatch")
                targets.append(entry)
                continue
            _set_provider_run_scope(provider, run_id)
            result = await _collect_with_deadline(provider, _target_from_plan(plan), deadline)
            _add_diagnostics(aggregate, getattr(result, "diagnostics", {}))
            status = getattr(result, "status", "failed_retryable")
            reason = getattr(result, "reason", None)
            if (
                wait_for_login
                and not _is_rate_limited(reason)
                and _is_manual_gate(result, provider)
                and getattr(provider, "has_manual_login_page", False)
            ):
                remaining = deadline - asyncio.get_running_loop().time()
                confirmed = False
                if remaining > 0:
                    try:
                        confirmed = await _confirm_manual_login(
                            provider,
                            remaining,
                            confirmation=login_confirmation,
                        )
                    except TimeoutError:
                        entry["manual_login_status"] = "timed_out"
                if confirmed:
                    await _close_manual_login_page(provider)
                    entry["manual_login_status"] = "confirmed_retry_once"
                    result = await _collect_with_deadline(provider, _target_from_plan(plan), deadline)
                    _add_diagnostics(aggregate, getattr(result, "diagnostics", {}))
                    status = getattr(result, "status", "failed_retryable")
                    reason = getattr(result, "reason", None)
                elif "manual_login_status" not in entry:
                    entry["manual_login_status"] = "not_confirmed"
            entry["status"] = status
            entry["reason"] = _safe_reason(reason) if reason else None
            identity_verified = _result_identity_verified(result)
            if identity_verified is None and status == "identity_unverified":
                identity_verified = False
            entry["identity_verified"] = identity_verified
            entry["identity_status"] = (
                "verified"
                if identity_verified is True
                else "unverified"
                if identity_verified is False
                else "not_tested"
            )
            entry["diagnostics"] = _safe_diagnostics(getattr(result, "diagnostics", {}))
            entry["fields"] = _audit_field_comparisons(
                plan.requested_fields,
                entry["database"],
                tuple(getattr(result, "candidates", ())),
                getattr(result, "field_outcomes", None),
            )
            targets.append(entry)
            if status in {"blocked", "needs_login"}:
                break
        if len(targets) < len(plans):
            for plan in plans[len(targets) :]:
                entry = _base_audit_target(plan)
                entry["status"] = "not_tested_after_block"
                targets.append(entry)
    finally:
        if provider is not None:
            await _close_provider(provider)

    report = {
        "schema_version": "web-audit-v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "mode": "read_only_audit",
        "canonical_updated": False,
        "task_rows_written": False,
        "observation_rows_written": False,
        "filters": {
            "target_type": filters.target_type,
            "target_ids": [str(value) for value in filters.target_ids] if filters.target_ids else None,
            "tid": str(filters.tid) if filters.tid else None,
            "fid": str(filters.fid) if filters.fid else None,
            "parent_pid": str(filters.parent_pid) if filters.parent_pid else None,
            "requested_fields": list(requested_fields),
            "limit": limit,
        },
        "targets": targets,
        "diagnostics": dict(aggregate),
    }
    return await _write_private_report(Path(output_dir), report)


def make_request_budget(global_rps: float):
    """Build one request gate for all controlled API/navigation actions in a CLI pass."""
    if global_rps <= 0:
        raise ValueError("rate_limit.global_rps must be positive")
    from src.core.auth import GateAwareSemaphore

    async def wait_ready() -> None:
        return None

    async def is_ready() -> bool:
        return True

    semaphore = asyncio.Semaphore(1)
    return GateAwareSemaphore(
        semaphore,
        wait_ready,
        is_ready,
        wait_ready,
        is_ready,
        minimum_interval_seconds=1 / global_rps,
    )


def _configured_web_settings() -> Any:
    from src.core.config import Config

    return Config().pydantic_config.web_fallback


def _make_browser_provider(*, budget: Any, profile_dir: str | Path, settings: Any, headed: bool, keep_login_page: bool):
    from .browser import PlaywrightWebFallbackProvider

    return PlaywrightWebFallbackProvider(
        budget=budget,
        profile_dir=profile_dir,
        max_pages=settings.max_pages,
        max_expansions=settings.max_expansions,
        timeout_seconds=settings.timeout_seconds,
        headless=not headed,
        keep_login_page=keep_login_page,
        reuse_parent_snapshots=getattr(settings, "web_parent_batch_enabled", False),
        parent_snapshot_ttl_seconds=float(getattr(settings, "parent_snapshot_ttl_seconds", 30.0)),
        parent_snapshot_max_targets=int(getattr(settings, "parent_snapshot_max_targets", 8)),
        block_nonessential_resources=bool(getattr(settings, "block_nonessential_resources", False)),
    )


def _filter_options(filters: TargetFilters) -> dict[str, Any]:
    return {
        "target_type": filters.target_type,
        "target_ids": filters.target_ids,
        "tid": filters.tid,
        "fid": filters.fid,
        "parent_pid": filters.parent_pid,
    }


def _validate_limits(limit: int, max_seconds: int) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer")
    if isinstance(max_seconds, bool) or not isinstance(max_seconds, int) or max_seconds < 1:
        raise ValueError("max_seconds must be a positive integer")


async def _collect_with_deadline(provider: Any, target: Target, deadline: float):
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        return _status_result("timeout", "max_seconds_exhausted")
    try:
        async with asyncio.timeout(remaining):
            return await provider.collect(target)
    except TimeoutError:
        return _status_result("timeout", "max_seconds_exhausted")
    except _TRANSPORT_ERRORS as error:
        return _transport_reset_result(error, "web_provider_collect")


def _set_provider_run_scope(provider: Any, run_id: str) -> None:
    set_scope = getattr(provider, "set_run_scope", None)
    if callable(set_scope):
        set_scope(run_id)


def _transport_reset_result(error: BaseException, component: str):
    from .browser import BrowserRunResult

    return BrowserRunResult(
        "failed_retryable",
        "transport_reset",
        (),
        {
            "transport_error_type": type(error).__name__,
            "transport_component": component,
            "transport_error_at": datetime.now(UTC).isoformat(),
        },
    )


def _status_result(status: str, reason: str):
    from .browser import BrowserRunResult

    return BrowserRunResult(status, reason, (), {"status": status})


def _not_observed_outcomes(requested_fields, candidates, field_outcomes):
    provided = dict(field_outcomes or {})
    present = {candidate.field_name for candidate in candidates}
    for field_name in requested_fields:
        if field_name not in present and field_name not in provided:
            provided[field_name] = {
                "status": "not_tested",
                "reason": "not_observed_in_bounded_page",
            }
    return provided


def _failure_outcomes(requested_fields, status: str, reason: str | None) -> dict[str, dict[str, str]]:
    field_status = {
        "needs_login": "needs_login",
        "blocked": "blocked",
        "identity_unverified": "identity_unverified",
        "unsupported_by_source": "unsupported_by_source",
        "not_applicable": "not_applicable",
        "failed_retryable": "failed_retryable",
        "failed_final": "failed_final",
    }.get(_task_failure_status(status, reason), "not_tested")
    reason_code = _safe_reason(reason or status)
    return {name: {"status": field_status, "reason": reason_code} for name in requested_fields}


def _task_failure_status(status: str, reason: str | None) -> str:
    if status == "blocked":
        if _is_rate_limited(reason):
            return "blocked"
        if _is_login_or_verification(status, reason):
            return "needs_login"
        return "blocked"
    if status == "needs_login":
        return "needs_login"
    if status == "identity_unverified":
        return "identity_unverified"
    if status == "not_applicable":
        return "not_applicable"
    if status == "unsupported_by_source":
        return "unsupported_by_source"
    if status in {"timeout", "dom_changed", "failed_retryable", "transport_reset"}:
        return "failed_retryable"
    return "failed_final"


def _is_rate_limited(reason: str | None) -> bool:
    return reason == "http_429"


def _is_login_or_verification(status: str, reason: str | None) -> bool:
    return status == "needs_login" or reason in {
        "needs_login",
        "http_auth_or_verification",
        "official_verification",
    }


def _is_manual_gate(result: Any, provider: Any) -> bool:
    status = getattr(result, "status", None)
    reason = getattr(result, "reason", None)
    diagnostics = getattr(result, "diagnostics", {})
    return (
        not _is_rate_limited(reason)
        and status in {"blocked", "needs_login"}
        and reason in {"blocked", "needs_login", "security_verification", "official_verification"}
        and isinstance(diagnostics, dict)
        and diagnostics.get("manual_gate_detected") is True
        and getattr(provider, "has_manual_login_page", False) is True
    )


def _safe_reason(value: str | None) -> str:
    if not value:
        return "unknown"
    safe = "".join(char for char in value[:128] if char.isalnum() or char in "_.-")
    return safe or "unknown"


def _add_diagnostics(counter: Counter[str], values: Any) -> None:
    if not isinstance(values, dict):
        return
    for name in (
        "requests",
        "nested_floor_requests",
        "user_card_requests",
        "pages",
        "expansions",
        "budgeted_actions",
        "observed_subresource_requests",
        "http_429",
        "auth_pauses",
        "page_cache_hits",
        "post_owner_scrolls",
        "growth_hover_attempts",
        "parent_snapshot_hits",
        "parent_snapshot_misses",
    ):
        value = values.get(name)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            counter[name] += value
    elapsed = values.get("elapsed_seconds")
    if isinstance(elapsed, (float, int)) and not isinstance(elapsed, bool) and elapsed >= 0:
        counter["elapsed_seconds"] += float(elapsed)


def _safe_diagnostics(values: Any) -> dict[str, Any]:
    if not isinstance(values, dict):
        return {}
    output: dict[str, Any] = {}
    for name in (
        "requests",
        "nested_floor_requests",
        "user_card_requests",
        "pages",
        "expansions",
        "budgeted_actions",
        "observed_subresource_requests",
        "http_429",
        "auth_pauses",
        "page_cache_hits",
        "post_owner_scrolls",
        "growth_hover_attempts",
    ):
        value = values.get(name)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            output[name] = value
    elapsed = values.get("elapsed_seconds")
    if isinstance(elapsed, (float, int)) and not isinstance(elapsed, bool) and elapsed >= 0:
        output["elapsed_seconds"] = round(float(elapsed), 3)
    for name in ("status", "blocked_reason", "user_card_status", "user_card_reason"):
        value = values.get(name)
        if isinstance(value, str):
            output[name] = _safe_reason(value)
    if values.get("manual_gate_detected") is True:
        output["manual_gate_detected"] = True
    for name in ("transport_error_type", "transport_component", "operation_phase"):
        value = values.get(name)
        if isinstance(value, str):
            output[name] = _safe_reason(value)
    error_at = values.get("transport_error_at")
    if isinstance(error_at, str):
        try:
            parsed = datetime.fromisoformat(error_at)
        except ValueError:
            pass
        else:
            if parsed.tzinfo is not None:
                output["transport_error_at"] = parsed.astimezone(UTC).isoformat()
    output.update(sanitize_page_readiness_diagnostics(values))
    return output


async def _complete_task(sessions, task_key, lease_token, candidates, **kwargs):
    from .reconcile import complete_web_enrichment_task

    return await complete_web_enrichment_task(sessions, task_key, lease_token, candidates, **kwargs)


async def _preflight_web_task(
    sessions,
    lease,
    *,
    min_ip_create_time: datetime | None = None,
):
    from .reconcile import preflight_web_enrichment_task

    return await preflight_web_enrichment_task(
        sessions,
        lease.task_key,
        lease.lease_token,
        min_ip_create_time=min_ip_create_time,
    )


def _already_satisfied_outcomes(fields):
    return {
        field_name: {"status": "confirmed", "reason": "already_satisfied"}
        for field_name in fields
    }


def _preflight_satisfied_outcomes(preflight):
    preserved = dict(preflight.preserved_outcomes or {})
    return {
        **_already_satisfied_outcomes(
            tuple(name for name in preflight.satisfied_fields if name not in preserved)
        ),
        **preserved,
    }


async def _fail_task(sessions, task_key, lease_token, **kwargs):
    from .reconcile import fail_web_enrichment_task

    return await fail_web_enrichment_task(sessions, task_key, lease_token, **kwargs)


async def _confirm_manual_login(provider: Any, timeout_seconds: float, *, confirmation: Any | None = None) -> bool:
    if confirmation is not None:
        result = confirmation(provider, timeout_seconds)
        if asyncio.iscoroutine(result):
            async with asyncio.timeout(timeout_seconds):
                return bool(await result)
        return bool(result)
    if not getattr(provider, "has_manual_login_page", False):
        return False
    print(
        "在当前项目隔离的浏览器窗口完成官方登录或验证后，回到交互式 Terminal 按 Enter 继续；"
        "等待时间计入 --max-seconds，Ctrl-C 可取消。"
    )
    try:
        return await _wait_for_terminal_enter(timeout_seconds)
    except (OSError, ValueError, NotImplementedError, TimeoutError):
        print("当前控制台不支持可取消的终端确认；请在交互式 Terminal 运行或取消本次任务。", file=sys.stderr)
        return False


async def _wait_for_stop_or_timeout(stop_event: Any, timeout_seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=max(0.05, timeout_seconds))
    except TimeoutError:
        pass


async def _wait_for_terminal_enter(timeout_seconds: float, *, stream: Any | None = None) -> bool:
    """Wait for Enter without spawning a blocking thread that can outlive cancellation."""
    stream = stream or sys.stdin
    if os.name == "nt":
        if not getattr(stream, "isatty", lambda: False)():
            return False
        return await _wait_for_windows_terminal_enter(timeout_seconds)
    fd = stream.fileno()
    loop = asyncio.get_running_loop()
    future: asyncio.Future[bool] = loop.create_future()
    pending = bytearray()

    def read_input() -> None:
        try:
            chunk = os.read(fd, 4096)
        except BlockingIOError:
            return
        except OSError as error:
            if not future.done():
                future.set_exception(error)
            return
        if not chunk:
            if not future.done():
                future.set_result(False)
            return
        pending.extend(chunk)
        if b"\n" in pending or b"\r" in pending:
            pending.clear()
            if not future.done():
                future.set_result(True)

    loop.add_reader(fd, read_input)
    try:
        try:
            async with asyncio.timeout(timeout_seconds):
                return await future
        except TimeoutError:
            return False
    finally:
        loop.remove_reader(fd)


async def _wait_for_windows_terminal_enter(timeout_seconds: float) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, timeout_seconds)
    while True:
        if _windows_console_enter_pressed():
            return True
        remaining = deadline - loop.time()
        if remaining <= 0:
            return False
        await asyncio.sleep(min(0.05, remaining))


def _windows_console_enter_pressed() -> bool:
    import msvcrt

    while msvcrt.kbhit():
        if msvcrt.getwch() in {"\r", "\n"}:
            return True
    return False


async def _close_manual_login_page(provider: Any) -> None:
    close_page = getattr(provider, "close_manual_login_page", None)
    if close_page is None:
        return
    result = close_page()
    if asyncio.iscoroutine(result):
        await result


async def _close_provider(provider: Any) -> None:
    close = getattr(provider, "close", None)
    if close is None:
        return
    result = close()
    if asyncio.iscoroutine(result):
        await result


def _base_audit_target(plan: WebEnrichmentTargetPlan) -> dict[str, Any]:
    from ..urls import canonical_thread_url

    return {
        "target_type": plan.target_type,
        "target_id": str(plan.target_id),
        "tid": str(plan.tid),
        "pid": str(plan.pid) if plan.pid is not None else None,
        "cid": str(plan.cid) if plan.cid is not None else None,
        "parent_post_pid": str(plan.parent_post_pid) if plan.parent_post_pid is not None else None,
        "author_user_id": str(plan.author_user_id),
        "fid": str(plan.fid),
        "canonical_thread_url": canonical_thread_url(plan.tid),
        "requested_fields": list(plan.requested_fields),
        "eligibility_status": plan.eligibility_status,
    }


async def _read_canonical_snapshot(sessions: Any, plan: WebEnrichmentTargetPlan) -> dict[str, Any]:
    from ..exporter import _csv_value
    from ..models import ResearchComment, ResearchPost, ResearchThread

    model, id_field = {
        "thread": (ResearchThread, ResearchThread.tid),
        "post": (ResearchPost, ResearchPost.pid),
        "comment": (ResearchComment, ResearchComment.cid),
    }[plan.target_type]
    async with sessions() as session:
        record = await session.scalar(select(model).where(id_field == plan.target_id))
    if record is None:
        return {"status": "target_missing"}
    actual_target_id = getattr(record, id_field.key, None)
    if (
        actual_target_id != plan.target_id
        or record.tid != plan.tid
        or record.fid != plan.fid
        or record.author_user_id != plan.author_user_id
        or (plan.author_portrait is not None and record.author_portrait != plan.author_portrait)
        or _utc_datetime(record.observed_at) != _utc_datetime(plan.target_observed_at)
    ):
        return {"status": "identity_snapshot_mismatch"}
    fields = tuple(plan.requested_fields)
    field_sources = dict(record.field_sources_json or {})
    missing_reasons = dict(record.missing_reasons_json or {})
    comparisons = {}
    for field_name in fields:
        value = getattr(record, field_name, None)
        comparisons[field_name] = {
            "canonical_value": value,
            "csv_value": _csv_value(value),
            "api_missing_reason": missing_reasons.get(field_name),
            "field_source_metadata": field_sources.get(field_name),
        }
    links = {}
    for name in (
        "thread_url",
        "thread_url_source",
        "thread_url_validation_status",
    ):
        if hasattr(record, name):
            links[name] = getattr(record, name)
    return {
        "status": "found",
        "source_api": record.source_api,
        "observed_at": record.observed_at.isoformat() if record.observed_at else None,
        "fields": comparisons,
        "urls": links,
    }


def _utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _audit_field_comparisons(requested_fields, database, candidates, field_outcomes):
    outcomes = _not_observed_outcomes(requested_fields, candidates, field_outcomes)
    candidates_by_field: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        identity = candidate.identity
        candidates_by_field.setdefault(candidate.field_name, []).append({
            "raw_value": candidate.raw_value,
            "normalized_value": candidate.normalized_value,
            "source_kind": candidate.source_kind,
            "source_path": candidate.source_path,
            "source_url": candidate.source_url,
            "observed_at": candidate.observed_at.isoformat(),
            "identity_verified": identity.identity_verified,
            "identity_match_methods": list(identity.match_methods),
        })
    output = {}
    db_fields = database.get("fields", {}) if isinstance(database, dict) else {}
    for name in requested_fields:
        value = db_fields.get(name, {})
        candidates_for_field = candidates_by_field.get(name, [])
        canonical = value.get("canonical_value") if isinstance(value, dict) else None
        candidate_values = {
            json.dumps(item["normalized_value"], sort_keys=True, ensure_ascii=False) for item in candidates_for_field
        }
        api_missing_reason = value.get("api_missing_reason") if isinstance(value, dict) else None
        metadata = value.get("field_source_metadata") if isinstance(value, dict) else None
        verified_web = metadata.get("verified_web") if isinstance(metadata, dict) else None
        unverified_zero_reason = api_missing_reason in {
            "zero_presence_unverified",
            "zero_presence_unverified_in_parsed_object",
        }
        legacy_bawu_false = name == "is_bawu" and canonical is False and not isinstance(verified_web, dict)
        unknown_zero = (canonical == 0 and unverified_zero_reason) or legacy_bawu_false
        known_current = canonical is not None and canonical != "" and not unknown_zero
        conflicts = (
            any(
                json.dumps(cand, sort_keys=True, ensure_ascii=False)
                != json.dumps(canonical, sort_keys=True, ensure_ascii=False)
                for cand in (item["normalized_value"] for item in candidates_for_field)
            )
            if known_current
            else False
        )
        output[name] = {
            "database_value": canonical,
            "csv_value": value.get("csv_value") if isinstance(value, dict) else None,
            "api_missing_reason": api_missing_reason,
            "database_value_status": "unknown_zero" if unknown_zero else "known_or_missing",
            "web_candidates": candidates_for_field,
            "web_candidate_values_distinct": len(candidate_values),
            "conflict_with_known_database_value": conflicts,
            "field_outcome": outcomes.get(name),
        }
    return output


def _result_identity_verified(result: Any) -> bool | None:
    value = getattr(result, "identity_verified", None)
    return value if isinstance(value, bool) else None


async def _write_private_report(directory: Path, report: dict[str, Any]) -> Path:
    return await asyncio.to_thread(_write_private_report_sync, directory, report)


def _write_private_report_sync(directory: Path, report: dict[str, Any]) -> Path:
    if directory.is_symlink():
        raise ValueError("audit report directory must not be a symbolic link")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not directory.is_dir():
        raise ValueError("audit report path must be a directory")
    directory.chmod(0o700)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    path = directory / f"web_audit_{stamp}_{uuid4().hex}.json"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(report, stream, ensure_ascii=False, separators=(",", ":"), default=_json_default)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, (set, tuple)):
        return list(value)
    return str(value)


__all__ = [
    "FieldBackfillSummary",
    "TargetFilters",
    "WebWorkerSummary",
    "drain_pending_web_enrichment",
    "make_request_budget",
    "normalize_target_filters",
    "run_field_backfill",
    "run_live_web_fallback_worker",
    "run_web_audit",
]
