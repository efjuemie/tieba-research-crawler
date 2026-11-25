"""Live API-to-web fallback orchestration for normal scraper runs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import or_, select

from src.core.datastore import RESEARCH_RUN_RESUMABLE_STATUSES
from src.core.models import ResearchRun

from ..models import ResearchWebEnrichmentTask
from .tasks import EnqueueSummary, enqueue_web_enrichment_tasks

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Sequence
    from datetime import datetime
    from typing import Any

MISSING_CONTENT_FIELDS = (
    "global_growth_level",
    "global_growth_icon_url",
    "ip_location_text",
)
_TERMINAL_WEB_TASK_STATUSES = ("completed", "cancelled", "unsupported_by_source", "not_applicable")
_CANCELLED_RUN_STATUSES = {"cancelled", "canceled"}


async def enqueue_missing_web_targets(
    sessions: Any,
    run_id: str | None,
    *,
    thread_ids: Sequence[int] = (),
    post_ids: Sequence[int] = (),
    comment_ids: Sequence[int] = (),
) -> tuple[EnqueueSummary, ...]:
    """Durably enqueue just-persisted targets whose API fields are still missing."""
    if not run_id:
        return ()

    summaries: list[EnqueueSummary] = []
    for target_type, target_ids in (
        ("thread", thread_ids),
        ("post", post_ids),
        ("comment", comment_ids),
    ):
        ids = tuple(sorted({int(target_id) for target_id in target_ids if int(target_id) > 0}))
        if not ids:
            continue
        summary = await enqueue_web_enrichment_tasks(
            sessions,
            run_id,
            MISSING_CONTENT_FIELDS,
            target_type=target_type,
            target_ids=ids,
            only_missing_fields=True,
            pair_growth_fields=True,
        )
        summaries.append(summary)
    return tuple(summaries)


async def discover_missing_web_targets(
    sessions: Any,
    run_ids: Sequence[str],
    *,
    min_thread_create_time: datetime | None = None,
) -> int:
    """Rescan persisted canonical rows after API producers stop or at startup."""
    created = 0
    extra_kwargs: dict[str, Any] = (
        {"min_thread_create_time": min_thread_create_time} if min_thread_create_time is not None else {}
    )
    for run_id in tuple(dict.fromkeys(run_ids)):
        for target_type in ("thread", "post", "comment"):
            summary = await enqueue_web_enrichment_tasks(
                sessions,
                run_id,
                MISSING_CONTENT_FIELDS,
                target_type=target_type,
                only_missing_fields=True,
                pair_growth_fields=True,
                **extra_kwargs,
            )
            created += summary.created_count
    return created


async def pending_web_run_ids(
    sessions: Any,
    active_run_id: str | None = None,
    *,
    responsible_forums: Sequence[tuple[int, str]] | None = None,
) -> tuple[str, ...]:
    """Find current, recoverable, and durable WEB runs for this forum scope."""
    async with sessions() as session:
        task_rows = await session.scalars(
            select(ResearchWebEnrichmentTask.run_id)
            .where(
                ResearchWebEnrichmentTask.status.not_in(_TERMINAL_WEB_TASK_STATUSES)
            )
            .distinct()
        )
        task_run_ids = set(task_rows.all())
        eligible_run_conditions = [ResearchRun.status.in_(RESEARCH_RUN_RESUMABLE_STATUSES)]
        if task_run_ids:
            eligible_run_conditions.append(ResearchRun.run_id.in_(task_run_ids))
        if active_run_id:
            eligible_run_conditions.append(ResearchRun.run_id == active_run_id)
        runs = await session.scalars(
            select(ResearchRun).where(
                or_(*eligible_run_conditions),
            )
        )
        stored_runs = runs.all()

    expected_forums = None
    if responsible_forums is not None:
        expected_forums = tuple(sorted({(int(fid), str(fname)) for fid, fname in responsible_forums}))

    run_ids = set()
    for run in stored_runs:
        if run.status in _CANCELLED_RUN_STATUSES:
            continue
        is_current = run.run_id == active_run_id
        is_resumable = run.status in RESEARCH_RUN_RESUMABLE_STATUSES
        has_durable_web_work = run.run_id in task_run_ids and run.status not in _CANCELLED_RUN_STATUSES
        if not (is_current or is_resumable or has_durable_web_work):
            continue
        if expected_forums is not None:
            try:
                stored_forums = tuple(
                    sorted(
                        {
                            (int(forum["fid"]), str(forum["fname"]))
                            for forum in run.forums_json
                        }
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
            if stored_forums != expected_forums:
                continue
        run_ids.add(run.run_id)
    return tuple(sorted(run_ids))


async def run_live_web_worker(
    sessions: Any,
    run_ids: Sequence[str],
    *,
    stop_event: asyncio.Event,
    api_done_event: asyncio.Event | None = None,
    browser_profile: str,
    request_budget: Any | None = None,
    browser_config: Any | None = None,
    min_thread_create_time: datetime | None = None,
    newest_first: bool = False,
) -> Any:
    """Run WEB work alongside API producers, then drain only on normal API completion."""
    from .service import run_live_web_fallback_worker

    stable_run_ids = tuple(dict.fromkeys(run_ids))
    extra_kwargs: dict[str, Any] = (
        {"min_thread_create_time": min_thread_create_time} if min_thread_create_time is not None else {}
    )
    if newest_first:
        extra_kwargs["newest_first"] = True
    return await run_live_web_fallback_worker(
        sessions,
        lambda: stable_run_ids,
        stop_event=stop_event,
        api_done_event=api_done_event,
        browser_profile=browser_profile,
        request_budget=request_budget,
        browser_config=browser_config,
        headed=True,
        wait_for_login=True,
        **extra_kwargs,
    )


__all__ = [
    "MISSING_CONTENT_FIELDS",
    "discover_missing_web_targets",
    "enqueue_missing_web_targets",
    "pending_web_run_ids",
    "run_live_web_worker",
]
