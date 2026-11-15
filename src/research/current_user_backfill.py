"""Bounded current-profile and forum-snapshot backfills for existing targets."""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass
from typing import Any

from sqlalchemy import func, select, union_all
from sqlalchemy.orm import aliased

from src.core.models import ResearchRun, ResearchTaskScope

from .models import ResearchComment, ResearchPost, ResearchThread
from .persistence import persist_research_page


@dataclass(frozen=True, slots=True)
class CurrentUserTarget:
    user_id: int
    fid: int
    portrait: str | None


@dataclass(frozen=True, slots=True)
class CurrentUserBackfillSummary:
    run_id: str
    selected_targets: int
    unique_users: int
    profile_observations: int
    forum_snapshots: int
    bawu_snapshots: int
    blocked: bool
    stopped_reason: str | None
    statuses: dict[str, int]


async def run_current_user_backfill(
    sessions: Any,
    client: Any,
    run_id: str,
    *,
    tid: int | None = None,
    fid: int | None = None,
    limit: int = 5,
    max_seconds: int = 120,
    profile: bool = False,
    forum: bool = False,
    bawu: bool = False,
    persist: Any = persist_research_page,
) -> CurrentUserBackfillSummary:
    """Fetch only current user records; content authors and historical rows stay untouched."""
    if not any((profile, forum, bawu)):
        raise ValueError("select at least one of --profile, --forum, or --bawu")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer")
    if isinstance(max_seconds, bool) or not isinstance(max_seconds, int) or max_seconds < 1:
        raise ValueError("max_seconds must be a positive integer")
    for name, value in (("tid", tid), ("fid", fid)):
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
            raise ValueError(f"{name} must be a positive integer")

    targets = await _select_targets(
        sessions, run_id, tid=tid, fid=fid, limit=limit, profile_only=profile and not (forum or bawu)
    )
    user_ids = tuple(sorted({target.user_id for target in targets}))
    profile_count = 0
    forum_count = 0
    bawu_count = 0
    statuses: Counter[str] = Counter()
    blocked = False
    stopped_reason = None
    deadline = asyncio.get_running_loop().time() + max_seconds

    if profile:
        for user_id in user_ids:
            if not _has_time(deadline):
                stopped_reason = "max_seconds_exhausted"
                break
            page = await _fetch_with_deadline(
                lambda user_id=user_id: client.get_user_info_research(user_id),
                deadline,
            )
            if page is None:
                stopped_reason = "max_seconds_exhausted"
                break
            dto = page.dto
            if dto.status == "blocked":
                statuses[dto.status] += 1
                blocked = True
                stopped_reason = "blocked"
                break
            if not _user_dto_identity_matches(dto, user_id):
                statuses["identity_unverified"] += 1
                continue
            statuses[dto.status] += 1
            if dto.status == "available":
                await persist(sessions, page)
                profile_count += 1

    if not blocked and (forum or bawu):
        for target in targets:
            if not _has_time(deadline):
                stopped_reason = "max_seconds_exhausted"
                break
            if forum:
                page = await _fetch_with_deadline(
                    lambda target=target: client.get_user_forum_info_research(
                        target.fid,
                        target.user_id,
                        target.portrait,
                    ),
                    deadline,
                )
                if page is None:
                    stopped_reason = "max_seconds_exhausted"
                    break
                dto = page.dto
                if dto.status == "blocked":
                    statuses[dto.status] += 1
                    blocked = True
                    stopped_reason = "blocked"
                    break
                if not _forum_dto_identity_matches(dto, target):
                    statuses["identity_unverified"] += 1
                else:
                    statuses[dto.status] += 1
                    if dto.status == "available":
                        await persist(sessions, page)
                        forum_count += 1
            if bawu:
                if not _has_time(deadline):
                    stopped_reason = "max_seconds_exhausted"
                    break
                page = await _fetch_with_deadline(
                    lambda target=target: client.get_bawu_info_research(target.fid, target.user_id),
                    deadline,
                )
                if page is None:
                    stopped_reason = "max_seconds_exhausted"
                    break
                dto = page.dto
                if dto.status == "blocked":
                    statuses[dto.status] += 1
                    blocked = True
                    stopped_reason = "blocked"
                    break
                if not _bawu_dto_identity_matches(dto, target):
                    statuses["identity_unverified"] += 1
                    continue
                statuses[dto.status] += 1
                if dto.status in {"available", "not_listed"}:
                    await persist(sessions, page)
                    bawu_count += 1
            if blocked:
                break

    return CurrentUserBackfillSummary(
        run_id=run_id,
        selected_targets=len(targets),
        unique_users=len(user_ids),
        profile_observations=profile_count,
        forum_snapshots=forum_count,
        bawu_snapshots=bawu_count,
        blocked=blocked,
        stopped_reason=stopped_reason,
        statuses=dict(statuses),
    )


async def _select_targets(
    sessions: Any,
    run_id: str,
    *,
    tid: int | None,
    fid: int | None,
    limit: int,
    profile_only: bool,
) -> tuple[CurrentUserTarget, ...]:
    async with sessions() as session:
        run = await session.get(ResearchRun, run_id)
        if run is None:
            raise ValueError("Unknown research run")

        thread_scope = aliased(ResearchTaskScope)
        post_scope = aliased(ResearchTaskScope)
        comment_scope = aliased(ResearchTaskScope)
        comment_thread_scope = aliased(ResearchTaskScope)

        thread_rows = (
            select(
                ResearchThread.author_user_id.label("user_id"),
                ResearchThread.fid.label("fid"),
                ResearchThread.author_portrait.label("portrait"),
                ResearchThread.observed_at.label("observed_at"),
            )
            # Only rows in the run's current generation qualify.
            .join(
                thread_scope,
                (thread_scope.run_id == run_id)
                & (thread_scope.tid == ResearchThread.tid)
                & (thread_scope.fid == ResearchThread.fid)
                & (thread_scope.scope_type == "thread_replies")
                & (thread_scope.status == "completed"),
            )
            .where(
                thread_scope.generation
                == select(func.max(ResearchTaskScope.generation))
                .where(
                    ResearchTaskScope.run_id == run_id,
                    ResearchTaskScope.tid == ResearchThread.tid,
                    ResearchTaskScope.fid == ResearchThread.fid,
                    ResearchTaskScope.scope_type == "thread_replies",
                )
                .scalar_subquery()
            )
            .where(ResearchThread.author_user_id.is_not(None), ResearchThread.fid.is_not(None))
        )
        post_rows = (
            select(
                ResearchPost.author_user_id.label("user_id"),
                ResearchPost.fid.label("fid"),
                ResearchPost.author_portrait.label("portrait"),
                ResearchPost.observed_at.label("observed_at"),
            )
            .join(
                post_scope,
                (post_scope.run_id == run_id)
                & (post_scope.tid == ResearchPost.tid)
                & (post_scope.fid == ResearchPost.fid)
                & (post_scope.scope_type == "thread_replies")
                & (post_scope.status == "completed"),
            )
            .where(
                post_scope.generation
                == select(func.max(ResearchTaskScope.generation))
                .where(
                    ResearchTaskScope.run_id == run_id,
                    ResearchTaskScope.tid == ResearchPost.tid,
                    ResearchTaskScope.fid == ResearchPost.fid,
                    ResearchTaskScope.scope_type == "thread_replies",
                )
                .scalar_subquery()
            )
            .where(ResearchPost.author_user_id.is_not(None), ResearchPost.fid.is_not(None))
        )
        comment_rows = (
            select(
                ResearchComment.author_user_id.label("user_id"),
                ResearchComment.fid.label("fid"),
                ResearchComment.author_portrait.label("portrait"),
                ResearchComment.observed_at.label("observed_at"),
            )
            .join(
                comment_scope,
                (comment_scope.run_id == run_id)
                & (comment_scope.tid == ResearchComment.tid)
                & (comment_scope.parent_pid == ResearchComment.parent_post_pid)
                & (comment_scope.fid == ResearchComment.fid)
                & (comment_scope.scope_type == "post_comments")
                & (comment_scope.status == "completed"),
            )
            .join(
                comment_thread_scope,
                (comment_thread_scope.run_id == run_id)
                & (comment_thread_scope.tid == ResearchComment.tid)
                & (comment_thread_scope.fid == ResearchComment.fid)
                & (comment_thread_scope.scope_type == "thread_replies")
                & (comment_thread_scope.status == "completed")
                & (comment_scope.parent_generation == comment_thread_scope.generation),
            )
            .where(
                comment_thread_scope.generation
                == select(func.max(ResearchTaskScope.generation))
                .where(
                    ResearchTaskScope.run_id == run_id,
                    ResearchTaskScope.tid == ResearchComment.tid,
                    ResearchTaskScope.fid == ResearchComment.fid,
                    ResearchTaskScope.scope_type == "thread_replies",
                )
                .scalar_subquery(),
                comment_scope.generation
                == select(func.max(ResearchTaskScope.generation))
                .where(
                    ResearchTaskScope.run_id == run_id,
                    ResearchTaskScope.tid == ResearchComment.tid,
                    ResearchTaskScope.parent_pid == ResearchComment.parent_post_pid,
                    ResearchTaskScope.fid == ResearchComment.fid,
                    ResearchTaskScope.parent_generation == comment_thread_scope.generation,
                    ResearchTaskScope.scope_type == "post_comments",
                )
                .scalar_subquery(),
            )
            .where(ResearchComment.author_user_id.is_not(None), ResearchComment.fid.is_not(None))
        )
        if tid is not None:
            thread_rows = thread_rows.where(ResearchThread.tid == tid)
            post_rows = post_rows.where(ResearchPost.tid == tid)
            comment_rows = comment_rows.where(ResearchComment.tid == tid)
        if fid is not None:
            thread_rows = thread_rows.where(ResearchThread.fid == fid)
            post_rows = post_rows.where(ResearchPost.fid == fid)
            comment_rows = comment_rows.where(ResearchComment.fid == fid)

        target_rows = union_all(thread_rows, post_rows, comment_rows).subquery("current_user_targets")
        if profile_only:
            statement = (
                select(
                    target_rows.c.user_id,
                    func.max(target_rows.c.fid).label("fid"),
                    func.max(target_rows.c.portrait).label("portrait"),
                )
                .group_by(target_rows.c.user_id)
                .order_by(target_rows.c.user_id)
                .limit(limit)
            )
        else:
            statement = (
                select(target_rows.c.user_id, target_rows.c.fid, func.max(target_rows.c.portrait).label("portrait"))
                .group_by(target_rows.c.user_id, target_rows.c.fid)
                .order_by(target_rows.c.user_id, target_rows.c.fid)
                .limit(limit)
            )
        rows = (await session.execute(statement)).all()
    return tuple(CurrentUserTarget(user_id=row.user_id, fid=row.fid, portrait=row.portrait) for row in rows)


async def _fetch_with_deadline(call, deadline: float):
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        return None
    try:
        async with asyncio.timeout(remaining):
            return await call()
    except TimeoutError:
        return None


def _has_time(deadline: float) -> bool:
    return asyncio.get_running_loop().time() < deadline


def _same_identifier(actual: Any, expected: int) -> bool:
    return type(actual) is int and actual == expected


def _user_dto_identity_matches(dto: Any, user_id: int) -> bool:
    if not _same_identifier(getattr(dto, "user_id", None), user_id):
        return False
    response_user_id = getattr(dto, "response_user_id", None)
    if getattr(dto, "status", None) == "available":
        return _same_identifier(response_user_id, user_id)
    return response_user_id is None or _same_identifier(response_user_id, user_id)


def _forum_dto_identity_matches(dto: Any, target: CurrentUserTarget) -> bool:
    return _user_dto_identity_matches(dto, target.user_id) and _same_identifier(getattr(dto, "fid", None), target.fid)


def _bawu_dto_identity_matches(dto: Any, target: CurrentUserTarget) -> bool:
    return _user_dto_identity_matches(dto, target.user_id) and _same_identifier(getattr(dto, "fid", None), target.fid)


__all__ = [
    "CurrentUserBackfillSummary",
    "CurrentUserTarget",
    "run_current_user_backfill",
]
