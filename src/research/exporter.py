"""Stream the local research database into private, UTF-8 CSV files."""

from __future__ import annotations

import asyncio
import csv
import ctypes
import json
import os
import re
import shutil
import sys
import tempfile
import threading
from collections.abc import AsyncIterable, Iterable, Mapping
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import case, func, or_, select
from tiebameow.models.orm import Comment as BaseComment
from tiebameow.models.orm import Post as BasePost
from tiebameow.models.orm import Thread as BaseThread
from tiebameow.models.orm import User as BaseUser

from .models import MediaAsset, ResearchComment, ResearchPost, ResearchThread, ResearchUser, UserForumSnapshot

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    type QualitySummary = Mapping[str, object]

EXPORT_BATCH_SIZE = 1000
_CSV_FIELD_LIMIT_LOCK = threading.Lock()


class ResearchExportValidationError(RuntimeError):
    """Raised when a staged export fails integrity validation."""


THREAD_FIELDS = (
    "tid",
    "first_pid",
    "fid",
    "fname",
    "title",
    "body_text",
    "contents_raw",
    "create_time",
    "create_time_raw",
    "last_time",
    "last_time_raw",
    "author_user_id",
    "author_user_name",
    "author_nickname",
    "author_portrait",
    "author_tieba_uid",
    "forum_level",
    "global_growth_level",
    "is_bawu",
    "author_icons",
    "forum_level_name",
    "role_type",
    "role_name",
    "role_badge_id",
    "role_badge_url",
    "ip_location_text",
    "ip_location_source",
    "ip_location_observed_at",
    "reply_count",
    "agree_count",
    "disagree_count",
    "collect_count",
    "view_count",
    "share_count",
    "thread_type",
    "is_top",
    "is_good",
    "is_livepost",
    "is_hide",
    "is_share",
    "is_help",
    "scraped_at",
    "source_api",
    "observed_at",
    "source_payload_json",
    "missing_reasons_json",
    "record_source",
    "legacy_base_snapshot_json",
)

POST_FIELDS = (
    "pid",
    "tid",
    "fid",
    "fname",
    "floor",
    "is_first_post",
    "create_time",
    "create_time_raw",
    "scraped_at",
    "text",
    "sign",
    "contents_raw",
    "author_user_id",
    "author_user_name",
    "author_nickname",
    "author_portrait",
    "author_tieba_uid",
    "thread_author_user_id",
    "forum_level",
    "global_growth_level",
    "is_bawu",
    "author_icons",
    "forum_level_name",
    "role_type",
    "role_name",
    "role_badge_id",
    "role_badge_url",
    "ip_location_text",
    "ip_location_source",
    "ip_location_observed_at",
    "agree_count",
    "disagree_count",
    "subreply_count_reported",
    "is_thread_author",
    "source_api",
    "observed_at",
    "source_payload_json",
    "missing_reasons_json",
    "record_source",
    "legacy_base_snapshot_json",
)

COMMENT_FIELDS = (
    "cid",
    "tid",
    "parent_post_pid",
    "parent_post_author_user_id",
    "fid",
    "fname",
    "floor",
    "create_time",
    "create_time_raw",
    "scraped_at",
    "text",
    "contents_raw",
    "author_user_id",
    "author_user_name",
    "author_nickname",
    "author_portrait",
    "author_tieba_uid",
    "forum_level",
    "global_growth_level",
    "is_bawu",
    "author_icons",
    "forum_level_name",
    "role_type",
    "role_name",
    "role_badge_id",
    "role_badge_url",
    "ip_location_text",
    "ip_location_source",
    "ip_location_observed_at",
    "reply_to_user_id",
    "reply_to_id_raw",
    "reply_target_source",
    "reply_target_confidence",
    "reply_resolution_note",
    "agree_count",
    "disagree_count",
    "is_thread_author",
    "source_api",
    "observed_at",
    "source_payload_json",
    "missing_reasons_json",
    "record_source",
    "legacy_base_snapshot_json",
)

USER_FIELDS = (
    "user_id",
    "user_name",
    "nickname",
    "portrait",
    "tieba_uid",
    "first_observed_at",
    "last_observed_at",
    "source_api",
    "observed_at",
    "source_payload_json",
    "missing_reasons_json",
    "record_source",
    "legacy_base_snapshot_json",
)

USER_FORUM_SNAPSHOT_FIELDS = (
    "user_id",
    "fid",
    "source_api",
    "observed_at",
    "response_user_id",
    "fname",
    "portrait",
    "display_name",
    "forum_level",
    "global_growth_level",
    "forum_level_name",
    "is_bawu",
    "role_name",
    "role_type",
    "icons",
    "ip_location_text",
    "ip_location_source",
    "status",
    "tieba_uid",
    "source_payload_json",
    "missing_reasons_json",
)

MEDIA_ASSET_FIELDS = (
    "owner_type",
    "owner_id",
    "position",
    "tid",
    "media_type",
    "media_id",
    "media_hash",
    "src_url",
    "original_url",
    "cover_url",
    "source_url",
    "width",
    "height",
    "duration",
    "api_fragment_type",
    "raw_fragment_json",
    "source_api",
    "observed_at",
    "missing_reason",
)

CRAWL_QUALITY_FIELDS = (
    "run_id",
    "scope_type",
    "fid",
    "tid",
    "parent_pid",
    "parent_kind",
    "reported_reply_num",
    "unique_saved_replies",
    "total_pages",
    "success_pages",
    "failure_pages",
    "status",
    "last_checked",
    "count_mismatch",
    "reason",
)


async def export_research_csvs(
    async_sessionmaker_: async_sessionmaker[AsyncSession],
    output_dir: str | Path,
    quality_summaries: Iterable[QualitySummary] | AsyncIterable[QualitySummary] = (),
    *,
    source_run_id: str | None = None,
) -> dict[str, Path]:
    """Write a validated seven-file all-runs batch without materializing tables.

    Extension rows take precedence as a complete research observation: a NULL
    in one of those rows is kept NULL. Legacy base rows are still included and
    marked ``legacy_base_unverified``; their stored values are never used to
    fill NULL extension fields. ``source_run_id`` is filename context only;
    entity tables have no run filter and are exported as all-runs. Pause
    collection while exporting so quality summaries cannot race active writes.
    """
    destination_root = Path(output_dir)
    if await asyncio.to_thread(destination_root.is_symlink):
        raise ResearchExportValidationError("Export root must not be a symbolic link.")
    await asyncio.to_thread(destination_root.mkdir, parents=True, exist_ok=True, mode=0o700)
    if not await asyncio.to_thread(destination_root.is_dir):
        raise ResearchExportValidationError("Export root must be a directory.")
    await asyncio.to_thread(destination_root.chmod, 0o700)

    batch_prefix, export_id, exported_at = _batch_identity(source_run_id)
    batch_directory = destination_root / batch_prefix
    already_exists = await asyncio.to_thread(batch_directory.exists)
    is_symlink = await asyncio.to_thread(batch_directory.is_symlink)
    if already_exists or is_symlink:
        raise ResearchExportValidationError("This export batch already exists; choose a new export root.")

    staging_directory = destination_root / f".{batch_prefix}.staging-{uuid4().hex}.csv"
    await asyncio.to_thread(staging_directory.mkdir, mode=0o700)
    paths = {
        name: staging_directory / f"{name}_{batch_prefix}.csv"
        for name in (
            "threads",
            "posts",
            "comments",
            "users",
            "user_forum_snapshots",
            "media_assets",
            "crawl_quality_report",
        )
    }

    try:
        specs = _query_specs()
        async with async_sessionmaker_() as session:
            await _begin_consistent_snapshot(session)
            expected_counts = await _validate_database(session, specs)
            csv_counts: dict[str, int] = {}
            for name, statement, fields, row_mapper, _unique_fields in specs:
                written = await _write_query_csv(session, paths[name], fields, statement, row_mapper)
                if written != expected_counts[name]:
                    raise ResearchExportValidationError(
                        f"Export validation failed: {name} database and staged row counts differ."
                    )
                parsed = await _count_csv_rows(paths[name], fields)
                if parsed != expected_counts[name]:
                    raise ResearchExportValidationError(
                        f"Export validation failed: {name} database and parsed CSV row counts differ."
                    )
                csv_counts[name] = parsed

            quality_written, quality_summary_count, quality_checked_range = await _write_quality_csv(
                paths["crawl_quality_report"],
                quality_summaries,
                source_run_id=source_run_id,
            )
            quality_parsed = await _count_csv_rows(paths["crawl_quality_report"], CRAWL_QUALITY_FIELDS)
            if quality_parsed != quality_written:
                raise ResearchExportValidationError(
                    "Export validation failed: quality summary and parsed CSV row counts differ."
                )
            csv_counts["crawl_quality_report"] = quality_parsed

        manifest_path = staging_directory / f"manifest_{batch_prefix}.json"
        await _write_manifest(
            manifest_path,
            {
                "export_id": export_id,
                "exported_at": exported_at.isoformat(timespec="microseconds"),
                "entity_scope": "all_database_rows",
                "source_run_id_context_only": source_run_id,
                "csv_files": {name: path.name for name, path in paths.items()},
                "database_row_counts": expected_counts,
                "quality_checkpoint_summary_count": quality_summary_count,
                "csv_row_counts": csv_counts,
                "validation_status": "passed",
                "quality_evidence_status": (
                    "present_not_collection_completeness_proof" if quality_summary_count else "unknown_not_tested"
                ),
                "quality_summary_last_checked_min": quality_checked_range[0],
                "quality_summary_last_checked_max": quality_checked_range[1],
            },
        )
        await asyncio.to_thread(staging_directory.rename, batch_directory)
        return {
            **{name: batch_directory / path.name for name, path in paths.items()},
            "manifest": batch_directory / manifest_path.name,
        }
    finally:
        if await asyncio.to_thread(staging_directory.exists):
            await asyncio.to_thread(shutil.rmtree, staging_directory, ignore_errors=True)


def _batch_identity(source_run_id: str | None) -> tuple[str, str, datetime]:
    safe_run_id = "unknown" if source_run_id is None else re.sub(r"[^A-Za-z0-9_-]+", "-", source_run_id).strip("-_")
    safe_run_id = safe_run_id[:48] or "unknown"
    exported_at = datetime.now(ZoneInfo("Asia/Shanghai"))
    timestamp = exported_at.strftime("%Y%m%d_%H%M%S_%f")
    export_id = uuid4().hex
    return f"all-runs_source-{safe_run_id}_{timestamp}_{export_id}", export_id, exported_at


def _query_specs():
    return [
        ("threads", _thread_query(), THREAD_FIELDS, _thread_row, ("export_id",)),
        ("posts", _post_query(), POST_FIELDS, _post_row, ("export_id",)),
        ("comments", _comment_query(), COMMENT_FIELDS, _comment_row, ("export_id",)),
        ("users", _user_query(), USER_FIELDS, _user_row, ("export_id",)),
        (
            "user_forum_snapshots",
            _snapshot_query(),
            USER_FORUM_SNAPSHOT_FIELDS,
            lambda row: _research_row(row[1], USER_FORUM_SNAPSHOT_FIELDS),
            ("user_id", "fid", "source_api", "observed_at"),
        ),
        (
            "media_assets",
            _media_query(),
            MEDIA_ASSET_FIELDS,
            lambda row: _research_row(row[3], MEDIA_ASSET_FIELDS),
            ("export_owner_type", "export_owner_id", "export_position"),
        ),
    ]


async def _begin_consistent_snapshot(session: AsyncSession) -> None:
    get_bind = getattr(session, "get_bind", None)
    if not callable(get_bind):
        return
    bind = get_bind()
    if getattr(getattr(bind, "dialect", None), "name", None) == "postgresql":
        await session.connection(execution_options={"isolation_level": "REPEATABLE READ"})


async def _validate_database(session: AsyncSession, specs: list[tuple]) -> dict[str, int]:
    expected_counts: dict[str, int] = {}
    for name, statement, _fields, _row_mapper, unique_fields in specs:
        subquery = statement.subquery()
        count = await session.scalar(select(func.count()).select_from(subquery))
        expected_counts[name] = int(count or 0)
        key_columns = [subquery.c[field] for field in unique_fields]
        duplicate_groups = select(*key_columns).group_by(*key_columns).having(func.count() > 1).limit(1).subquery()
        duplicates = await session.scalar(select(func.count()).select_from(duplicate_groups))
        if duplicates:
            raise ResearchExportValidationError(f"Export validation failed: {name} contains duplicate primary keys.")

    await _validate_relationships(session)
    return expected_counts


async def _validate_relationships(session: AsyncSession) -> None:
    threads = _thread_query().subquery("export_threads")
    posts = _post_query().subquery("export_posts")
    comments = _comment_query().subquery("export_comments")

    post_thread_exists = select(1).select_from(threads).where(threads.c.export_id == posts.c.export_tid).exists()
    orphan_posts = await session.scalar(
        select(func.count()).select_from(posts).where(or_(posts.c.export_tid.is_(None), ~post_thread_exists))
    )
    if orphan_posts:
        raise ResearchExportValidationError(
            f"Export validation failed: {int(orphan_posts)} post rows do not resolve to an exported thread."
        )

    comment_post_exists = (
        select(1)
        .select_from(posts)
        .where(
            posts.c.export_id == comments.c.export_parent_pid,
            posts.c.export_tid == comments.c.export_tid,
        )
        .exists()
    )
    comment_first_post_exists = (
        select(1)
        .select_from(threads)
        .where(
            threads.c.export_id == comments.c.export_tid,
            threads.c.export_first_pid == comments.c.export_parent_pid,
        )
        .exists()
    )
    orphan_comments = await session.scalar(
        select(func.count())
        .select_from(comments)
        .where(
            or_(
                comments.c.export_tid.is_(None),
                comments.c.export_parent_pid.is_(None),
                ~(comment_post_exists | comment_first_post_exists),
            )
        )
    )
    if orphan_comments:
        raise ResearchExportValidationError(
            f"Export validation failed: {int(orphan_comments)} comment rows do not resolve to an exported post."
        )


def _thread_query():
    export_id = case(
        (ResearchThread.tid.is_not(None), ResearchThread.tid),
        else_=BaseThread.tid,
    ).label("export_id")
    export_first_pid = case(
        (ResearchThread.tid.is_not(None), ResearchThread.first_pid),
        else_=(
            select(BasePost.pid)
            .where(BasePost.tid == BaseThread.tid, BasePost.floor == 1)
            .order_by(BasePost.create_time.asc(), BasePost.pid.asc())
            .limit(1)
            .scalar_subquery()
        ),
    ).label("export_first_pid")
    author_id = case(
        (ResearchThread.tid.is_not(None), ResearchThread.author_user_id),
        else_=BaseThread.author_id,
    )
    return (
        select(export_id, export_first_pid, BaseThread, ResearchThread, BaseUser)
        .select_from(BaseThread)
        .join(ResearchThread, BaseThread.tid == ResearchThread.tid, full=True)
        .outerjoin(BaseUser, author_id == BaseUser.user_id)
    )


def _post_query():
    tid = case((ResearchPost.pid.is_not(None), ResearchPost.tid), else_=BasePost.tid)
    export_id = case((ResearchPost.pid.is_not(None), ResearchPost.pid), else_=BasePost.pid).label("export_id")
    export_tid = tid.label("export_tid")
    author_id = case(
        (ResearchPost.pid.is_not(None), ResearchPost.author_user_id),
        else_=BasePost.author_id,
    )
    return (
        select(export_id, export_tid, BasePost, ResearchPost, ResearchThread, BaseUser)
        .select_from(BasePost)
        .join(ResearchPost, BasePost.pid == ResearchPost.pid, full=True)
        .outerjoin(ResearchThread, ResearchThread.tid == tid)
        .outerjoin(BaseUser, author_id == BaseUser.user_id)
    )


def _comment_query():
    export_id = case((ResearchComment.cid.is_not(None), ResearchComment.cid), else_=BaseComment.cid).label("export_id")
    export_tid = case(
        (ResearchComment.cid.is_not(None), ResearchComment.tid),
        else_=BaseComment.tid,
    ).label("export_tid")
    export_parent_pid = case(
        (ResearchComment.cid.is_not(None), ResearchComment.parent_post_pid),
        else_=BaseComment.pid,
    ).label("export_parent_pid")
    author_id = case(
        (ResearchComment.cid.is_not(None), ResearchComment.author_user_id),
        else_=BaseComment.author_id,
    )
    return (
        select(export_id, export_tid, export_parent_pid, BaseComment, ResearchComment, BaseUser)
        .select_from(BaseComment)
        .join(ResearchComment, BaseComment.cid == ResearchComment.cid, full=True)
        .outerjoin(BaseUser, author_id == BaseUser.user_id)
    )


def _user_query():
    export_id = case((ResearchUser.user_id.is_not(None), ResearchUser.user_id), else_=BaseUser.user_id).label(
        "export_id"
    )
    return (
        select(export_id, BaseUser, ResearchUser)
        .select_from(BaseUser)
        .join(ResearchUser, BaseUser.user_id == ResearchUser.user_id, full=True)
    )


def _snapshot_query():
    return select(UserForumSnapshot.user_id.label("export_id"), UserForumSnapshot)


def _media_query():
    return select(
        MediaAsset.owner_type.label("export_owner_type"),
        MediaAsset.owner_id.label("export_owner_id"),
        MediaAsset.position.label("export_position"),
        MediaAsset,
    )


def _thread_row(row: Any) -> dict[str, object]:
    _export_id, export_first_pid, base, research, author = row
    if research is not None:
        values = _research_values(research, THREAD_FIELDS)
        values["record_source"] = "research_api"
    else:
        values = {
            "tid": base.tid,
            "first_pid": export_first_pid,
            "fid": base.fid,
            "title": base.title,
            "body_text": base.text,
            "contents_raw": base.contents,
            "create_time": base.create_time,
            "last_time": base.last_time,
            "author_user_id": base.author_id,
            "author_user_name": getattr(author, "user_name", None),
            "author_nickname": getattr(author, "nick_name", None),
            "author_portrait": getattr(author, "portrait", None),
            "forum_level": base.author_level,
            "reply_count": base.reply_num,
            "scraped_at": base.scrape_time,
            "source_api": "legacy_base",
            "missing_reasons_json": _legacy_missing_reasons(),
            "record_source": "legacy_base_unverified",
        }
    values["legacy_base_snapshot_json"] = _legacy_snapshot(base, author)
    return values


def _post_row(row: Any) -> dict[str, object]:
    _export_id, _export_tid, base, research, thread, author = row
    post = research if research is not None else base
    pid = getattr(post, "pid", None)
    floor = getattr(post, "floor", None)
    first_pid_match = thread is not None and thread.first_pid is not None and thread.first_pid == pid
    is_first_post = first_pid_match or floor == 1

    if research is not None:
        values = _research_values(research, POST_FIELDS)
        values["record_source"] = "research_api"
        values["is_first_post"] = is_first_post
    else:
        values = {
            "pid": base.pid,
            "tid": base.tid,
            "fid": base.fid,
            "floor": base.floor,
            "is_first_post": is_first_post,
            "create_time": base.create_time,
            "scraped_at": base.scrape_time,
            "text": base.text,
            "contents_raw": base.contents,
            "author_user_id": base.author_id,
            "author_user_name": getattr(author, "user_name", None),
            "author_nickname": getattr(author, "nick_name", None),
            "author_portrait": getattr(author, "portrait", None),
            "forum_level": base.author_level,
            "subreply_count_reported": base.reply_num,
            "source_api": "legacy_base",
            "missing_reasons_json": _legacy_missing_reasons(),
            "record_source": "legacy_base_unverified",
        }
    values["legacy_base_snapshot_json"] = _legacy_snapshot(base, author)
    return values


def _comment_row(row: Any) -> dict[str, object]:
    _export_id, _export_tid, _export_parent_pid, base, research, author = row
    if research is not None:
        values = _research_values(research, COMMENT_FIELDS)
        values["record_source"] = "research_api"
    else:
        values = {
            "cid": base.cid,
            "tid": base.tid,
            "parent_post_pid": base.pid,
            "fid": base.fid,
            "create_time": base.create_time,
            "scraped_at": base.scrape_time,
            "text": base.text,
            "contents_raw": base.contents,
            "author_user_id": base.author_id,
            "author_user_name": getattr(author, "user_name", None),
            "author_nickname": getattr(author, "nick_name", None),
            "author_portrait": getattr(author, "portrait", None),
            "forum_level": base.author_level,
            "reply_to_user_id": base.reply_to_id,
            "reply_to_id_raw": base.reply_to_id,
            "source_api": "legacy_base",
            "missing_reasons_json": _legacy_missing_reasons(
                reply_to_user_id="reply_target_source_unverified_for_legacy_base_row"
            ),
            "record_source": "legacy_base_unverified",
        }
    values["legacy_base_snapshot_json"] = _legacy_snapshot(base, author)
    return values


def _user_row(row: Any) -> dict[str, object]:
    _export_id, base, research = row
    if research is not None:
        values = _research_values(research, USER_FIELDS)
        values["record_source"] = "research_api"
    else:
        values = {
            "user_id": base.user_id,
            "user_name": base.user_name,
            "nickname": base.nick_name,
            "portrait": base.portrait,
            "source_api": "legacy_base",
            "missing_reasons_json": _legacy_missing_reasons(),
            "record_source": "legacy_base_unverified",
        }
    values["legacy_base_snapshot_json"] = _legacy_snapshot(base)
    return values


def _research_values(record: object, fields: tuple[str, ...]) -> dict[str, object]:
    return {
        field: getattr(record, field, None)
        for field in fields
        if field not in {"record_source", "legacy_base_snapshot_json"}
    }


def _research_row(record: object, fields: tuple[str, ...]) -> dict[str, object]:
    return _research_values(record, fields)


def _legacy_snapshot(record: object | None, author: object | None = None) -> dict[str, object] | None:
    if record is None:
        return None
    snapshot: dict[str, object] = {
        "record": {column.name: getattr(record, column.name) for column in record.__table__.columns}
    }
    if author is not None:
        snapshot["author"] = {column.name: getattr(author, column.name) for column in author.__table__.columns}
    return snapshot


def _legacy_missing_reasons(**extra: str) -> dict[str, str]:
    return {
        "record_source": "legacy_base_without_research_extension",
        **extra,
    }


async def _write_query_csv(
    session: AsyncSession,
    path: Path,
    fields: tuple[str, ...],
    statement: Any,
    row_mapper: Any,
) -> int:
    row_count = 0

    async def write(file: Any) -> None:
        nonlocal row_count
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        result = await session.stream(statement.execution_options(yield_per=EXPORT_BATCH_SIZE))
        try:
            async for row in result:
                writer.writerow(_csv_row(row_mapper(row), fields))
                row_count += 1
        finally:
            await result.close()

    await _write_atomically(path, write)
    return row_count


async def _write_quality_csv(
    path: Path,
    summaries: Iterable[QualitySummary] | AsyncIterable[QualitySummary],
    *,
    source_run_id: str | None,
) -> tuple[int, int, tuple[str | None, str | None]]:
    row_count = 0
    summary_count = 0
    checked_min: str | None = None
    checked_max: str | None = None

    async def write(file: Any) -> None:
        nonlocal row_count, summary_count, checked_min, checked_max
        writer = csv.DictWriter(file, fieldnames=CRAWL_QUALITY_FIELDS, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        if isinstance(summaries, AsyncIterable):
            async for summary in summaries:
                _write_quality_row(writer, summary)
        else:
            for summary in summaries:
                _write_quality_row(writer, summary)
        if summary_count == 0:
            writer.writerow(
                _csv_row(
                    {
                        "run_id": source_run_id,
                        "status": "unknown",
                        "reason": "No checkpoint summaries; run ID is export context only; entity scope is all-runs.",
                    },
                    CRAWL_QUALITY_FIELDS,
                )
            )
            row_count += 1

    def _write_quality_row(writer: csv.DictWriter, summary: Mapping[str, object]) -> None:
        nonlocal row_count, summary_count, checked_min, checked_max
        writer.writerow(_csv_row(dict(summary), CRAWL_QUALITY_FIELDS))
        row_count += 1
        summary_count += 1
        checked_at = summary.get("last_checked")
        if isinstance(checked_at, datetime):
            if checked_at.tzinfo is not None:
                checked_at = checked_at.astimezone(UTC)
            checked_text = checked_at.isoformat()
        elif checked_at is None:
            checked_text = None
        else:
            checked_text = str(checked_at)
        if checked_text is not None:
            checked_min = checked_text if checked_min is None else min(checked_min, checked_text)
            checked_max = checked_text if checked_max is None else max(checked_max, checked_text)

    await _write_atomically(path, write)
    return row_count, summary_count, (checked_min, checked_max)


async def _count_csv_rows(path: Path, fields: tuple[str, ...]) -> int:
    def count_rows() -> int:
        with _CSV_FIELD_LIMIT_LOCK:
            original_limit = csv.field_size_limit()
            try:
                try:
                    csv.field_size_limit(sys.maxsize)
                except OverflowError:
                    c_long_limit = (1 << (ctypes.sizeof(ctypes.c_long) * 8 - 1)) - 1
                    csv.field_size_limit(c_long_limit)

                with path.open(encoding="utf-8-sig", newline="") as csv_file:
                    reader = csv.DictReader(csv_file)
                    if reader.fieldnames != list(fields):
                        raise ResearchExportValidationError("Export validation failed: staged CSV header is incorrect.")
                    return sum(1 for _row in reader)
            finally:
                csv.field_size_limit(original_limit)

    try:
        return await asyncio.to_thread(count_rows)
    except csv.Error:
        raise ResearchExportValidationError("Export validation failed: staged CSV is not parseable.") from None


async def _write_manifest(path: Path, manifest: Mapping[str, object]) -> None:
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as manifest_file:
            temporary_path = Path(manifest_file.name)
            os.fchmod(manifest_file.fileno(), 0o600)
            json.dump(manifest, manifest_file, ensure_ascii=False, separators=(",", ":"))
            manifest_file.write("\n")
            manifest_file.flush()
            await asyncio.to_thread(os.fsync, manifest_file.fileno())
        await asyncio.to_thread(temporary_path.replace, path)
    finally:
        if temporary_path is not None and await asyncio.to_thread(temporary_path.exists):
            await asyncio.to_thread(temporary_path.unlink)


async def _write_atomically(path: Path, writer_callback: Any) -> None:
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8-sig",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as csv_file:
            temporary_path = Path(csv_file.name)
            os.fchmod(csv_file.fileno(), 0o600)
            await writer_callback(csv_file)
            csv_file.flush()
            await asyncio.to_thread(os.fsync, csv_file.fileno())

        await asyncio.to_thread(temporary_path.replace, path)
    finally:
        if temporary_path is not None and await asyncio.to_thread(temporary_path.exists):
            await asyncio.to_thread(temporary_path.unlink)


def _csv_row(row: Mapping[str, object], fields: tuple[str, ...]) -> dict[str, str]:
    return {field: _csv_value(row.get(field)) for field in fields}


def _csv_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=_json_default)
    if isinstance(value, str):
        return value
    return str(value)


def _json_default(value: object) -> object:
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
