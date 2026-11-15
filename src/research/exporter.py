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
from collections import Counter
from collections.abc import AsyncIterable, Iterable, Mapping
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import and_, case, func, literal, or_, select, tuple_, union
from sqlalchemy.orm import aliased
from tiebameow.models.orm import Comment as BaseComment
from tiebameow.models.orm import Post as BasePost
from tiebameow.models.orm import Thread as BaseThread
from tiebameow.models.orm import User as BaseUser

from src.core.models import ResearchTaskScope

from .models import (
    MediaAsset,
    ResearchComment,
    ResearchContentObservation,
    ResearchFieldObservation,
    ResearchPost,
    ResearchThread,
    ResearchUser,
    ResearchWebEnrichmentAttempt,
    ResearchWebEnrichmentFailure,
    ResearchWebEnrichmentTask,
    UserForumSnapshot,
)
from .urls import canonical_thread_url

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    type QualitySummary = Mapping[str, object]

EXPORT_BATCH_SIZE = 1000
MEDIA_OWNER_BATCH_SIZE = 250
_CSV_FIELD_LIMIT_LOCK = threading.Lock()
_UNKNOWN_ZERO_REASONS = frozenset({"zero_presence_unverified", "zero_presence_unverified_in_parsed_object"})


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
    "media_urls",
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
    "global_growth_icon_url",
    "is_bawu",
    "forum_level_name",
    "ip_location_text",
    "ip_location_source",
    "ip_location_observed_at",
    "reply_count",
    "agree_count",
    "collect_count",
    "view_count",
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
    "field_sources_json",
    "record_source",
    "legacy_base_snapshot_json",
    "thread_url",
    "thread_url_source",
    "thread_url_validation_status",
    "thread_url_observed_at",
    "latest_scope_status",
    "latest_scope_run_id",
    "latest_scope_generation",
    "latest_comment_scope_status",
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
    "media_urls",
    "author_user_id",
    "author_user_name",
    "author_nickname",
    "author_portrait",
    "author_tieba_uid",
    "thread_author_user_id",
    "forum_level",
    "global_growth_level",
    "global_growth_icon_url",
    "is_bawu",
    "forum_level_name",
    "ip_location_text",
    "ip_location_source",
    "ip_location_observed_at",
    "agree_count",
    "is_thread_author",
    "source_api",
    "observed_at",
    "source_payload_json",
    "missing_reasons_json",
    "field_sources_json",
    "record_source",
    "legacy_base_snapshot_json",
    "thread_url",
    "thread_url_source",
    "thread_url_validation_status",
    "thread_url_observed_at",
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
    "media_urls",
    "author_user_id",
    "author_user_name",
    "author_nickname",
    "author_portrait",
    "author_tieba_uid",
    "forum_level",
    "global_growth_level",
    "global_growth_icon_url",
    "is_bawu",
    "forum_level_name",
    "ip_location_text",
    "ip_location_source",
    "ip_location_observed_at",
    "reply_to_user_id",
    "reply_to_display_name",
    "reply_to_id_raw",
    "reply_target_source",
    "reply_target_confidence",
    "reply_resolution_note",
    "agree_count",
    "is_thread_author",
    "source_api",
    "observed_at",
    "source_payload_json",
    "missing_reasons_json",
    "field_sources_json",
    "record_source",
    "legacy_base_snapshot_json",
    "thread_url",
    "thread_url_source",
    "thread_url_validation_status",
    "thread_url_observed_at",
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
    "global_growth_level",
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
    "ip_location_text",
    "ip_location_source",
    "status",
    "tieba_uid",
    "source_payload_json",
    "missing_reasons_json",
)

WEB_ENRICHMENT_TASK_FIELDS = tuple(column.name for column in ResearchWebEnrichmentTask.__table__.columns)
FIELD_OBSERVATION_FIELDS = tuple(column.name for column in ResearchFieldObservation.__table__.columns)
WEB_ENRICHMENT_FAILURE_FIELDS = ("run_id", *(column.name for column in ResearchWebEnrichmentFailure.__table__.columns))
WEB_ENRICHMENT_ATTEMPT_FIELDS = tuple(column.name for column in ResearchWebEnrichmentAttempt.__table__.columns)
FIELD_QUALITY_FIELDS = (
    "entity_type",
    "field_name",
    "entity_count",
    "canonical_nonempty_count",
    "api_nonempty_count",
    "verified_content_api_entity_count",
    "web_applied_entity_count",
    "api_observation_count",
    "web_observation_count",
    "unknown_source_observation_count",
    "unscoped_observation_count",
    "known_zero_count",
    "unknown_zero_count",
    "missing_reason_count",
    "missing_reason_counts_json",
    "api_conflict_count",
    "api_observation_conflict_count",
    "web_conflict_count",
    "identity_failure_count",
    "entity_population",
    "web_evidence_run_filter",
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

THREAD_SCOPE_QUALITY_FIELDS = (
    "tid",
    "latest_scope_status",
    "latest_scope_run_id",
    "latest_scope_generation",
    "latest_comment_scope_status",
    "entity_scope",
    "selection_status",
    "exclusion_reason",
)


async def export_research_csvs(
    async_sessionmaker_: async_sessionmaker[AsyncSession],
    output_dir: str | Path,
    quality_summaries: Iterable[QualitySummary] | AsyncIterable[QualitySummary] = (),
    *,
    source_run_id: str | None = None,
    entity_scope: str = "all",
) -> dict[str, Path]:
    """Write a validated CSV batch without materializing entity tables.

    Extension rows take precedence as a complete research observation: a NULL
    in one of those rows is kept NULL. Legacy base rows are still included and
    marked ``legacy_base_unverified``; their stored values are never used to
    fill NULL extension fields. ``source_run_id`` is filename and quality
    report context only. ``all`` is unfiltered; ``completed-threads`` applies
    a conservative current-scope filter to threads, posts, comments, and media.
    Pause collection while exporting so quality summaries cannot race writes.
    """
    if entity_scope not in {"all", "completed-threads"}:
        raise ValueError("entity_scope must be 'all' or 'completed-threads'.")
    destination_root = Path(output_dir)
    if await asyncio.to_thread(destination_root.is_symlink):
        raise ResearchExportValidationError("Export root must not be a symbolic link.")
    await asyncio.to_thread(destination_root.mkdir, parents=True, exist_ok=True, mode=0o700)
    if not await asyncio.to_thread(destination_root.is_dir):
        raise ResearchExportValidationError("Export root must be a directory.")
    await asyncio.to_thread(destination_root.chmod, 0o700)

    batch_prefix, export_id, exported_at = _batch_identity(source_run_id, entity_scope)
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
            "thread_scope_quality_report",
            "crawl_quality_report",
            "web_enrichment_tasks",
            "web_enrichment_attempts",
            "field_observations",
            "web_enrichment_failures",
            "field_quality_report",
        )
    }

    try:
        specs = _query_specs(entity_scope, source_run_id=source_run_id)
        async with async_sessionmaker_() as session:
            await _begin_consistent_snapshot(session)
            expected_counts = await _validate_database(session, specs, entity_scope)
            scope_selection_counts = await _thread_scope_selection_counts(session, entity_scope)
            field_quality_rows = await _collect_field_quality_rows(
                session,
                entity_scope=entity_scope,
                source_run_id=source_run_id,
            )
            csv_counts: dict[str, int] = {}
            for name, statement, fields, row_mapper, _unique_fields in specs:
                media_owner_type = {
                    "threads": "thread",
                    "posts": "post",
                    "comments": "comment",
                }.get(name)
                written = await _write_query_csv(
                    session,
                    paths[name],
                    fields,
                    statement,
                    row_mapper,
                    media_owner_type=media_owner_type,
                )
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

            await _write_rows_csv(paths["field_quality_report"], FIELD_QUALITY_FIELDS, field_quality_rows)
            field_quality_parsed = await _count_csv_rows(paths["field_quality_report"], FIELD_QUALITY_FIELDS)
            if field_quality_parsed != len(field_quality_rows):
                raise ResearchExportValidationError(
                    "Export validation failed: field quality summary and parsed CSV row counts differ."
                )
            expected_counts["field_quality_report"] = len(field_quality_rows)
            csv_counts["field_quality_report"] = field_quality_parsed

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
                "entity_scope": "all_database_rows" if entity_scope == "all" else "completed_threads_latest_scope",
                "entity_scope_filter": entity_scope,
                "entity_scope_table_semantics": {
                    "threads": entity_scope,
                    "posts": entity_scope,
                    "comments": entity_scope,
                    "media_assets": ("all_database_rows" if entity_scope == "all" else "selected_content_owners_only"),
                    "users": "all_database_rows_unfiltered",
                    "user_forum_snapshots": "all_database_rows_unfiltered",
                    "web_enrichment_tasks": "source_run_id_filtered" if source_run_id is not None else "all_runs",
                    "web_enrichment_attempts": (
                        "joined_to_source_run_id_tasks" if source_run_id is not None else "all_runs"
                    ),
                    "field_observations": (
                        "selected_run_id_excludes_unscoped_null_run_id"
                        if source_run_id is not None
                        else "all_runs_including_unscoped_null_run_id"
                    ),
                    "web_enrichment_failures": (
                        "joined_to_source_run_id_tasks" if source_run_id is not None else "all_runs"
                    ),
                    "field_quality_report": (
                        "canonical counts match exported content population; "
                        "evidence counts follow source_run_id filter"
                    ),
                },
                "scope_selection_semantics": (
                    "latest ResearchTaskScope per tid and logical scope across runs by last_checked DESC, run_id DESC; "
                    "generation is current within its run; all known child comment scopes must be completed; "
                    "a missing child scope excludes only when comments or a positive reported count require it"
                    if entity_scope == "completed-threads"
                    else "all database rows; latest scope status is descriptive only"
                ),
                "database_export_validation_status": "passed",
                "source_scope_completion_status": (
                    "latest_scope_completed_for_included_threads_only"
                    if entity_scope == "completed-threads"
                    else ("known_scope_summaries_present" if quality_summary_count else "unknown_not_tested")
                ),
                "forum_coverage_status": "unknown_not_proven",
                "thread_scope_selection_counts": scope_selection_counts,
                "source_run_id_context_only": source_run_id,
                "audit_csv_semantics": {
                    "web_enrichment_tasks": "operational audit data, not an analysis sample",
                    "web_enrichment_attempts": "bounded request and page-action audit data, not an analysis sample",
                    "field_observations": (
                        "source evidence, not an analysis sample; source_kind distinguishes "
                        "web_dom/web_network, api_raw/user_forum_api, and unknown; "
                        "all-runs includes null run_id; selected-run exports exclude null run_id"
                    ),
                    "web_enrichment_failures": "operational failure audit, not an analysis sample",
                    "field_quality_report": {
                        "canonical_population": "all exported research/legacy content entities; see entity_scope",
                        "source_run_id": (
                            "filters task and observation evidence only; does not filter canonical entities; "
                            "selected-run observation filters exclude unscoped null run_id rows"
                        ),
                    },
                },
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


def _batch_identity(source_run_id: str | None, entity_scope: str = "all") -> tuple[str, str, datetime]:
    safe_run_id = "unknown" if source_run_id is None else re.sub(r"[^A-Za-z0-9_-]+", "-", source_run_id).strip("-_")
    safe_run_id = safe_run_id[:48] or "unknown"
    exported_at = datetime.now(ZoneInfo("Asia/Shanghai"))
    timestamp = exported_at.strftime("%Y%m%d_%H%M%S_%f")
    export_id = uuid4().hex
    scope_prefix = "all-runs" if entity_scope == "all" else "completed-threads"
    return f"{scope_prefix}_source-{safe_run_id}_{timestamp}_{export_id}", export_id, exported_at


def _query_specs(entity_scope: str = "all", *, source_run_id: str | None = None):
    return [
        ("threads", _thread_query(entity_scope), THREAD_FIELDS, _thread_row, ("export_id",)),
        ("posts", _post_query(entity_scope), POST_FIELDS, _post_row, ("export_id",)),
        ("comments", _comment_query(entity_scope), COMMENT_FIELDS, _comment_row, ("export_id",)),
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
            _media_query(entity_scope),
            MEDIA_ASSET_FIELDS,
            lambda row: _research_row(row[3], MEDIA_ASSET_FIELDS),
            ("export_owner_type", "export_owner_id", "export_position"),
        ),
        (
            "thread_scope_quality_report",
            _thread_scope_quality_query(entity_scope),
            THREAD_SCOPE_QUALITY_FIELDS,
            _thread_scope_quality_row,
            ("tid",),
        ),
        (
            "web_enrichment_tasks",
            _web_task_query(source_run_id),
            WEB_ENRICHMENT_TASK_FIELDS,
            lambda row: _table_record_row(row[0], WEB_ENRICHMENT_TASK_FIELDS),
            ("task_key",),
        ),
        (
            "web_enrichment_attempts",
            _web_attempt_query(source_run_id),
            WEB_ENRICHMENT_ATTEMPT_FIELDS,
            lambda row: _table_record_row(row[0], WEB_ENRICHMENT_ATTEMPT_FIELDS),
            ("task_key", "lease_generation"),
        ),
        (
            "field_observations",
            _field_observation_query(source_run_id),
            FIELD_OBSERVATION_FIELDS,
            lambda row: _table_record_row(row[0], FIELD_OBSERVATION_FIELDS),
            ("observation_key",),
        ),
        (
            "web_enrichment_failures",
            _web_failure_query(source_run_id),
            WEB_ENRICHMENT_FAILURE_FIELDS,
            _web_failure_row,
            ("failure_id",),
        ),
    ]


def _web_task_query(source_run_id: str | None):
    statement = select(ResearchWebEnrichmentTask)
    if source_run_id is not None:
        statement = statement.where(ResearchWebEnrichmentTask.run_id == source_run_id)
    return statement.order_by(
        ResearchWebEnrichmentTask.run_id, ResearchWebEnrichmentTask.created_at, ResearchWebEnrichmentTask.task_key
    )


def _web_attempt_query(source_run_id: str | None):
    statement = select(ResearchWebEnrichmentAttempt)
    if source_run_id is not None:
        statement = statement.join(
            ResearchWebEnrichmentTask,
            ResearchWebEnrichmentTask.task_key == ResearchWebEnrichmentAttempt.task_key,
        ).where(ResearchWebEnrichmentTask.run_id == source_run_id)
    return statement.order_by(ResearchWebEnrichmentAttempt.task_key, ResearchWebEnrichmentAttempt.lease_generation)


def _field_observation_query(source_run_id: str | None):
    statement = select(ResearchFieldObservation)
    if source_run_id is not None:
        statement = statement.where(ResearchFieldObservation.run_id == source_run_id)
    return statement.order_by(
        ResearchFieldObservation.run_id,
        ResearchFieldObservation.target_type,
        ResearchFieldObservation.target_id,
        ResearchFieldObservation.field_name,
        ResearchFieldObservation.observed_at,
    )


def _web_failure_query(source_run_id: str | None):
    statement = select(ResearchWebEnrichmentFailure, ResearchWebEnrichmentTask.run_id).outerjoin(
        ResearchWebEnrichmentTask,
        ResearchWebEnrichmentTask.task_key == ResearchWebEnrichmentFailure.task_key,
    )
    if source_run_id is not None:
        statement = statement.where(ResearchWebEnrichmentTask.run_id == source_run_id)
    return statement.order_by(ResearchWebEnrichmentFailure.occurred_at, ResearchWebEnrichmentFailure.failure_id)


def _table_record_row(record: Any, fields: tuple[str, ...]) -> dict[str, object]:
    return {field: getattr(record, field, None) for field in fields}


def _web_failure_row(row: Any) -> dict[str, object]:
    values = _table_record_row(row[0], tuple(field for field in WEB_ENRICHMENT_FAILURE_FIELDS if field != "run_id"))
    values["run_id"] = row[1] or "unknown_task_run"
    return values


async def _begin_consistent_snapshot(session: AsyncSession) -> None:
    get_bind = getattr(session, "get_bind", None)
    if not callable(get_bind):
        return
    bind = get_bind()
    if getattr(getattr(bind, "dialect", None), "name", None) == "postgresql":
        await session.connection(execution_options={"isolation_level": "REPEATABLE READ"})


async def _validate_database(
    session: AsyncSession,
    specs: list[tuple],
    entity_scope: str = "all",
) -> dict[str, int]:
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

    await _validate_relationships(session, entity_scope)
    return expected_counts


async def _validate_relationships(session: AsyncSession, entity_scope: str = "all") -> None:
    threads = _thread_query(entity_scope).subquery("export_threads")
    posts = _post_query(entity_scope).subquery("export_posts")
    comments = _comment_query(entity_scope).subquery("export_comments")

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


async def _collect_field_quality_rows(
    session: AsyncSession,
    *,
    entity_scope: str,
    source_run_id: str | None,
) -> list[dict[str, object]]:
    """Stream exported content rows and aggregate compact per-field evidence counts."""
    entity_specs = (
        ("thread", _thread_query(entity_scope), _thread_row),
        ("post", _post_query(entity_scope), _post_row),
        ("comment", _comment_query(entity_scope), _comment_row),
    )
    metrics: dict[tuple[str, str], Counter[str]] = {}
    missing_counts: dict[tuple[str, str], Counter[str]] = {}

    for entity_type, statement, row_mapper in entity_specs:
        result = await session.stream(statement.execution_options(yield_per=EXPORT_BATCH_SIZE))
        try:
            async for row in result:
                values = row_mapper(row)
                sources = values.get("field_sources_json")
                sources = sources if isinstance(sources, dict) else {}
                reasons = values.get("missing_reasons_json")
                reasons = reasons if isinstance(reasons, dict) else {}
                source_api = values.get("source_api")
                record_source = values.get("record_source")
                for field_name in _content_quality_fields(entity_type):
                    key = (entity_type, field_name)
                    counts = metrics.setdefault(key, Counter())
                    counts["entity_count"] += 1
                    value = values.get(field_name)
                    present = value is not None and not (isinstance(value, str) and not value.strip())
                    if present:
                        counts["canonical_nonempty_count"] += 1
                    source = sources.get(field_name)
                    source = source if isinstance(source, dict) else {}
                    verified_web = source.get("verified_web")
                    verified_content_api = source.get("verified_content_api")
                    if isinstance(verified_content_api, dict):
                        counts["verified_content_api_entity_count"] += 1
                    if isinstance(verified_web, dict):
                        counts["web_applied_entity_count"] += 1
                    elif present and (
                        isinstance(verified_content_api, dict)
                        or (
                            record_source == "research_api"
                            and isinstance(source_api, str)
                            and source_api.startswith("aiotieba.")
                        )
                    ):
                        counts["api_nonempty_count"] += 1
                    reason = reasons.get(field_name)
                    if isinstance(reason, str) and reason:
                        counts["missing_reason_count"] += 1
                        missing_counts.setdefault(key, Counter())[reason] += 1
                    is_zero = value is False or (isinstance(value, int) and not isinstance(value, bool) and value == 0)
                    has_presence_evidence = isinstance(verified_web, dict) or isinstance(verified_content_api, dict)
                    legacy_base_zero = record_source == "legacy_base_unverified" and is_zero
                    legacy_bawu_false = (
                        field_name == "is_bawu"
                        and value is False
                        and record_source == "research_api"
                        and not has_presence_evidence
                    )
                    unknown_zero = is_zero and (
                        (reason in _UNKNOWN_ZERO_REASONS and not has_presence_evidence)
                        or legacy_base_zero
                        or legacy_bawu_false
                    )
                    if unknown_zero:
                        counts["unknown_zero_count"] += 1
                        if (legacy_base_zero or legacy_bawu_false) and not reason:
                            counts["missing_reason_count"] += 1
                            missing_counts.setdefault(key, Counter())["legacy_presence_unverified"] += 1
                    elif is_zero:
                        counts["known_zero_count"] += 1
                    api_conflicts = source.get("api_conflicts")
                    if isinstance(api_conflicts, list):
                        counts["api_conflict_count"] += len(api_conflicts)
        finally:
            await result.close()

    evidence_counts: dict[tuple[str, str], Counter[str]] = {}
    observation_result = await session.stream(
        _field_observation_query(source_run_id).execution_options(yield_per=EXPORT_BATCH_SIZE)
    )
    try:
        async for row in observation_result:
            observation = row[0]
            key = (observation.target_type, observation.field_name)
            counts = evidence_counts.setdefault(key, Counter())
            source_kind = observation.source_kind
            if observation.run_id is None:
                counts["unscoped_observation_count"] += 1
            if source_kind in {"web_dom", "web_network"}:
                counts["web_observation_count"] += 1
                if observation.applied_status == "conflict":
                    counts["web_conflict_count"] += 1
            elif source_kind in {"api_raw", "user_forum_api"}:
                counts["api_observation_count"] += 1
                if observation.applied_status == "conflict":
                    counts["api_observation_conflict_count"] += 1
            else:
                counts["unknown_source_observation_count"] += 1
    finally:
        await observation_result.close()

    identity_counts: Counter[tuple[str, str]] = Counter()
    task_result = await session.stream(_web_task_query(source_run_id).execution_options(yield_per=EXPORT_BATCH_SIZE))
    try:
        async for row in task_result:
            task = row[0]
            statuses = task.field_statuses_json if isinstance(task.field_statuses_json, dict) else {}
            requested = task.requested_fields_json if isinstance(task.requested_fields_json, list) else []
            identity_failed = task.status == "identity_unverified" or task.identity_verified is False
            for field_name in requested:
                state = statuses.get(field_name)
                state_status = state.get("status") if isinstance(state, dict) else state
                key = (task.target_type, field_name)
                if identity_failed or state_status == "identity_unverified":
                    identity_counts[key] += 1
    finally:
        await task_result.close()

    rows: list[dict[str, object]] = []
    population = "all_exported_content_entities" if entity_scope == "all" else "completed_threads_scope_entities"
    evidence_filter = "all_runs_including_unscoped" if source_run_id is None else "selected_run_id_excluding_unscoped"
    for entity_type in ("thread", "post", "comment"):
        for field_name in _content_quality_fields(entity_type):
            key = (entity_type, field_name)
            counts = metrics.get(key, Counter())
            evidence = evidence_counts.get(key, Counter())
            rows.append({
                "entity_type": entity_type,
                "field_name": field_name,
                "entity_count": counts["entity_count"],
                "canonical_nonempty_count": counts["canonical_nonempty_count"],
                "api_nonempty_count": counts["api_nonempty_count"],
                "verified_content_api_entity_count": counts["verified_content_api_entity_count"],
                "web_applied_entity_count": counts["web_applied_entity_count"],
                "api_observation_count": evidence["api_observation_count"],
                "web_observation_count": evidence["web_observation_count"],
                "unknown_source_observation_count": evidence["unknown_source_observation_count"],
                "unscoped_observation_count": evidence["unscoped_observation_count"],
                "known_zero_count": counts["known_zero_count"],
                "unknown_zero_count": counts["unknown_zero_count"],
                "missing_reason_count": counts["missing_reason_count"],
                "missing_reason_counts_json": dict(sorted(missing_counts.get(key, Counter()).items())),
                "api_conflict_count": counts["api_conflict_count"],
                "api_observation_conflict_count": evidence["api_observation_conflict_count"],
                "web_conflict_count": evidence["web_conflict_count"],
                "identity_failure_count": identity_counts[key],
                "entity_population": population,
                "web_evidence_run_filter": evidence_filter,
            })
    return rows


_CONTENT_QUALITY_FIELDS = {
    "thread": (
        "author_user_name",
        "author_nickname",
        "author_portrait",
        "forum_level",
        "global_growth_level",
        "global_growth_icon_url",
        "is_bawu",
        "forum_level_name",
        "ip_location_text",
        "reply_count",
        "agree_count",
        "disagree_count",
        "collect_count",
        "view_count",
        "share_count",
    ),
    "post": (
        "author_user_name",
        "author_nickname",
        "author_portrait",
        "forum_level",
        "global_growth_level",
        "global_growth_icon_url",
        "is_bawu",
        "forum_level_name",
        "ip_location_text",
        "agree_count",
        "disagree_count",
        "subreply_count_reported",
    ),
    "comment": (
        "author_user_name",
        "author_nickname",
        "author_portrait",
        "forum_level",
        "global_growth_level",
        "global_growth_icon_url",
        "is_bawu",
        "forum_level_name",
        "ip_location_text",
        "reply_to_user_id",
        "reply_to_display_name",
        "agree_count",
        "disagree_count",
    ),
}


def _content_quality_fields(entity_type: str) -> tuple[str, ...]:
    return _CONTENT_QUALITY_FIELDS[entity_type]


def _add_internal_quality_values(values: dict[str, object], record: object, fields: tuple[str, ...]) -> None:
    for field in fields:
        value = getattr(record, field, None)
        if field == "subreply_count_reported" and value is None:
            value = getattr(record, "reply_num", None)
        values[field] = value


async def _write_rows_csv(path: Path, fields: tuple[str, ...], rows: Iterable[Mapping[str, object]]) -> int:
    row_count = 0

    async def write(file: Any) -> None:
        nonlocal row_count
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(_csv_row(row, fields))
            row_count += 1

    await _write_atomically(path, write)
    return row_count


def _latest_scope_rows(scope_type: str):
    scope = aliased(ResearchTaskScope)
    partition_columns = [scope.tid]
    conditions = [scope.scope_type == scope_type, scope.tid.is_not(None)]
    if scope_type == "post_comments":
        partition_columns.append(scope.parent_pid)
        conditions.append(scope.parent_pid.is_not(None))
    ranked = (
        select(
            scope.run_id.label("run_id"),
            scope.logical_task_key.label("logical_task_key"),
            scope.tid.label("tid"),
            scope.parent_pid.label("parent_pid"),
            scope.generation.label("generation"),
            scope.status.label("status"),
            scope.last_checked.label("last_checked"),
            func
            .row_number()
            .over(
                partition_by=partition_columns,
                order_by=(scope.last_checked.desc(), scope.run_id.desc(), scope.logical_task_key.desc()),
            )
            .label("scope_rank"),
        )
        .where(*conditions)
        .subquery(f"ranked_{scope_type}")
    )
    return (
        select(
            ranked.c.run_id,
            ranked.c.logical_task_key,
            ranked.c.tid,
            ranked.c.parent_pid,
            ranked.c.generation,
            ranked.c.status,
            ranked.c.last_checked,
        )
        .where(ranked.c.scope_rank == 1)
        .subquery(f"latest_{scope_type}")
    )


def _comment_scope_requirements():
    comment_rows = _comment_query().subquery("known_export_comments")
    post_tid = case((ResearchPost.pid.is_not(None), ResearchPost.tid), else_=BasePost.tid)
    post_pid = case((ResearchPost.pid.is_not(None), ResearchPost.pid), else_=BasePost.pid)
    post_subreply_count = case(
        (ResearchPost.pid.is_not(None), ResearchPost.subreply_count_reported),
        else_=BasePost.reply_num,
    )
    positive_post_parents = (
        select(post_tid.label("tid"), post_pid.label("parent_pid"))
        .select_from(BasePost)
        .join(ResearchPost, BasePost.pid == ResearchPost.pid, full=True)
        .where(post_tid.is_not(None), post_pid.is_not(None), post_subreply_count > 0)
    )
    first_post_subreply_count = ResearchThread.source_payload_json["first_post"]["subreply_count_reported"].as_integer()
    positive_first_post_parents = select(
        ResearchThread.tid.label("tid"),
        ResearchThread.first_pid.label("parent_pid"),
    ).where(
        first_post_subreply_count > 0,
    )
    known_comment_parents = select(
        comment_rows.c.export_tid.label("tid"),
        comment_rows.c.export_parent_pid.label("parent_pid"),
    ).where(
        comment_rows.c.export_tid.is_not(None),
        comment_rows.c.export_parent_pid.is_not(None),
    )
    return union(known_comment_parents, positive_post_parents, positive_first_post_parents).subquery(
        "required_comment_scope_parents"
    )


def _scope_state_queries():
    latest_thread = _latest_scope_rows("thread_replies")
    latest_comments = _latest_scope_rows("post_comments")
    comment_scope_summary = (
        select(
            latest_comments.c.tid,
            func.count().label("scope_count"),
            func.sum(case((latest_comments.c.status != "completed", 1), else_=0)).label("incomplete_count"),
        )
        .group_by(latest_comments.c.tid)
        .subquery("latest_comment_scope_summary")
    )
    required_parents = _comment_scope_requirements()
    missing_comment_scopes = (
        select(required_parents.c.tid, func.count().label("missing_count"))
        .select_from(required_parents)
        .outerjoin(
            latest_comments,
            and_(
                latest_comments.c.tid == required_parents.c.tid,
                latest_comments.c.parent_pid == required_parents.c.parent_pid,
            ),
        )
        .where(latest_comments.c.run_id.is_(None))
        .group_by(required_parents.c.tid)
        .subquery("missing_comment_scope_summary")
    )
    return latest_thread, latest_comments, comment_scope_summary, missing_comment_scopes


def _comment_scope_status(summary: Any, missing: Any):
    child_scope_count = func.coalesce(summary.c.scope_count, 0)
    incomplete_child_scope_count = func.coalesce(summary.c.incomplete_count, 0)
    missing_child_scope_count = func.coalesce(missing.c.missing_count, 0)
    return case(
        (incomplete_child_scope_count > 0, literal("incomplete")),
        (missing_child_scope_count > 0, literal("missing_completion_evidence")),
        (child_scope_count > 0, literal("completed")),
        else_=literal("no_known_comment_scope_required"),
    )


def _completed_thread_ids():
    latest_thread, _latest_comments, comment_summary, missing_summary = _scope_state_queries()
    thread_ids = _thread_id_query().subquery("all_export_thread_ids")
    comment_status = _comment_scope_status(comment_summary, missing_summary)
    return (
        select(thread_ids.c.export_id)
        .select_from(thread_ids)
        .join(latest_thread, latest_thread.c.tid == thread_ids.c.export_id)
        .outerjoin(comment_summary, comment_summary.c.tid == thread_ids.c.export_id)
        .outerjoin(missing_summary, missing_summary.c.tid == thread_ids.c.export_id)
        .where(
            latest_thread.c.status == "completed",
            comment_status.in_(("completed", "no_known_comment_scope_required")),
        )
    )


def _thread_scope_quality_query(entity_scope: str):
    export_id = case(
        (ResearchThread.tid.is_not(None), ResearchThread.tid),
        else_=BaseThread.tid,
    ).label("tid")
    latest_thread, _latest_comments, comment_summary, missing_summary = _scope_state_queries()
    latest_scope_status = func.coalesce(latest_thread.c.status, literal("no_evidence"))
    latest_comment_scope_status = _comment_scope_status(comment_summary, missing_summary)
    is_completed = and_(
        latest_scope_status == "completed",
        latest_comment_scope_status.in_(("completed", "no_known_comment_scope_required")),
    )
    if entity_scope == "all":
        selection_status = literal("unfiltered")
        exclusion_reason = literal(None)
    else:
        selection_status = case((is_completed, literal("included")), else_=literal("excluded"))
        exclusion_reason = case(
            (latest_scope_status == "no_evidence", literal("missing_detail_evidence")),
            (latest_scope_status != "completed", literal("latest_scope_not_completed")),
            (
                latest_comment_scope_status == "missing_completion_evidence",
                literal("missing_completion_evidence"),
            ),
            (
                latest_comment_scope_status == "incomplete",
                literal("latest_child_comment_scope_not_completed"),
            ),
            else_=literal(None),
        )
    return (
        select(
            export_id,
            latest_scope_status.label("latest_scope_status"),
            latest_thread.c.run_id.label("latest_scope_run_id"),
            latest_thread.c.generation.label("latest_scope_generation"),
            latest_comment_scope_status.label("latest_comment_scope_status"),
            literal(entity_scope).label("entity_scope"),
            selection_status.label("selection_status"),
            exclusion_reason.label("exclusion_reason"),
        )
        .select_from(BaseThread)
        .join(ResearchThread, BaseThread.tid == ResearchThread.tid, full=True)
        .outerjoin(latest_thread, latest_thread.c.tid == export_id)
        .outerjoin(comment_summary, comment_summary.c.tid == export_id)
        .outerjoin(missing_summary, missing_summary.c.tid == export_id)
    )


async def _thread_scope_selection_counts(session: AsyncSession, entity_scope: str) -> dict[str, object]:
    report = _thread_scope_quality_query(entity_scope).subquery("thread_scope_selection")
    result = await session.execute(
        select(report.c.selection_status, report.c.exclusion_reason, func.count())
        .group_by(report.c.selection_status, report.c.exclusion_reason)
        .order_by(report.c.selection_status, report.c.exclusion_reason)
    )
    selection_counts: dict[str, int] = {}
    exclusion_counts: dict[str, int] = {}
    candidate_count = 0
    for status, reason, count in result:
        count = int(count)
        candidate_count += count
        selection_counts[status] = selection_counts.get(status, 0) + count
        if reason is not None:
            exclusion_counts[reason] = exclusion_counts.get(reason, 0) + count
    return {
        "candidate_thread_count": candidate_count,
        "selection_status_counts": selection_counts,
        "exclusion_reason_counts": exclusion_counts,
    }


def _thread_scope_quality_row(row: Any) -> dict[str, object]:
    return dict(zip(THREAD_SCOPE_QUALITY_FIELDS, row, strict=True))


def _thread_id_query():
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
    return (
        select(export_id, export_first_pid)
        .select_from(BaseThread)
        .join(ResearchThread, BaseThread.tid == ResearchThread.tid, full=True)
    )


def _thread_query(entity_scope: str = "all"):
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
    fallback_author_id = func.coalesce(ResearchThread.author_user_id, BaseThread.author_id)
    # This matches _thread_row's source selection: a research row is used as a
    # whole observation, so its NULL create_time must remain NULL for ordering.
    export_create_time = case(
        (ResearchThread.tid.is_not(None), ResearchThread.create_time),
        else_=BaseThread.create_time,
    )
    legacy_first_post_text = (
        select(BasePost.text)
        .where(
            BasePost.tid == export_id,
            BasePost.floor == 1,
            BasePost.author_id == fallback_author_id,
        )
        .order_by(BasePost.create_time.asc(), BasePost.pid.asc())
        .limit(1)
        .scalar_subquery()
    )
    legacy_first_post_contents = (
        select(BasePost.contents)
        .where(
            BasePost.tid == export_id,
            BasePost.floor == 1,
            BasePost.author_id == fallback_author_id,
        )
        .order_by(BasePost.create_time.asc(), BasePost.pid.asc())
        .limit(1)
        .scalar_subquery()
    )
    latest_thread, _latest_comments, comment_summary, missing_summary = _scope_state_queries()
    latest_scope_status = func.coalesce(latest_thread.c.status, literal("no_evidence"))
    latest_comment_scope_status = _comment_scope_status(comment_summary, missing_summary)
    statement = (
        select(
            export_id,
            export_first_pid,
            BaseThread,
            ResearchThread,
            BaseUser,
            fallback_author_id.label("fallback_author_id"),
            legacy_first_post_text.label("legacy_first_post_text"),
            legacy_first_post_contents.label("legacy_first_post_contents"),
            latest_scope_status.label("latest_scope_status"),
            latest_thread.c.run_id.label("latest_scope_run_id"),
            latest_thread.c.generation.label("latest_scope_generation"),
            latest_comment_scope_status.label("latest_comment_scope_status"),
        )
        .select_from(BaseThread)
        .join(ResearchThread, BaseThread.tid == ResearchThread.tid, full=True)
        .outerjoin(BaseUser, author_id == BaseUser.user_id)
        .outerjoin(latest_thread, latest_thread.c.tid == export_id)
        .outerjoin(comment_summary, comment_summary.c.tid == export_id)
        .outerjoin(missing_summary, missing_summary.c.tid == export_id)
        .order_by(export_create_time.desc().nulls_last(), export_id.desc())
    )
    if entity_scope == "completed-threads":
        statement = statement.where(export_id.in_(_completed_thread_ids()))
    return statement


def _post_query(entity_scope: str = "all"):
    tid = case((ResearchPost.pid.is_not(None), ResearchPost.tid), else_=BasePost.tid)
    export_id = case((ResearchPost.pid.is_not(None), ResearchPost.pid), else_=BasePost.pid).label("export_id")
    export_tid = tid.label("export_tid")
    author_id = case(
        (ResearchPost.pid.is_not(None), ResearchPost.author_user_id),
        else_=BasePost.author_id,
    )
    export_create_time = case(
        (ResearchPost.pid.is_not(None), ResearchPost.create_time),
        else_=BasePost.create_time,
    )
    statement = (
        select(export_id, export_tid, BasePost, ResearchPost, ResearchThread, BaseUser)
        .select_from(BasePost)
        .join(ResearchPost, BasePost.pid == ResearchPost.pid, full=True)
        .outerjoin(ResearchThread, ResearchThread.tid == tid)
        .outerjoin(BaseUser, author_id == BaseUser.user_id)
        .order_by(tid.asc().nulls_last(), export_create_time.asc().nulls_last(), export_id.asc())
    )
    if entity_scope == "completed-threads":
        statement = statement.where(tid.in_(_completed_thread_ids()))
    return statement


def _comment_query(entity_scope: str = "all"):
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
    export_create_time = case(
        (ResearchComment.cid.is_not(None), ResearchComment.create_time),
        else_=BaseComment.create_time,
    )
    statement = (
        select(export_id, export_tid, export_parent_pid, BaseComment, ResearchComment, BaseUser)
        .select_from(BaseComment)
        .join(ResearchComment, BaseComment.cid == ResearchComment.cid, full=True)
        .outerjoin(BaseUser, author_id == BaseUser.user_id)
        .order_by(
            export_tid.asc().nulls_last(),
            export_parent_pid.asc().nulls_last(),
            export_create_time.asc().nulls_last(),
            export_id.asc(),
        )
    )
    if entity_scope == "completed-threads":
        statement = statement.where(export_tid.in_(_completed_thread_ids()))
    return statement


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


def _media_query(entity_scope: str = "all"):
    statement = select(
        MediaAsset.owner_type.label("export_owner_type"),
        MediaAsset.owner_id.label("export_owner_id"),
        MediaAsset.position.label("export_position"),
        MediaAsset,
    )
    if entity_scope == "all":
        return statement

    thread_query = _thread_id_query()
    threads = thread_query.where(thread_query.selected_columns.export_id.in_(_completed_thread_ids())).subquery(
        "selected_thread_owners"
    )
    posts = _post_query("completed-threads").subquery("selected_post_owners")
    comments = _comment_query("completed-threads").subquery("selected_comment_owners")
    thread_owner_exists = (
        select(1)
        .select_from(threads)
        .where(
            threads.c.export_id == MediaAsset.tid,
            threads.c.export_id == MediaAsset.owner_id,
        )
        .exists()
    )
    legacy_thread_owner_exists = (
        select(1)
        .select_from(threads)
        .where(
            threads.c.export_id == MediaAsset.tid,
            threads.c.export_first_pid == MediaAsset.owner_id,
        )
        .exists()
    )
    post_owner_exists = (
        select(1)
        .select_from(posts)
        .where(
            posts.c.export_tid == MediaAsset.tid,
            posts.c.export_id == MediaAsset.owner_id,
        )
        .exists()
    )
    comment_owner_exists = (
        select(1)
        .select_from(comments)
        .where(
            comments.c.export_tid == MediaAsset.tid,
            comments.c.export_id == MediaAsset.owner_id,
        )
        .exists()
    )
    return statement.where(
        or_(
            and_(MediaAsset.owner_type == "thread", thread_owner_exists),
            and_(MediaAsset.owner_type == "thread_first_post", legacy_thread_owner_exists),
            and_(MediaAsset.owner_type == "post", post_owner_exists),
            and_(MediaAsset.owner_type == "comment", comment_owner_exists),
        )
    )


def _thread_row(row: Any) -> dict[str, object]:
    (
        _export_id,
        export_first_pid,
        base,
        research,
        author,
        fallback_author_id,
        legacy_first_post_text,
        legacy_first_post_contents,
        latest_scope_status,
        latest_scope_run_id,
        latest_scope_generation,
        latest_comment_scope_status,
    ) = row
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
    _add_legacy_first_post_fallback(
        values,
        legacy_first_post_text,
        legacy_first_post_contents,
        base,
        fallback_author_id,
        prefer_legacy=research is None,
    )
    _add_internal_quality_values(
        values,
        research if research is not None else base,
        ("disagree_count", "share_count"),
    )
    values["legacy_base_snapshot_json"] = _legacy_snapshot(base, author)
    _add_thread_url_metadata(values)
    values["latest_scope_status"] = latest_scope_status
    values["latest_scope_run_id"] = latest_scope_run_id
    values["latest_scope_generation"] = latest_scope_generation
    values["latest_comment_scope_status"] = latest_comment_scope_status
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
    _add_internal_quality_values(values, post, ("disagree_count", "subreply_count_reported"))
    values["legacy_base_snapshot_json"] = _legacy_snapshot(base, author)
    _add_thread_url_metadata(values)
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
    _add_internal_quality_values(values, research if research is not None else base, ("disagree_count",))
    values["legacy_base_snapshot_json"] = _legacy_snapshot(base, author)
    _add_thread_url_metadata(values)
    return values


def _add_thread_url_metadata(values: dict[str, object]) -> None:
    url = canonical_thread_url(values.get("tid"))
    values["thread_url"] = url
    values["thread_url_source"] = "constructed_from_tid" if url is not None else None
    values["thread_url_validation_status"] = "not_live_verified" if url is not None else "invalid_tid"
    # URL construction is local computation, not a live observation.
    values["thread_url_observed_at"] = None
    if url is None:
        _add_missing_reasons(values, {"thread_url": "invalid_tid"})


def _add_legacy_first_post_fallback(
    values: dict[str, object],
    first_post_text: object,
    first_post_contents: object,
    base: object | None,
    expected_author_id: object,
    *,
    prefer_legacy: bool,
) -> None:
    if not prefer_legacy and (values.get("body_text") is not None or values.get("contents_raw")):
        return
    has_text = isinstance(first_post_text, str) and bool(first_post_text)
    has_contents = isinstance(first_post_contents, (dict, list)) and bool(first_post_contents)
    if has_text or has_contents:
        values["body_text"] = first_post_text
        values["contents_raw"] = first_post_contents
        _add_body_source_metadata(
            values,
            "legacy_base_fallback",
            {
                "source": "BasePost.floor=1",
                "source_quality": "legacy_detail",
                "completeness": "response_returned_unverified",
            },
            clear_missing_reason=True,
        )
        return

    if base is None:
        return
    base_author_id = getattr(base, "author_id", None)
    if expected_author_id is not None and base_author_id is not None and base_author_id != expected_author_id:
        return
    list_text = getattr(base, "text", None)
    list_contents = getattr(base, "contents", None)
    has_list_text = isinstance(list_text, str) and bool(list_text)
    has_list_contents = isinstance(list_contents, (dict, list)) and bool(list_contents)
    if not has_list_text and not has_list_contents:
        return

    values["body_text"] = list_text
    values["contents_raw"] = list_contents
    _add_body_source_metadata(
        values,
        "legacy_list_content",
        {
            "source": "BaseThread.text/contents",
            "completeness": "unverified",
        },
        clear_missing_reason=False,
    )
    _add_missing_reasons(values, {"body_text": "legacy_list_content_completeness_unverified"})


def _add_body_source_metadata(
    values: dict[str, object],
    source_name: str,
    metadata: dict[str, object],
    *,
    clear_missing_reason: bool,
) -> None:
    sources = values.get("field_sources_json")
    source_map = dict(sources) if isinstance(sources, Mapping) else {}
    body_source = source_map.get("body_text")
    body_source_map = dict(body_source) if isinstance(body_source, Mapping) else {}
    body_source_map[source_name] = metadata
    source_map["body_text"] = body_source_map
    values["field_sources_json"] = source_map
    if clear_missing_reason:
        reasons = values.get("missing_reasons_json")
        reason_map = dict(reasons) if isinstance(reasons, Mapping) else {}
        reason_map.pop("body_text", None)
        values["missing_reasons_json"] = reason_map


def _add_missing_reasons(values: dict[str, object], additions: Mapping[str, str]) -> None:
    existing = values.get("missing_reasons_json")
    reasons = dict(existing) if isinstance(existing, Mapping) else {}
    reasons.update(additions)
    values["missing_reasons_json"] = reasons


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
    *,
    media_owner_type: str | None = None,
) -> int:
    row_count = 0

    async def write(file: Any) -> None:
        nonlocal row_count
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        result = await session.stream(statement.execution_options(yield_per=EXPORT_BATCH_SIZE))
        try:
            rows: list[Any] = []

            async def write_batch() -> None:
                nonlocal row_count
                owner_keys = (
                    [_media_owner_keys(row, media_owner_type) for row in rows]
                    if media_owner_type is not None
                    else []
                )
                urls_by_owner = await _load_media_urls(
                    session,
                    list(dict.fromkeys(key for keys in owner_keys for key in keys)),
                ) if owner_keys else {}
                for index, row in enumerate(rows):
                    values = row_mapper(row)
                    if media_owner_type is not None:
                        media_urls = next(
                            (urls_by_owner[key] for key in owner_keys[index] if urls_by_owner.get(key)),
                            [],
                        )
                        values["media_urls"] = json.dumps(media_urls, ensure_ascii=False, separators=(",", ":"))
                    writer.writerow(_csv_row(values, fields))
                    row_count += 1
                rows.clear()

            async for row in result:
                rows.append(row)
                if len(rows) >= EXPORT_BATCH_SIZE:
                    await write_batch()
            if rows:
                await write_batch()
        finally:
            await result.close()

    await _write_atomically(path, write)
    return row_count


def _media_owner_keys(row: Any, owner_type: str) -> tuple[tuple[str, int, int], ...]:
    owner_id = row[0]
    if owner_type == "thread":
        tid = owner_id
        keys = [("thread", owner_id, tid)]
        first_pid = row[1]
        if first_pid is not None:
            # Older snapshots keyed the thread's first-post media by its PID.
            keys.append(("thread_first_post", first_pid, tid))
        return tuple(keys)
    return ((owner_type, owner_id, row[1]),)


async def _load_media_urls(
    session: AsyncSession,
    owner_keys: list[tuple[str, int, int]],
) -> dict[tuple[str, int, int], list[str]]:
    urls_by_owner: dict[tuple[str, int, int], list[str]] = {}
    seen_by_owner: dict[tuple[str, int, int], set[str]] = {}
    for start in range(0, len(owner_keys), MEDIA_OWNER_BATCH_SIZE):
        owner_key_batch = owner_keys[start : start + MEDIA_OWNER_BATCH_SIZE]
        thread_tids = {key[2] for key in owner_key_batch if key[0] == "thread_first_post"}
        observed_thread_tids = await _load_observed_thread_media_tids(session, thread_tids)
        owner_key_batch = [
            key
            for key in owner_key_batch
            if not (key[0] == "thread_first_post" and key[2] in observed_thread_tids)
        ]
        if not owner_key_batch:
            continue
        result = await session.stream(
            select(
                MediaAsset.owner_type,
                MediaAsset.owner_id,
                MediaAsset.tid,
                MediaAsset.media_type,
                MediaAsset.position,
                MediaAsset.original_url,
                MediaAsset.src_url,
                MediaAsset.source_url,
            )
            .where(
                tuple_(MediaAsset.owner_type, MediaAsset.owner_id, MediaAsset.tid).in_(owner_key_batch),
                MediaAsset.media_type.in_(("image", "video")),
            )
            .order_by(MediaAsset.owner_type, MediaAsset.owner_id, MediaAsset.tid, MediaAsset.position)
            .execution_options(yield_per=EXPORT_BATCH_SIZE)
        )
        try:
            async for asset in result:
                owner_key = (asset[0], asset[1], asset[2])
                url = next(
                    (value for value in (asset[5], asset[6], asset[7]) if isinstance(value, str) and value.strip()),
                    None,
                )
                if url is None:
                    continue
                seen = seen_by_owner.setdefault(owner_key, set())
                if url not in seen:
                    seen.add(url)
                    urls_by_owner.setdefault(owner_key, []).append(url)
        finally:
            await result.close()
    return urls_by_owner


async def _load_observed_thread_media_tids(session: AsyncSession, tids: set[int]) -> set[int]:
    if not tids:
        return set()
    result = await session.stream(
        select(ResearchContentObservation.owner_id).where(
            ResearchContentObservation.owner_type == "thread_first_post",
            tuple_(ResearchContentObservation.owner_id, ResearchContentObservation.tid).in_(
                [(tid, tid) for tid in sorted(tids)]
            ),
        ).execution_options(yield_per=EXPORT_BATCH_SIZE)
    )
    try:
        return {row[0] async for row in result}
    finally:
        await result.close()


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
