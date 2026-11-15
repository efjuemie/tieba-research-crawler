"""Mapping and idempotent persistence for parsed research API responses."""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import ipaddress
import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from itertools import starmap
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from aiotieba.enums import ThreadType
from sqlalchemy import and_, case, delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from tiebameow.models.orm import Post as BasePost
from tiebameow.utils.time_utils import SHANGHAI_TZ

from .models import (
    MediaAsset,
    ResearchComment,
    ResearchContentObservation,
    ResearchFieldObservation,
    ResearchPost,
    ResearchThread,
    ResearchUser,
    UserForumSnapshot,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from .client import ResearchPage, ResearchUserBawuInfo, ResearchUserForumInfo, ResearchUserProfileInfo

SIGNED_BIGINT_MAX = (1 << 63) - 1
_RESEARCH_USER_LAST_KNOWN_FIELDS = (
    "user_name",
    "nickname",
    "portrait",
    "tieba_uid",
    "global_growth_level",
)
_CONTENT_LAST_KNOWN_FIELDS = frozenset(
    {
        "fid",
        "tid",
        "parent_post_pid",
        "author_user_id",
        "author_user_name",
        "author_nickname",
        "author_portrait",
        "author_tieba_uid",
    }
)
_COMMENT_PREVIEW_FIELDS = frozenset(
    {
        "author_user_name",
        "author_nickname",
        "author_portrait",
        "forum_level",
        "global_growth_level",
        "global_growth_icon_url",
        "is_bawu",
        "ip_location_text",
        "reply_to_display_name",
        "reply_to_user_id",
    }
)


@dataclass(slots=True)
class ResearchRecords:
    threads: list[ResearchThread] = field(default_factory=list)
    posts: list[ResearchPost] = field(default_factory=list)
    comments: list[ResearchComment] = field(default_factory=list)
    users: list[ResearchUser] = field(default_factory=list)
    user_forum_snapshots: list[UserForumSnapshot] = field(default_factory=list)
    media_assets: list[MediaAsset] = field(default_factory=list)
    content_observations: list[ResearchContentObservation] = field(default_factory=list)
    api_preview_comment_identities: list[tuple[int, int, int, int, int]] = field(default_factory=list)
    api_field_observations: list[ResearchFieldObservation] = field(default_factory=list)


async def persist_research_page(
    async_sessionmaker_: async_sessionmaker | None,
    page: ResearchPage,
    *,
    session: AsyncSession | None = None,
    run_id: str | None = None,
) -> dict[str, int]:
    """Upsert one already-fetched response without advancing any checkpoint.

    API requests must finish before calling this function so a login prompt or
    rate-limit wait never holds a database transaction open. If ``session`` is
    passed, its caller owns the transaction and this function neither manages
    nor closes it.
    """
    records = build_research_records(page)
    for observation in records.api_field_observations:
        observation.run_id = run_id
        observation.observation_key = _api_field_observation_key(observation)
    if session is not None:
        return await _persist_research_records(session, records)
    if async_sessionmaker_ is None:
        raise ValueError("An async sessionmaker is required when no session is supplied.")

    async with async_sessionmaker_() as managed_session:
        async with managed_session.begin():
            return await _persist_research_records(managed_session, records)


async def _persist_research_records(session: Any, records: ResearchRecords) -> dict[str, int]:
    persisted_media_count = 0
    persisted_post_count = 0
    persisted_comment_count = 0
    await _exclude_known_first_post_duplicates(session, records)
    await _validate_api_preview_identity_rows(session, records.api_preview_comment_identities)
    if records.comments:
        await _fill_parent_post_authors(session, records.comments)
    accepted_content = await _upsert_content_observations(
        session,
        _dedupe(records.content_observations, ("owner_type", "owner_id")),
    )
    thread_rows = _dedupe(records.threads, ("tid",))
    if any(row.source_api == "aiotieba.get_posts" for row in thread_rows):
        thread_rows = [
            row
            for row in thread_rows
            if row.source_api != "aiotieba.get_posts" or ("thread_first_post", row.tid) in accepted_content
        ]
    post_rows = [row for row in _dedupe(records.posts, ("pid",)) if ("post", row.pid) in accepted_content]
    comment_rows = [
        row for row in _dedupe(records.comments, ("cid",)) if ("comment", row.cid) in accepted_content
    ]

    # Validate all existing content identities under their row locks before any
    # canonical row (including ResearchUser) is changed for this page.
    await _validate_content_identity_rows(session, ResearchThread, thread_rows)
    await _validate_content_identity_rows(session, ResearchPost, post_rows)
    await _validate_content_identity_rows(session, ResearchComment, comment_rows)

    accepted_media_owners = set(starmap(_content_owner_to_media_owner, accepted_content))
    for owner_type, owner_id in sorted(accepted_media_owners):
        await session.execute(
            delete(MediaAsset).where(
                MediaAsset.owner_type == owner_type,
                MediaAsset.owner_id == owner_id,
            )
        )

    await _upsert_research_users(session, records.users)
    await _upsert_rows(
        session,
        UserForumSnapshot,
        _dedupe(records.user_forum_snapshots, ("user_id", "fid", "source_api", "observed_at")),
    )
    await _upsert_research_threads(
        session,
        thread_rows,
        accepted_content,
    )
    await _upsert_content_rows(session, ResearchPost, post_rows)
    await _upsert_content_rows(session, ResearchComment, comment_rows)
    await _persist_api_field_observations(session, records.api_field_observations)
    api_comment_ids = {row.target_id for row in records.api_field_observations}
    await _apply_latest_api_comment_observations(session, api_comment_ids | {row.cid for row in comment_rows})
    persisted_post_count = len(post_rows)
    persisted_comment_count = len(comment_rows)
    media_rows = _dedupe(
        [row for row in records.media_assets if _media_asset_observation_key(row) in accepted_content],
        ("owner_type", "owner_id", "position"),
    )
    await _upsert_rows(session, MediaAsset, media_rows)
    persisted_media_count = len(media_rows)

    return {
        "threads": len(_dedupe(records.threads, ("tid",))),
        "posts": persisted_post_count,
        "comments": persisted_comment_count,
        "users": len(_dedupe(records.users, ("user_id",))),
        "user_forum_snapshots": len(
            _dedupe(records.user_forum_snapshots, ("user_id", "fid", "source_api", "observed_at"))
        ),
        "media_assets": persisted_media_count,
    }


async def _exclude_known_first_post_duplicates(session: Any, records: ResearchRecords) -> None:
    post_tids = {_positive_id(row.tid) for row in records.posts}
    post_tids.discard(None)
    if not post_tids:
        return

    result = await session.execute(
        select(ResearchThread.tid, ResearchThread.first_pid)
        .where(ResearchThread.tid.in_(post_tids))
        .with_for_update()
    )
    first_pid_by_tid = dict(result.all())
    for thread in records.threads:
        thread_tid = _positive_id(thread.tid)
        thread_first_pid = _positive_id(thread.first_pid)
        if thread_tid is not None and thread_first_pid is not None:
            if first_pid_by_tid.get(thread_tid) is None:
                first_pid_by_tid[thread_tid] = thread_first_pid

    excluded = {
        (post.tid, post.pid)
        for post in records.posts
        if first_pid_by_tid.get(post.tid) == post.pid
    }
    if not excluded:
        return

    records.posts[:] = [post for post in records.posts if (post.tid, post.pid) not in excluded]
    records.content_observations[:] = [
        row
        for row in records.content_observations
        if not (
            row.owner_type == "post"
            and (row.tid, row.owner_id) in excluded
        )
    ]
    records.media_assets[:] = [
        row
        for row in records.media_assets
        if not (
            row.owner_type == "post"
            and (row.tid, row.owner_id) in excluded
        )
    ]


def _content_identity_fields(model: type) -> tuple[str, ...]:
    if model is ResearchThread:
        return ("fid", "first_pid", "author_user_id")
    if model is ResearchPost:
        return ("tid", "fid", "author_user_id")
    if model is ResearchComment:
        return ("tid", "parent_post_pid", "fid", "author_user_id")
    raise ValueError(f"Unsupported content identity model: {model.__name__}.")


def _identity_value_is_known(value: Any) -> bool:
    return value is not None and not (isinstance(value, int) and not isinstance(value, bool) and value == 0)


def _assert_content_identity_stable(model: type, current: Any, incoming: Any) -> None:
    for field_name in _content_identity_fields(model):
        previous_value = getattr(current, field_name)
        incoming_value = getattr(incoming, field_name)
        if (
            _identity_value_is_known(previous_value)
            and _identity_value_is_known(incoming_value)
            and previous_value != incoming_value
        ):
            raise ValueError(
                f"{model.__name__}.{field_name} identity changed for an existing primary key."
            )


async def _validate_content_identity_rows(session: Any, model: type, rows: list[Any]) -> None:
    if not rows:
        return
    primary_key = tuple(column.name for column in model.__table__.primary_key.columns)
    if len(primary_key) != 1:
        raise RuntimeError("Research content rows must have a single identity key.")
    pk_name = primary_key[0]
    for incoming in sorted(rows, key=lambda row: getattr(row, pk_name)):
        current = await session.scalar(
            select(model)
            .where(model.__table__.c[pk_name] == getattr(incoming, pk_name))
            .with_for_update()
        )
        if current is None or _utc_datetime(incoming.observed_at) < _utc_datetime(current.observed_at):
            continue
        _assert_content_identity_stable(model, current, incoming)


def _api_field_observation_key(observation: ResearchFieldObservation) -> str:
    stable = {
        "run_id": observation.run_id,
        "target_type": observation.target_type,
        "target_id": observation.target_id,
        "tid": observation.tid,
        "cid": observation.cid,
        "parent_post_pid": observation.parent_post_pid,
        "author_user_id": observation.author_user_id,
        "fid": observation.fid,
        "field_name": observation.field_name,
        "raw_value": observation.raw_value_json,
        "normalized_value": observation.normalized_value_json,
        "source_path": observation.source_path,
        "observed_at": _utc_datetime(observation.observed_at).isoformat(),
    }
    encoded = json.dumps(stable, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


async def _validate_api_preview_identity_rows(
    session: Any,
    identities: list[tuple[int, int, int, int, int]],
) -> None:
    targets = {
        cid: (tid, parent_post_pid, fid, author_user_id)
        for cid, tid, parent_post_pid, fid, author_user_id in identities
    }
    for cid, observed_identity in sorted(targets.items()):
        current = await session.scalar(
            select(ResearchComment).where(ResearchComment.cid == cid).with_for_update()
        )
        if current is None:
            continue
        identity = (current.tid, current.parent_post_pid, current.fid, current.author_user_id)
        if identity != observed_identity:
            raise ValueError("get_posts preview identity conflicts with the existing comment CID.")


async def _persist_api_field_observations(session: Any, observations: list[ResearchFieldObservation]) -> None:
    if not observations:
        return
    insert_factory = _insert_factory(session.get_bind().dialect.name)
    for observation in observations:
        statement = insert_factory(ResearchFieldObservation).values(**_row_values(observation))
        await session.execute(
            statement.on_conflict_do_nothing(index_elements=[ResearchFieldObservation.observation_key])
        )


async def _apply_latest_api_comment_observations(session: Any, comment_ids: set[int]) -> None:
    if not comment_ids:
        return
    rows = (
        await session.scalars(
            select(ResearchFieldObservation)
            .where(
                ResearchFieldObservation.target_type == "comment",
                ResearchFieldObservation.target_id.in_(sorted(comment_ids)),
                ResearchFieldObservation.source_kind == "api_raw",
                ResearchFieldObservation.validation_status == "verified",
            )
            .order_by(ResearchFieldObservation.observed_at.desc(), ResearchFieldObservation.observation_key.desc())
        )
    ).all()
    latest: dict[tuple[int, str], ResearchFieldObservation] = {}
    for observation in rows:
        if not observation.source_path.startswith("api.comments_preview."):
            continue
        latest.setdefault((observation.target_id, observation.field_name), observation)

    for cid in sorted({target_id for target_id, _ in latest}):
        current = await session.scalar(select(ResearchComment).where(ResearchComment.cid == cid).with_for_update())
        if current is None:
            continue
        sources = deepcopy(current.field_sources_json or {})
        missing = dict(current.missing_reasons_json or {})
        changed = False
        for (target_id, field_name), observation in sorted(latest.items()):
            if target_id != cid or field_name not in _COMMENT_PREVIEW_FIELDS:
                continue
            if (
                current.tid,
                current.parent_post_pid,
                current.fid,
                current.author_user_id,
            ) != (
                observation.tid,
                observation.parent_post_pid,
                observation.fid,
                observation.author_user_id,
            ):
                continue
            value = observation.normalized_value_json
            if not _comment_preview_value_is_known(field_name, value):
                continue
            source = sources.get(field_name)
            source = dict(source) if isinstance(source, dict) else {}
            current_value = getattr(current, field_name)
            reply_target_uid = None
            display_only_reply_target = False
            if field_name == "reply_to_display_name":
                raw_reply_target = observation.raw_value_json
                if isinstance(raw_reply_target, Mapping):
                    reply_target_uid = _positive_id(raw_reply_target.get("reply_to_user_id"))
                name_bindings = _reply_name_binding_uids(sources)
                _, unbound_names = _reply_name_provenance_state(sources)
                current_reply_uid = _positive_id(current.reply_to_user_id)
                binding_conflict = bool(name_bindings) and (
                    reply_target_uid is None or name_bindings != {reply_target_uid}
                )
                name_conflict = bool(unbound_names) and value not in unbound_names
                uid_conflict = reply_target_uid is not None and (
                    current_reply_uid is not None and current_reply_uid != reply_target_uid
                )
                unbound_uid_conflict = reply_target_uid is None and current_reply_uid is not None
                if binding_conflict or name_conflict or uid_conflict or unbound_uid_conflict:
                    _append_api_field_conflict(source, observation, value)
                    sources[field_name] = source
                    if binding_conflict:
                        missing[field_name] = "api_reply_target_uid_conflicts_with_verified_name_binding"
                    elif name_conflict:
                        missing[field_name] = "api_reply_target_uid_unlinked_from_display_name"
                    elif unbound_uid_conflict:
                        missing[field_name] = "api_reply_target_uid_missing_for_known_canonical_uid"
                    else:
                        missing[field_name] = "api_reply_target_uid_conflict"
                    _record_reply_target_candidate(current, observation, "uid_or_name_binding_conflict")
                    changed = True
                    continue
                display_only_reply_target = reply_target_uid is None
                _record_reply_target_candidate(
                    current,
                    observation,
                    "display_name_only" if display_only_reply_target else "uid_match",
                )
                changed = True
                if reply_target_uid is not None and current_reply_uid is None:
                    current.reply_to_user_id = reply_target_uid
                    missing.pop("reply_to_user_id", None)
                current.reply_target_source = (
                    "api_preview_reply_prefix_type4_display_only"
                    if display_only_reply_target
                    else "api_preview_reply_prefix_type4_mention"
                )
                current.reply_target_confidence = (
                    "display_name_only"
                    if display_only_reply_target
                    else "explicit_leading_reply_prefix_and_type4_uid"
                )
                current.reply_resolution_note = (
                    "Target name came from the exact leading reply prefix and type-4 mention; its UID was unavailable."
                    if display_only_reply_target
                    else "Target name and UID came from the exact leading reply prefix and type-4 mention."
                )
                if reply_target_uid is not None:
                    uid_source = dict(sources.get("reply_to_user_id") or {})
                    uid_field_source = dict(_api_field_source(observation, reply_target_uid))
                    uid_field_source["source_path"] = "api.comments_preview.reply_target.user_id"
                    uid_field_source["confidence"] = "explicit_leading_reply_prefix_and_type4_uid"
                    uid_source["verified_content_api"] = uid_field_source
                    sources["reply_to_user_id"] = uid_source
            web_value = source.get("verified_web")
            if isinstance(web_value, dict):
                if current_value != value:
                    _append_api_field_conflict(source, observation, value)
                    changed = True
                elif field_name == "reply_to_display_name":
                    content_api_source = _api_field_source(observation, value)
                    content_api_source["confidence"] = (
                        "display_name_only"
                        if display_only_reply_target
                        else "explicit_leading_reply_prefix_and_type4_uid"
                    )
                    content_api_source["reply_target_user_id"] = reply_target_uid
                    source["verified_content_api"] = content_api_source
                    changed = True
                if changed:
                    sources[field_name] = source
                continue
            if not _comment_preview_value_is_unknown(field_name, current_value, missing):
                if current_value != value:
                    _append_api_field_conflict(source, observation, value)
                else:
                    content_api_source = _api_field_source(observation, value)
                    if field_name == "reply_to_display_name":
                        content_api_source["confidence"] = (
                            "display_name_only"
                            if display_only_reply_target
                            else "explicit_leading_reply_prefix_and_type4_uid"
                        )
                        content_api_source["reply_target_user_id"] = reply_target_uid
                    source["verified_content_api"] = content_api_source
                sources[field_name] = source
                changed = True
                missing.pop(field_name, None)
                continue

            setattr(current, field_name, value)
            content_api_source = _api_field_source(observation, value)
            if field_name == "reply_to_display_name":
                content_api_source["confidence"] = (
                    "display_name_only"
                    if display_only_reply_target
                    else "explicit_leading_reply_prefix_and_type4_uid"
                )
                content_api_source["reply_target_user_id"] = reply_target_uid
            source["verified_content_api"] = content_api_source
            sources[field_name] = source
            missing.pop(field_name, None)
            if field_name == "ip_location_text":
                current.ip_location_source = observation.source_path
                current.ip_location_observed_at = observation.observed_at
            changed = True
        if changed:
            current.field_sources_json = sources or None
            current.missing_reasons_json = missing


def _record_reply_target_candidate(
    current: ResearchComment,
    observation: ResearchFieldObservation,
    resolution: str,
) -> None:
    payload = dict(current.source_payload_json or {})
    candidates = list(payload.get("reply_target_candidates") or [])
    candidate = {
        "source_path": observation.source_path,
        "observed_at": _utc_datetime(observation.observed_at).isoformat(),
        "raw": _jsonable(observation.raw_value_json),
        "resolution": resolution,
    }
    if candidate not in candidates:
        candidates.append(candidate)
    payload["reply_target_candidates"] = candidates
    current.source_payload_json = payload


def _api_field_source(observation: ResearchFieldObservation, value: Any) -> dict[str, Any]:
    return {
        "source_kind": "api_raw",
        "source_path": observation.source_path,
        "observed_at": _utc_datetime(observation.observed_at).isoformat(),
        "temporal_scope": observation.temporal_scope,
        "value": _jsonable(value),
        "applied_at": datetime.now(UTC).isoformat(),
    }


def _append_api_field_conflict(source: dict[str, Any], observation: ResearchFieldObservation, value: Any) -> None:
    conflicts = list(source.get("api_conflicts") or [])
    conflict = {
        "source_kind": "api_raw",
        "source_path": observation.source_path,
        "observed_at": _utc_datetime(observation.observed_at).isoformat(),
        "value": _jsonable(value),
    }
    if conflict not in conflicts:
        conflicts.append(conflict)
    source["api_conflicts"] = conflicts


def _comment_preview_value_is_known(field_name: str, value: Any) -> bool:
    if field_name in {
        "author_user_name",
        "author_nickname",
        "author_portrait",
        "global_growth_icon_url",
        "ip_location_text",
        "reply_to_display_name",
    }:
        return isinstance(value, str) and bool(value.strip())
    if field_name in {"forum_level", "global_growth_level", "reply_to_user_id"}:
        return isinstance(value, int) and not isinstance(value, bool) and value > 0
    return field_name == "is_bawu" and value is True


def _comment_preview_value_is_unknown(field_name: str, value: Any, missing: Any) -> bool:
    if value is None or (isinstance(value, str) and not value.strip()):
        return True
    if field_name in {"forum_level", "global_growth_level", "reply_to_user_id"}:
        return isinstance(value, int) and not isinstance(value, bool) and value <= 0
    if field_name == "is_bawu":
        return value is False
    return False


def build_research_records(page: ResearchPage) -> ResearchRecords:
    """Map a raw response into project-owned ORM rows using source values only."""
    if page.raw is None:
        if page.source_api == "aiotieba.get_user_forum_info":
            return _records_from_user_forum_page(page)
        if page.source_api in {"aiotieba.get_user_info", "aiotieba.get_bawu_info"}:
            return ResearchRecords()
        raise ValueError(f"Missing raw response for {page.source_api}.")

    if page.source_api == "aiotieba.get_threads":
        return _records_from_threads_page(page)
    if page.source_api == "aiotieba.get_posts":
        return _records_from_posts_page(page)
    if page.source_api == "aiotieba.get_comments":
        return _records_from_comments_page(page)
    if page.source_api == "aiotieba.get_user_forum_info":
        return _records_from_user_forum_page(page)
    if page.source_api == "aiotieba.get_user_info":
        return _records_from_user_profile_page(page)
    if page.source_api == "aiotieba.get_bawu_info":
        return _records_from_bawu_page(page)
    raise ValueError(f"Unsupported research source: {page.source_api}.")


def _records_from_threads_page(page: ResearchPage) -> ResearchRecords:
    records = ResearchRecords()
    for thread in page.raw.objs:
        tid = _positive_id(getattr(thread, "tid", None))
        if tid is None:
            raise ValueError("get_threads response contains a thread without a valid tid.")

        user = getattr(thread, "user", None)
        missing = {
            "body_text": "requires_get_posts_detail",
            "contents_raw": "requires_get_posts_detail",
            "collect_count": "not_exposed_by_get_threads",
            "role_name": "not_exposed_by_get_threads",
            "role_type": "not_exposed_by_get_threads",
            "role_badge_id": "icon_semantics_unverified",
            "role_badge_url": "not_exposed_by_get_threads",
            "ip_location_text": "not_exposed_by_get_threads",
            "forum_level_name": "not_exposed_by_get_threads",
        }
        missing.update(_missing_user_reasons(user, page.source_api, record=thread))
        if _positive_id(getattr(thread, "pid", None)) is None:
            missing["first_pid"] = "aiotieba_zero_sentinel"
        if _positive_id(getattr(thread, "fid", None)) is None:
            missing["fid"] = "aiotieba_zero_sentinel"
        thread_row = ResearchThread(
            tid=tid,
            first_pid=_positive_id(getattr(thread, "pid", None)),
            fid=_positive_id(getattr(thread, "fid", None)),
            fname=_optional_text(getattr(thread, "fname", None)),
            title=_optional_text(getattr(thread, "title", None)),
            body_text=None,
            contents_raw=None,
            create_time=_epoch_datetime(getattr(thread, "create_time", None)),
            create_time_raw=_int_value(getattr(thread, "create_time", None)),
            last_time=_epoch_datetime(getattr(thread, "last_time", None)),
            last_time_raw=_int_value(getattr(thread, "last_time", None)),
            author_user_id=_author_user_id(thread, user),
            author_user_name=_optional_text(getattr(user, "user_name", None)),
            author_nickname=_optional_text(_nickname(user)),
            author_portrait=_optional_text(getattr(user, "portrait", None)),
            author_tieba_uid=_safe_tieba_uid(getattr(user, "tieba_uid", None), "author_tieba_uid", missing),
            forum_level=_protobuf_int_field(getattr(user, "level", None), "forum_level", missing),
            global_growth_level=_protobuf_int_field(getattr(user, "glevel", None), "global_growth_level", missing),
            global_growth_icon_url=_optional_text(getattr(user, "global_growth_icon_url", None)),
            is_bawu=_content_is_bawu(getattr(user, "is_bawu", None), missing),
            author_icons=_jsonable(getattr(user, "icons", None)),
            role_type=None,
            role_name=None,
            role_badge_id=None,
            role_badge_url=None,
            ip_location_text=_ip_location_text(user),
            ip_location_source=_ip_location_source(page, thread) if _ip_location_text(user) else None,
            ip_location_observed_at=page.observed_at if _ip_location_text(user) else None,
            reply_count=_count_value(getattr(thread, "reply_num", None), "reply_count", missing),
            agree_count=_count_value(getattr(thread, "agree", None), "agree_count", missing),
            disagree_count=_count_value(getattr(thread, "disagree", None), "disagree_count", missing),
            collect_count=None,
            view_count=_count_value(getattr(thread, "view_num", None), "view_count", missing),
            share_count=_count_value(getattr(thread, "share_num", None), "share_count", missing),
            thread_type=_enum_int(getattr(thread, "type", None)),
            is_top=_optional_bool(getattr(thread, "is_top", None)),
            is_good=_optional_bool(getattr(thread, "is_good", None)),
            is_livepost=_optional_bool(getattr(thread, "is_livepost", None)),
            is_hide=_optional_bool(getattr(thread, "is_hide", None)),
            is_share=_optional_bool(getattr(thread, "is_share", None)),
            is_help=_enum_name(getattr(thread, "type", None)) == ThreadType.HELP.name,
            scraped_at=page.observed_at,
            source_api=page.source_api,
            observed_at=page.observed_at,
            source_payload_json=_thread_list_payload(thread, user),
            missing_reasons_json=missing,
        )
        records.threads.append(thread_row)
        _append_user_observations(records, user, thread_row.fid, thread_row.fname, page, record=thread)
    return records


def _records_from_posts_page(page: ResearchPage) -> ResearchRecords:
    records = ResearchRecords()
    response = page.raw
    raw_thread = getattr(response, "thread", None)
    thread_tid = _positive_id(getattr(raw_thread, "tid", None))
    raw_thread_fid = _positive_id(getattr(raw_thread, "fid", None))
    forum_fid = _positive_id(getattr(getattr(response, "forum", None), "fid", None))
    if raw_thread_fid is not None and forum_fid is not None and raw_thread_fid != forum_fid:
        raise ValueError("get_posts thread TID/FID does not match its response forum.")
    thread_fid = raw_thread_fid or forum_fid
    first_pid = None
    first_post = None
    page_posts = tuple(getattr(response, "objs", ()) or ())
    if thread_tid is not None:
        floor_one_posts = [post for post in page_posts if _is_floor_one_post(post)]
        first_post_candidates = []
        for post in floor_one_posts:
            raw_post_tid = _positive_id(getattr(post, "tid", None))
            raw_post_fid = _positive_id(getattr(post, "fid", None))
            if (raw_post_tid is not None and raw_post_tid != thread_tid) or (
                raw_post_fid is not None and thread_fid is not None and raw_post_fid != thread_fid
            ):
                raise ValueError("get_posts floor=1 post TID/FID does not match its thread.")
            post_tid = raw_post_tid or thread_tid
            post_fid = raw_post_fid or thread_fid
            if post_tid == thread_tid and thread_fid is not None and post_fid == thread_fid:
                first_post_candidates.append(post)

        candidate_pids = {_positive_id(getattr(post, "pid", None)) for post in first_post_candidates}
        if None in candidate_pids:
            raise ValueError("get_posts floor=1 post is missing a valid pid.")
        if len(candidate_pids) > 1:
            raise ValueError("get_posts page contains multiple floor=1 post IDs for one thread.")

        if first_post_candidates:
            first_post = first_post_candidates[0]
            raw_user = getattr(raw_thread, "user", None)
            thread_author_id = _author_user_id(raw_thread, raw_user)
            for candidate in first_post_candidates:
                post_author_id = _author_user_id(candidate, getattr(candidate, "user", None))
                if (
                    thread_author_id is not None
                    and post_author_id is not None
                    and thread_author_id != post_author_id
                ):
                    raise ValueError("get_posts floor=1 post author does not match its thread author.")

        is_share = bool(getattr(raw_thread, "is_share", False))
        thread_contents = getattr(raw_thread, "contents", None)
        thread_fragments = _content_fragments(thread_contents)
        raw_user = getattr(raw_thread, "user", None)
        missing = _missing_user_reasons(raw_user, page.source_api, record=raw_thread)
        for field_name in ("forum_level_name", "role_type", "role_name"):
            missing.setdefault(field_name, "not_exposed_by_get_posts_author")
        missing["role_badge_id"] = "icon_semantics_unverified"
        missing["role_badge_url"] = "not_exposed_by_get_posts_author"
        if thread_fid is None:
            missing["fid"] = "aiotieba_zero_sentinel"

        first_post_pid = _positive_id(getattr(first_post, "pid", None)) if first_post is not None else None
        first_pid = first_post_pid

        # Thread_p is returned on every page, but an empty parsed Contents object
        # does not prove that the first post itself was observed on this page.
        # Only a uniquely matched floor 1 Post makes its body and media
        # authoritative for replacement.
        if first_post is not None:
            first_contents = getattr(first_post, "contents", None)
            if _has_content_data(first_contents):
                contents = first_contents
            elif _has_content_data(thread_contents):
                contents = thread_contents
            else:
                contents = first_contents if first_contents is not None else thread_contents
            fragments = _content_fragments(contents)
            body_missing = is_share and not thread_fragments
            if body_missing:
                missing["body_text"] = "shared_thread_own_body_unavailable_in_get_posts"

        first_post_raw_count = _int_value(getattr(first_post, "reply_num", None)) if first_post is not None else None
        if first_post is None:
            first_post_reported_count = None
            first_post_missing_reason = "floor1_post_not_returned_by_get_posts_objs"
            missing["first_post.subreply_count_reported"] = first_post_missing_reason
        else:
            first_post_reported_count = _count_value(
                first_post_raw_count,
                "first_post.subreply_count_reported",
                missing,
            )
            first_post_missing_reason = missing.get("first_post.subreply_count_reported")
            if first_post_raw_count is None and first_post_missing_reason is None:
                first_post_missing_reason = "not_returned_by_source"
                missing["first_post.subreply_count_reported"] = first_post_missing_reason

            source_payload = _thread_detail_payload(raw_thread, raw_user)
            source_payload["first_post"] = {
                "pid": first_post_pid,
                "floor": _int_value(getattr(first_post, "floor", None)),
                "observed_at": page.observed_at.isoformat(),
                "subreply_count_reported_raw": first_post_raw_count,
                "subreply_count_reported": first_post_reported_count,
                "source": "aiotieba.get_posts.objs[].reply_num",
                "missing_reason": first_post_missing_reason,
            }

            thread_row = ResearchThread(
                tid=thread_tid,
                first_pid=first_pid,
                fid=thread_fid,
                fname=_optional_text(getattr(raw_thread, "fname", None)),
                title=_optional_text(getattr(raw_thread, "title", None)),
                body_text=None if body_missing else _content_text(contents),
                contents_raw=_jsonable(fragments),
                create_time=_epoch_datetime(getattr(raw_thread, "create_time", None)),
                create_time_raw=_int_value(getattr(raw_thread, "create_time", None)),
                last_time=None,
                last_time_raw=None,
                author_user_id=_author_user_id(raw_thread, raw_user),
                author_user_name=_optional_text(getattr(raw_user, "user_name", None)),
                author_nickname=_optional_text(_nickname(raw_user)),
                author_portrait=_optional_text(getattr(raw_user, "portrait", None)),
                author_tieba_uid=_safe_tieba_uid(getattr(raw_user, "tieba_uid", None), "author_tieba_uid", missing),
                forum_level=_protobuf_int_field(getattr(raw_user, "level", None), "forum_level", missing),
                global_growth_level=_protobuf_int_field(
                    getattr(raw_user, "glevel", None), "global_growth_level", missing
                ),
                global_growth_icon_url=_optional_text(getattr(raw_user, "global_growth_icon_url", None)),
                is_bawu=_content_is_bawu(getattr(raw_user, "is_bawu", None), missing),
                author_icons=_jsonable(getattr(raw_user, "icons", None)),
                forum_level_name=None,
                role_type=None,
                role_name=None,
                role_badge_id=None,
                role_badge_url=None,
                ip_location_text=_ip_location_text(raw_user),
                ip_location_source=_ip_location_source(page, raw_thread) if _ip_location_text(raw_user) else None,
                ip_location_observed_at=page.observed_at if _ip_location_text(raw_user) else None,
                reply_count=_count_value(getattr(raw_thread, "reply_num", None), "reply_count", missing),
                agree_count=_count_value(getattr(raw_thread, "agree", None), "agree_count", missing),
                disagree_count=_count_value(getattr(raw_thread, "disagree", None), "disagree_count", missing),
                collect_count=_count_value(getattr(raw_thread, "collect_num", None), "collect_count", missing),
                view_count=_count_value(getattr(raw_thread, "view_num", None), "view_count", missing),
                share_count=_count_value(getattr(raw_thread, "share_num", None), "share_count", missing),
                thread_type=_enum_int(getattr(raw_thread, "type", None)),
                is_top=None,
                is_good=None,
                is_livepost=None,
                is_hide=None,
                is_share=is_share,
                is_help=_enum_name(getattr(raw_thread, "type", None)) == ThreadType.HELP.name,
                scraped_at=page.observed_at,
                source_api=page.source_api,
                observed_at=page.observed_at,
                source_payload_json=source_payload,
                missing_reasons_json=missing,
            )
            records.threads.append(thread_row)
            records.content_observations.append(
                ResearchContentObservation(
                    owner_type="thread_first_post",
                    owner_id=thread_tid,
                    tid=thread_tid,
                    source_api=page.source_api,
                    observed_at=page.observed_at,
                )
            )
            records.media_assets.extend(_media_rows("thread", thread_tid, thread_tid, fragments, page))
            _append_user_observations(
                records,
                raw_user,
                thread_fid,
                _optional_text(getattr(raw_thread, "fname", None)),
                page,
                record=raw_thread,
            )

    thread_author_id = _author_user_id(raw_thread, getattr(raw_thread, "user", None))
    seen_preview_comments: dict[int, tuple[tuple[int, int, int, int], dict[str, Any]]] = {}
    if thread_tid is not None:
        for post in page_posts:
            _append_comment_preview_observations(
                records,
                page,
                raw_thread,
                post,
                seen_preview_comments,
                thread_fid_override=thread_fid,
            )

    for post in page_posts:
        pid = _positive_id(getattr(post, "pid", None))
        if pid is None:
            raise ValueError("get_posts response contains a post without a valid pid.")
        if first_post is not None and pid == first_pid and _is_floor_one_post(post):
            continue

        user = getattr(post, "user", None)
        tid = _positive_id(getattr(post, "tid", None)) or thread_tid
        if tid is None:
            raise ValueError("get_posts response contains a post without a valid tid.")
        contents = getattr(post, "contents", None)
        fragments = _content_fragments(contents)
        post_fid = _positive_id(getattr(post, "fid", None)) or thread_fid
        post_fname = _optional_text(getattr(post, "fname", None)) or _optional_text(getattr(raw_thread, "fname", None))
        missing = _missing_user_reasons(user, page.source_api, record=post)
        records.posts.append(
            ResearchPost(
                pid=pid,
                tid=tid,
                fid=post_fid,
                fname=post_fname,
                floor=_int_value(getattr(post, "floor", None)),
                create_time=_epoch_datetime(getattr(post, "create_time", None)),
                create_time_raw=_int_value(getattr(post, "create_time", None)),
                scraped_at=page.observed_at,
                text=_content_text(contents),
                sign=_optional_text(getattr(post, "sign", None)),
                contents_raw=_jsonable(fragments),
                author_user_id=_author_user_id(post, user),
                author_user_name=_optional_text(getattr(user, "user_name", None)),
                author_nickname=_optional_text(_nickname(user)),
                author_portrait=_optional_text(getattr(user, "portrait", None)),
                author_tieba_uid=_safe_tieba_uid(getattr(user, "tieba_uid", None), "author_tieba_uid", missing),
                thread_author_user_id=thread_author_id,
                forum_level=_protobuf_int_field(getattr(user, "level", None), "forum_level", missing),
                global_growth_level=_protobuf_int_field(getattr(user, "glevel", None), "global_growth_level", missing),
                global_growth_icon_url=_optional_text(getattr(user, "global_growth_icon_url", None)),
                is_bawu=_content_is_bawu(getattr(user, "is_bawu", None), missing),
                author_icons=_jsonable(getattr(user, "icons", None)),
                forum_level_name=None,
                role_type=None,
                role_name=None,
                role_badge_id=None,
                role_badge_url=None,
                ip_location_text=_ip_location_text(user),
                ip_location_source=_ip_location_source(page, post) if _ip_location_text(user) else None,
                ip_location_observed_at=page.observed_at if _ip_location_text(user) else None,
                agree_count=_count_value(getattr(post, "agree", None), "agree_count", missing),
                disagree_count=_count_value(getattr(post, "disagree", None), "disagree_count", missing),
                subreply_count_reported=_count_value(
                    getattr(post, "reply_num", None), "subreply_count_reported", missing
                ),
                is_thread_author=_optional_bool(getattr(post, "is_thread_author", None)),
                source_api=page.source_api,
                observed_at=page.observed_at,
                source_payload_json=_post_payload(post, user),
                missing_reasons_json=missing,
            )
        )
        records.content_observations.append(
            ResearchContentObservation(
                owner_type="post",
                owner_id=pid,
                tid=tid,
                source_api=page.source_api,
                observed_at=page.observed_at,
            )
        )
        _append_user_observations(records, user, post_fid, post_fname, page, record=post)
        if contents is None:
            missing["contents_raw"] = "not_returned_by_source"
        records.media_assets.extend(_media_rows("post", pid, tid, fragments, page))
    return records


def _append_comment_preview_observations(
    records: ResearchRecords,
    page: ResearchPage,
    thread: Any,
    post: Any,
    seen: dict[int, tuple[tuple[int, int, int, int], dict[str, Any]]],
    *,
    thread_fid_override: int | None = None,
) -> None:
    comments = getattr(post, "comments", ()) or ()
    if not comments:
        return

    tid = _positive_id(getattr(thread, "tid", None))
    thread_fid = _positive_id(getattr(thread, "fid", None)) or thread_fid_override
    parent_pid = _positive_id(getattr(post, "pid", None))
    post_tid = _positive_id(getattr(post, "tid", None))
    post_fid = _positive_id(getattr(post, "fid", None))
    if tid is None or thread_fid is None or parent_pid is None:
        raise ValueError("get_posts comment preview is missing thread, forum, or parent identity.")
    if (post_tid is not None and post_tid != tid) or (post_fid is not None and post_fid != thread_fid):
        raise ValueError("get_posts comment preview parent scope does not match its thread.")

    for comment in comments:
        cid = _positive_id(getattr(comment, "pid", None))
        comment_tid = _positive_id(getattr(comment, "tid", None))
        comment_parent_pid = _positive_id(getattr(comment, "ppid", None))
        comment_fid = _positive_id(getattr(comment, "fid", None))
        user = getattr(comment, "user", None)
        record_author_id = _positive_id(getattr(comment, "author_id", None))
        user_author_id = _positive_id(getattr(user, "user_id", None))
        if cid is None or (record_author_id is None or record_author_id != user_author_id):
            raise ValueError("get_posts comment preview is missing or disagrees on author identity.")
        identity = (tid, parent_pid, thread_fid, record_author_id)
        if comment_tid != tid or comment_parent_pid != parent_pid or comment_fid != thread_fid:
            raise ValueError("get_posts comment preview identity does not match its thread and parent post.")

        raw_values = {
            "author_user_name": getattr(user, "user_name", None),
            "author_nickname": _nickname(user),
            "author_portrait": getattr(user, "portrait", None),
            "forum_level": getattr(user, "level", None),
            "global_growth_level": getattr(user, "glevel", None),
            "global_growth_icon_url": getattr(user, "global_growth_icon_url", None),
            "is_bawu": getattr(user, "is_bawu", None),
            "ip_location_text": getattr(user, "ip", None),
        }
        reply_target = _reply_target_candidate(comment)
        values = {
            "author_user_name": _identity_text(raw_values["author_user_name"]),
            "author_nickname": _identity_text(raw_values["author_nickname"]),
            "author_portrait": _identity_text(raw_values["author_portrait"]),
            "forum_level": _positive_metadata_int(raw_values["forum_level"]),
            "global_growth_level": _positive_metadata_int(raw_values["global_growth_level"]),
            "global_growth_icon_url": _optional_text(raw_values["global_growth_icon_url"]),
            "is_bawu": raw_values["is_bawu"] is True,
            "ip_location_text": _ip_location_text(user),
        }
        raw_reply_to_id = _positive_id(getattr(comment, "reply_to_id", None))
        reply_conflict_reason = None
        if reply_target is not None:
            if reply_target["display_name"] is None:
                reply_conflict_reason = "reply_target_missing_display_name"
            elif reply_target["user_id"] is None:
                raw_values["reply_to_display_name"] = reply_target["raw"]
                values["reply_to_display_name"] = reply_target["display_name"]
            elif raw_reply_to_id is not None and raw_reply_to_id != reply_target["user_id"]:
                reply_conflict_reason = "reply_target_uid_conflicts_with_aiotieba_reply_to_id"
            else:
                raw_values["reply_to_display_name"] = reply_target["raw"]
                values["reply_to_display_name"] = reply_target["display_name"]
        signature_values = dict(values)
        if reply_target is not None:
            signature_values["_reply_target_user_id"] = reply_target["user_id"]
        values = {field_name: value for field_name, value in values.items() if value is not None and value is not False}

        previous = seen.get(cid)
        if previous is not None:
            if previous[0] != identity:
                raise ValueError("Repeated get_posts preview CID has conflicting identity tuples.")
            if previous[1] != signature_values:
                raise ValueError("Repeated get_posts preview CID has conflicting metadata values.")
            continue
        seen[cid] = (identity, signature_values)
        records.api_preview_comment_identities.append((cid, tid, parent_pid, thread_fid, record_author_id))

        identity_evidence = {
            "identity_verified": True,
            "proof": {
                "thread_tid": tid,
                "thread_fid": thread_fid,
                "parent_post_pid": parent_pid,
                "comment_cid": cid,
                "comment_tid": comment_tid,
                "comment_ppid": comment_parent_pid,
                "comment_fid": comment_fid,
                "comment_author_id": record_author_id,
                "user_user_id": user_author_id,
            },
        }
        if reply_target is not None:
            identity_evidence["reply_target"] = reply_target["raw"]
        for field_name, value in values.items():
            source_path = f"api.comments_preview.author.{field_name}"
            if field_name == "ip_location_text":
                source_path = "api.comments_preview.author.ip"
            elif field_name == "reply_to_display_name":
                source_path = "api.comments_preview.reply_target.display_name"
            raw_value = _jsonable(raw_values[field_name])
            evidence_key = {
                "target_type": "comment",
                "target_id": cid,
                "tid": tid,
                "parent_post_pid": parent_pid,
                "author_user_id": record_author_id,
                "fid": thread_fid,
                "field_name": field_name,
                "raw_value": raw_value,
                "normalized_value": value,
                "source_path": source_path,
                "observed_at": page.observed_at.isoformat(),
            }
            encoded = json.dumps(evidence_key, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
            records.api_field_observations.append(
                ResearchFieldObservation(
                    observation_id=str(uuid4()),
                    observation_key=hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
                    task_key=None,
                    run_id=None,
                    target_type="comment",
                    target_id=cid,
                    tid=tid,
                    pid=None,
                    cid=cid,
                    parent_post_pid=parent_pid,
                    author_user_id=record_author_id,
                    fid=thread_fid,
                    field_name=field_name,
                    raw_value_json=raw_value,
                    normalized_value_json=value,
                    source_url=None,
                    source_path=source_path,
                    source_kind="api_raw",
                    observed_at=page.observed_at,
                    temporal_scope="api_observation_time_not_publication_time",
                    parser_version="aiotieba-parsed-response-v1",
                    identity_evidence_json=identity_evidence,
                    validation_status="verified",
                    applied_status="observed_only",
                    missing_reason=None,
                    created_at=page.observed_at,
                    applied_at=None,
                )
            )
        if reply_target is not None and reply_conflict_reason is not None:
            source_path = "api.comments_preview.reply_target.display_name"
            evidence_key = {
                "target_type": "comment",
                "target_id": cid,
                "tid": tid,
                "parent_post_pid": parent_pid,
                "author_user_id": record_author_id,
                "fid": thread_fid,
                "field_name": "reply_to_display_name",
                "raw_value": reply_target["raw"],
                "normalized_value": None,
                "source_path": source_path,
                "observed_at": page.observed_at.isoformat(),
            }
            encoded = json.dumps(evidence_key, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
            records.api_field_observations.append(
                ResearchFieldObservation(
                    observation_id=str(uuid4()),
                    observation_key=hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
                    task_key=None,
                    run_id=None,
                    target_type="comment",
                    target_id=cid,
                    tid=tid,
                    pid=None,
                    cid=cid,
                    parent_post_pid=parent_pid,
                    author_user_id=record_author_id,
                    fid=thread_fid,
                    field_name="reply_to_display_name",
                    raw_value_json=reply_target["raw"],
                    normalized_value_json=None,
                    source_url=None,
                    source_path=source_path,
                    source_kind="api_raw",
                    observed_at=page.observed_at,
                    temporal_scope="api_observation_time_not_publication_time",
                    parser_version="aiotieba-parsed-response-v1",
                    identity_evidence_json=identity_evidence,
                    validation_status="conflict",
                    applied_status="observed_only",
                    missing_reason=reply_conflict_reason,
                    created_at=page.observed_at,
                    applied_at=None,
                )
            )


def _records_from_comments_page(page: ResearchPage) -> ResearchRecords:
    records = ResearchRecords()
    response = page.raw
    response_fid = _positive_id(getattr(getattr(response, "forum", None), "fid", None))
    response_fname = _optional_text(getattr(getattr(response, "forum", None), "fname", None))
    for comment in getattr(response, "objs", ()):
        cid = _positive_id(getattr(comment, "pid", None))
        if cid is None:
            raise ValueError("get_comments response contains a comment without a valid cid.")
        tid = _positive_id(getattr(comment, "tid", None))
        parent_pid = _positive_id(getattr(comment, "ppid", None))
        if tid is None:
            raise ValueError("get_comments response contains a comment without a valid tid.")

        user = getattr(comment, "user", None)
        raw_reply_to_id = _int_value(getattr(comment, "reply_to_id", None))
        reply_to_user_id = _positive_id(raw_reply_to_id)
        missing = _missing_user_reasons(user, page.source_api, record=comment)
        if reply_to_user_id is None:
            missing["reply_to_user_id"] = "aiotieba_zero_sentinel"
        if parent_pid is None:
            missing["parent_post_pid"] = "aiotieba_zero_sentinel"
        contents = getattr(comment, "contents", None)
        fragments = _content_fragments(contents)
        reply_target = _reply_target_candidate(comment)
        reply_to_display_name = None
        reply_source = None
        reply_confidence = None
        reply_note = None
        comment_field_sources: dict[str, Any] = {}
        source_payload = _comment_payload(comment, user)
        if reply_target is None:
            missing["reply_to_display_name"] = (
                "aiotieba_parser_removed_reply_prefix_fragments"
                if reply_to_user_id is not None
                else "no_explicit_reply_target"
            )
            reply_note = (
                "aiotieba 4.8.2 exposes reply_to_id but removes the leading reply and type-4 mention fragments."
                if reply_to_user_id is not None
                else "No explicit leading reply prefix and type-4 mention were observed."
            )
        else:
            source_payload["reply_target_evidence"] = {
                "source_path": "api.get_comments.reply_prefix.type4_mention",
                "observed_at": page.observed_at.isoformat(),
                "raw": reply_target["raw"],
            }
            if reply_target["display_name"] is None:
                missing["reply_to_display_name"] = "reply_target_missing_display_name"
                reply_note = "A leading type-4 mention did not contain a usable display name."
            elif reply_target["user_id"] is None and reply_to_user_id is not None:
                missing["reply_to_display_name"] = "reply_target_uid_missing_for_known_reply_to_user_id"
                reply_note = "A leading type-4 mention had no UID while reply_to_id was already known."
            elif reply_target["user_id"] is None:
                reply_to_display_name = reply_target["display_name"]
                missing.pop("reply_to_display_name", None)
                reply_source = "api_raw_reply_prefix_type4_display_only"
                reply_confidence = "display_name_only"
                reply_note = (
                    "Target name came from the exact leading reply prefix and type-4 mention; its UID was unavailable."
                )
                comment_field_sources["reply_to_display_name"] = {
                    "verified_content_api": {
                        "source_kind": "api_raw",
                        "source_path": "api.get_comments.reply_prefix.type4_mention",
                        "observed_at": page.observed_at.isoformat(),
                        "temporal_scope": "api_observation_time_not_publication_time",
                        "confidence": reply_confidence,
                        "reply_target_user_id": None,
                        "value": reply_to_display_name,
                    }
                }
            elif reply_to_user_id is not None and reply_to_user_id != reply_target["user_id"]:
                missing["reply_to_display_name"] = "reply_target_uid_conflicts_with_aiotieba_reply_to_id"
                source_payload["reply_target_evidence"]["conflict"] = missing["reply_to_display_name"]
                reply_note = "The leading type-4 mention UID conflicts with aiotieba reply_to_id."
            else:
                reply_to_user_id = reply_target["user_id"]
                missing.pop("reply_to_user_id", None)
                missing.pop("reply_to_display_name", None)
                reply_to_display_name = reply_target["display_name"]
                reply_source = "api_raw_reply_prefix_type4_mention"
                reply_confidence = "explicit_leading_reply_prefix_and_type4_uid"
                reply_note = "Target name and UID came from the exact leading reply prefix and type-4 mention."
                source_path = "api.get_comments.reply_prefix.type4_mention"
                source_observed_at = page.observed_at.isoformat()
                comment_field_sources["reply_to_display_name"] = {
                    "verified_content_api": {
                        "source_kind": "api_raw",
                        "source_path": source_path,
                        "observed_at": source_observed_at,
                        "temporal_scope": "api_observation_time_not_publication_time",
                        "confidence": reply_confidence,
                        "reply_target_user_id": reply_to_user_id,
                        "value": reply_to_display_name,
                    }
                }
                comment_field_sources["reply_to_user_id"] = {
                    "verified_content_api": {
                        "source_kind": "api_raw",
                        "source_path": "api.get_comments.reply_prefix.type4_mention.uid",
                        "observed_at": source_observed_at,
                        "temporal_scope": "api_observation_time_not_publication_time",
                        "confidence": reply_confidence,
                        "value": reply_to_user_id,
                    }
                }
        comment_fid = _positive_id(getattr(comment, "fid", None)) or response_fid
        comment_fname = _optional_text(getattr(comment, "fname", None)) or response_fname
        records.comments.append(
            ResearchComment(
                cid=cid,
                tid=tid,
                parent_post_pid=parent_pid,
                parent_post_author_user_id=None,
                fid=comment_fid,
                fname=comment_fname,
                floor=_int_value(getattr(comment, "floor", None)),
                create_time=_epoch_datetime(getattr(comment, "create_time", None)),
                create_time_raw=_int_value(getattr(comment, "create_time", None)),
                scraped_at=page.observed_at,
                text=_content_text(contents),
                contents_raw=_jsonable(fragments),
                author_user_id=_author_user_id(comment, user),
                author_user_name=_optional_text(getattr(user, "user_name", None)),
                author_nickname=_optional_text(_nickname(user)),
                author_portrait=_optional_text(getattr(user, "portrait", None)),
                author_tieba_uid=_safe_tieba_uid(getattr(user, "tieba_uid", None), "author_tieba_uid", missing),
                forum_level=_protobuf_int_field(getattr(user, "level", None), "forum_level", missing),
                global_growth_level=_protobuf_int_field(getattr(user, "glevel", None), "global_growth_level", missing),
                global_growth_icon_url=_optional_text(getattr(user, "global_growth_icon_url", None)),
                is_bawu=_content_is_bawu(getattr(user, "is_bawu", None), missing),
                author_icons=_jsonable(getattr(user, "icons", None)),
                forum_level_name=None,
                role_type=None,
                role_name=None,
                role_badge_id=None,
                role_badge_url=None,
                ip_location_text=_ip_location_text(user),
                ip_location_source=_ip_location_source(page, comment) if _ip_location_text(user) else None,
                ip_location_observed_at=page.observed_at if _ip_location_text(user) else None,
                reply_to_user_id=reply_to_user_id,
                reply_to_display_name=reply_to_display_name,
                reply_to_id_raw=raw_reply_to_id,
                reply_target_source=reply_source
                or ("aiotieba_parsed_reply_prefix" if reply_to_user_id is not None else "unresolved"),
                reply_target_confidence=reply_confidence
                or ("parsed_from_reply_prefix" if reply_to_user_id is not None else "unknown"),
                reply_resolution_note=reply_note,
                agree_count=_count_value(getattr(comment, "agree", None), "agree_count", missing),
                disagree_count=_count_value(getattr(comment, "disagree", None), "disagree_count", missing),
                is_thread_author=_optional_bool(getattr(comment, "is_thread_author", None)),
                source_api=page.source_api,
                observed_at=page.observed_at,
                source_payload_json=source_payload,
                missing_reasons_json=missing,
                field_sources_json=comment_field_sources or None,
            )
        )
        records.content_observations.append(
            ResearchContentObservation(
                owner_type="comment",
                owner_id=cid,
                tid=tid,
                source_api=page.source_api,
                observed_at=page.observed_at,
            )
        )
        _append_user_observations(records, user, comment_fid, comment_fname, page, record=comment)
        if contents is None:
            missing["contents_raw"] = "not_returned_by_source"
        records.media_assets.extend(_media_rows("comment", cid, tid, fragments, page))
    return records


def _records_from_user_forum_page(page: ResearchPage) -> ResearchRecords:
    records = ResearchRecords()
    dto: ResearchUserForumInfo = page.dto
    if dto.user_id <= 0:
        return records

    raw = page.raw
    raw_user = getattr(raw, "user", None) if raw is not None else None
    missing = dict(dto.missing_reasons)
    tieba_uid = _safe_tieba_uid(dto.tieba_uid, "tieba_uid", missing)
    for field_name, reason in {
        "global_growth_level": "not_exposed_by_get_user_forum_info_4_8_2",
        "is_bawu": "not_exposed_by_get_user_forum_info_4_8_2",
        "role_type": "not_exposed_by_get_user_forum_info_4_8_2",
        "icons": "not_exposed_by_get_user_forum_info_4_8_2",
        "ip_location_text": "not_exposed_by_get_user_forum_info_4_8_2",
    }.items():
        missing.setdefault(field_name, reason)

    source_payload = {
        "user": {
            "user_id": _int_value(getattr(raw_user, "user_id", None)),
            "portrait": _optional_text(getattr(raw_user, "portrait", None)),
            "show_name": _optional_text(getattr(raw_user, "show_name", None)),
        }
        if raw_user is not None
        else None,
        "fname": _optional_text(getattr(raw, "fname", None)) if raw is not None else None,
        "level": _int_value(getattr(raw, "level", None)) if raw is not None else None,
        "level_name": _optional_text(getattr(raw, "level_name", None)) if raw is not None else None,
        "role_name": _optional_text(getattr(raw, "role_name", None)) if raw is not None else None,
        "tieba_uid": _raw_tieba_uid(dto.tieba_uid),
    }
    records.user_forum_snapshots.append(
        UserForumSnapshot(
            user_id=dto.user_id,
            fid=dto.fid,
            source_api=page.source_api,
            observed_at=page.observed_at,
            response_user_id=dto.response_user_id,
            fname=dto.fname,
            portrait=dto.portrait,
            display_name=dto.display_name,
            forum_level=dto.forum_level,
            global_growth_level=None,
            forum_level_name=dto.forum_level_name,
            is_bawu=None,
            role_name=dto.role_name,
            role_type=None,
            icons=None,
            ip_location_text=None,
            ip_location_source=None,
            status=dto.status,
            tieba_uid=tieba_uid,
            source_payload_json=source_payload,
            missing_reasons_json=missing,
        )
    )
    return records


def _records_from_user_profile_page(page: ResearchPage) -> ResearchRecords:
    records = ResearchRecords()
    dto: ResearchUserProfileInfo = page.dto
    raw = page.raw
    response_user_id = _positive_id(getattr(raw, "user_id", None))
    if dto.status != "available" or response_user_id != dto.user_id or dto.response_user_id != dto.user_id:
        return records

    missing = dict(dto.missing_reasons)
    tieba_uid = _safe_tieba_uid(getattr(raw, "tieba_uid", None), "tieba_uid", missing)
    global_growth_level = _protobuf_int_field(
        getattr(raw, "glevel", None), "global_growth_level", missing
    )
    records.users.append(
        ResearchUser(
            user_id=dto.user_id,
            user_name=_identity_text(getattr(raw, "user_name", None)),
            nickname=_optional_text(_nickname(raw)),
            portrait=_identity_text(getattr(raw, "portrait", None)),
            tieba_uid=tieba_uid,
            global_growth_level=global_growth_level,
            first_observed_at=page.observed_at,
            last_observed_at=page.observed_at,
            source_api=page.source_api,
            observed_at=page.observed_at,
            source_payload_json=_user_profile_payload(raw),
            missing_reasons_json=missing,
        )
    )
    return records


def _records_from_bawu_page(page: ResearchPage) -> ResearchRecords:
    records = ResearchRecords()
    dto: ResearchUserBawuInfo = page.dto
    matched = dto.status == "available" and dto.response_user_id == dto.user_id and dto.is_bawu is True
    not_listed = dto.status == "not_listed" and dto.response_user_id is None
    if not matched and not not_listed:
        return records

    missing = dict(dto.missing_reasons)
    for field_name in (
        "portrait",
        "display_name",
        "forum_level",
        "forum_level_name",
        "global_growth_level",
        "tieba_uid",
        "icons",
        "ip_location_text",
    ):
        missing.setdefault(field_name, "not_exposed_by_get_bawu_info_4_8_2")
    records.user_forum_snapshots.append(
        UserForumSnapshot(
            user_id=dto.user_id,
            fid=dto.fid,
            source_api=page.source_api,
            observed_at=page.observed_at,
            response_user_id=dto.response_user_id,
            fname=None,
            portrait=None,
            display_name=None,
            forum_level=None,
            global_growth_level=None,
            forum_level_name=None,
            is_bawu=True if matched else None,
            role_name=dto.role_name if matched else None,
            role_type=dto.role_type if matched else None,
            icons=None,
            ip_location_text=None,
            ip_location_source=None,
            status="available" if matched else "not_listed",
            tieba_uid=None,
            source_payload_json={
                "observation_scope": "current_forum_bawu_roster",
                "fid": dto.fid,
                "role_type": dto.role_type,
                "role_name": dto.role_name,
                "listed": matched,
            },
            missing_reasons_json=missing,
        )
    )
    return records


async def _upsert_content_observations(
    session: Any,
    rows: list[ResearchContentObservation],
) -> set[tuple[str, int]]:
    if not rows:
        return set()

    dialect = session.get_bind().dialect.name
    insert_factory = _insert_factory(dialect)
    accepted: set[tuple[str, int]] = set()
    for row in sorted(rows, key=lambda value: (value.owner_type, value.owner_id)):
        media_owner_type, media_owner_id = _content_owner_to_media_owner(row.owner_type, row.owner_id)
        previous_media = await session.execute(
            select(MediaAsset.observed_at, MediaAsset.source_api)
            .where(
                MediaAsset.owner_type == media_owner_type,
                MediaAsset.owner_id == media_owner_id,
            )
            .order_by(MediaAsset.observed_at.desc())
            .limit(1)
        )
        previous_media_row = previous_media.first()
        if previous_media_row is not None:
            legacy_insert = insert_factory(ResearchContentObservation).values(
                owner_type=row.owner_type,
                owner_id=row.owner_id,
                tid=row.tid,
                source_api=previous_media_row.source_api,
                observed_at=previous_media_row.observed_at,
            )
            await session.execute(
                legacy_insert.on_conflict_do_nothing(
                    index_elements=[
                        ResearchContentObservation.owner_type,
                        ResearchContentObservation.owner_id,
                    ]
                )
            )

        statement = insert_factory(ResearchContentObservation).values(**_row_values(row))
        statement = statement.on_conflict_do_update(
            index_elements=[ResearchContentObservation.owner_type, ResearchContentObservation.owner_id],
            set_={
                "tid": statement.excluded.tid,
                "source_api": statement.excluded.source_api,
                "observed_at": statement.excluded.observed_at,
            },
            where=statement.excluded.observed_at >= ResearchContentObservation.observed_at,
        ).returning(ResearchContentObservation.owner_type, ResearchContentObservation.owner_id)
        result = await session.execute(statement)
        accepted.update((owner_type, owner_id) for owner_type, owner_id in result.all())

    return accepted


def _insert_factory(dialect: str) -> Any:
    if dialect == "postgresql":
        return pg_insert
    if dialect == "sqlite":
        return sqlite_insert
    raise ValueError(f"Unsupported database dialect for research persistence: {dialect}.")


def _content_owner_to_media_owner(owner_type: str, owner_id: int) -> tuple[str, int]:
    if owner_type == "thread_first_post":
        return "thread", owner_id
    if owner_type in {"post", "comment"}:
        return owner_type, owner_id
    raise ValueError(f"Unsupported content owner type: {owner_type}.")


def _media_asset_observation_key(asset: MediaAsset) -> tuple[str, int]:
    if asset.owner_type == "thread":
        return "thread_first_post", asset.owner_id
    return asset.owner_type, asset.owner_id


async def _upsert_research_threads(
    session: Any,
    rows: list[ResearchThread],
    accepted_content: set[tuple[str, int]],
) -> None:
    if not rows:
        return

    insert_factory = _insert_factory(session.get_bind().dialect.name)
    for incoming in sorted(rows, key=lambda value: value.tid):
        statement = (
            insert_factory(ResearchThread)
            .values(**_row_values(incoming))
            .on_conflict_do_nothing(index_elements=[ResearchThread.tid])
        )
        await session.execute(statement)
        current = await session.scalar(
            select(ResearchThread).where(ResearchThread.tid == incoming.tid).with_for_update()
        )
        if current is None:
            raise RuntimeError("Research thread disappeared during a locked upsert.")

        content_key = ("thread_first_post", incoming.tid)
        content_accepted = content_key in accepted_content
        is_current = _utc_datetime(incoming.observed_at) >= _utc_datetime(current.observed_at)
        if incoming.source_api == "aiotieba.get_posts" and not content_accepted:
            is_current = False
        if is_current:
            _assert_content_identity_stable(ResearchThread, current, incoming)
        marker_exists = await session.scalar(
            select(ResearchContentObservation.owner_id)
            .where(
                ResearchContentObservation.owner_type == "thread_first_post",
                ResearchContentObservation.owner_id == incoming.tid,
            )
            .limit(1)
        )
        has_content_marker = marker_exists is not None
        incoming_values = _row_values(incoming)
        field_sources = deepcopy(current.field_sources_json or {})
        protected_fields: set[str] = set()

        if incoming.source_api == "aiotieba.get_posts" and content_accepted:
            if incoming.first_pid is not None:
                current.first_pid = incoming.first_pid
            for name in _THREAD_DETAIL_ONLY_FIELDS:
                if name.startswith("ip_location_") and not _thread_source_supports(
                    incoming.source_api, "ip_location_text", incoming.missing_reasons_json
                ):
                    continue
                if _thread_source_supports(incoming.source_api, name, incoming.missing_reasons_json):
                    if _protect_verified_web_field(
                        current,
                        name,
                        incoming_values[name],
                        incoming,
                        field_sources,
                    ):
                        protected_fields.add(name)
                        continue
                    if _protect_last_known_content_value(
                        current,
                        name,
                        incoming_values[name],
                        incoming,
                        field_sources,
                    ):
                        protected_fields.add(name)
                        continue
                    setattr(current, name, incoming_values[name])
        elif incoming.source_api == "aiotieba.get_threads" and is_current and not has_content_marker:
            if incoming.first_pid is not None:
                current.first_pid = incoming.first_pid

        for column in ResearchThread.__table__.columns:
            name = column.name
            if name in {
                "tid",
                "first_pid",
                *_THREAD_DETAIL_ONLY_FIELDS,
                "source_payload_json",
                "missing_reasons_json",
            }:
                continue
            if not _thread_source_supports(incoming.source_api, name, incoming.missing_reasons_json):
                continue
            if is_current:
                if _protect_verified_web_field(
                    current,
                    name,
                    incoming_values[name],
                    incoming,
                    field_sources,
                ):
                    protected_fields.add(name)
                    continue
                if _protect_last_known_content_value(
                    current,
                    name,
                    incoming_values[name],
                    incoming,
                    field_sources,
                ):
                    protected_fields.add(name)
                    continue
                setattr(current, name, incoming_values[name])

        if incoming.source_api == "aiotieba.get_threads":
            if is_current:
                current.source_payload_json = _merge_list_payload(
                    current.source_payload_json,
                    incoming.source_payload_json,
                    preserve_first_post=has_content_marker,
                )
                current.missing_reasons_json = _merge_list_missing_reasons(
                    current.missing_reasons_json,
                    incoming.missing_reasons_json,
                    preserve_detail=has_content_marker,
                )
        elif incoming.source_api == "aiotieba.get_posts" and content_accepted:
            if is_current:
                current.source_payload_json = _merge_current_detail_payload(
                    current.source_payload_json,
                    incoming.source_payload_json,
                    incoming.missing_reasons_json,
                )
                current.missing_reasons_json = _merge_current_detail_missing_reasons(
                    current.missing_reasons_json,
                    incoming.missing_reasons_json,
                )
            else:
                current.source_payload_json = _merge_stale_detail_payload(
                    current.source_payload_json,
                    incoming.source_payload_json,
                    incoming.missing_reasons_json,
                )
                current.missing_reasons_json = _merge_stale_detail_missing_reasons(
                    current.missing_reasons_json,
                    incoming.missing_reasons_json,
                )
        elif is_current:
            current.source_payload_json = dict(incoming.source_payload_json or {})
            current.missing_reasons_json = dict(incoming.missing_reasons_json or {})

        current.field_sources_json = field_sources or None
        for field_name in protected_fields:
            if _api_value_is_unknown(
                field_name,
                incoming_values.get(field_name),
                incoming.missing_reasons_json,
            ):
                current.missing_reasons_json.pop(field_name, None)


def _canonical_source_field(field_name: str) -> str:
    if field_name in {"ip_location_source", "ip_location_observed_at"}:
        return "ip_location_text"
    return field_name


def _api_value_is_unknown(field_name: str, value: Any, missing_reasons: Any) -> bool:
    reason = (missing_reasons or {}).get(field_name)
    if value is None or (isinstance(value, str) and not value.strip()):
        return True
    if reason in {"zero_presence_unverified", "zero_presence_unverified_in_parsed_object"}:
        if isinstance(value, bool):
            return field_name == "is_bawu" and value is False
        return isinstance(value, int) and value == 0
    return False


def _protect_verified_web_field(
    current: Any,
    field_name: str,
    incoming_value: Any,
    incoming: Any,
    field_sources: dict[str, Any],
) -> bool:
    """Keep a verified web value against API unknowns and retain explicit conflicts."""
    source_field = _canonical_source_field(field_name)
    source = field_sources.get(source_field)
    verified_web = source.get("verified_web") if isinstance(source, dict) else None
    if not isinstance(verified_web, dict):
        return False
    if field_name != source_field:
        return True

    existing_value = getattr(current, field_name)
    if _api_value_is_unknown(field_name, incoming_value, incoming.missing_reasons_json):
        return True
    if existing_value == incoming_value:
        return True

    source = dict(source)
    conflicts = list(source.get("api_conflicts") or [])
    conflict = {
        "source_api": incoming.source_api,
        "observed_at": _utc_datetime(incoming.observed_at).isoformat(),
        "value": _jsonable(incoming_value),
    }
    if conflict not in conflicts:
        conflicts.append(conflict)
    source["api_conflicts"] = conflicts
    field_sources[source_field] = source
    return True


def _protect_last_known_content_value(
    current: Any,
    field_name: str,
    incoming_value: Any,
    incoming: Any,
    field_sources: dict[str, Any],
) -> bool:
    """Keep last-known API content identity values when a later response omits them."""
    if field_name not in _CONTENT_LAST_KNOWN_FIELDS:
        return False
    existing_value = getattr(current, field_name)
    existing_is_known = _identity_value_is_known(existing_value) and not (
        isinstance(existing_value, str) and not existing_value.strip()
    )
    if not existing_is_known:
        return False
    if not _api_value_is_unknown(field_name, incoming_value, incoming.missing_reasons_json):
        source = field_sources.get(field_name)
        if isinstance(source, dict) and "last_known_api" in source:
            source = dict(source)
            source.pop("last_known_api", None)
            if source:
                field_sources[field_name] = source
            else:
                field_sources.pop(field_name, None)
        return False
    previous = field_sources.get(field_name)
    previous_last_known = previous.get("last_known_api") if isinstance(previous, dict) else None
    if (
        isinstance(previous_last_known, dict)
        and previous_last_known.get("retained_value") == existing_value
    ):
        retained_source_api = previous_last_known.get("retained_source_api")
        retained_observed_at = previous_last_known.get("retained_observed_at")
    else:
        retained_source_api = current.source_api
        retained_observed_at = _utc_datetime(current.observed_at).isoformat()
    field_sources[field_name] = {
        **(previous if isinstance(previous, dict) else {}),
        "last_known_api": {
            "retained_source_api": retained_source_api,
            "retained_observed_at": retained_observed_at,
            "retained_value": existing_value,
            "missing_source_api": incoming.source_api,
            "missing_observed_at": _utc_datetime(incoming.observed_at).isoformat(),
            "missing_reason": (incoming.missing_reasons_json or {}).get(field_name),
        },
    }
    return True


async def _upsert_content_rows(session: Any, model: type, rows: list[Any]) -> None:
    """Lock one content row at a time so web field provenance survives API refreshes."""
    if not rows:
        return
    insert_factory = _insert_factory(session.get_bind().dialect.name)
    primary_key = tuple(column.name for column in model.__table__.primary_key.columns)
    if len(primary_key) != 1:
        raise RuntimeError("Research content rows must have a single identity key.")
    pk_name = primary_key[0]
    for incoming in sorted(rows, key=lambda row: getattr(row, pk_name)):
        insert = insert_factory(model).values(**_row_values(incoming)).on_conflict_do_nothing(
            index_elements=[model.__table__.c[pk_name]]
        )
        await session.execute(insert)
        current = await session.scalar(
            select(model)
            .where(model.__table__.c[pk_name] == getattr(incoming, pk_name))
            .with_for_update()
        )
        if current is None:
            raise RuntimeError(f"{model.__name__} disappeared during a locked upsert.")
        if _utc_datetime(incoming.observed_at) < _utc_datetime(current.observed_at):
            continue
        _assert_content_identity_stable(model, current, incoming)

        field_sources = deepcopy(current.field_sources_json or {})
        incoming_values = _row_values(incoming)
        updates: dict[str, Any] = {}
        protected_fields: set[str] = set()
        incoming_missing = dict(incoming.missing_reasons_json or {})
        retained_reply_uid = (
            _reply_uid_after_refresh(current, incoming, field_sources) if model is ResearchComment else None
        )
        reply_name_bindings, unbound_reply_names = (
            _reply_name_provenance_state(field_sources) if model is ResearchComment else (set(), set())
        )
        reply_pair_conflict = False
        for column in model.__table__.columns:
            name = column.name
            if name in primary_key or name in {"field_sources_json", "missing_reasons_json"}:
                continue
            value = incoming_values[name]
            if model is ResearchComment and name == "reply_to_user_id" and _positive_id(value) is not None:
                incoming_uid = _positive_id(value)
                current_uid = _positive_id(current.reply_to_user_id)
                mention_uid = _reply_target_uid_from_payload(incoming.source_payload_json)
                incoming_name = _identity_text(incoming.reply_to_display_name)
                binding_conflict = reply_name_bindings and reply_name_bindings != {incoming_uid}
                unbound_pair_matches = (
                    mention_uid == incoming_uid
                    and incoming_name in unbound_reply_names
                    and current_uid in (None, incoming_uid)
                )
                unbound_conflict = bool(unbound_reply_names) and not unbound_pair_matches
                if binding_conflict or unbound_conflict:
                    reason = (
                        "api_reply_target_uid_conflicts_with_verified_name_binding"
                        if binding_conflict
                        else "api_reply_target_uid_unlinked_from_display_name"
                    )
                    _append_reply_uid_binding_conflict(
                        field_sources,
                        incoming,
                        incoming_uid=incoming_uid,
                        name_bindings=reply_name_bindings,
                        reason=reason,
                    )
                    payload = dict(incoming.source_payload_json or {})
                    candidates = list(payload.get("reply_target_candidates") or [])
                    candidate = {
                        "source_path": f"{incoming.source_api}.reply_to_id",
                        "observed_at": _utc_datetime(incoming.observed_at).isoformat(),
                        "raw": {
                            "reply_to_user_id": incoming_uid,
                            "reply_to_id_raw": incoming.reply_to_id_raw,
                        },
                        "resolution": reason,
                    }
                    if candidate not in candidates:
                        candidates.append(candidate)
                    payload["reply_target_candidates"] = candidates
                    incoming.source_payload_json = payload
                    incoming_values["source_payload_json"] = payload
                    incoming_missing["reply_to_user_id"] = reason
                    protected_fields.add(name)
                    reply_pair_conflict = True
                    continue
            if model is ResearchComment and name == "reply_to_display_name" and _comment_preview_value_is_known(
                name, value
            ):
                mention_uid = _reply_target_uid_from_payload(incoming.source_payload_json)
                has_display_only_source = _reply_name_display_only_source(incoming.field_sources_json)
                display_only_allowed = (
                    has_display_only_source
                    and retained_reply_uid is None
                    and not reply_name_bindings
                    and not unbound_reply_names
                )
                if not display_only_allowed and (mention_uid is None or mention_uid != retained_reply_uid):
                    reason = (
                        "api_reply_target_uid_conflict"
                        if mention_uid is not None
                        else "api_reply_target_uid_unverified"
                    )
                    _append_content_reply_target_conflict(
                        field_sources,
                        incoming,
                        mention_uid=mention_uid,
                        retained_uid=retained_reply_uid,
                        reason=reason,
                    )
                    evidence = dict((incoming.source_payload_json or {}).get("reply_target_evidence") or {})
                    evidence["conflict"] = reason
                    incoming.source_payload_json = {
                        **dict(incoming.source_payload_json or {}),
                        "reply_target_evidence": evidence,
                    }
                    incoming_values["source_payload_json"] = incoming.source_payload_json
                    incoming_missing["reply_to_display_name"] = reason
                    protected_fields.add(name)
                    reply_pair_conflict = True
                    continue
                if unbound_reply_names and value not in unbound_reply_names:
                    reason = "api_reply_target_uid_unlinked_from_display_name"
                    _append_content_reply_target_conflict(
                        field_sources,
                        incoming,
                        mention_uid=mention_uid,
                        retained_uid=retained_reply_uid,
                        reason=reason,
                    )
                    incoming_missing["reply_to_display_name"] = reason
                    protected_fields.add(name)
                    reply_pair_conflict = True
                    continue
            if model is ResearchComment and reply_pair_conflict and name in {
                "reply_target_source",
                "reply_target_confidence",
                "reply_resolution_note",
            }:
                protected_fields.add(name)
                continue
            if _protect_verified_web_field(current, name, value, incoming, field_sources):
                protected_fields.add(name)
                continue
            if model is ResearchComment and _protect_verified_content_api_field(
                current, name, value, incoming, field_sources
            ):
                protected_fields.add(name)
                continue
            if model is ResearchComment and _protect_last_known_comment_metadata(
                current, name, value, incoming, field_sources
            ):
                protected_fields.add(name)
                continue
            if _protect_last_known_content_value(current, name, value, incoming, field_sources):
                protected_fields.add(name)
                continue
            updates[name] = value

        for field_name in protected_fields:
            if _api_value_is_unknown(field_name, incoming_values.get(field_name), incoming_missing):
                incoming_missing.pop(field_name, None)
        if model is ResearchComment:
            for field_name, incoming_source in (incoming.field_sources_json or {}).items():
                verified_content_api = (
                    incoming_source.get("verified_content_api") if isinstance(incoming_source, dict) else None
                )
                if not isinstance(verified_content_api, dict):
                    continue
                current_source = field_sources.get(field_name)
                current_source = dict(current_source) if isinstance(current_source, dict) else {}
                if field_name in protected_fields:
                    if field_name == "reply_to_display_name" and current.reply_to_display_name == incoming_values.get(
                        field_name
                    ):
                        mention_uid = _reply_target_uid_from_payload(incoming.source_payload_json)
                        display_only = _reply_name_display_only_source(incoming.field_sources_json)
                        pair_matches = (
                            mention_uid == retained_reply_uid
                            if mention_uid is not None
                            else display_only and retained_reply_uid is None
                        )
                        if pair_matches:
                            current_source["verified_content_api"] = verified_content_api
                            field_sources[field_name] = current_source
                    continue
                if isinstance(current_source.get("verified_web"), dict):
                    continue
                if not _comment_preview_value_is_unknown(
                    field_name,
                    incoming_values.get(field_name),
                    incoming_missing,
                ):
                    current_source["verified_content_api"] = verified_content_api
                    field_sources[field_name] = current_source
        updates["missing_reasons_json"] = incoming_missing
        updates["field_sources_json"] = field_sources or None
        await session.execute(
            update(model)
            .where(model.__table__.c[pk_name] == getattr(incoming, pk_name))
            .values(**updates)
        )


def _reply_target_uid_from_payload(source_payload: Any) -> int | None:
    if not isinstance(source_payload, Mapping):
        return None
    evidence = source_payload.get("reply_target_evidence")
    if not isinstance(evidence, Mapping):
        return None
    raw = evidence.get("raw")
    if not isinstance(raw, Mapping):
        return None
    return _positive_id(raw.get("reply_to_user_id"))


def _reply_uid_after_refresh(
    current: ResearchComment,
    incoming: ResearchComment,
    field_sources: Mapping[str, Any],
) -> int | None:
    current_uid = _positive_id(current.reply_to_user_id)
    incoming_uid = _positive_id(incoming.reply_to_user_id)
    source = field_sources.get("reply_to_user_id")
    if current_uid is not None and isinstance(source, Mapping) and (
        isinstance(source.get("verified_web"), Mapping)
        or isinstance(source.get("verified_content_api"), Mapping)
    ):
        return current_uid
    name_bindings = _reply_name_binding_uids(field_sources)
    if current_uid is None and len(name_bindings) == 1:
        return next(iter(name_bindings))
    if current_uid is None and name_bindings:
        return None
    _, unbound_names = _reply_name_provenance_state(field_sources)
    if current_uid is None and unbound_names:
        incoming_name = _identity_text(incoming.reply_to_display_name)
        candidate_uid = _reply_target_uid_from_payload(incoming.source_payload_json)
        if candidate_uid is not None and incoming_uid == candidate_uid and incoming_name in unbound_names:
            return candidate_uid
        return None
    return incoming_uid or current_uid


def _reply_name_binding_uids(field_sources: Mapping[str, Any]) -> set[int]:
    bindings, _ = _reply_name_provenance_state(field_sources)
    return bindings


def _reply_name_provenance_state(field_sources: Mapping[str, Any]) -> tuple[set[int], set[str]]:
    source = field_sources.get("reply_to_display_name")
    if not isinstance(source, Mapping):
        return set(), set()
    bindings = set()
    unbound_names = set()
    for provenance_key in ("verified_web", "verified_content_api"):
        provenance = source.get(provenance_key)
        if isinstance(provenance, Mapping):
            display_name = _identity_text(provenance.get("value"))
            uid = _positive_id(provenance.get("reply_target_user_id"))
            if uid is not None:
                bindings.add(uid)
            elif display_name is not None:
                unbound_names.add(display_name)
    return bindings, unbound_names


def _reply_name_display_only_source(field_sources: Mapping[str, Any] | None) -> bool:
    source = (field_sources or {}).get("reply_to_display_name")
    if not isinstance(source, Mapping):
        return False
    for provenance_key in ("verified_web", "verified_content_api"):
        provenance = source.get(provenance_key)
        if (
            isinstance(provenance, Mapping)
            and provenance.get("confidence") == "display_name_only"
            and provenance.get("reply_target_user_id") is None
        ):
            return True
    return False


def _append_reply_uid_binding_conflict(
    field_sources: dict[str, Any],
    incoming: ResearchComment,
    *,
    incoming_uid: int,
    name_bindings: set[int],
    reason: str,
) -> None:
    source = field_sources.get("reply_to_user_id")
    source = dict(source) if isinstance(source, dict) else {}
    conflicts = list(source.get("api_conflicts") or [])
    conflict = {
        "source_api": incoming.source_api,
        "observed_at": _utc_datetime(incoming.observed_at).isoformat(),
        "value": incoming_uid,
        "reply_target_user_id_bindings": sorted(name_bindings),
        "reason": reason,
    }
    if conflict not in conflicts:
        conflicts.append(conflict)
    source["api_conflicts"] = conflicts
    field_sources["reply_to_user_id"] = source


def _append_content_reply_target_conflict(
    field_sources: dict[str, Any],
    incoming: ResearchComment,
    *,
    mention_uid: int | None,
    retained_uid: int | None,
    reason: str,
) -> None:
    source = field_sources.get("reply_to_display_name")
    source = dict(source) if isinstance(source, dict) else {}
    conflicts = list(source.get("api_conflicts") or [])
    incoming_source = (incoming.field_sources_json or {}).get("reply_to_display_name")
    incoming_verified = incoming_source.get("verified_content_api") if isinstance(incoming_source, dict) else None
    conflict = {
        "source_kind": "api_raw",
        "source_path": (
            incoming_verified.get("source_path")
            if isinstance(incoming_verified, dict)
            else "api.get_comments.reply_prefix.type4_mention"
        ),
        "observed_at": _utc_datetime(incoming.observed_at).isoformat(),
        "value": _jsonable(incoming.reply_to_display_name),
        "mention_user_id": mention_uid,
        "retained_reply_to_user_id": retained_uid,
        "reason": reason,
    }
    if conflict not in conflicts:
        conflicts.append(conflict)
    source["api_conflicts"] = conflicts
    field_sources["reply_to_display_name"] = source


def _protect_verified_content_api_field(
    current: ResearchComment,
    field_name: str,
    incoming_value: Any,
    incoming: ResearchComment,
    field_sources: dict[str, Any],
) -> bool:
    source_field = _canonical_source_field(field_name)
    if source_field not in _COMMENT_PREVIEW_FIELDS:
        return False
    source = field_sources.get(source_field)
    verified = source.get("verified_content_api") if isinstance(source, dict) else None
    if not isinstance(verified, dict):
        return False
    if field_name != source_field:
        return True
    current_value = getattr(current, field_name)
    if _comment_preview_value_is_unknown(field_name, incoming_value, incoming.missing_reasons_json):
        return True
    if current_value == incoming_value:
        return True
    source = dict(source)
    conflicts = list(source.get("api_conflicts") or [])
    conflict = {
        "source_api": incoming.source_api,
        "observed_at": _utc_datetime(incoming.observed_at).isoformat(),
        "value": _jsonable(incoming_value),
    }
    if conflict not in conflicts:
        conflicts.append(conflict)
    source["api_conflicts"] = conflicts
    field_sources[source_field] = source
    return True


def _protect_last_known_comment_metadata(
    current: ResearchComment,
    field_name: str,
    incoming_value: Any,
    incoming: ResearchComment,
    field_sources: dict[str, Any],
) -> bool:
    source_field = _canonical_source_field(field_name)
    if source_field not in _COMMENT_PREVIEW_FIELDS:
        return False
    current_value = getattr(current, source_field)
    if not _comment_preview_value_is_known(source_field, current_value):
        return False
    if not _comment_preview_value_is_unknown(
        source_field,
        getattr(incoming, source_field),
        incoming.missing_reasons_json,
    ):
        return False
    previous = field_sources.get(source_field)
    previous = dict(previous) if isinstance(previous, dict) else {}
    previous_last_known = previous.get("last_known_api")
    if isinstance(previous_last_known, dict) and previous_last_known.get("retained_value") == current_value:
        retained_source_api = previous_last_known.get("retained_source_api")
        retained_observed_at = previous_last_known.get("retained_observed_at")
    else:
        retained_source_api = current.source_api
        retained_observed_at = _utc_datetime(current.observed_at).isoformat()
    previous["last_known_api"] = {
        "retained_source_api": retained_source_api,
        "retained_observed_at": retained_observed_at,
        "retained_value": current_value,
        "missing_source_api": incoming.source_api,
        "missing_observed_at": _utc_datetime(incoming.observed_at).isoformat(),
        "missing_reason": (incoming.missing_reasons_json or {}).get(source_field),
    }
    field_sources[source_field] = previous
    return True


def _thread_source_supports(source_api: str, field_name: str, missing_reasons: Any) -> bool:
    reason = (missing_reasons or {}).get(field_name)
    if isinstance(reason, str) and reason.startswith("not_exposed_by_"):
        return False
    if source_api == "aiotieba.get_threads":
        return field_name not in {
            "body_text",
            "contents_raw",
            "collect_count",
            "forum_level_name",
            "role_type",
            "role_name",
            "role_badge_id",
            "role_badge_url",
            "ip_location_text",
            "ip_location_source",
            "ip_location_observed_at",
        }
    if source_api == "aiotieba.get_posts":
        return field_name not in {
            "last_time",
            "last_time_raw",
            "is_top",
            "is_good",
            "is_livepost",
            "is_hide",
            "forum_level_name",
            "role_type",
            "role_name",
            "role_badge_id",
            "role_badge_url",
        }
    return False


# These are supported by get_posts but not get_threads, according to
# docs/FIELD_SOURCE_AUDIT.md. They are governed by the accepted detail watermark
# rather than the thread row's latest list observation.
_THREAD_DETAIL_ONLY_FIELDS = frozenset({
    "body_text",
    "contents_raw",
    "collect_count",
    "ip_location_text",
    "ip_location_source",
    "ip_location_observed_at",
})


def _merge_list_payload(existing: Any, incoming: Any, *, preserve_first_post: bool) -> dict[str, Any]:
    merged = dict(incoming or {})
    previous = dict(existing or {})
    if preserve_first_post:
        if "first_post" in previous:
            merged["first_post"] = previous["first_post"]
        previous_counts = previous.get("counts")
        if isinstance(previous_counts, dict) and "collect_count" in previous_counts:
            merged_counts = dict(merged.get("counts") or {})
            merged_counts["collect_count"] = previous_counts["collect_count"]
            merged["counts"] = merged_counts
        previous_author = previous.get("author")
        if isinstance(previous_author, dict) and "ip_location_text" in previous_author:
            merged_author = dict(merged.get("author") or {})
            merged_author["ip_location_text"] = previous_author["ip_location_text"]
            merged["author"] = merged_author
    return merged


def _merge_list_missing_reasons(existing: Any, incoming: Any, *, preserve_detail: bool) -> dict[str, Any]:
    merged = dict(incoming or {})
    if not preserve_detail:
        return merged
    previous = dict(existing or {})
    detail_fields = {"first_pid", "first_post.subreply_count_reported", *_THREAD_DETAIL_ONLY_FIELDS}
    for field_name in previous:
        if field_name not in detail_fields and not _thread_source_supports(
            "aiotieba.get_threads", field_name, incoming
        ):
            merged[field_name] = previous[field_name]
    for field_name in detail_fields:
        merged.pop(field_name, None)
        if field_name in previous:
            merged[field_name] = previous[field_name]
    return merged


def _merge_current_detail_missing_reasons(existing: Any, incoming: Any) -> dict[str, Any]:
    merged = dict(incoming or {})
    previous = dict(existing or {})
    for field_name in previous:
        if not _thread_source_supports("aiotieba.get_posts", field_name, incoming):
            merged[field_name] = previous[field_name]
    return merged


def _merge_stale_detail_payload(existing: Any, incoming: Any, missing_reasons: Any) -> dict[str, Any]:
    merged = dict(existing or {})
    new_payload = dict(incoming or {})
    if "first_post" in new_payload:
        merged["first_post"] = new_payload["first_post"]
    new_counts = new_payload.get("counts")
    missing = missing_reasons or {}
    if (
        isinstance(new_counts, dict)
        and "collect_count" in new_counts
        and _thread_source_supports("aiotieba.get_posts", "collect_count", missing)
    ):
        merged_counts = dict(merged.get("counts") or {})
        merged_counts["collect_count"] = new_counts["collect_count"]
        merged["counts"] = merged_counts
    new_author = new_payload.get("author")
    old_author = merged.get("author")
    if isinstance(new_author, dict):
        merged_author = dict(old_author or {})
        if _thread_source_supports("aiotieba.get_posts", "ip_location_text", missing):
            if "ip_location_text" in new_author:
                merged_author["ip_location_text"] = new_author["ip_location_text"]
            else:
                merged_author.pop("ip_location_text", None)
        merged["author"] = merged_author
    return merged


def _merge_stale_detail_missing_reasons(existing: Any, incoming: Any) -> dict[str, Any]:
    merged = dict(existing or {})
    detail = dict(incoming or {})
    for field_name in _THREAD_DETAIL_ONLY_FIELDS:
        if not _thread_source_supports("aiotieba.get_posts", field_name, detail):
            continue
        merged.pop(field_name, None)
        if field_name in detail:
            merged[field_name] = detail[field_name]
    for field_name in ("first_pid", "first_post.subreply_count_reported"):
        merged.pop(field_name, None)
        if field_name in detail:
            merged[field_name] = detail[field_name]
    return merged


def _merge_current_detail_payload(existing: Any, incoming: Any, missing_reasons: Any) -> dict[str, Any]:
    merged = dict(existing or {})
    detail = dict(incoming or {})
    missing = missing_reasons or {}
    for name, value in detail.items():
        if isinstance(value, dict) and isinstance(merged.get(name), dict):
            merged_value = {**merged[name], **value}
            if name == "counts" and not _thread_source_supports("aiotieba.get_posts", "collect_count", missing):
                if "collect_count" in merged[name]:
                    merged_value["collect_count"] = merged[name]["collect_count"]
                else:
                    merged_value.pop("collect_count", None)
            if name == "author":
                if _thread_source_supports("aiotieba.get_posts", "ip_location_text", missing):
                    merged_value.pop("ip_location_text", None)
                    if "ip_location_text" in value:
                        merged_value["ip_location_text"] = value["ip_location_text"]
            merged[name] = merged_value
        else:
            merged[name] = value
    return merged


def _utc_datetime(value: datetime) -> datetime:
    return (value.replace(tzinfo=UTC) if value.tzinfo is None else value).astimezone(UTC)


async def _upsert_research_users(session: Any, rows: list[ResearchUser]) -> None:
    rows = _merge_research_user_batch(rows)
    if not rows:
        return

    rows_by_user_id: dict[int, list[ResearchUser]] = {}
    for row in rows:
        rows_by_user_id.setdefault(row.user_id, []).append(row)

    for observation_index in range(max(map(len, rows_by_user_id.values()))):
        batch = [
            user_rows[observation_index]
            for user_rows in rows_by_user_id.values()
            if len(user_rows) > observation_index
        ]
        await _upsert_research_user_batch(session, batch)


async def _upsert_research_user_batch(session: Any, rows: list[ResearchUser]) -> None:
    returned_rows = await _upsert_rows(
        session,
        ResearchUser,
        rows,
        returning_fields=("user_id", "observed_at", *_RESEARCH_USER_LAST_KNOWN_FIELDS),
    )
    for row in rows:
        final = returned_rows[row.user_id]
        if _utc_datetime(final["observed_at"]) != _utc_datetime(row.observed_at):
            continue

        retained = {}
        for field_name in _RESEARCH_USER_LAST_KNOWN_FIELDS:
            incoming_value = getattr(row, field_name)
            if not _research_user_field_present(incoming_value) and _research_user_field_present(
                final[field_name]
            ):
                retained[field_name] = "retained_last_known_due_to_missing_current_observation"
        if retained:
            missing = dict(row.missing_reasons_json or {})
            retained_reasons = missing.get("retained_last_known")
            retained_reasons = dict(retained_reasons) if isinstance(retained_reasons, dict) else {}
            retained_reasons.update(retained)
            missing["retained_last_known"] = retained_reasons
            await session.execute(
                update(ResearchUser)
                .where(
                    ResearchUser.user_id == row.user_id,
                    ResearchUser.observed_at == row.observed_at,
                )
                .values(missing_reasons_json=missing)
            )


def _merge_research_user_batch(rows: list[ResearchUser]) -> list[ResearchUser]:
    grouped: dict[tuple[int, datetime], list[tuple[int, ResearchUser]]] = {}
    for index, row in enumerate(rows):
        key = (row.user_id, _utc_datetime(row.observed_at))
        grouped.setdefault(key, []).append((index, row))

    merged_rows = []
    for observations in grouped.values():
        _, latest = max(
            observations,
            key=lambda item: (_utc_datetime(item[1].observed_at), item[0]),
        )
        merged = ResearchUser(**_row_values(latest))
        first = min(observations, key=lambda item: _utc_datetime(item[1].first_observed_at))[1]
        last = max(observations, key=lambda item: _utc_datetime(item[1].last_observed_at))[1]
        merged.first_observed_at = first.first_observed_at
        merged.last_observed_at = last.last_observed_at

        retained = {}
        for field_name in _RESEARCH_USER_LAST_KNOWN_FIELDS:
            latest_value = getattr(latest, field_name)
            candidates = [
                item
                for item in observations
                if _research_user_field_present(getattr(item[1], field_name))
            ]
            if candidates:
                _, newest_value = max(
                    candidates,
                    key=lambda item: (_utc_datetime(item[1].observed_at), item[0]),
                )
                setattr(merged, field_name, getattr(newest_value, field_name))
                if not _research_user_field_present(latest_value):
                    retained[field_name] = "retained_last_known_due_to_missing_current_observation"
            else:
                setattr(merged, field_name, None)

        missing = dict(latest.missing_reasons_json or {})
        missing.pop("retained_last_known", None)
        if retained:
            missing["retained_last_known"] = retained
        merged.missing_reasons_json = missing
        merged_rows.append(merged)

    return sorted(merged_rows, key=lambda row: (row.user_id, _utc_datetime(row.observed_at)))


def _research_user_field_present(value: Any) -> bool:
    return value is not None and not (isinstance(value, str) and not value.strip())


async def _upsert_rows(
    session: Any,
    model: type,
    rows: list[Any],
    *,
    returning_fields: tuple[str, ...] = (),
) -> dict[int, Any] | None:
    if not rows:
        return
    values = [_row_values(row) for row in rows]
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        statement = pg_insert(model).values(values)
    elif dialect == "sqlite":
        statement = sqlite_insert(model).values(values)
    else:
        raise ValueError(f"Unsupported database dialect for research persistence: {dialect}.")

    table = model.__table__
    primary_key = {column.name for column in table.primary_key.columns}
    updates: dict[str, Any] = {}
    observed_column = table.c.get("observed_at")
    for column in table.columns:
        name = column.name
        if name in primary_key:
            continue
        excluded_value = statement.excluded[name]
        existing_value = table.c[name]
        if model is ResearchUser and name == "first_observed_at":
            updates[name] = case(
                (existing_value <= excluded_value, existing_value),
                else_=excluded_value,
            )
        elif model is ResearchUser and name == "last_observed_at":
            updates[name] = case(
                (existing_value >= excluded_value, existing_value),
                else_=excluded_value,
            )
        else:
            if model is ResearchUser:
                is_current = statement.excluded.observed_at >= table.c.last_observed_at
            elif observed_column is not None:
                is_current = statement.excluded.observed_at >= observed_column
            else:
                is_current = True
            if model is ResearchUser and name in _RESEARCH_USER_LAST_KNOWN_FIELDS:
                is_present = excluded_value.is_not(None)
                if name in {"user_name", "nickname", "portrait"}:
                    is_present = and_(is_present, excluded_value != "")
                current_or_last_known = case(
                    (is_present, excluded_value),
                    else_=existing_value,
                )
                updates[name] = case((is_current, current_or_last_known), else_=existing_value)
            else:
                updates[name] = case((is_current, excluded_value), else_=existing_value)

    statement = statement.on_conflict_do_update(index_elements=list(primary_key), set_=updates)
    if returning_fields:
        statement = statement.returning(*(table.c[name] for name in returning_fields))
        result = await session.execute(statement)
        return {row["user_id"]: row for row in result.mappings().all()}
    await session.execute(statement)
    return None


def _append_user_observations(
    records: ResearchRecords,
    user: Any,
    fid: int | None,
    fname: str | None,
    page: ResearchPage,
    *,
    record: Any = None,
) -> None:
    user_id = _author_user_id(record, user)
    if user_id is None:
        return

    missing = _missing_user_reasons(user, page.source_api, record=record)
    user_name = _optional_text(getattr(user, "user_name", None))
    nickname = _optional_text(_nickname(user))
    portrait = _optional_text(getattr(user, "portrait", None))
    tieba_uid = _safe_tieba_uid(getattr(user, "tieba_uid", None), "author_tieba_uid", missing)
    global_growth_level = _protobuf_int_field(
        getattr(user, "glevel", None), "global_growth_level", missing
    )
    records.users.append(
        ResearchUser(
            user_id=user_id,
            user_name=_identity_text(user_name),
            nickname=nickname,
            portrait=_identity_text(portrait),
            tieba_uid=tieba_uid,
            global_growth_level=global_growth_level,
            first_observed_at=page.observed_at,
            last_observed_at=page.observed_at,
            source_api=page.source_api,
            observed_at=page.observed_at,
            source_payload_json=_user_payload(user),
            missing_reasons_json=missing,
        )
    )
    if fid is None:
        missing["fid"] = "not_exposed_by_source"
        return

    snapshot_missing = dict(missing)
    for field_name, source_attr in (
        ("forum_level_name", "level_name"),
        ("role_name", "role_name"),
        ("role_type", "role_type"),
    ):
        if not hasattr(user, source_attr):
            snapshot_missing[field_name] = "not_exposed_by_content_endpoint"
    if not hasattr(user, "glevel"):
        snapshot_missing["global_growth_level"] = "not_exposed_by_content_endpoint"
    if _ip_location_text(user) is None:
        raw_ip_value = _optional_text(getattr(user, "ip", None))
        if raw_ip_value and _is_ip_address(raw_ip_value):
            snapshot_missing["ip_location_text"] = "source_value_is_ip_address_not_public_location"
        elif hasattr(user, "ip"):
            snapshot_missing["ip_location_text"] = "not_returned_by_source"
        else:
            snapshot_missing["ip_location_text"] = "not_exposed_by_content_endpoint"
    if not hasattr(user, "is_bawu"):
        snapshot_missing["is_bawu"] = "not_exposed_by_content_endpoint"
    if not hasattr(user, "icons"):
        snapshot_missing["icons"] = "not_exposed_by_content_endpoint"
    if tieba_uid is None:
        snapshot_missing.setdefault("tieba_uid", "not_exposed_by_content_endpoint")

    records.user_forum_snapshots.append(
        UserForumSnapshot(
            user_id=user_id,
            fid=fid,
            source_api=page.source_api,
            observed_at=page.observed_at,
            response_user_id=_positive_id(getattr(user, "user_id", None)),
            fname=fname,
            portrait=portrait,
            display_name=None,
            forum_level=_protobuf_int_field(getattr(user, "level", None), "forum_level", snapshot_missing),
            global_growth_level=_protobuf_int_field(
                getattr(user, "glevel", None), "global_growth_level", snapshot_missing
            ),
            forum_level_name=_optional_text(getattr(user, "level_name", None)),
            is_bawu=_optional_bool(getattr(user, "is_bawu", None)),
            role_name=_optional_text(getattr(user, "role_name", None)),
            role_type=_optional_text(getattr(user, "role_type", None)),
            icons=_jsonable(getattr(user, "icons", None)),
            ip_location_text=_ip_location_text(user),
            ip_location_source=_ip_location_source(page, record) if _ip_location_text(user) else None,
            status="available",
            tieba_uid=tieba_uid,
            source_payload_json=_user_payload(user),
            missing_reasons_json=snapshot_missing,
        )
    )


async def _fill_parent_post_authors(session: Any, comments: list[ResearchComment]) -> None:
    parent_pids = {comment.parent_post_pid for comment in comments if comment.parent_post_pid is not None}
    if not parent_pids:
        return

    authors: dict[int, int | None] = {}
    result = await session.execute(
        select(ResearchPost.pid, ResearchPost.author_user_id).where(ResearchPost.pid.in_(parent_pids))
    )
    for pid, author_id in result.all():
        if normalized_author := _positive_id(author_id):
            authors[pid] = normalized_author

    first_posts = await session.execute(
        select(ResearchThread.first_pid, ResearchThread.author_user_id).where(ResearchThread.first_pid.in_(parent_pids))
    )
    for pid, author_id in first_posts.all():
        if normalized_author := _positive_id(author_id):
            authors.setdefault(pid, normalized_author)

    legacy_posts = await session.execute(select(BasePost.pid, BasePost.author_id).where(BasePost.pid.in_(parent_pids)))
    for pid, author_id in legacy_posts.all():
        if normalized_author := _positive_id(author_id):
            authors.setdefault(pid, normalized_author)

    for comment in comments:
        if comment.parent_post_pid is None:
            continue
        comment.parent_post_author_user_id = authors.get(comment.parent_post_pid)
        if comment.parent_post_author_user_id is None:
            reasons = dict(comment.missing_reasons_json or {})
            reasons["parent_post_author_user_id"] = "parent_post_author_not_found_in_local_store"
            comment.missing_reasons_json = reasons


def _thread_list_payload(thread: Any, user: Any) -> dict[str, Any]:
    return {
        "api_type": type(thread).__name__,
        "tid": _int_value(getattr(thread, "tid", None)),
        "first_pid": _int_value(getattr(thread, "pid", None)),
        "fid": _int_value(getattr(thread, "fid", None)),
        "fname": _optional_text(getattr(thread, "fname", None)),
        "title": _optional_text(getattr(thread, "title", None)),
        "create_time": _int_value(getattr(thread, "create_time", None)),
        "last_time": _int_value(getattr(thread, "last_time", None)),
        "author": _user_payload(user),
        "counts": {
            name: _int_value(getattr(thread, source, None))
            for name, source in (
                ("reply_count", "reply_num"),
                ("agree_count", "agree"),
                ("disagree_count", "disagree"),
                ("view_count", "view_num"),
                ("share_count", "share_num"),
            )
        },
        "flags": {
            name: _optional_bool(getattr(thread, source, None))
            for name, source in (
                ("is_top", "is_top"),
                ("is_good", "is_good"),
                ("is_livepost", "is_livepost"),
                ("is_hide", "is_hide"),
                ("is_share", "is_share"),
            )
        },
        "contents": _jsonable(_content_fragments(getattr(thread, "contents", None))),
    }


def _thread_detail_payload(thread: Any, user: Any) -> dict[str, Any]:
    return {
        "api_type": type(thread).__name__,
        "tid": _int_value(getattr(thread, "tid", None)),
        "first_pid": _int_value(getattr(thread, "pid", None)),
        "fid": _int_value(getattr(thread, "fid", None)),
        "fname": _optional_text(getattr(thread, "fname", None)),
        "title": _optional_text(getattr(thread, "title", None)),
        "create_time": _int_value(getattr(thread, "create_time", None)),
        "author": _user_payload(user),
        "counts": {
            name: _int_value(getattr(thread, source, None))
            for name, source in (
                ("reply_count", "reply_num"),
                ("agree_count", "agree"),
                ("disagree_count", "disagree"),
                ("collect_count", "collect_num"),
                ("view_count", "view_num"),
                ("share_count", "share_num"),
            )
        },
        "is_share": bool(getattr(thread, "is_share", False)),
        "thread_type": _enum_int(getattr(thread, "type", None)),
    }


def _post_payload(post: Any, user: Any) -> dict[str, Any]:
    return {
        "api_type": type(post).__name__,
        "pid": _int_value(getattr(post, "pid", None)),
        "tid": _int_value(getattr(post, "tid", None)),
        "fid": _int_value(getattr(post, "fid", None)),
        "fname": _optional_text(getattr(post, "fname", None)),
        "floor": _int_value(getattr(post, "floor", None)),
        "create_time": _int_value(getattr(post, "create_time", None)),
        "author": _user_payload(user),
        "counts": {
            "agree_count": _int_value(getattr(post, "agree", None)),
            "disagree_count": _int_value(getattr(post, "disagree", None)),
            "subreply_count_reported": _int_value(getattr(post, "reply_num", None)),
        },
        "is_thread_author": _optional_bool(getattr(post, "is_thread_author", None)),
        "sign": _optional_text(getattr(post, "sign", None)),
    }


def _comment_payload(comment: Any, user: Any) -> dict[str, Any]:
    return {
        "api_type": type(comment).__name__,
        "cid": _int_value(getattr(comment, "pid", None)),
        "tid": _int_value(getattr(comment, "tid", None)),
        "parent_post_pid": _int_value(getattr(comment, "ppid", None)),
        "fid": _int_value(getattr(comment, "fid", None)),
        "fname": _optional_text(getattr(comment, "fname", None)),
        "floor": _int_value(getattr(comment, "floor", None)),
        "create_time": _int_value(getattr(comment, "create_time", None)),
        "author": _user_payload(user),
        "reply_to_id": _int_value(getattr(comment, "reply_to_id", None)),
        "counts": {
            "agree_count": _int_value(getattr(comment, "agree", None)),
            "disagree_count": _int_value(getattr(comment, "disagree", None)),
        },
    }


def _user_payload(user: Any) -> dict[str, Any] | None:
    if user is None:
        return None
    names = (
        "user_id",
        "portrait",
        "user_name",
        "nick_name_new",
        "nick_name_old",
        "level",
        "glevel",
        "global_growth_icon_url",
        "is_bawu",
        "icons",
    )
    payload = {name: _jsonable(getattr(user, name)) for name in names if hasattr(user, name)}
    if hasattr(user, "tieba_uid"):
        raw_tieba_uid = user.tieba_uid
        payload["tieba_uid"] = _raw_tieba_uid(raw_tieba_uid)
        if _int_value(raw_tieba_uid) is not None and _int_value(raw_tieba_uid) > SIGNED_BIGINT_MAX:
            payload["tieba_uid_raw"] = str(raw_tieba_uid)
    if ip_location_text := _ip_location_text(user):
        payload["ip_location_text"] = ip_location_text
    return payload


def _user_profile_payload(user: Any) -> dict[str, Any]:
    payload = {
        "observation_scope": "current_profile",
        "api_type": type(user).__name__,
    }
    for name in ("user_id", "user_name", "nick_name_new", "nick_name_old", "portrait", "glevel"):
        if hasattr(user, name):
            payload[name] = _jsonable(getattr(user, name))
    if hasattr(user, "tieba_uid"):
        raw_tieba_uid = user.tieba_uid
        payload["tieba_uid"] = _raw_tieba_uid(raw_tieba_uid)
        if _int_value(raw_tieba_uid) is not None and _int_value(raw_tieba_uid) > SIGNED_BIGINT_MAX:
            payload["tieba_uid_raw"] = str(raw_tieba_uid)
    return payload


def _raw_tieba_uid(value: Any) -> int | str | None:
    integer = _int_value(value)
    if integer is not None and integer > SIGNED_BIGINT_MAX:
        return str(value)
    return _jsonable(value)


def _ip_location_text(user: Any) -> str | None:
    value = _optional_text(getattr(user, "ip", None))
    if value is None or _is_ip_address(value):
        return None
    return value


def _ip_location_source(page: ResearchPage, record: Any) -> str:
    if page.source_api == "aiotieba.get_posts":
        if record is getattr(page.raw, "thread", None):
            return "aiotieba.get_posts.thread.author.ip"
        return "aiotieba.get_posts.author.ip"
    return f"{page.source_api}.author.ip"


def _is_ip_address(value: str) -> bool:
    try:
        ipaddress.ip_address(value.strip())
    except ValueError:
        return False
    return True


def _missing_user_reasons(user: Any, source_api: str, *, record: Any = None) -> dict[str, str]:
    reasons: dict[str, str] = {}
    if _author_user_id(record, user) is None:
        reasons["author_user_id"] = "aiotieba_zero_sentinel"
    if not _identity_text(getattr(user, "user_name", None)):
        reasons["author_user_name"] = "not_returned_by_source"
    if not _identity_text(_nickname(user)):
        reasons["author_nickname"] = "not_returned_by_source"
    if not _identity_text(getattr(user, "portrait", None)):
        reasons["author_portrait"] = "not_returned_by_source"
    tieba_uid = _int_value(getattr(user, "tieba_uid", None))
    if tieba_uid is not None and tieba_uid > SIGNED_BIGINT_MAX:
        reasons["author_tieba_uid"] = "outside_signed_bigint_range"
    elif _positive_id(tieba_uid) is None:
        reasons["author_tieba_uid"] = "not_returned_by_content_endpoint"
    for source_name, field_name in (("level", "forum_level"), ("glevel", "global_growth_level")):
        if not hasattr(user, source_name):
            reasons[field_name] = "not_exposed_by_source"
        elif _int_value(getattr(user, source_name)) == 0:
            reasons[field_name] = "zero_presence_unverified_in_parsed_object"
        elif getattr(user, source_name) is None:
            reasons[field_name] = "not_returned_by_source"
    if not hasattr(user, "glevel"):
        reasons["global_growth_level"] = "not_exposed_by_source"
    if not hasattr(user, "global_growth_icon_url"):
        reasons["global_growth_icon_url"] = "not_exposed_by_aiotieba_parsed_content"
    elif _identity_text(getattr(user, "global_growth_icon_url", None)) is None:
        reasons["global_growth_icon_url"] = "not_returned_by_source"
    if _ip_location_text(user) is None:
        raw_ip_value = _optional_text(getattr(user, "ip", None))
        if raw_ip_value and _is_ip_address(raw_ip_value):
            reasons["ip_location_text"] = "source_value_is_ip_address_not_public_location"
        elif hasattr(user, "ip"):
            reasons["ip_location_text"] = "not_returned_by_source"
        else:
            reasons["ip_location_text"] = "not_exposed_by_source"
    if not hasattr(user, "is_bawu"):
        reasons["is_bawu"] = "not_exposed_by_source"
    if not hasattr(user, "icons"):
        reasons["icons"] = "not_exposed_by_source"
    if not hasattr(user, "level_name"):
        reasons["forum_level_name"] = "not_exposed_by_content_endpoint"
    if not hasattr(user, "role_name"):
        reasons["role_name"] = "not_exposed_by_content_endpoint"
    if not hasattr(user, "role_type"):
        reasons["role_type"] = "not_exposed_by_source"
    return reasons


def _media_rows(
    owner_type: str,
    owner_id: int,
    tid: int,
    fragments: list[Any],
    page: ResearchPage,
) -> list[MediaAsset]:
    rows = []
    for position, fragment in enumerate(fragments):
        fragment_name = type(fragment).__name__
        if "FragImage" in fragment_name:
            media_type = "image"
            src = _optional_text(getattr(fragment, "src", None))
            original = _optional_text(getattr(fragment, "origin_src", None))
            cover = None
            source = None
            media_id = None
            media_hash = _optional_text(getattr(fragment, "hash", None))
            width = _positive_int(getattr(fragment, "show_width", None))
            height = _positive_int(getattr(fragment, "show_height", None))
            duration = None
        elif "FragVideo" in fragment_name:
            media_type = "video"
            src = None
            original = None
            cover = _optional_text(getattr(fragment, "cover_src", None))
            source = _optional_text(getattr(fragment, "src", None))
            media_id = None
            media_hash = None
            width = _positive_int(getattr(fragment, "width", None))
            height = _positive_int(getattr(fragment, "height", None))
            duration = _positive_int(getattr(fragment, "duration", None))
        elif "FragVoice" in fragment_name:
            media_type = "voice"
            src = None
            original = None
            cover = None
            source = None
            media_id = None
            media_hash = _optional_text(getattr(fragment, "md5", None))
            width = None
            height = None
            duration = _positive_int(getattr(fragment, "duration", None))
        elif "FragEmoji" in fragment_name:
            media_type = "emoji"
            src = None
            original = None
            cover = None
            source = None
            media_id = _optional_text(getattr(fragment, "id", None))
            media_hash = None
            width = None
            height = None
            duration = None
        elif "FragUnknown" in fragment_name:
            media_type = "other"
            src = original = cover = source = media_id = media_hash = None
            width = height = duration = None
        else:
            continue

        missing_reason = None
        if media_type == "video":
            missing_reason = "stable_video_id_not_exposed_by_parsed_fragment"
        rows.append(
            MediaAsset(
                owner_type=owner_type,
                owner_id=owner_id,
                position=position,
                tid=tid,
                media_type=media_type,
                media_id=media_id,
                media_hash=media_hash,
                src_url=src,
                original_url=original,
                cover_url=cover,
                source_url=source,
                width=width,
                height=height,
                duration=duration,
                api_fragment_type=fragment_name,
                raw_fragment_json=_jsonable(fragment),
                source_api=page.source_api,
                observed_at=page.observed_at,
                missing_reason=missing_reason,
            )
        )
    return rows


def _content_fragments(contents: Any) -> list[Any]:
    if contents is None:
        return []
    fragments = getattr(contents, "objs", contents)
    return list(fragments or [])


def _reply_target_candidate(comment: Any) -> dict[str, Any] | None:
    raw_content = getattr(comment, "content", None)
    if isinstance(raw_content, Mapping):
        raw_content = raw_content.get("content", raw_content.get("objs"))
    if raw_content is None:
        contents = getattr(comment, "contents", None)
        if isinstance(contents, Mapping):
            raw_content = contents.get("content", contents.get("objs"))
            fragments = list(raw_content or ())
        else:
            fragments = _content_fragments(contents)
    elif isinstance(raw_content, (str, bytes)):
        fragments = []
    else:
        fragments = list(getattr(raw_content, "objs", raw_content) or ())
    if len(fragments) < 2:
        return None

    prefix, mention = fragments[:2]
    prefix_text = prefix.get("text") if isinstance(prefix, Mapping) else getattr(prefix, "text", None)
    prefix_type = prefix.get("type") if isinstance(prefix, Mapping) else getattr(prefix, "type", None)
    prefix_type = _int_value(prefix_type)
    if prefix_text != "回复 " or not (
        prefix_type == 0 if prefix_type is not None else type(prefix).__name__.startswith("FragText")
    ):
        return None

    mention_type = mention.get("type") if isinstance(mention, Mapping) else getattr(mention, "type", None)
    mention_type = _int_value(mention_type)
    is_type4_mention = (
        mention_type == 4 if mention_type is not None else type(mention).__name__.startswith("FragAt")
    )
    if not is_type4_mention:
        return None

    mention_text = mention.get("text") if isinstance(mention, Mapping) else getattr(mention, "text", None)
    mention_user_id = (
        mention.get("uid", mention.get("user_id"))
        if isinstance(mention, Mapping)
        else getattr(mention, "user_id", getattr(mention, "uid", None))
    )
    display_name = _identity_text(mention_text)
    if display_name is not None:
        display_name = display_name.removeprefix("@")
        display_name = _identity_text(display_name)
    user_id = _positive_id(mention_user_id)
    return {
        "display_name": display_name,
        "user_id": user_id,
        "raw": {
            "prefix": _jsonable(prefix),
            "mention": _jsonable(mention),
            "reply_to_user_id": user_id,
        },
    }


def _content_text(contents: Any) -> str | None:
    if contents is None:
        return None
    text = getattr(contents, "text", None)
    return text if isinstance(text, str) else None


def _has_content_data(contents: Any) -> bool:
    return bool(_content_text(contents) or _content_fragments(contents))


def _nickname(user: Any) -> Any:
    if user is None:
        return None
    for name in ("nick_name_new", "nick_name_old", "nick_name"):
        if value := _identity_text(getattr(user, name, None)):
            return value
    return None


def _author_user_id(record: Any, user: Any) -> int | None:
    record_id = _positive_id(getattr(record, "author_id", None)) if record is not None else None
    user_id = _positive_id(getattr(user, "user_id", None)) if user is not None else None
    if record_id is not None and user_id is not None and record_id != user_id:
        raise ValueError("Record author ID and parsed user ID disagree; refusing to persist identity.")
    return record_id or user_id


def _epoch_datetime(value: Any) -> datetime | None:
    if not isinstance(value, (int, float)) or value <= 0:
        return None
    return datetime.fromtimestamp(value, SHANGHAI_TZ)


def _int_value(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, Enum):
        value = value.value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _count_value(value: Any, field_name: str, missing: dict[str, str]) -> int | None:
    """Keep nonzero counts; mark parser-default zero as unavailable but retain it raw."""
    count = _int_value(value)
    if count == 0:
        missing[field_name] = "zero_presence_unverified_in_parsed_object"
        return None
    return count


def _protobuf_int_field(value: Any, field_name: str, missing: dict[str, str]) -> int | None:
    number = _int_value(value)
    if number == 0:
        missing[field_name] = "zero_presence_unverified_in_parsed_object"
        return None
    return number


def _positive_id(value: Any) -> int | None:
    integer = _int_value(value)
    return integer if integer is not None and integer > 0 else None


def _is_floor_one_post(post: Any) -> bool:
    floor = getattr(post, "floor", None)
    return not isinstance(floor, bool) and _int_value(floor) == 1


def _positive_metadata_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    integer = _int_value(value)
    return integer if integer is not None and integer > 0 else None


def _safe_tieba_uid(value: Any, field_name: str, missing: dict[str, str]) -> int | None:
    integer = _int_value(value)
    if integer is None or integer <= 0:
        if integer == 0:
            missing.setdefault(field_name, "zero_presence_unverified_in_parsed_object")
        return None
    if integer > SIGNED_BIGINT_MAX:
        missing[field_name] = "outside_signed_bigint_range"
        return None
    return integer


def _positive_int(value: Any) -> int | None:
    return _positive_id(value)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def _identity_text(value: Any) -> str | None:
    text = _optional_text(value)
    return text if text is not None and text.strip() else None


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _content_is_bawu(value: Any, missing: dict[str, str]) -> bool | None:
    """Treat aiotieba's protobuf default false as unverified unless it is true."""
    if value is False:
        missing["is_bawu"] = "zero_presence_unverified_in_parsed_object"
        return None
    return _optional_bool(value)


def _enum_int(value: Any) -> int | None:
    return _int_value(value)


def _enum_name(value: Any) -> str | None:
    name = getattr(value, "name", None)
    return name if isinstance(name, str) else None


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump(mode="json"))
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {item.name: _jsonable(getattr(value, item.name)) for item in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "__dict__"):
        return {str(key): _jsonable(item) for key, item in vars(value).items() if not key.startswith("_")}
    return str(value)


def _row_values(row: Any) -> dict[str, Any]:
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}


def _dedupe(rows: list[Any], key_fields: tuple[str, ...]) -> list[Any]:
    unique: dict[tuple[Any, ...], Any] = {}
    for row in rows:
        key = tuple(getattr(row, field_name) for field_name in key_fields)
        unique[key] = row
    return list(unique.values())
