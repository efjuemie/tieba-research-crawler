"""Mapping and idempotent persistence for parsed research API responses."""

from __future__ import annotations

import base64
import dataclasses
import ipaddress
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from itertools import starmap
from typing import TYPE_CHECKING, Any

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


@dataclass(slots=True)
class ResearchRecords:
    threads: list[ResearchThread] = field(default_factory=list)
    posts: list[ResearchPost] = field(default_factory=list)
    comments: list[ResearchComment] = field(default_factory=list)
    users: list[ResearchUser] = field(default_factory=list)
    user_forum_snapshots: list[UserForumSnapshot] = field(default_factory=list)
    media_assets: list[MediaAsset] = field(default_factory=list)
    content_observations: list[ResearchContentObservation] = field(default_factory=list)


async def persist_research_page(
    async_sessionmaker_: async_sessionmaker | None,
    page: ResearchPage,
    *,
    session: AsyncSession | None = None,
) -> dict[str, int]:
    """Upsert one already-fetched response without advancing any checkpoint.

    API requests must finish before calling this function so a login prompt or
    rate-limit wait never holds a database transaction open. If ``session`` is
    passed, its caller owns the transaction and this function neither manages
    nor closes it.
    """
    records = build_research_records(page)
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
    if records.comments:
        await _fill_parent_post_authors(session, records.comments)
    accepted_content = await _upsert_content_observations(
        session,
        _dedupe(records.content_observations, ("owner_type", "owner_id")),
    )
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
        _dedupe(records.threads, ("tid",)),
        accepted_content,
    )
    post_rows = [row for row in _dedupe(records.posts, ("pid",)) if ("post", row.pid) in accepted_content]
    comment_rows = [
        row for row in _dedupe(records.comments, ("cid",)) if ("comment", row.cid) in accepted_content
    ]
    await _upsert_rows(session, ResearchPost, post_rows)
    await _upsert_rows(session, ResearchComment, comment_rows)
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
            is_bawu=_optional_bool(getattr(user, "is_bawu", None)),
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
    first_pid = _positive_id(getattr(raw_thread, "pid", None))
    page_posts = tuple(getattr(response, "objs", ()) or ())
    if thread_tid is not None:
        is_share = bool(getattr(raw_thread, "is_share", False))
        thread_contents = getattr(raw_thread, "contents", None)
        thread_fragments = _content_fragments(thread_contents)
        raw_user = getattr(raw_thread, "user", None)
        missing = _missing_user_reasons(raw_user, page.source_api, record=raw_thread)
        for field_name in ("forum_level_name", "role_type", "role_name"):
            missing.setdefault(field_name, "not_exposed_by_get_posts_author")
        missing["role_badge_id"] = "icon_semantics_unverified"
        missing["role_badge_url"] = "not_exposed_by_get_posts_author"
        if _positive_id(getattr(raw_thread, "fid", None)) is None:
            missing["fid"] = "aiotieba_zero_sentinel"

        raw_first_pid = _positive_id(getattr(raw_thread, "pid", None))
        first_post = next(
            (
                post
                for post in page_posts
                if _positive_id(getattr(post, "pid", None)) == raw_first_pid
                and _positive_id(getattr(post, "tid", None)) in (None, thread_tid)
            ),
            None,
        )
        if first_post is None:
            first_post = next(
                (
                    post
                    for post in page_posts
                    if getattr(post, "floor", None) == 1
                    and _positive_id(getattr(post, "tid", None)) in (None, thread_tid)
                ),
                None,
            )
        first_post_pid = _positive_id(getattr(first_post, "pid", None)) if first_post is not None else None
        first_pid = raw_first_pid or first_post_pid
        if first_pid is None:
            missing["first_pid"] = "aiotieba_zero_sentinel"
        else:
            missing.pop("first_pid", None)

        # Thread_p is returned on every page, but an empty parsed Contents object
        # does not prove that the first post itself was observed on this page.
        # Only a matching Post object (or explicit floor 1 Post) makes its body
        # and media authoritative for replacement.
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
                fid=_positive_id(getattr(raw_thread, "fid", None)),
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
                is_bawu=_optional_bool(getattr(raw_user, "is_bawu", None)),
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
            _positive_id(getattr(raw_thread, "fid", None)),
            _optional_text(getattr(raw_thread, "fname", None)),
            page,
            record=raw_thread,
        )

    thread_author_id = _author_user_id(raw_thread, getattr(raw_thread, "user", None))
    for post in page_posts:
        pid = _positive_id(getattr(post, "pid", None))
        if pid is None:
            raise ValueError("get_posts response contains a post without a valid pid.")
        if pid == first_pid or getattr(post, "floor", None) == 1:
            continue

        user = getattr(post, "user", None)
        tid = _positive_id(getattr(post, "tid", None)) or thread_tid
        if tid is None:
            raise ValueError("get_posts response contains a post without a valid tid.")
        contents = getattr(post, "contents", None)
        fragments = _content_fragments(contents)
        post_fid = _positive_id(getattr(post, "fid", None)) or _positive_id(getattr(raw_thread, "fid", None))
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
                is_bawu=_optional_bool(getattr(user, "is_bawu", None)),
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
                is_bawu=_optional_bool(getattr(user, "is_bawu", None)),
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
                reply_to_id_raw=raw_reply_to_id,
                reply_target_source=("aiotieba_parsed_reply_prefix" if reply_to_user_id is not None else "unresolved"),
                reply_target_confidence=("parsed_from_reply_prefix" if reply_to_user_id is not None else "unknown"),
                reply_resolution_note=(
                    "aiotieba 4.8.2 derives this user ID from a leading reply prefix and at-mention fragment; "
                    "live direction has not been verified."
                    if reply_to_user_id is not None
                    else "aiotieba 4.8.2 uses zero when no reply target is parsed."
                ),
                agree_count=_count_value(getattr(comment, "agree", None), "agree_count", missing),
                disagree_count=_count_value(getattr(comment, "disagree", None), "disagree_count", missing),
                is_thread_author=_optional_bool(getattr(comment, "is_thread_author", None)),
                source_api=page.source_api,
                observed_at=page.observed_at,
                source_payload_json=_comment_payload(comment, user),
                missing_reasons_json=missing,
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

        if incoming.source_api == "aiotieba.get_posts" and content_accepted:
            if incoming.first_pid is not None:
                current.first_pid = incoming.first_pid
            for name in _THREAD_DETAIL_ONLY_FIELDS:
                if name.startswith("ip_location_") and not _thread_source_supports(
                    incoming.source_api, "ip_location_text", incoming.missing_reasons_json
                ):
                    continue
                if _thread_source_supports(incoming.source_api, name, incoming.missing_reasons_json):
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
