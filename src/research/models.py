"""Project-owned extension tables for research metadata."""

from __future__ import annotations

from datetime import datetime  # noqa: TC003

from sqlalchemy import BIGINT, JSON, Boolean, DateTime, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from tiebameow.models.orm import Base

JSON_TYPE = JSON().with_variant(JSONB(), "postgresql")


class ResearchRecordMixin:
    source_api: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_payload_json: Mapped[dict] = mapped_column(JSON_TYPE, nullable=False, default=dict)
    missing_reasons_json: Mapped[dict] = mapped_column(JSON_TYPE, nullable=False, default=dict)


class ResearchThread(ResearchRecordMixin, Base):
    """Canonical thread row, enriched from list and first-post responses."""

    __tablename__ = "research_thread"

    tid: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    first_pid: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    fid: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    fname: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    body_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    contents_raw: Mapped[dict | list | None] = mapped_column(JSON_TYPE, nullable=True)
    create_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_time_raw: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    last_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_time_raw: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    author_user_id: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    author_user_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    author_nickname: Mapped[str | None] = mapped_column(Text, nullable=True)
    author_portrait: Mapped[str | None] = mapped_column(Text, nullable=True)
    author_tieba_uid: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    forum_level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    global_growth_level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_bawu: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    author_icons: Mapped[list | None] = mapped_column(JSON_TYPE, nullable=True)
    forum_level_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_badge_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_badge_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_location_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_location_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_location_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reply_count: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    agree_count: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    disagree_count: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    collect_count: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    view_count: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    share_count: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    thread_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_top: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_good: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_livepost: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_hide: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_share: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_help: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    scraped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ResearchPost(ResearchRecordMixin, Base):
    """First-level reply row with author and response-time metrics."""

    __tablename__ = "research_post"

    pid: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    tid: Mapped[int] = mapped_column(BIGINT, nullable=False, index=True)
    fid: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    fname: Mapped[str | None] = mapped_column(Text, nullable=True)
    floor: Mapped[int | None] = mapped_column(Integer, nullable=True)
    create_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_time_raw: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    scraped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    sign: Mapped[str | None] = mapped_column(Text, nullable=True)
    contents_raw: Mapped[dict | list | None] = mapped_column(JSON_TYPE, nullable=True)
    author_user_id: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    author_user_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    author_nickname: Mapped[str | None] = mapped_column(Text, nullable=True)
    author_portrait: Mapped[str | None] = mapped_column(Text, nullable=True)
    author_tieba_uid: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    thread_author_user_id: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    forum_level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    global_growth_level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_bawu: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    author_icons: Mapped[list | None] = mapped_column(JSON_TYPE, nullable=True)
    forum_level_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_badge_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_badge_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_location_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_location_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_location_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    agree_count: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    disagree_count: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    subreply_count_reported: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    is_thread_author: Mapped[bool | None] = mapped_column(Boolean, nullable=True)


class ResearchComment(ResearchRecordMixin, Base):
    """Second-level reply row preserving its parent and reply target IDs."""

    __tablename__ = "research_comment"

    cid: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    tid: Mapped[int] = mapped_column(BIGINT, nullable=False, index=True)
    parent_post_pid: Mapped[int | None] = mapped_column(BIGINT, nullable=True, index=True)
    parent_post_author_user_id: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    fid: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    fname: Mapped[str | None] = mapped_column(Text, nullable=True)
    floor: Mapped[int | None] = mapped_column(Integer, nullable=True)
    create_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_time_raw: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    scraped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    contents_raw: Mapped[dict | list | None] = mapped_column(JSON_TYPE, nullable=True)
    author_user_id: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    author_user_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    author_nickname: Mapped[str | None] = mapped_column(Text, nullable=True)
    author_portrait: Mapped[str | None] = mapped_column(Text, nullable=True)
    author_tieba_uid: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    forum_level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    global_growth_level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_bawu: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    author_icons: Mapped[list | None] = mapped_column(JSON_TYPE, nullable=True)
    forum_level_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_badge_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_badge_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_location_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_location_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_location_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reply_to_user_id: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    reply_to_id_raw: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    reply_target_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    reply_target_confidence: Mapped[str | None] = mapped_column(Text, nullable=True)
    reply_resolution_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    agree_count: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    disagree_count: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    is_thread_author: Mapped[bool | None] = mapped_column(Boolean, nullable=True)


class ResearchUser(ResearchRecordMixin, Base):
    """Latest identity fields observed for a real Tieba user ID."""

    __tablename__ = "research_user"

    user_id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    user_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    nickname: Mapped[str | None] = mapped_column(Text, nullable=True)
    portrait: Mapped[str | None] = mapped_column(Text, nullable=True)
    tieba_uid: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    first_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class UserForumSnapshot(ResearchRecordMixin, Base):
    """Forum-specific user attributes captured at an observation time."""

    __tablename__ = "research_user_forum_snapshot"

    user_id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    fid: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    source_api: Mapped[str] = mapped_column(Text, primary_key=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    response_user_id: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    fname: Mapped[str | None] = mapped_column(Text, nullable=True)
    portrait: Mapped[str | None] = mapped_column(Text, nullable=True)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    forum_level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    global_growth_level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    forum_level_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_bawu: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    role_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    icons: Mapped[list | None] = mapped_column(JSON_TYPE, nullable=True)
    ip_location_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_location_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="available")
    tieba_uid: Mapped[int | None] = mapped_column(BIGINT, nullable=True)


class MediaAsset(Base):
    """Normalized references to inline media; no binary files are downloaded."""

    __tablename__ = "research_media_asset"

    owner_type: Mapped[str] = mapped_column(Text, primary_key=True)
    owner_id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    position: Mapped[int] = mapped_column(Integer, primary_key=True)
    tid: Mapped[int] = mapped_column(BIGINT, nullable=False, index=True)
    media_type: Mapped[str] = mapped_column(Text, nullable=False)
    media_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    media_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    src_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    original_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    cover_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration: Mapped[int | None] = mapped_column(Integer, nullable=True)
    api_fragment_type: Mapped[str] = mapped_column(Text, nullable=False)
    raw_fragment_json: Mapped[dict] = mapped_column(JSON_TYPE, nullable=False)
    source_api: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    missing_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class ResearchContentObservation(Base):
    """Watermark for each complete content/media snapshot, including empty media sets."""

    __tablename__ = "research_content_observation"

    owner_type: Mapped[str] = mapped_column(Text, primary_key=True)
    owner_id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    tid: Mapped[int] = mapped_column(BIGINT, nullable=False, index=True)
    source_api: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


RESEARCH_TABLES = (
    ResearchThread.__table__,
    ResearchPost.__table__,
    ResearchComment.__table__,
    ResearchUser.__table__,
    UserForumSnapshot.__table__,
    MediaAsset.__table__,
    ResearchContentObservation.__table__,
)
