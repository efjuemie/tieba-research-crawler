"""本地 ORM 模型。

定义 TiebaScraper 内部使用的辅助数据库模型。
"""

from __future__ import annotations

from datetime import datetime  # noqa: TC003
from typing import Literal

from sqlalchemy import JSON, BigInteger, Boolean, DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from tiebameow.models.orm import Base

JSON_TYPE = JSON().with_variant(JSONB(), "postgresql")

REPLY_BACKFILL_MODE = "reply_backfill"
REPLY_BACKFILL_SOURCE_API = "aiotieba.get_threads"
REPLY_BACKFILL_RN = 100
REPLY_BACKFILL_SORT_TYPE = 6
REPLY_BACKFILL_CONTRACT_VERSION = 1


def research_thread_list_task_key(
    fid: int,
    is_good: bool,
    sort_mode: Literal["create", "reply"] = "create",
    rn: int = 30,
) -> str:
    if sort_mode not in {"create", "reply"} or rn < 1:
        raise ValueError("Invalid thread list checkpoint contract.")
    if sort_mode == "create" and rn == 30:
        return f"thread_list:{fid}:create:{int(is_good)}"
    return f"thread_list:{fid}:{sort_mode}:rn{rn}:{int(is_good)}"


def research_task_checkpoint_key(logical_task_key: str, generation: int) -> str:
    """Return the page-checkpoint key for one durable task generation."""
    if generation < 0:
        raise ValueError("Research task generation cannot be negative.")
    return logical_task_key if generation == 0 else f"{logical_task_key}:g{generation}"


def research_posts_task_key(tid: int, generation: int = 0) -> str:
    return research_task_checkpoint_key(f"thread_replies:{tid}", generation)


def research_comments_task_key(tid: int, parent_pid: int, generation: int = 0) -> str:
    return research_task_checkpoint_key(f"post_comments:{tid}:{parent_pid}", generation)


class ResearchRun(Base):
    """Persistent identity and run-level state for resumable research crawls."""

    __tablename__ = "research_run"

    run_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    scope_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    forums_json: Mapped[list] = mapped_column(JSON_TYPE, nullable=False, default=list)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="running", index=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_checked: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ResearchTaskScope(Base):
    """Authoritative current generation and recovery metadata for a detail task."""

    __tablename__ = "research_task_scope"

    run_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    logical_task_key: Mapped[str] = mapped_column(String(512), primary_key=True)
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    task_type: Mapped[str] = mapped_column(String(32), nullable=False)
    scope_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    fid: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    fname: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tid: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    parent_pid: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    parent_kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    is_good: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    backfill: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    rn: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    parent_logical_task_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    parent_generation: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="pending", index=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_checked: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    @property
    def logical_key(self) -> str:
        return self.logical_task_key

    @property
    def current_generation(self) -> int:
        return self.generation


class ResearchCheckpoint(Base):
    """One page-level checkpoint for list, post-detail, or comment scans.

    The non-null ``task_key`` avoids PostgreSQL's nullable-unique-key behavior
    for scopes whose tid or parent pid is not applicable.
    """

    __tablename__ = "research_checkpoint"

    run_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_key: Mapped[str] = mapped_column(String(512), primary_key=True)
    page_number: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_type: Mapped[str] = mapped_column(String(32), nullable=False)
    scope_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    sort_mode: Mapped[str | None] = mapped_column(String(16), nullable=True)
    fid: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    fname: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tid: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    parent_pid: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    parent_kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    is_good: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    scope_status: Mapped[str | None] = mapped_column(String(24), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    has_more: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    next_page: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_pages: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reported_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    reported_count_missing_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    page_item_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    continuation_json: Mapped[dict | None] = mapped_column(JSON_TYPE, nullable=True)
    error_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_checked: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class PendingThreadScan(Base):
    """标记全量扫描尚未完成的 Thread。

    程序重启时可读取本表恢复未完成的扫描任务。

    Attributes:
        tid: 主题贴 tid（主键）
        fid: 贴吧 fid（恢复任务时用于按实例过滤）
        fname: 贴吧名（恢复任务时用于日志）
        backfill: 是否为回溯任务
        created_at: 记录创建时间
    """

    __tablename__ = "pending_thread_scan"

    tid: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    fname: Mapped[str] = mapped_column(String(255), nullable=False)
    backfill: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    def to_dict(self) -> dict:
        result = {}
        for c in self.__table__.columns:
            value = getattr(self, c.name)
            if value is not None:
                result[c.name] = value
        return result


class PendingCommentScan(Base):
    """标记楼中楼扫描尚未完成的 Post。

    用于恢复 FullScanCommentsTask / IncrementalScanCommentsTask。

    Attributes:
        tid: 主题贴 tid
        pid: 回复 pid
        fid: 贴吧 fid（恢复任务时用于按实例过滤）
        fname: 贴吧名（恢复任务时用于日志）
        backfill: 是否为回溯任务
        task_kind: 任务类型（"full" 或 "incremental"）
        created_at: 记录创建时间
    """

    __tablename__ = "pending_comment_scan"

    tid: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    pid: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    fname: Mapped[str] = mapped_column(String(255), nullable=False)
    backfill: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    task_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    def to_dict(self) -> dict:
        result = {}
        for c in self.__table__.columns:
            value = getattr(self, c.name)
            if value is not None:
                result[c.name] = value
        return result
