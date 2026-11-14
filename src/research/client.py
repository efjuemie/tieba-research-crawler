"""Capture aiotieba response objects alongside their standard DTOs."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal

from aiotieba.enums import PostSortType, ReqUInfo, ThreadSortType
from tiebameow.client.tieba_client import Client, RetriableApiError, UnretriableApiError, with_ensure
from tiebameow.parser import convert_aiotieba_comments, convert_aiotieba_posts, convert_aiotieba_threads

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from aiotieba.api.get_user_forum_info._classdef import UserForumInfo
    from tiebameow.models.dto import CommentsDTO, PostsDTO, ThreadsDTO


@dataclass(frozen=True, slots=True)
class ResearchPage[RawPage, DTOPage]:
    """A single API response with its standard DTO and observation metadata.

    ``raw`` is the parsed aiotieba response object, not the original HTTP body.
    """

    raw: RawPage
    dto: DTOPage
    observed_at: datetime
    source_api: str


@dataclass(frozen=True, slots=True)
class ResearchUserForumInfo:
    """Small, timestamped projection of the authenticated forum-info response."""

    user_id: int
    response_user_id: int | None
    fid: int
    fname: str | None
    portrait: str | None
    display_name: str | None
    tieba_uid: int | None
    forum_level: int | None
    forum_level_name: str | None
    role_name: str | None
    status: Literal["available", "unavailable", "blocked", "retryable_failure"]
    missing_reasons: dict[str, str]
    error_type: str | None = None
    error_code: int | None = None


class ResearchRunCache:
    """Deduplicate forum-info lookups across workers within one crawl run."""

    def __init__(self, max_entries: int = 4096) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._user_forum: OrderedDict[
            tuple[int, int], ResearchPage[object | None, ResearchUserForumInfo]
        ] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get_or_create(
        self,
        key: tuple[int, int],
        factory: Callable[[], Awaitable[ResearchPage[object | None, ResearchUserForumInfo]]],
    ) -> ResearchPage[object | None, ResearchUserForumInfo]:
        async with self._lock:
            if page := self._user_forum.get(key):
                self._user_forum.move_to_end(key)
                return page
            page = await factory()
            if page.dto.status != "retryable_failure":
                self._user_forum[key] = page
                if len(self._user_forum) > self._max_entries:
                    self._user_forum.popitem(last=False)
            return page

    def clear(self) -> None:
        """Start a new cache scope before a new crawl run."""
        self._user_forum.clear()


class ResearchClient(Client):
    """TiebaMeow client that exposes parsed API objects for research storage."""

    def __init__(
        self,
        *args: object,
        research_run_cache: ResearchRunCache | None = None,
        **kwargs: object,
    ) -> None:
        if "BDUSS" in kwargs and kwargs["BDUSS"] is not None:
            from .credentials import parse_bduss

            bduss = kwargs["BDUSS"]
            if not isinstance(bduss, str):
                raise ValueError("BDUSS must be provided as a 192-character string.")
            kwargs["BDUSS"] = parse_bduss(bduss)
        super().__init__(*args, **kwargs)
        self.research_run_cache = research_run_cache or ResearchRunCache()

    async def get_threads_research(
        self,
        fname_or_fid: str | int,
        /,
        pn: int = 1,
        *,
        rn: int = 30,
        sort: ThreadSortType = ThreadSortType.REPLY,
        is_good: bool = False,
    ) -> ResearchPage[object, ThreadsDTO]:
        raw = await self.get_threads(fname_or_fid, pn, rn=rn, sort=sort, is_good=is_good)
        dto = convert_aiotieba_threads(raw)
        dto.page.total_count = raw.page.total_count
        return ResearchPage(
            raw=raw,
            dto=dto,
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_threads",
        )

    async def get_posts_research(
        self,
        tid: int,
        /,
        pn: int = 1,
        *,
        rn: int = 30,
        sort: PostSortType = PostSortType.ASC,
        only_thread_author: bool = False,
        with_comments: bool = False,
        comment_sort_by_agree: bool = True,
        comment_rn: int = 4,
    ) -> ResearchPage[object, PostsDTO]:
        raw = await self.get_posts(
            tid,
            pn,
            rn=rn,
            sort=sort,
            only_thread_author=only_thread_author,
            with_comments=with_comments,
            comment_sort_by_agree=comment_sort_by_agree,
            comment_rn=comment_rn,
        )
        # aiotieba 4.8.2 Page_p does not expose total_count.
        dto = convert_aiotieba_posts(raw)
        return ResearchPage(
            raw=raw,
            dto=dto,
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_posts",
        )

    async def get_comments_research(
        self,
        tid: int,
        pid: int,
        /,
        pn: int = 1,
        *,
        is_comment: bool = False,
    ) -> ResearchPage[object, CommentsDTO]:
        raw = await self.get_comments(tid, pid, pn, is_comment=is_comment)
        dto = convert_aiotieba_comments(raw)
        dto.page.total_count = raw.page.total_count
        return ResearchPage(
            raw=raw,
            dto=dto,
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_comments",
        )

    async def get_user_forum_info_research(
        self,
        fid: int,
        user_id: int,
        portrait: str | None = None,
        /,
    ) -> ResearchPage[UserForumInfo | None, ResearchUserForumInfo]:
        """Fetch one current forum snapshot, cached by ``(user_id, fid)``.

        The underlying aiotieba 4.8.2 endpoint requires a valid BDUSS. Callers
        should share one ``ResearchRunCache`` across clients for each run.
        """
        if user_id <= 0:
            observed_at = datetime.now(UTC)
            return self._missing_user_forum_page(
                fid,
                user_id,
                observed_at,
                reason="missing_user_id",
            )

        return await self.research_run_cache.get_or_create(
            (user_id, fid),
            lambda: self._fetch_user_forum_info_research(fid, user_id, portrait),
        )

    async def _fetch_user_forum_info_research(
        self,
        fid: int,
        user_id: int,
        portrait: str | None,
    ) -> ResearchPage[UserForumInfo | None, ResearchUserForumInfo]:
        tieba_uid: int | None = None
        resolved_portrait = portrait or None

        try:
            if resolved_portrait is None:
                user = await self.get_user_info(user_id, require=ReqUInfo.PORTRAIT)
                resolved_portrait = user.portrait or None
                tieba_uid = user.tieba_uid or None
        except UnretriableApiError as error:
            return self._failed_user_forum_page(
                fid,
                user_id,
                resolved_portrait,
                tieba_uid,
                status="blocked",
                error=error,
            )
        except RetriableApiError as error:
            return self._failed_user_forum_page(
                fid,
                user_id,
                resolved_portrait,
                tieba_uid,
                status="retryable_failure",
                error=error,
            )

        if resolved_portrait is None:
            return self._missing_user_forum_page(
                fid,
                user_id,
                datetime.now(UTC),
                reason="portrait_unavailable_from_user_info",
                tieba_uid=tieba_uid,
            )

        try:
            raw = await self._get_user_forum_info_by_portrait(fid, resolved_portrait)
        except UnretriableApiError as error:
            return self._failed_user_forum_page(
                fid,
                user_id,
                resolved_portrait,
                tieba_uid,
                status="blocked",
                error=error,
            )
        except RetriableApiError as error:
            return self._failed_user_forum_page(
                fid,
                user_id,
                resolved_portrait,
                tieba_uid,
                status="retryable_failure",
                error=error,
            )

        observed_at = datetime.now(UTC)
        response_user = raw.user
        returned_user_id = response_user.user_id or None
        missing_reasons: dict[str, str] = {}
        identity_mismatch = returned_user_id != user_id
        if returned_user_id is None:
            missing_reasons["response_user_id"] = "aiotieba_zero_sentinel"
        if identity_mismatch:
            mismatch_reason = "response_user_id_missing" if returned_user_id is None else "response_user_id_mismatch"
            missing_reasons.update(
                dict.fromkeys(
                    ("portrait", "display_name", "forum_level", "forum_level_name", "role_name"),
                    mismatch_reason,
                )
            )

        dto = ResearchUserForumInfo(
            user_id=user_id,
            response_user_id=returned_user_id,
            fid=fid,
            fname=raw.fname or None,
            portrait=None if identity_mismatch else (response_user.portrait or resolved_portrait),
            display_name=None if identity_mismatch else (response_user.show_name or None),
            tieba_uid=None if identity_mismatch else tieba_uid,
            forum_level=None if identity_mismatch else raw.level,
            forum_level_name=None if identity_mismatch else (raw.level_name or None),
            role_name=None if identity_mismatch else (raw.role_name or None),
            status="unavailable" if identity_mismatch else "available",
            missing_reasons=missing_reasons,
        )
        return ResearchPage(
            raw=raw,
            dto=dto,
            observed_at=observed_at,
            source_api="aiotieba.get_user_forum_info",
        )

    @with_ensure
    async def _get_user_forum_info_by_portrait(self, fid: int, portrait: str) -> UserForumInfo:
        """Limit the 4.8.2 endpoint call while avoiding nested profile lookups."""
        return await super().get_user_forum_info(fid, portrait)

    @staticmethod
    def _missing_user_forum_page(
        fid: int,
        user_id: int,
        observed_at: datetime,
        *,
        reason: str,
        tieba_uid: int | None = None,
    ) -> ResearchPage[None, ResearchUserForumInfo]:
        return ResearchPage(
            raw=None,
            dto=ResearchUserForumInfo(
                user_id=user_id,
                response_user_id=None,
                fid=fid,
                fname=None,
                portrait=None,
                display_name=None,
                tieba_uid=tieba_uid,
                forum_level=None,
                forum_level_name=None,
                role_name=None,
                status="unavailable",
                missing_reasons={
                    "fname": reason,
                    "portrait": reason,
                    "display_name": reason,
                    "forum_level": reason,
                    "forum_level_name": reason,
                    "role_name": reason,
                },
            ),
            observed_at=observed_at,
            source_api="aiotieba.get_user_forum_info",
        )

    @classmethod
    def _failed_user_forum_page(
        cls,
        fid: int,
        user_id: int,
        portrait: str | None,
        tieba_uid: int | None,
        *,
        status: Literal["blocked", "retryable_failure"],
        error: RetriableApiError | UnretriableApiError,
    ) -> ResearchPage[None, ResearchUserForumInfo]:
        observed_at = datetime.now(UTC)
        reason = f"{type(error).__name__}:{error.code}"
        return ResearchPage(
            raw=None,
            dto=ResearchUserForumInfo(
                user_id=user_id,
                response_user_id=None,
                fid=fid,
                fname=None,
                portrait=portrait,
                display_name=None,
                tieba_uid=tieba_uid,
                forum_level=None,
                forum_level_name=None,
                role_name=None,
                status=status,
                missing_reasons={
                    "fname": reason,
                    "display_name": reason,
                    "forum_level": reason,
                    "forum_level_name": reason,
                    "role_name": reason,
                },
                error_type=type(error).__name__,
                error_code=error.code,
            ),
            observed_at=observed_at,
            source_api="aiotieba.get_user_forum_info",
        )
