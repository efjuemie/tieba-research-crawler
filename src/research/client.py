"""Capture aiotieba response objects alongside their standard DTOs."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal

from aiotieba.const import APP_BASE_HOST
from aiotieba.enums import BawuType, PostSortType, ReqUInfo, ThreadSortType
from aiotieba.helper.cache import ForumInfoCache
from tiebameow.client.tieba_client import Client, RetriableApiError, UnretriableApiError, with_ensure
from tiebameow.parser import convert_aiotieba_comments, convert_aiotieba_posts, convert_aiotieba_threads

from .zero_comment_evidence import extract_zero_comment_evidence

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Generator

    from aiotieba.api._classdef.user import UserInfo
    from aiotieba.api.get_bawu_info._classdef import BawuInfo
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
    zero_comment_evidence: dict[int, dict[str, object]] = field(default_factory=dict)


@dataclass(slots=True)
class _CapturedGetPostsResponse:
    body: bytes | None = None
    matching_response_count: int = 0


_GET_POSTS_RESPONSE_CAPTURE: ContextVar[_CapturedGetPostsResponse | None] = ContextVar(
    "research_get_posts_response_capture", default=None
)


def _is_get_posts_http_request(request: object) -> bool:
    url = getattr(request, "url", None)
    if (
        getattr(request, "method", None) != "POST"
        or getattr(url, "host", None) != APP_BASE_HOST
        or getattr(url, "path", None) != "/c/f/pb/page"
    ):
        return False
    query = getattr(url, "query", None)
    try:
        return len(query) == 1 and query.getall("cmd", []) == ["302001"]
    except (AttributeError, TypeError):
        return False


class _GetPostsResponseCapture:
    """Temporarily wrap one client's NetCore.send without changing transport."""

    def __init__(self, net_core: object) -> None:
        self._net_core = net_core
        self._active_captures = 0
        self._original_send = None
        self._wrapped_send = None

    @contextmanager
    def capture(self) -> Generator[_CapturedGetPostsResponse, None, None]:
        holder = _CapturedGetPostsResponse()
        if not self._activate():
            yield holder
            return

        token = _GET_POSTS_RESPONSE_CAPTURE.set(holder)
        try:
            yield holder
        finally:
            _GET_POSTS_RESPONSE_CAPTURE.reset(token)
            self._deactivate()

    def _activate(self) -> bool:
        if self._active_captures == 0:
            original_send = getattr(self._net_core, "send", None)
            if not callable(original_send):
                return False

            async def capture_send(request: object, *args: object, **kwargs: object) -> bytes:
                body = await original_send(request, *args, **kwargs)
                holder = _GET_POSTS_RESPONSE_CAPTURE.get()
                if holder is not None and _is_get_posts_http_request(request):
                    holder.matching_response_count += 1
                    holder.body = body if holder.matching_response_count == 1 and isinstance(body, bytes) else None
                return body

            try:
                self._net_core.send = capture_send
            except (AttributeError, TypeError):
                return False
            self._original_send = original_send
            self._wrapped_send = capture_send

        self._active_captures += 1
        return True

    def _deactivate(self) -> None:
        self._active_captures -= 1
        if self._active_captures:
            return
        if getattr(self._net_core, "send", None) is self._wrapped_send:
            try:
                self._net_core.send = self._original_send
            except (AttributeError, TypeError):
                pass
        self._original_send = None
        self._wrapped_send = None


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


@dataclass(frozen=True, slots=True)
class ResearchUserProfileInfo:
    user_id: int
    response_user_id: int | None
    user_name: str | None
    nickname: str | None
    portrait: str | None
    tieba_uid: int | None
    global_growth_level: int | None
    status: Literal["available", "unavailable", "blocked", "retryable_failure"]
    missing_reasons: dict[str, str]
    error_type: str | None = None
    error_code: int | None = None


@dataclass(frozen=True, slots=True)
class ResearchUserBawuInfo:
    user_id: int
    response_user_id: int | None
    fid: int
    role_type: str | None
    role_name: str | None
    is_bawu: bool | None
    status: Literal["available", "not_listed", "unavailable", "blocked", "retryable_failure"]
    missing_reasons: dict[str, str]
    error_type: str | None = None
    error_code: int | None = None


@dataclass(frozen=True, slots=True)
class ResearchBawuRoster:
    fid: int
    status: Literal["available", "blocked", "retryable_failure"]
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
        self._user_profiles: OrderedDict[int, ResearchPage[UserInfo | None, ResearchUserProfileInfo]] = OrderedDict()
        self._bawu_rosters: OrderedDict[int, ResearchPage[BawuInfo | None, ResearchBawuRoster]] = OrderedDict()
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

    async def get_user_profile_or_create(
        self,
        user_id: int,
        factory: Callable[[], Awaitable[ResearchPage[UserInfo | None, ResearchUserProfileInfo]]],
    ) -> ResearchPage[UserInfo | None, ResearchUserProfileInfo]:
        async with self._lock:
            if page := self._user_profiles.get(user_id):
                self._user_profiles.move_to_end(user_id)
                return page
            page = await factory()
            if page.dto.status != "retryable_failure":
                self._user_profiles[user_id] = page
                if len(self._user_profiles) > self._max_entries:
                    self._user_profiles.popitem(last=False)
            return page

    async def get_bawu_roster_or_create(
        self,
        fid: int,
        factory: Callable[[], Awaitable[ResearchPage[BawuInfo | None, ResearchBawuRoster]]],
    ) -> ResearchPage[BawuInfo | None, ResearchBawuRoster]:
        async with self._lock:
            if page := self._bawu_rosters.get(fid):
                self._bawu_rosters.move_to_end(fid)
                return page
            page = await factory()
            if page.dto.status != "retryable_failure":
                self._bawu_rosters[fid] = page
                if len(self._bawu_rosters) > self._max_entries:
                    self._bawu_rosters.popitem(last=False)
            return page

    def clear(self) -> None:
        """Start a new cache scope before a new crawl run."""
        self._user_forum.clear()
        self._user_profiles.clear()
        self._bawu_rosters.clear()


_BAWU_ROLE_GROUPS = (
    ("admin", BawuType.ADMIN.value, "大吧主"),
    ("manager", BawuType.MANAGER.value, "小吧主"),
    ("voice_editor", BawuType.VOICE_EDITOR.value, "语音小编"),
    ("image_editor", BawuType.IMAGE_EDITOR.value, "图片小编"),
    ("video_editor", BawuType.VIDEO_EDITOR.value, "视频小编"),
    ("broadcast_editor", BawuType.BROADCAST_EDITOR.value, "广播小编"),
    ("journal_chief_editor", BawuType.JOURNAL_CHIEF_EDITOR.value, "吧刊主编"),
    ("journal_editor", BawuType.JOURNAL_EDITOR.value, "吧刊小编"),
    ("profess_admin", BawuType.PROFESS_ADMIN.value, "职业吧主"),
    ("fourth_admin", BawuType.FOURTH_ADMIN.value, "第四吧主"),
)


def _nonblank_profile_text(value: str | None) -> str | None:
    return value if value and value.strip() else None


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
        self._research_get_posts_response_capture: _GetPostsResponseCapture | None = None

    def _get_posts_response_capture(self) -> _GetPostsResponseCapture | None:
        http_core = getattr(self, "_http_core", None)
        net_core = getattr(http_core, "net_core", None)
        if not callable(getattr(net_core, "send", None)):
            return None
        capture = getattr(self, "_research_get_posts_response_capture", None)
        if capture is None or capture._net_core is not net_core:
            capture = _GetPostsResponseCapture(net_core)
            self._research_get_posts_response_capture = capture
        return capture

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
        # Resolve outside get_threads' admission: aiotieba's internal resolver
        # bypasses that wrapper and would issue two HTTP calls in one slot.
        if isinstance(fname_or_fid, int):
            if isinstance(fname_or_fid, bool) or not 0 < fname_or_fid < (1 << 63):
                raise ValueError("Invalid forum ID.")
            fname_or_fid = ForumInfoCache.get_fname(fname_or_fid) or str(await self.get_fname(fname_or_fid))
            if not fname_or_fid:
                raise ValueError("Forum name resolution returned no name.")
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
        capture = self._get_posts_response_capture()
        request = self.get_posts(
            tid,
            pn,
            rn=rn,
            sort=sort,
            only_thread_author=only_thread_author,
            with_comments=with_comments,
            comment_sort_by_agree=comment_sort_by_agree,
            comment_rn=comment_rn,
        )
        captured_body = None
        if capture is None:
            raw = await request
        else:
            with capture.capture() as captured:
                raw = await request
            captured_body = captured.body
        # aiotieba 4.8.2 Page_p does not expose total_count.
        dto = convert_aiotieba_posts(raw)
        zero_comment_evidence = extract_zero_comment_evidence(
            captured_body,
            requested_tid=tid,
            source_page=pn,
            raw_page=raw,
        )
        return ResearchPage(
            raw=raw,
            dto=dto,
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_posts",
            zero_comment_evidence=zero_comment_evidence,
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

    async def get_user_info_research(
        self,
        user_id: int,
        /,
    ) -> ResearchPage[UserInfo | None, ResearchUserProfileInfo]:
        """Fetch one current user profile, cached by the confirmed legacy user ID."""
        if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
            return self._missing_user_profile_page(user_id, reason="invalid_user_id")

        return await self.research_run_cache.get_user_profile_or_create(
            user_id,
            lambda: self._fetch_user_info_research(user_id),
        )

    async def _fetch_user_info_research(
        self,
        user_id: int,
    ) -> ResearchPage[UserInfo | None, ResearchUserProfileInfo]:
        try:
            # glevel is returned by the OTHER profile field group; ALL is required
            # to request it together with identity and tieba_uid in aiotieba 4.8.2.
            raw = await self.get_user_info(user_id, require=ReqUInfo.ALL)
        except UnretriableApiError as error:
            return self._failed_user_profile_page(user_id, status="blocked", error=error)
        except RetriableApiError as error:
            return self._failed_user_profile_page(user_id, status="retryable_failure", error=error)

        observed_at = datetime.now(UTC)
        response_user_id = raw.user_id or None
        identity_mismatch = response_user_id != user_id
        missing: dict[str, str] = {}
        if response_user_id is None:
            missing["response_user_id"] = "aiotieba_zero_sentinel"
        if identity_mismatch:
            reason = "response_user_id_missing" if response_user_id is None else "response_user_id_mismatch"
            missing.update(
                dict.fromkeys(
                    ("user_name", "nickname", "portrait", "tieba_uid", "global_growth_level"),
                    reason,
                )
            )
        else:
            user_name = _nonblank_profile_text(raw.user_name)
            nickname = _nonblank_profile_text(raw.nick_name_new) or _nonblank_profile_text(raw.nick_name_old)
            portrait = _nonblank_profile_text(raw.portrait)
            if user_name is None:
                missing["user_name"] = "not_returned_by_get_user_info_4_8_2"
            if nickname is None:
                missing["nickname"] = "not_returned_by_get_user_info_4_8_2"
            if portrait is None:
                missing["portrait"] = "not_returned_by_get_user_info_4_8_2"
            if not raw.tieba_uid:
                missing["tieba_uid"] = "zero_presence_unverified_in_parsed_object"
            if not raw.glevel:
                missing["global_growth_level"] = "zero_presence_unverified_in_parsed_object"

        dto = ResearchUserProfileInfo(
            user_id=user_id,
            response_user_id=response_user_id,
            user_name=None if identity_mismatch else _nonblank_profile_text(raw.user_name),
            nickname=None
            if identity_mismatch
            else (_nonblank_profile_text(raw.nick_name_new) or _nonblank_profile_text(raw.nick_name_old)),
            portrait=None if identity_mismatch else _nonblank_profile_text(raw.portrait),
            tieba_uid=None if identity_mismatch else (raw.tieba_uid or None),
            global_growth_level=None if identity_mismatch else raw.glevel,
            status="unavailable" if identity_mismatch else "available",
            missing_reasons=missing,
        )
        return ResearchPage(
            raw=raw,
            dto=dto,
            observed_at=observed_at,
            source_api="aiotieba.get_user_info",
        )

    async def get_bawu_info_research(
        self,
        fid: int,
        user_id: int,
        /,
    ) -> ResearchPage[BawuInfo | None, ResearchUserBawuInfo]:
        """Project one exact user from a cached current forum bawu roster."""
        if isinstance(fid, bool) or not isinstance(fid, int) or fid <= 0:
            return self._missing_user_bawu_page(fid, user_id, reason="invalid_fid")
        if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
            return self._missing_user_bawu_page(fid, user_id, reason="invalid_user_id")

        roster_page = await self.research_run_cache.get_bawu_roster_or_create(
            fid,
            lambda: self._fetch_bawu_roster(fid),
        )
        if roster_page.raw is None:
            dto = ResearchUserBawuInfo(
                user_id=user_id,
                response_user_id=None,
                fid=fid,
                role_type=None,
                role_name=None,
                is_bawu=None,
                status=roster_page.dto.status,
                missing_reasons=dict(roster_page.dto.missing_reasons),
                error_type=roster_page.dto.error_type,
                error_code=roster_page.dto.error_code,
            )
        else:
            matches = []
            for attribute, raw_role_type, role_name in _BAWU_ROLE_GROUPS:
                if any(member.user_id == user_id for member in getattr(roster_page.raw, attribute, ())):
                    matches.append((raw_role_type, role_name))
            if matches:
                dto = ResearchUserBawuInfo(
                    user_id=user_id,
                    response_user_id=user_id,
                    fid=fid,
                    role_type=",".join(role_type for role_type, _ in matches),
                    role_name="、".join(role_name for _, role_name in matches),
                    is_bawu=True,
                    status="available",
                    missing_reasons={},
                )
            else:
                dto = ResearchUserBawuInfo(
                    user_id=user_id,
                    response_user_id=None,
                    fid=fid,
                    role_type=None,
                    role_name=None,
                    is_bawu=None,
                    status="not_listed",
                    missing_reasons={
                        "is_bawu": "not_listed_in_observed_roster",
                        "role_type": "not_listed_in_observed_roster",
                        "role_name": "not_listed_in_observed_roster",
                    },
                )

        return ResearchPage(
            raw=roster_page.raw,
            dto=dto,
            observed_at=roster_page.observed_at,
            source_api="aiotieba.get_bawu_info",
        )

    async def _fetch_bawu_roster(self, fid: int) -> ResearchPage[BawuInfo | None, ResearchBawuRoster]:
        try:
            raw = await self._get_bawu_info_by_fid(fid)
        except UnretriableApiError as error:
            return self._failed_bawu_roster_page(fid, status="blocked", error=error)
        except RetriableApiError as error:
            return self._failed_bawu_roster_page(fid, status="retryable_failure", error=error)
        return ResearchPage(
            raw=raw,
            dto=ResearchBawuRoster(fid=fid, status="available", missing_reasons={}),
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_bawu_info",
        )

    @with_ensure
    async def _get_bawu_info_by_fid(self, fid: int) -> BawuInfo:
        return await super().get_bawu_info(fid)

    @staticmethod
    def _missing_user_profile_page(
        user_id: int,
        *,
        reason: str,
    ) -> ResearchPage[None, ResearchUserProfileInfo]:
        return ResearchPage(
            raw=None,
            dto=ResearchUserProfileInfo(
                user_id=user_id,
                response_user_id=None,
                user_name=None,
                nickname=None,
                portrait=None,
                tieba_uid=None,
                global_growth_level=None,
                status="unavailable",
                missing_reasons=dict.fromkeys(
                    ("user_name", "nickname", "portrait", "tieba_uid", "global_growth_level"),
                    reason,
                ),
            ),
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_user_info",
        )

    @classmethod
    def _failed_user_profile_page(
        cls,
        user_id: int,
        *,
        status: Literal["blocked", "retryable_failure"],
        error: RetriableApiError | UnretriableApiError,
    ) -> ResearchPage[None, ResearchUserProfileInfo]:
        reason = f"{type(error).__name__}:{error.code}"
        page = cls._missing_user_profile_page(user_id, reason=reason)
        return ResearchPage(
            raw=None,
            dto=ResearchUserProfileInfo(
                user_id=user_id,
                response_user_id=None,
                user_name=None,
                nickname=None,
                portrait=None,
                tieba_uid=None,
                global_growth_level=None,
                status=status,
                missing_reasons=page.dto.missing_reasons,
                error_type=type(error).__name__,
                error_code=error.code,
            ),
            observed_at=page.observed_at,
            source_api=page.source_api,
        )

    @staticmethod
    def _missing_user_bawu_page(
        fid: int,
        user_id: int,
        *,
        reason: str,
    ) -> ResearchPage[None, ResearchUserBawuInfo]:
        return ResearchPage(
            raw=None,
            dto=ResearchUserBawuInfo(
                user_id=user_id,
                response_user_id=None,
                fid=fid,
                role_type=None,
                role_name=None,
                is_bawu=None,
                status="unavailable",
                missing_reasons={
                    "is_bawu": reason,
                    "role_type": reason,
                    "role_name": reason,
                },
            ),
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_bawu_info",
        )

    @classmethod
    def _failed_bawu_roster_page(
        cls,
        fid: int,
        *,
        status: Literal["blocked", "retryable_failure"],
        error: RetriableApiError | UnretriableApiError,
    ) -> ResearchPage[None, ResearchBawuRoster]:
        return ResearchPage(
            raw=None,
            dto=ResearchBawuRoster(
                fid=fid,
                status=status,
                missing_reasons={"role_type": f"{type(error).__name__}:{error.code}"},
                error_type=type(error).__name__,
                error_code=error.code,
            ),
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_bawu_info",
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
