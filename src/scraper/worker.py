"""工作器模块。

该模块实现了多种任务处理器和工作器，负责执行爬虫的具体工作。
使用策略模式设计，每种任务类型都有对应的处理器。

主要组件：
- TaskHandler: 任务处理器抽象基类
- ThreadsTaskHandler: 处理贴吧页面扫描任务
- FullPostsTaskHandler: 处理单个主题贴内回复扫描任务
- IncrementalScanPostsTaskHandler: 处理单个主题贴增量回复扫描任务
- FullScanCommentsTaskHandler: 处理单个回复下的楼中楼扫描任务
- IncrementalScanCommentsTaskHandler: 处理单个回复下的楼中楼增量扫描任务
- Worker: 工作器主类，统一调度各种处理器
"""

from __future__ import annotations

import asyncio
import dataclasses
import random
import time
from abc import ABC, abstractmethod
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Literal

from aiotieba.enums import PostSortType, ThreadSortType
from tiebameow.client.tieba_client import RetriableApiError, UnretriableApiError
from tiebameow.models.orm import Comment, Post, User
from tiebameow.utils.logger import logger

from ..core.error_codes import normalize_checkpoint_error_code
from ..core.metrics import (
    ACTIVE_WORKERS,
    API_REQUEST_DURATION,
    RATELIMIT_WAIT_DURATION,
    SCRAPED_ITEMS,
    TASK_DURATION,
    WORKER_ERRORS,
)
from ..core.models import (
    REPLY_BACKFILL_CONTRACT_VERSION,
    REPLY_BACKFILL_RN,
    REPLY_BACKFILL_SORT_TYPE,
    REPLY_BACKFILL_SOURCE_API,
    research_comments_task_key,
    research_posts_task_key,
    research_thread_list_task_key,
)
from .tasks import (
    DeepScanTask,
    FullScanCommentsTask,
    FullScanPostsTask,
    IncrementalScanCommentsTask,
    IncrementalScanPostsTask,
    Priority,
    ScanThreadsTask,
    Task,
    ThreadPageResult,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from tiebameow.models.dto import CommentDTO, CommentsDTO, PostDTO, PostsDTO, ThreadDTO, ThreadsDTO

    from ..core import Container, DataStore
    from ..core.lock import LockManager
    from .queue import UniquePriorityQueue
    from .router import QueueRouter


@dataclasses.dataclass(frozen=True, slots=True)
class FetchedPage:
    response: Any
    attempts: int


class PageIntegrityError(ValueError):
    """Raised when a returned API page does not match the requested scope."""


def _positive_int_id(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdecimal():
        parsed = int(value)
    else:
        return None
    return parsed if parsed > 0 else None


def _page_first_post_pid(page: Any, raw_thread: Any, expected_tid: int) -> int:
    raw_tid = _positive_int_id(getattr(raw_thread, "tid", None))
    raw_fid = _positive_int_id(getattr(raw_thread, "fid", None))
    if raw_tid != expected_tid or raw_fid is None:
        return 0

    matching_floor_one = [
        post
        for post in getattr(page, "objs", ())
        if type(getattr(post, "floor", None)) is int
        and post.floor == 1
        and _positive_int_id(getattr(post, "tid", None)) == raw_tid
        and _positive_int_id(getattr(post, "fid", None)) == raw_fid
    ]
    if len(matching_floor_one) != 1:
        return 0
    return _positive_int_id(getattr(matching_floor_one[0], "pid", None)) or 0


def _explicit_zero_evidence_for_post(
    response: Any,
    page: Any,
    post: Any,
    task: FullScanPostsTask,
    page_number: int,
) -> dict[str, object] | None:
    evidence_by_pid = getattr(response, "zero_comment_evidence", None)
    pid = getattr(post, "pid", None)
    tid = getattr(post, "tid", None)
    fid = getattr(post, "fid", None)
    author_id = getattr(post, "author_id", None)
    author = getattr(post, "author", None) or getattr(post, "user", None)
    author_user_id = getattr(author, "user_id", None)
    forum = getattr(page, "forum", None)
    page_data = getattr(page, "page", None)
    current_page = getattr(page_data, "current_page", None)
    expected_fid = _positive_int_id(task.fid) or _positive_int_id(getattr(forum, "fid", None))
    if (
        not isinstance(evidence_by_pid, dict)
        or type(pid) is not int
        or type(tid) is not int
        or type(fid) is not int
        or type(author_id) is not int
        or type(author_user_id) is not int
        or type(current_page) is not int
        or pid <= 0
        or tid != task.tid
        or fid <= 0
        or expected_fid is None
        or fid != expected_fid
        or author_id <= 0
        or author_user_id != author_id
        or current_page != page_number
        or getattr(response, "source_api", None) != "aiotieba.get_posts"
        or type(getattr(post, "reply_num", None)) is not int
        or post.reply_num != 0
        or bool(getattr(post, "comments", ()) or ())
    ):
        return None

    evidence = evidence_by_pid.get(pid)
    if not isinstance(evidence, dict):
        return None
    identities = {
        "tid": task.tid,
        "pid": pid,
        "fid": fid,
        "author_user_id": author_id,
        "source_page": page_number,
    }
    if any(type(evidence.get(key)) is not int or evidence[key] != value for key, value in identities.items()):
        return None
    if (
        type(evidence.get("reported_count")) is not int
        or evidence["reported_count"] != 0
        or evidence.get("count_field_present") is not True
        or evidence.get("source_api") != "aiotieba.get_posts"
        or evidence.get("evidence_kind") != "protobuf_wire_explicit_zero"
        or evidence.get("count_field_path") != "data.post_list.sub_post_number"
        or type(evidence.get("count_wire_field_number")) is not int
        or evidence["count_wire_field_number"] != 13
    ):
        return None
    return dict(evidence)


def _retry_delay(wait_seconds: float, attempt: int) -> float:
    """Return a bounded exponential delay with light jitter for ordinary retries."""
    if wait_seconds <= 0:
        return 0.0
    cap = 30.0
    delay = min(wait_seconds * (2 ** min(max(attempt - 1, 0), 32)), cap)
    return min(delay * random.uniform(0.9, 1.1), cap)


class TaskHandler(ABC):
    """任务处理器抽象基类。

    Attributes:
        worker_id: Worker ID，用于日志标识。
        container: 依赖注入容器。
        datastore: 数据存储层实例。
        router: 多通道队列路由器，用于向正确的通道投递子任务。
        lock_manager: 分布式锁管理器。
        log: 日志记录器。
    """

    def __init__(
        self,
        worker_id: int,
        container: Container,
        datastore: DataStore,
        router: QueueRouter,
        *,
        stop_event: asyncio.Event | None = None,
    ):
        self.worker_id = worker_id
        self.container = container
        self.datastore = datastore
        self.router = router
        self.stop_event = stop_event
        self.lock_manager: LockManager = container.lock_manager
        self.batch_size = 100
        self.log = logger.bind(name=f"Worker-{worker_id}")

    def _stop_requested(self) -> bool:
        return self.stop_event is not None and self.stop_event.is_set()

    async def _await_api_gate(self, awaitable: Any) -> Any:
        """Let shutdown interrupt waits that have not started an API request."""
        if self.stop_event is None:
            return await awaitable
        if self.stop_event.is_set():
            close = getattr(awaitable, "close", None)
            if close is not None:
                close()
            raise asyncio.CancelledError

        operation = asyncio.ensure_future(awaitable)
        stopped = asyncio.create_task(self.stop_event.wait())
        try:
            done, _pending = await asyncio.wait(
                {operation, stopped},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stopped in done:
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)
                raise asyncio.CancelledError
            return await operation
        finally:
            if not operation.done():
                operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            if not stopped.done():
                stopped.cancel()
            await asyncio.gather(stopped, return_exceptions=True)

    async def _enqueue_task_unless_stopping(self, task: Task) -> bool:
        """Avoid blocking shutdown while a bounded API queue is full."""
        if self.stop_event is None:
            await self.router.put(task)
            return True
        if self.stop_event.is_set():
            return False

        try:
            self.router.put_nowait(task)
            return True
        except asyncio.QueueFull:
            pass

        enqueue = asyncio.create_task(self.router.put(task))
        stopped = asyncio.create_task(self.stop_event.wait())
        try:
            done, _pending = await asyncio.wait(
                {enqueue, stopped},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if enqueue in done:
                await enqueue
                return True
            enqueue.cancel()
            await asyncio.gather(enqueue, return_exceptions=True)
            return False
        finally:
            if not enqueue.done():
                enqueue.cancel()
            await asyncio.gather(enqueue, return_exceptions=True)
            if not stopped.done():
                stopped.cancel()
            await asyncio.gather(stopped, return_exceptions=True)

    async def try_acquire_lock(self, key: str, ttl: int = 300) -> bool:
        """尝试获取锁。

        Args:
            key: 锁的唯一标识键。
            ttl: 锁的过期时间（秒），默认300秒。

        Returns:
            bool: 是否成功获取锁。
        """
        return await self.lock_manager.acquire(key, ttl)

    async def release_lock(self, key: str):
        """释放锁（幂等操作）。"""
        await self.lock_manager.release(key)

    @staticmethod
    def _dto(response: Any) -> Any:
        return getattr(response, "dto", response)

    def _get_client_and_limiter(self, api_type: Literal["threads", "posts", "comments"]) -> tuple[Any, Any]:
        """获取客户端和对应的限流器（支持多代理轮换）。

        Args:
            api_type: API 类型，可选 "threads", "posts", "comments"。

        Returns:
            tuple: (client, limiter)
        """
        # 多代理模式：轮询获取客户端和对应的限流器
        if self.container.tb_clients:
            idx = self.container.get_next_client_index()
            client = self.container.tb_clients[idx]
            limiters = self.container.get_limiters(idx)
            limiter_map = {"threads": limiters[0], "posts": limiters[1], "comments": limiters[2]}
            return client, limiter_map[api_type]

        # 单客户端模式
        limiter_map = {
            "threads": self.container.threads_limiter,
            "posts": self.container.posts_limiter,
            "comments": self.container.comments_limiter,
        }
        return self.container.tb_client, limiter_map.get(api_type, self.container.threads_limiter)

    async def get_threads(self, fname: str, **kwargs: Any) -> FetchedPage:
        """带限流的 get_threads_dto 调用（支持多代理轮换）。

        Args:
            fname: 贴吧名称。
            **kwargs: 传递给 get_threads_dto 的其他参数。

        Returns:
            ThreadsDTO: 主题贴列表数据。
        """
        expected_fid = kwargs.pop("_expected_fid", None)
        require_research_method = kwargs.pop("_require_research_method", False)
        requested_page = kwargs.get("pn", 1)
        fetched = await self._request_api(
            "threads",
            "get_threads_research",
            "get_threads_dto",
            (fname,),
            kwargs,
            require_research_method=require_research_method,
        )
        page = self._dto(fetched.response)
        objects = getattr(page, "objs", None)
        if self.research_enabled and not objects:
            raw_page = getattr(getattr(fetched.response, "raw", None), "page", None)
            await self._validate_research_empty_page(
                "thread list", page, requested_page, "page", raw_page=raw_page
            )
            forum_fid = _positive_int_id(getattr(getattr(page, "forum", None), "fid", None))
            if forum_fid is None or (expected_fid is not None and forum_fid != expected_fid):
                await self._record_research_api_response(
                    suspicious_empty=True,
                    api_type="threads",
                    page_number=requested_page,
                )
                raise PageIntegrityError("Thread list empty response is missing or mismatching forum identity.")
        self._validate_page_number("thread list", requested_page, page.page.current_page)
        if expected_fid is not None:
            wrong_fids = {thread.fid for thread in page.objs if thread.fid != expected_fid}
            if wrong_fids:
                raise PageIntegrityError(
                    f"Thread list returned forum scope {sorted(wrong_fids)} instead of {expected_fid}."
                )
        return fetched

    async def get_posts(self, tid: int, pn: int, **kwargs: Any) -> FetchedPage:
        """带限流的 get_posts_dto 调用（支持多代理轮换）。

        Args:
            tid: 主题贴 ID。
            pn: 页码。
            **kwargs: 传递给 get_posts_dto 的其他参数。

        Returns:
            PostsDTO: 回复列表数据。
        """
        fetched = await self._request_api(
            "posts",
            "get_posts_research",
            "get_posts_dto",
            (tid, pn),
            kwargs,
        )
        page = self._dto(fetched.response)
        objects = getattr(page, "objs", None)
        if self.research_enabled and not objects:
            confirmation_page = 1 if pn == 0xFFFF else pn
            raw_page = getattr(getattr(fetched.response, "raw", None), "page", None)
            await self._validate_research_empty_page(
                "post detail", page, confirmation_page, "page", raw_page=raw_page
            )
        if pn != 0xFFFF:
            self._validate_page_number("post detail", pn, page.page.current_page)
        wrong_tids = {post.tid for post in page.objs if post.tid != tid}
        if wrong_tids:
            raise PageIntegrityError(f"Post page returned tids {sorted(wrong_tids)} instead of {tid}.")
        raw_thread = getattr(getattr(fetched.response, "raw", None), "thread", None)
        raw_tid = getattr(raw_thread, "tid", None)
        if self.research_enabled and not objects and _positive_int_id(raw_tid) != tid:
            await self._record_research_api_response(
                suspicious_empty=True,
                api_type="posts",
                page_number=pn,
            )
            raise PageIntegrityError("Post page empty response is missing or mismatching thread identity.")
        if raw_thread is not None and raw_tid != tid:
            raise PageIntegrityError(f"Post page first-post detail returned tid {raw_tid} instead of {tid}.")
        return fetched

    async def get_comments(self, tid: int, pid: int, pn: int = 1) -> FetchedPage:
        """带限流的 get_comments_dto 调用（支持多代理轮换）。

        Args:
            tid: 主题贴 ID。
            pid: 回复 ID。
            pn: 页码，默认为 1。

        Returns:
            CommentsDTO: 楼中楼列表数据。
        """
        fetched = await self._request_api(
            "comments",
            "get_comments_research",
            "get_comments_dto",
            (tid, pid, pn),
            {},
        )
        page = self._dto(fetched.response)
        objects = getattr(page, "objs", None)
        if self.research_enabled and not objects:
            raw_page = getattr(getattr(fetched.response, "raw", None), "page", None)
            await self._validate_research_empty_page(
                "comment detail", page, pn, "page", raw_page=raw_page
            )
        self._validate_page_number("comment detail", pn, page.page.current_page)
        wrong_scope = {(comment.tid, comment.pid) for comment in page.objs if comment.tid != tid or comment.pid != pid}
        if wrong_scope:
            raise PageIntegrityError(
                f"Comment page returned parent scopes {sorted(wrong_scope)} instead of {(tid, pid)}."
            )
        return fetched

    async def _validate_research_empty_page(
        self,
        scope: str,
        page: Any,
        requested_page: int,
        identity: str,
        *,
        raw_page: Any = None,
    ) -> None:
        page_data = getattr(page, "page", None)
        current_page = _positive_int_id(getattr(page_data, "current_page", None)) if page_data is not None else None
        if current_page is not None and self._is_confirmed_empty(
            page_data, requested_page, raw_page=raw_page
        ):
            return
        api_type = {"thread list": "threads", "post detail": "posts", "comment detail": "comments"}[scope]
        await self._record_research_api_response(
            suspicious_empty=True,
            api_type=api_type,
            page_number=requested_page,
        )
        reason = (
            f"missing or has zero {identity} identity"
            if current_page is None
            else "not a confirmed terminal page"
        )
        raise PageIntegrityError(
            f"{scope.capitalize()} empty response is {reason} for page {requested_page}."
        )

    async def _record_research_api_response(
        self,
        *,
        suspicious_empty: bool,
        api_type: str,
        page_number: int,
    ) -> None:
        if not self.research_enabled:
            return
        record_response = getattr(self.container, "record_research_api_response", None)
        if record_response is None:
            return
        cooldown_required = await record_response(suspicious_empty=suspicious_empty)
        if not cooldown_required:
            return
        cooldown_seconds = float(self.container.config.cooldown_seconds_429)
        run_id = getattr(self.container, "research_run_id", None)
        self.log.warning(
            "Three consecutive suspicious empty research API responses; endpoint={} page={} cooldown_seconds={}",
            api_type,
            page_number,
            cooldown_seconds,
        )
        await self._await_api_gate(self.container.wait_for_rate_limit(cooldown_seconds, run_id=run_id))
        self.log.info("Research API cooldown ended; requests will continue at the configured rate.")

    async def _record_validated_research_page(self, api_type: str, page_number: int, page: Any) -> None:
        if not self.research_enabled:
            return
        await self._record_research_api_response(
            suspicious_empty=False,
            api_type=api_type,
            page_number=page_number,
        )
        page_data = getattr(page, "page", None)
        total_pages = getattr(page_data, "total_page", 0)
        item_count = len(getattr(page, "objs", ()) or ())
        self.log.info(
            "Validated research API page endpoint={} page={}/{} items={}",
            api_type,
            page_number,
            total_pages,
            item_count,
        )

    @staticmethod
    def _validate_page_number(scope: str, requested: int, returned: int) -> None:
        if requested != returned:
            raise PageIntegrityError(f"{scope} returned page {returned} when page {requested} was requested.")

    async def _request_api(
        self,
        api_type: Literal["threads", "posts", "comments"],
        research_method: str,
        dto_method: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        require_research_method: bool = False,
    ) -> FetchedPage:
        client, limiter = self._get_client_and_limiter(api_type)
        config = self.container.config
        research_enabled = config.research_enabled
        if require_research_method and not hasattr(client, research_method):
            raise PageIntegrityError(f"API client does not provide the required {research_method} method.")
        method_name = research_method if research_enabled and hasattr(client, research_method) else dto_method
        method = getattr(client, method_name)
        attempt_limit = getattr(self.container.config, "research_max_api_retries", 3) if research_enabled else 1

        async def request_once(*, bypass_pause: bool = False):
            if research_enabled and not bypass_pause:
                await self._await_api_gate(self.container.wait_for_research_ready())
            if self._stop_requested():
                raise asyncio.CancelledError
            with RATELIMIT_WAIT_DURATION.labels(limiter_type=api_type).time():
                await self._await_api_gate(limiter.acquire())
            if self._stop_requested():
                raise asyncio.CancelledError

            start = time.perf_counter()
            status = "success"
            try:
                api_lane = getattr(getattr(self.container, "semaphore", None), "api_lane", None)
                if api_lane is None:
                    return await method(*args, **kwargs)
                async with api_lane(api_type):
                    return await method(*args, **kwargs)
            except UnretriableApiError as error:
                status = "error_deleted" if error.code == 4 else f"error_{error.code}"
                raise
            except RetriableApiError as error:
                status = f"error_{error.code}"
                raise
            except Exception:
                status = "unexpected_error"
                raise
            finally:
                API_REQUEST_DURATION.labels(method=f"get_{api_type}", status=status).observe(
                    time.perf_counter() - start
                )

        for attempt in range(1, attempt_limit + 1):
            try:
                response = await request_once()
                run_id = getattr(self.container, "research_run_id", None)
                if research_enabled and run_id:
                    await self.datastore.resume_research_run_if_status(run_id, "paused_rate_limit")
                return FetchedPage(response=response, attempts=attempt)
            except RetriableApiError as error:
                if self._stop_requested():
                    raise asyncio.CancelledError from error
                if research_enabled and error.code == 4011:
                    run_id = getattr(self.container, "research_run_id", None)
                    if run_id:
                        await self.datastore.set_research_run_status_if(
                            run_id,
                            {"running", "paused_rate_limit"},
                            "paused_auth",
                            f"{api_type}_api_error_code_4011",
                        )
                    try:
                        response = await self._await_api_gate(
                            self.container.wait_for_auth_refresh(
                                lambda: request_once(bypass_pause=True),
                                run_id=run_id,
                            )
                        )
                    except RetriableApiError as refresh_error:
                        if refresh_error.code in {429, 220034}:
                            if run_id:
                                await self.datastore.set_research_run_status_if(
                                    run_id,
                                    {"paused_auth"},
                                    "paused_rate_limit",
                                    f"{api_type}_api_error_code_{refresh_error.code}_after_auth_refresh",
                                )
                            cooldown_seconds = self.container.config.cooldown_seconds_429
                            self.log.warning(
                                "Research API {} returned frequency limit code {}; cooling down for {} seconds.",
                                api_type,
                                refresh_error.code,
                                cooldown_seconds,
                            )
                            await self._await_api_gate(
                                self.container.wait_for_rate_limit(cooldown_seconds, run_id=run_id)
                            )
                            if run_id:
                                await self.datastore.resume_research_run_if_status(run_id, "paused_rate_limit")
                            if attempt >= attempt_limit:
                                raise
                            self.log.info("Research API cooldown ended; retrying at the configured request rate.")
                            continue
                        raise
                    if run_id:
                        await self.datastore.resume_research_run_if_status(run_id, "paused_auth")
                    return FetchedPage(response=response, attempts=attempt + 1)
                if research_enabled and error.code in {429, 220034}:
                    run_id = getattr(self.container, "research_run_id", None)
                    if run_id:
                        await self.datastore.set_research_run_status_if(
                            run_id,
                            {"running", "paused_auth", "paused_rate_limit"},
                            "paused_rate_limit",
                            f"{api_type}_api_error_code_{error.code}",
                        )
                    cooldown_seconds = self.container.config.cooldown_seconds_429
                    self.log.warning(
                        "Research API {} returned frequency limit code {}; cooling down for {} seconds.",
                        api_type,
                        error.code,
                        cooldown_seconds,
                    )
                    await self._await_api_gate(
                        self.container.wait_for_rate_limit(
                            cooldown_seconds,
                            run_id=run_id,
                        )
                    )
                    if run_id:
                        await self.datastore.resume_research_run_if_status(run_id, "paused_rate_limit")
                    if attempt >= attempt_limit:
                        raise
                    self.log.info("Research API cooldown ended; retrying at the configured request rate.")
                    continue

                if attempt >= attempt_limit:
                    raise
                wait_seconds = getattr(self.container.config, "research_retry_wait_seconds", 0.5)
                await self._await_api_gate(asyncio.sleep(_retry_delay(wait_seconds, attempt)))

        raise RuntimeError("Research API retry loop exited without a response.")

    async def _prepare_research_page(self, response: Any) -> None:
        if not getattr(response, "source_api", None):
            return
        sessionmaker_ = getattr(self.container, "async_sessionmaker", None)
        if sessionmaker_ is None:
            raise RuntimeError("Research response cannot be persisted without a database session factory.")
        if (
            self.research_enabled
            and self.container.config.fetch_user_forum_info
            and response.source_api in {"aiotieba.get_posts", "aiotieba.get_comments"}
            and not self._stop_requested()
        ):
            await self._persist_user_forum_snapshots(response, sessionmaker_)

    async def _persist_research_page(self, response: Any, *, session: Any = None) -> None:
        if not getattr(response, "source_api", None):
            return
        sessionmaker_ = getattr(self.container, "async_sessionmaker", None)
        if sessionmaker_ is None:
            raise RuntimeError("Research response cannot be persisted without a database session factory.")
        from ..research.persistence import persist_research_page

        if session is None:
            await self._prepare_research_page(response)
        await persist_research_page(
            sessionmaker_,
            response,
            session=session,
            run_id=getattr(self.container, "research_run_id", None),
        )

    async def _persist_user_forum_snapshots(self, response: Any, sessionmaker_: Any) -> None:
        raw = getattr(response, "raw", None)
        if raw is None:
            return

        default_fid = 0
        if response.source_api == "aiotieba.get_posts":
            raw_thread = getattr(raw, "thread", None)
            default_fid = int(getattr(raw_thread, "fid", 0) or 0)
            response_dto = self._dto(response)
            if default_fid <= 0:
                default_fid = int(getattr(getattr(response_dto, "forum", None), "fid", 0) or 0)
            authors = ([raw_thread] if raw_thread is not None else []) + list(getattr(raw, "objs", ()) or ())
        else:
            response_dto = self._dto(response)
            default_fid = int(getattr(getattr(response_dto, "forum", None), "fid", 0) or 0)
            authors = list(getattr(raw, "objs", ()) or ())

        client = self.container.tb_client
        fetch_user_forum_info = getattr(client, "get_user_forum_info_research", None)
        if fetch_user_forum_info is None:
            raise RuntimeError("Forum user snapshots require ResearchClient when research enrichment is enabled.")

        targets: dict[tuple[int, int], str | None] = {}
        for author in authors:
            user = getattr(author, "user", None)
            try:
                user_id = int(getattr(author, "author_id", 0) or getattr(user, "user_id", 0) or 0)
                fid = int(getattr(author, "fid", 0) or default_fid)
            except (TypeError, ValueError):
                continue
            if user_id > 0 and fid > 0:
                targets.setdefault((fid, user_id), getattr(user, "portrait", None) or None)

        for (fid, user_id), portrait in targets.items():
            if self._stop_requested():
                break
            try:
                await self._fetch_user_forum_snapshot(
                    fetch_user_forum_info,
                    sessionmaker_,
                    fid=fid,
                    user_id=user_id,
                    portrait=portrait,
                )
            except asyncio.CancelledError:
                if not self._stop_requested():
                    raise
                break

    async def _fetch_user_forum_snapshot(
        self,
        fetch_user_forum_info: Any,
        sessionmaker_: Any,
        *,
        fid: int,
        user_id: int,
        portrait: str | None,
    ) -> None:
        from ..research.persistence import persist_research_page

        run_id = getattr(self.container, "research_run_id", None)
        attempt_limit = self.container.config.research_max_api_retries
        latest_page: Any = None

        async def fetch_or_raise_expected_errors() -> Any:
            nonlocal latest_page
            page = await fetch_user_forum_info(fid, user_id, portrait)
            latest_page = page
            dto = page.dto
            if dto.status == "retryable_failure" and dto.error_code in {4011, 429, 220034}:
                raise RetriableApiError(dto.error_code, "Research user-forum request was rejected.")
            return page

        for attempt in range(1, attempt_limit + 1):
            latest_page = await fetch_user_forum_info(fid, user_id, portrait)
            dto = latest_page.dto
            if dto.status != "retryable_failure":
                break

            if self._stop_requested():
                await persist_research_page(sessionmaker_, latest_page)
                return

            if dto.error_code == 4011:
                if run_id:
                    await self.datastore.set_research_run_status_if(
                        run_id,
                        {"running", "paused_rate_limit"},
                        "paused_auth",
                        "user_forum_api_error_code_4011",
                    )
                self.container.research_auth_ready.clear()
                try:
                    latest_page = await self._await_api_gate(
                        self.container.wait_for_auth_refresh(
                            fetch_or_raise_expected_errors,
                            run_id=run_id,
                        )
                    )
                except RetriableApiError as error:
                    if latest_page is not None:
                        await persist_research_page(sessionmaker_, latest_page)
                    if error.code in {429, 220034}:
                        await self._handle_user_forum_rate_limit(
                            run_id,
                            attempt,
                            attempt_limit,
                            error_code=error.code,
                        )
                        continue
                    if run_id:
                        await self.datastore.resume_research_run_if_status(run_id, "paused_auth")
                    raise
                if run_id:
                    await self.datastore.resume_research_run_if_status(run_id, "paused_auth")
                break

            if dto.error_code in {429, 220034}:
                if attempt >= attempt_limit:
                    await persist_research_page(sessionmaker_, latest_page)
                await self._handle_user_forum_rate_limit(
                    run_id,
                    attempt,
                    attempt_limit,
                    error_code=dto.error_code,
                )
                continue

            if attempt < attempt_limit:
                await self._await_api_gate(asyncio.sleep(self.container.config.research_retry_wait_seconds * attempt))
                continue

            await persist_research_page(sessionmaker_, latest_page)
            error_code = dto.error_code or -65536
            raise RetriableApiError(error_code, "Research user-forum request failed after bounded retries.")

        assert latest_page is not None
        await persist_research_page(sessionmaker_, latest_page)

    async def _handle_user_forum_rate_limit(
        self,
        run_id: str | None,
        attempt: int,
        attempt_limit: int,
        *,
        error_code: int,
    ) -> None:
        if run_id:
            await self.datastore.set_research_run_status_if(
                run_id,
                {"running", "paused_auth", "paused_rate_limit"},
                "paused_rate_limit",
                f"user_forum_api_error_code_{error_code}",
            )
        cooldown_seconds = self.container.config.cooldown_seconds_429
        self.log.warning(
            "Research API user-forum lookup returned frequency limit code {}; cooling down for {} seconds.",
            error_code,
            cooldown_seconds,
        )
        await self._await_api_gate(
            self.container.wait_for_rate_limit(
                cooldown_seconds,
                run_id=run_id,
            )
        )
        if run_id:
            await self.datastore.resume_research_run_if_status(run_id, "paused_rate_limit")
        if attempt >= attempt_limit:
            raise RetriableApiError(error_code, "Research user-forum request exhausted bounded retries.")
        self.log.info("Research API cooldown ended; retrying the user-forum lookup at the configured request rate.")

    @property
    def research_enabled(self) -> bool:
        return self.container.config.research_enabled

    async def _record_research_checkpoint(self, **values: Any) -> bool:
        run_id = values.pop("run_id", None) or getattr(self.container, "research_run_id", None)
        if run_id and hasattr(self.datastore, "record_research_checkpoint"):
            return await self.datastore.record_research_checkpoint(run_id=run_id, **values)
        return True

    async def _enqueue_missing_web_targets(
        self,
        run_id: str | None,
        *,
        thread_ids: Sequence[int] = (),
        post_ids: Sequence[int] = (),
        comment_ids: Sequence[int] = (),
    ) -> None:
        if not self.research_enabled or not run_id or not hasattr(self.datastore, "async_sessionmaker"):
            return
        from ..research.web_fallback.live import enqueue_missing_web_targets

        await enqueue_missing_web_targets(
            self.datastore.async_sessionmaker,
            run_id,
            thread_ids=thread_ids,
            post_ids=post_ids,
            comment_ids=comment_ids,
        )

    async def _enqueue_missing_web_targets_after_commit(
        self,
        run_id: str | None,
        *,
        task_type: str,
        tid: int,
        page_number: int,
        parent_pid: int | None = None,
        thread_ids: Sequence[int] = (),
        post_ids: Sequence[int] = (),
        comment_ids: Sequence[int] = (),
    ) -> None:
        try:
            await self._enqueue_missing_web_targets(
                run_id,
                thread_ids=thread_ids,
                post_ids=post_ids,
                comment_ids=comment_ids,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.log.warning(
                "WEB enqueue failed after committed API page; canonical missing-target discovery can recover it "
                "(run_id={} task_type={} tid={} parent_pid={} page={} error_type={})",
                run_id,
                task_type,
                tid,
                parent_pid,
                page_number,
                type(error).__name__,
            )

    @staticmethod
    def _continuation_from_page(
        page_ids: list[int], previous: dict[str, object] | None, *, has_more: bool
    ) -> dict[str, object]:
        previous_pages = list((previous or {}).get("recent_page_fingerprints", []))
        previous_pages.append(page_ids)
        previous_pages = previous_pages[-3:]
        return {"recent_page_fingerprints": previous_pages} if has_more else {}

    @staticmethod
    def _validate_terminal_metadata(scope: str, page_number: int, has_more: bool, total_pages: int) -> None:
        if total_pages < 0:
            raise PageIntegrityError(f"{scope} returned negative total_pages on page {page_number}.")
        if has_more and total_pages > 0 and page_number >= total_pages:
            raise PageIntegrityError(f"{scope} reports more pages beyond total_pages on page {page_number}.")
        if not has_more and total_pages > 0 and page_number < total_pages:
            raise PageIntegrityError(f"{scope} terminated before total_pages on page {page_number}.")

    @staticmethod
    def _is_confirmed_empty(page: Any, page_number: int, *, raw_page: Any = None) -> bool:
        if raw_page is None:
            if page.total_page != 1:
                return False
            total_count = page.total_count
        else:
            if not hasattr(raw_page, "total_count"):
                return False
            total_count = raw_page.total_count
        return (
            page_number == 1
            and _positive_int_id(getattr(page, "current_page", None)) == 1
            and not page.has_more
            and total_count == 0
            and page.total_page in (0, 1)
        )

    @staticmethod
    def _validate_repeated_page(ids: list[int], cursor: dict[str, object], *, has_more: bool, scope: str) -> None:
        recent = cursor.get("recent_page_fingerprints", [])
        if has_more and ids and len(recent) >= 3 and all(fingerprint == ids for fingerprint in recent[-3:]):
            raise PageIntegrityError(f"{scope} repeated a recent page fingerprint.")

    async def _record_detail_failure(
        self,
        *,
        run_id: str | None,
        task_key: str,
        page_number: int,
        task_type: str,
        scope_type: str,
        tid: int,
        error: Exception,
        continuation_json: dict[str, object],
        parent_pid: int | None = None,
        parent_kind: str | None = None,
        generation: int | None = None,
    ) -> None:
        if run_id:
            error_code = normalize_checkpoint_error_code(getattr(error, "code", None))
            self.log.error(
                "Original operation failed (type={} numeric_error_code={} "
                "run_id={} task_type={} scope_type={} page={})",
                type(error).__name__,
                error_code,
                run_id,
                task_type,
                scope_type,
                page_number,
            )
            status = {
                4011: "paused_auth",
                429: "failed",
                220034: "failed",
            }.get(error_code, "blocked" if error_code == 4 else "failed")
            scope_status = status if status in {"paused_auth", "paused_rate_limit", "blocked"} else "incomplete"
            try:
                current_generation = await self.datastore.record_research_checkpoint(
                    run_id=run_id,
                    task_key=task_key,
                    page_number=page_number,
                    task_type=task_type,
                    scope_type=scope_type,
                    tid=tid,
                    parent_pid=parent_pid,
                    parent_kind=parent_kind,
                    status=status,
                    scope_status=scope_status,
                    next_page=page_number,
                    error_type=type(error).__name__,
                    error_code=error_code,
                    reason="content_unavailable" if error_code == 4 else type(error).__name__,
                    continuation_json=continuation_json,
                    generation=generation,
                )
            except asyncio.CancelledError:
                raise
            except Exception as checkpoint_error:
                self.log.error(
                    "Failure checkpoint persistence failed (original_type={} checkpoint_type={})",
                    type(error).__name__,
                    type(checkpoint_error).__name__,
                )
                raise
            if generation is not None and not current_generation:
                return
            if error_code not in {4011, 429, 220034}:
                await self.datastore.set_research_run_status_if(
                    run_id,
                    {"running", "paused_rate_limit"},
                    "incomplete",
                    type(error).__name__,
                )

    async def ensure_users(self, items: Sequence[ThreadDTO | PostDTO | CommentDTO]):
        """确保用户信息已保存到数据库。

        Args:
            items: 包含用户信息的主题贴、回复或评论列表。
        """
        if not items:
            return

        user_infos = [item.author for item in items if item.author and item.author.user_id]
        if not user_infos:
            return

        all_user_ids = [u.user_id for u in user_infos]
        new_user_ids = await self.datastore.filter_new_ids("user", all_user_ids)

        if not new_user_ids:
            return

        new_users_map = {u.user_id: u for u in user_infos if u.user_id in new_user_ids}
        user_info_models = [User.from_dto(u) for u in new_users_map.values()]

        await self.datastore.save_items(user_info_models)
        self.log.debug("Saved {} new users to DB.", len(user_info_models))

    @abstractmethod
    async def handle(self, task_content):
        """处理任务的抽象方法"""
        raise NotImplementedError


class ThreadsTaskHandler(TaskHandler):
    """处理扫描指定贴吧特定页码的任务处理器。

    负责处理ScanThreadsTask类型的任务，扫描指定贴吧的特定页码，
    获取主题贴列表并分别处理新旧主题贴。
    """

    @staticmethod
    def _validate_reply_history_page(response: Any, *, pn: int, fid: int) -> None:
        from ..research.client import ResearchPage

        if not isinstance(response, ResearchPage) or response.source_api != REPLY_BACKFILL_SOURCE_API:
            raise PageIntegrityError("REPLY history requires a ResearchPage from aiotieba.get_threads.")
        raw, dto = response.raw, response.dto
        raw_page, dto_page = getattr(raw, "page", None), getattr(dto, "page", None)
        raw_forum, dto_forum = getattr(raw, "forum", None), getattr(dto, "forum", None)
        raw_threads, dto_threads = getattr(raw, "objs", None), getattr(dto, "objs", None)
        if any(value is None for value in (raw_page, dto_page, raw_forum, dto_forum, raw_threads, dto_threads)):
            raise PageIntegrityError("REPLY history response is missing raw or DTO identity fields.")
        if (
            _positive_int_id(getattr(raw_page, "current_page", None)) != pn
            or _positive_int_id(getattr(dto_page, "current_page", None)) != pn
        ):
            raise PageIntegrityError(f"REPLY history raw/DTO page identity mismatch for requested page {pn}.")
        if (
            _positive_int_id(getattr(raw_forum, "fid", None)) != fid
            or _positive_int_id(getattr(dto_forum, "fid", None)) != fid
        ):
            raise PageIntegrityError(f"REPLY history raw/DTO forum identity mismatch for fid {fid}.")
        raw_tids: set[int] = set()
        dto_tids: set[int] = set()
        for threads, target in ((raw_threads, raw_tids), (dto_threads, dto_tids)):
            for thread in threads:
                tid = _positive_int_id(getattr(thread, "tid", None))
                item_fid = _positive_int_id(getattr(thread, "fid", None))
                if tid is None or item_fid != fid:
                    raise PageIntegrityError(f"REPLY history item identity mismatch for fid {fid} page {pn}.")
                target.add(tid)
        if raw_tids != dto_tids:
            raise PageIntegrityError(f"REPLY history raw/DTO TID sets differ for fid {fid} page {pn}.")
        if getattr(raw_page, "has_more", None) != getattr(dto_page, "has_more", None):
            raise PageIntegrityError(f"REPLY history raw/DTO has_more values differ for fid {fid} page {pn}.")

    async def handle(self, task_content: ScanThreadsTask) -> ThreadPageResult:
        """Fetch one page; the backfill scheduler owns page continuation."""
        if self._stop_requested():
            return ThreadPageResult(
                has_more=False,
                current_page=task_content.pn,
                total_pages=0,
                total_count=0,
                tids=(),
                non_top_create_times=(),
                non_top_tids=(),
                continuation_json=task_content.continuation_json or {},
            )
        fname = task_content.fname
        pn = task_content.pn
        is_good = task_content.is_good
        sort_mode = task_content.sort_mode or ("create" if task_content.backfill else "reply")
        reply_history = task_content.backfill and sort_mode == "reply"
        if reply_history:
            if not self.research_enabled:
                raise PageIntegrityError("REPLY history requires research.enabled=true.")
            if task_content.rn != REPLY_BACKFILL_RN or is_good:
                raise PageIntegrityError("REPLY history requires rn=100 and the normal forum list.")
            expected_contract = {
                "list_contract_version": REPLY_BACKFILL_CONTRACT_VERSION,
                "list_sort_mode": "reply",
                "source_api": REPLY_BACKFILL_SOURCE_API,
                "rn": REPLY_BACKFILL_RN,
                "sort_type": REPLY_BACKFILL_SORT_TYPE,
                "is_good": False,
            }
            for key, value in expected_contract.items():
                if key in (task_content.continuation_json or {}) and task_content.continuation_json[key] != value:
                    raise PageIntegrityError(f"REPLY history checkpoint contract mismatch for {key}.")
        else:
            expected_contract = {}
        sort = ThreadSortType.CREATE if sort_mode == "create" else ThreadSortType.REPLY
        fetched_page = await self.get_threads(
            fname,
            pn=pn,
            rn=task_content.rn,
            sort=sort,
            is_good=is_good,
            _expected_fid=task_content.fid,
            _require_research_method=reply_history,
        )
        if reply_history:
            self._validate_reply_history_page(fetched_page.response, pn=pn, fid=task_content.fid)
        threads_data = self._dto(fetched_page.response)
        if reply_history:
            if not threads_data.objs and threads_data.page.has_more:
                raise PageIntegrityError(f"REPLY history returned an empty page with has_more for {fname} pn={pn}.")
        else:
            if threads_data.page.total_page < 0 or threads_data.page.total_count < 0:
                raise PageIntegrityError(f"Thread list returned negative page metadata for {fname} pn={pn}.")
            if not threads_data.objs and not self._is_confirmed_empty(
                threads_data.page,
                pn,
                raw_page=getattr(getattr(fetched_page.response, "raw", None), "page", None),
            ):
                raise PageIntegrityError(f"Thread list returned an unconfirmed empty page for {fname} pn={pn}.")
            if threads_data.page.has_more and threads_data.page.total_page > 0 and pn >= threads_data.page.total_page:
                raise PageIntegrityError(f"Thread list has_more exceeds total_page for {fname} pn={pn}.")
            if (
                not threads_data.page.has_more
                and threads_data.page.total_page > 0
                and pn < threads_data.page.total_page
            ):
                raise PageIntegrityError(f"Thread list ended before total_page for {fname} pn={pn}.")

        identity_constraints: dict[int, dict[str, int | None]] = {}
        unique_threads = self._dedupe_thread_list_items(
            threads_data.objs,
            fname=fname,
            pn=pn,
            identity_constraints=identity_constraints,
        )
        threads_data.objs[:] = unique_threads
        raw_page = getattr(fetched_page.response, "raw", None)
        raw_threads = getattr(raw_page, "objs", None)
        if raw_threads is not None and raw_threads is not threads_data.objs:
            raw_threads[:] = self._dedupe_thread_list_items(
                raw_threads,
                fname=fname,
                pn=pn,
                identity_constraints=identity_constraints,
            )

        all_tids = [thread.tid for thread in threads_data.objs]
        non_top_threads = [thread for thread in threads_data.objs if not thread.is_top]
        non_top_create_times = tuple(thread.create_time for thread in non_top_threads)
        non_top_tids = tuple(thread.tid for thread in non_top_threads)
        cursor = task_content.continuation_json or {}
        if task_content.backfill and sort_mode == "create" and any(
            earlier < later
            for earlier, later in zip(non_top_create_times, non_top_create_times[1:], strict=False)
        ):
            self.log.warning(
                "CREATE page time order moved forward for {} is_good={} pn={}; continuing by page number.",
                fname,
                is_good,
                pn,
            )
        if task_content.backfill:
            last_create_time = cursor.get("last_create_time")
            if sort_mode == "create" and non_top_create_times and last_create_time:
                from datetime import datetime

                previous_oldest = datetime.fromisoformat(str(last_create_time))
                if non_top_create_times[0] > previous_oldest:
                    self.log.warning(
                        "CREATE page time order moved forward across pages for {} is_good={} pn={}; "
                        "continuing by page number.",
                        fname,
                        is_good,
                        pn,
                    )
            flow_seen = {int(tid) for tid in cursor.get("_flow_seen_tids", [])}
            duplicate_tids = flow_seen.intersection(non_top_tids)
            fingerprints = cursor.get("recent_page_fingerprints", [])
            if threads_data.page.has_more and all_tids and all_tids in fingerprints:
                self.log.warning(
                    "{} page fingerprint repeated for {} is_good={} pn={}; continuing by page number.",
                    sort_mode.upper(),
                    fname,
                    is_good,
                    pn,
                )
            replay = cursor.get("_resume_replay_page") == pn
            page_seen = {int(tid) for tid in cursor.get("_resume_page_seen_tids", [])} if replay else set()
            page_seen.update(non_top_tids)
            duplicate_ratio = (
                len(duplicate_tids) / len(non_top_tids)
                if non_top_tids and (not replay or reply_history)
                else None
            )
            duplicate_streak = int(cursor.get("_flow_duplicate_streak", 0))
            if not replay:
                duplicate_streak = duplicate_streak + 1 if duplicate_ratio is not None and duplicate_ratio >= 0.9 else 0
            duplicate_pause_reason = (
                "three_consecutive_pages_at_least_90_percent_seen"
                if not replay and duplicate_streak >= 3
                else None
            )
            untrusted_terminal_reason = None
            if reply_history and not threads_data.page.has_more:
                if threads_data.objs and not non_top_tids:
                    untrusted_terminal_reason = "terminal_page_without_non_top_threads"
                elif duplicate_ratio is not None and duplicate_ratio >= 0.9:
                    untrusted_terminal_reason = "terminal_page_at_least_90_percent_seen"
        else:
            duplicate_tids = set()
            page_seen = set(non_top_tids)
            duplicate_ratio = None
            duplicate_streak = 0
            duplicate_pause_reason = None
            untrusted_terminal_reason = None

        continuation_json = self._thread_list_continuation(
            cursor,
            non_top_create_times,
            non_top_tids,
            all_tids,
            duplicate_tids,
            page_seen,
            duplicate_ratio=duplicate_ratio,
            duplicate_streak=duplicate_streak,
            duplicate_pause_reason=duplicate_pause_reason,
            untrusted_terminal_reason=untrusted_terminal_reason,
            has_more=threads_data.page.has_more,
            include_create_time=sort_mode == "create",
        )
        continuation_json.update(expected_contract)
        await self._record_validated_research_page("threads", pn, threads_data)
        await self._persist_research_page(fetched_page.response)

        if not threads_data.objs:
            self.log.debug("No threads found for {}吧, is_good={}, pn={}", fname, is_good, pn)
        elif task_content.force:
            new_tids = set(all_tids)
            old_tids = set()
            self.log.info("{}吧, pn={}: Force scan enabled. Processing {} threads as new.", fname, pn, len(all_tids))
        else:
            new_tids = await self.datastore.filter_new_ids("thread", all_tids)
            old_tids = set(all_tids) - new_tids
            self.log.debug("{}吧, pn={}: Found {} threads, {} are new.", fname, pn, len(all_tids), len(new_tids))

        if threads_data.objs:
            await self._process_new_threads(threads_data, new_tids, task_content.backfill, task_content.force)
            await self._process_old_threads(threads_data, old_tids, task_content.backfill)

        run_id = getattr(self.container, "research_run_id", None)
        await self._enqueue_missing_web_targets(run_id, thread_ids=tuple(all_tids))

        if task_content.backfill and run_id and hasattr(self.datastore, "record_research_checkpoint"):
            task_key = research_thread_list_task_key(task_content.fid, is_good, sort_mode, task_content.rn)
            await self.datastore.record_research_checkpoint(
                run_id=run_id,
                task_key=task_key,
                page_number=pn,
                task_type="thread_list",
                scope_type="thread_list",
                sort_mode=sort_mode,
                fid=task_content.fid,
                fname=fname,
                is_good=is_good,
                status="completed",
                scope_status=(
                    "incomplete"
                    if duplicate_pause_reason or untrusted_terminal_reason
                    else "running"
                    if threads_data.page.has_more
                    else "completed"
                ),
                attempts=fetched_page.attempts,
                has_more=threads_data.page.has_more,
                next_page=pn + 1 if threads_data.page.has_more else None,
                total_pages=threads_data.page.total_page,
                reported_count=threads_data.page.total_count,
                        reported_count_missing_reason=(
                            "zero_presence_unverified_in_parsed_object"
                            if threads_data.page.total_count == 0
                            else None
                        ),
                page_item_count=len(all_tids),
                reason=duplicate_pause_reason or untrusted_terminal_reason,
                continuation_json=continuation_json,
            )

        return ThreadPageResult(
            has_more=threads_data.page.has_more,
            current_page=threads_data.page.current_page,
            total_pages=threads_data.page.total_page,
            total_count=threads_data.page.total_count,
            tids=tuple(all_tids),
            non_top_create_times=non_top_create_times,
            non_top_tids=non_top_tids,
            continuation_json=continuation_json,
        )

    @staticmethod
    def _thread_list_continuation(
        previous: dict[str, object],
        page_times: tuple[datetime, ...],
        non_top_tids: tuple[int, ...],
        page_tids: list[int],
        duplicate_tids: set[int],
        page_seen_tids: set[int],
        *,
        duplicate_ratio: float | None,
        duplicate_streak: int,
        duplicate_pause_reason: str | None,
        untrusted_terminal_reason: str | None,
        has_more: bool,
        include_create_time: bool,
    ) -> dict[str, object]:
        previous_tids = list(
            dict.fromkeys([int(tid) for tid in previous.get("recent_non_top_tids", [])] + list(non_top_tids))
        )
        fingerprints = list(previous.get("recent_page_fingerprints", []))
        fingerprints.append(page_tids)
        result: dict[str, object] = {
            "recent_non_top_tids": previous_tids[-90:],
            "recent_page_fingerprints": fingerprints[-3:],
            "page_fingerprint": page_tids,
            "page_duplicate_tids": sorted(duplicate_tids),
            "page_non_top_tids": sorted(page_seen_tids),
            "flow_duplicate_streak": duplicate_streak,
        }
        if duplicate_ratio is not None:
            result["page_duplicate_ratio"] = duplicate_ratio
        if duplicate_pause_reason:
            result["high_duplicate_pause_reason"] = duplicate_pause_reason
        if untrusted_terminal_reason:
            result["untrusted_terminal_reason"] = untrusted_terminal_reason
        if include_create_time and page_times:
            result["last_create_time"] = page_times[-1].isoformat()
        elif include_create_time and previous.get("last_create_time"):
            result["last_create_time"] = previous["last_create_time"]
        return result

    @staticmethod
    def _dedupe_thread_list_items(
        items: Sequence[Any],
        *,
        fname: str,
        pn: int,
        identity_constraints: dict[int, dict[str, int | None]] | None = None,
    ) -> list[Any]:
        unique: dict[int, Any] = {}
        known_by_tid = identity_constraints if identity_constraints is not None else {}
        for item in items:
            tid = _positive_int_id(getattr(item, "tid", None))
            if tid is None:
                raise PageIntegrityError(f"Thread list contains an invalid TID for {fname} pn={pn}.")
            known_identity = known_by_tid.setdefault(tid, {})
            ThreadsTaskHandler._accumulate_thread_list_identity(known_identity, item, fname=fname, pn=pn)
            previous = unique.get(tid)
            if previous is None:
                unique[tid] = item
                continue

            if bool(getattr(item, "is_top", False)) and not bool(getattr(previous, "is_top", False)):
                unique[tid] = item
        return list(unique.values())

    @staticmethod
    def _thread_list_identity(item: Any) -> dict[str, int | None]:
        user = getattr(item, "user", None) or getattr(item, "author", None)
        record_author_id = _positive_int_id(getattr(item, "author_id", None))
        user_id = _positive_int_id(getattr(user, "user_id", None))
        if record_author_id is not None and user_id is not None and record_author_id != user_id:
            raise PageIntegrityError("Thread list record author ID conflicts with its parsed user identity.")
        return {
            "fid": _positive_int_id(getattr(item, "fid", None)),
            "first_pid": _positive_int_id(getattr(item, "pid", None) or getattr(item, "first_pid", None)),
            "author_user_id": record_author_id or user_id,
        }

    @staticmethod
    def _accumulate_thread_list_identity(
        known_identity: dict[str, int | None], incoming: Any, *, fname: str, pn: int
    ) -> None:
        incoming_identity = ThreadsTaskHandler._thread_list_identity(incoming)
        if any(
            known_identity.get(field) is not None
            and incoming_identity[field] is not None
            and known_identity[field] != incoming_identity[field]
            for field in incoming_identity
        ):
            raise PageIntegrityError(f"Duplicate thread TID has conflicting core identity for {fname} pn={pn}.")
        for field, value in incoming_identity.items():
            if known_identity.get(field) is None and value is not None:
                known_identity[field] = value

    async def _process_new_threads(
        self,
        threads_data: ThreadsDTO,
        new_tids: set[int],
        backfill: bool,
        force: bool,
    ):
        """处理新发现的主题贴。

        主要流程：
        1. 确保用户信息已保存
        2. 立即保存 thread 元数据到 thread 表
        3. 非回溯模式下推送到消费者队列
        4. 对有回复的 thread 写入 pending_scan 标记并生成全量扫描任务

        Args:
            threads_data: aiotieba返回的Threads对象。
            new_tids: 新主题贴的tid集合。
            backfill: 是否为回溯任务。
            force: 是否为强制扫描。
        """
        if not new_tids:
            return

        new_threads = [t for t in threads_data.objs if t.tid in new_tids]

        await self.ensure_users(new_threads)

        # 原子保存：新 thread 元数据 + pending_scan 标记
        run_id = getattr(self.container, "research_run_id", None)
        generations = await self.datastore.save_threads_and_pending_scans(
            new_threads,
            backfill=backfill,
            upsert_threads=force,
            force_pending_scan=self.research_enabled,
            research_run_id=run_id if self.research_enabled else None,
        )
        generations = generations or {}

        priority = Priority.BACKFILL_POSTS if backfill else Priority.MEDIUM

        for thread in new_threads:
            SCRAPED_ITEMS.labels(type="thread", forum=thread.fname).inc(1)
            if not backfill:
                await self.datastore.push_object_event("thread", thread)
                self.log.debug("Pushed new thread tid={} to ID queue or object stream.", thread.tid)

            if thread.reply_num == 0 and not backfill and not self.research_enabled:
                self.log.debug("Thread tid={} has no replies, saved metadata only.", thread.tid)
                continue

            new_task_content = FullScanPostsTask(
                tid=thread.tid,
                backfill=backfill,
                run_id=run_id if self.research_enabled else None,
                generation=generations.get(thread.tid, 0),
                fid=thread.fid,
                fname=thread.fname,
            )
            if self._stop_requested():
                continue
            if not await self._enqueue_task_unless_stopping(Task(priority=priority, content=new_task_content)):
                continue
            self.log.info("[{}吧] Scheduled FullScanPostsTask for new tid={}", thread.fname, thread.tid)

    async def _process_old_threads(self, threads_data: ThreadsDTO, old_tids: set[int], backfill: bool):
        """处理已存在的主题贴，检查是否有更新。

        主要流程：
        1. 对比已存储的主题贴的最后回复时间，检查是否有更新
        2. 如果有更新，生成增量扫描任务

        Args:
            threads_data: aiotieba返回的Threads对象。
            old_tids: 已存在主题贴的tid集合。
            backfill: 是否为回溯任务。
        """
        if not old_tids:
            return

        old_threads = [t for t in threads_data.objs if t.tid in old_tids]
        stored_threads = await self.datastore.get_threads_by_tids(old_tids)
        stored_threads_map = {t.tid: t for t in stored_threads}
        pending_full_scan_tids = await self.datastore.get_pending_thread_scan_tids(old_tids)
        research_detail_tids: set[int] = set()
        if self.research_enabled and hasattr(
            self.datastore, "needs_research_thread_detail"
        ):
            research_detail_tids = await self.datastore.needs_research_thread_detail(old_tids)

        priority = Priority.BACKFILL_POSTS if backfill else Priority.HIGH

        for thread_data in old_threads:
            stored_thread = stored_threads_map.get(thread_data.tid)
            content_changed = (
                backfill
                and stored_thread is not None
                and thread_data.reply_num != stored_thread.reply_num
            )
            if thread_data.tid in pending_full_scan_tids and not content_changed:
                self.log.debug(
                    "Thread tid={} still has pending full scan. Skipping incremental scheduling.",
                    thread_data.tid,
                )
                continue
            needs_full_research_scan = thread_data.tid in research_detail_tids or content_changed
            if needs_full_research_scan:
                run_id = getattr(self.container, "research_run_id", None)
                generations = await self.datastore.save_threads_and_pending_scans(
                    [thread_data],
                    backfill=backfill,
                    upsert_threads=True,
                    force_pending_scan=True,
                    refresh_research_run_id=run_id,
                )
                generations = generations or {}
                full_task = FullScanPostsTask(
                    tid=thread_data.tid,
                    backfill=backfill,
                    run_id=run_id,
                    generation=generations.get(thread_data.tid, 0),
                    fid=thread_data.fid,
                    fname=thread_data.fname,
                )
                if self._stop_requested():
                    continue
                if not await self._enqueue_task_unless_stopping(
                    Task(priority=Priority.BACKFILL_POSTS if backfill else Priority.MEDIUM, content=full_task)
                ):
                    continue
                self.log.info("[{}吧] Scheduled research detail scan for tid={}", thread_data.fname, thread_data.tid)
            elif stored_thread and thread_data.last_time > stored_thread.last_time:
                self.log.debug(
                    "Thread tid={} has updates. Reply count: {} -> {}",
                    thread_data.tid,
                    stored_thread.reply_num,
                    thread_data.reply_num,
                )

                update_task_content = IncrementalScanPostsTask(
                    tid=thread_data.tid,
                    stored_last_time=stored_thread.last_time,
                    stored_reply_num=stored_thread.reply_num,
                    backfill=backfill,
                    target_last_time=thread_data.last_time,
                    target_reply_num=thread_data.reply_num,
                )
                if self._stop_requested():
                    continue
                if not await self._enqueue_task_unless_stopping(
                    Task(priority=priority, content=update_task_content)
                ):
                    continue
                self.log.info(
                    "[{}吧] Scheduled IncrementalScanPostsTask for updated tid={}",
                    thread_data.fname,
                    thread_data.tid,
                )


class FullScanPostsTaskHandler(TaskHandler):
    """处理全量扫描单个主题贴内容的任务处理器。

    负责处理FullScanPostsTask类型的任务，全量扫描指定主题贴的所有回复内容。
    """

    async def _commit_research_posts_page(
        self,
        task: FullScanPostsTask,
        page: PostsDTO,
        response: Any,
        *,
        page_number: int,
        cursor: dict[str, object],
        attempts: int,
    ) -> tuple[bool, list[FullScanCommentsTask]]:
        from ..research.persistence import persist_research_page

        run_id = task.run_id or getattr(self.container, "research_run_id", None)
        logical_key = research_posts_task_key(task.tid)
        page_posts = [post for post in page.objs if post.floor != 1]
        await self.ensure_users(page_posts)
        await self.ensure_users([comment for post in page_posts for comment in post.comments])
        await self._prepare_research_page(response)

        child_tasks: list[FullScanCommentsTask] = []
        parent_priority = Priority.BACKFILL_POSTS if task.backfill else Priority.LOW
        raw_thread = getattr(getattr(response, "raw", None), "thread", None)
        first_pid = _page_first_post_pid(page, raw_thread, task.tid)

        async with self.datastore.async_sessionmaker() as session, session.begin():
            scope = await self.datastore.get_research_task_scope_for_update(session, run_id, logical_key)
            if scope is None or scope.generation != task.generation:
                return False, []

            if first_pid <= 0 and self.research_enabled and hasattr(
                self.datastore, "get_research_first_post_pids"
            ):
                stored_first_pids = await self.datastore.get_research_first_post_pids(task.tid, session=session)
                if len(stored_first_pids) == 1:
                    first_pid = _positive_int_id(next(iter(stored_first_pids))) or 0

            posts = [
                post
                for post in page_posts
                if first_pid <= 0 or _positive_int_id(getattr(post, "pid", None)) != first_pid
            ]
            post_models = [Post.from_dto(post) for post in posts]

            await persist_research_page(
                self.datastore.async_sessionmaker,
                response,
                session=session,
                run_id=run_id,
            )
            await self.datastore.save_items_in_session(session, post_models)

            async def register_comment_scope(
                post: Any,
                parent_kind: str,
                *,
                allow_zero_evidence: bool = True,
            ) -> None:
                fid = int(post.fid or task.fid or 0)
                fname = post.fname or task.fname or ""
                child = await self.datastore.ensure_research_task_scope(
                    run_id=run_id,
                    logical_task_key=research_comments_task_key(task.tid, post.pid),
                    task_type="full_scan_comments",
                    scope_type="post_comments",
                    fid=fid,
                    fname=fname,
                    tid=task.tid,
                    parent_pid=post.pid,
                    parent_kind=parent_kind,
                    backfill=task.backfill,
                    rn=task.rn,
                    parent_logical_task_key=logical_key,
                    parent_generation=task.generation,
                    session=session,
                )
                if child is None:
                    raise PageIntegrityError(
                        f"Parent generation changed while registering comments for tid={task.tid} pid={post.pid}."
                    )
                if child.status != "pending":
                    return

                zero_evidence = (
                    _explicit_zero_evidence_for_post(response, page, post, task, page_number)
                    if allow_zero_evidence
                    else None
                )
                has_checkpoint = getattr(self.datastore, "has_research_checkpoint_in_session", None)
                if (
                    child.status == "pending"
                    and child.parent_generation == task.generation
                    and child.parent_logical_task_key == logical_key
                    and zero_evidence is not None
                    and has_checkpoint is not None
                ):
                    comment_task_key = research_comments_task_key(task.tid, post.pid, child.generation)
                    if not await has_checkpoint(session, run_id, comment_task_key):
                        observed_at = getattr(response, "observed_at", None)
                        continuation = {
                            "checkpoint_kind": "scope_completion",
                            "completion_basis": "parent_api_explicit_zero",
                            "comment_api_called": False,
                            "source_api": zero_evidence["source_api"],
                            "source_page": zero_evidence["source_page"],
                            "source_observed_at": observed_at.isoformat()
                            if hasattr(observed_at, "isoformat")
                            else None,
                            "zero_comment_evidence": zero_evidence,
                        }
                        committed_zero = await self.datastore.record_research_checkpoint(
                            run_id=run_id,
                            task_key=comment_task_key,
                            page_number=1,
                            task_type="full_scan_comments",
                            scope_type="post_comments",
                            fid=fid,
                            fname=fname,
                            tid=task.tid,
                            parent_pid=post.pid,
                            parent_kind=parent_kind,
                            status="completed",
                            scope_status="completed",
                            attempts=0,
                            has_more=None,
                            next_page=None,
                            total_pages=None,
                            reported_count=0,
                            page_item_count=None,
                            continuation_json=continuation,
                            reason="parent_api_explicit_zero",
                            generation=child.generation,
                            session=session,
                        )
                        if committed_zero:
                            await self.datastore.remove_pending_comment_scan_in_session(
                                session,
                                task.tid,
                                post.pid,
                                task_kind="full",
                            )
                            return

                await self.datastore.add_pending_comment_scan_in_session(
                    session,
                    task.tid,
                    post.pid,
                    fid=fid,
                    fname=fname,
                    backfill=task.backfill,
                )
                child_tasks.append(
                    FullScanCommentsTask(
                        tid=task.tid,
                        pid=post.pid,
                        backfill=task.backfill,
                        parent_kind=parent_kind,
                        run_id=run_id,
                        generation=child.generation,
                        parent_generation=task.generation,
                        fid=fid,
                        fname=fname,
                        rn=task.rn,
                        pending_only=True,
                    )
                )

            for post in posts:
                comments = list(post.comments)
                await self.datastore.save_items_in_session(
                    session,
                    [Comment.from_dto(comment) for comment in comments],
                )
                await register_comment_scope(post, "reply_post")

            if first_pid > 0:
                first_post = next(
                    (post for post in page.objs if _positive_int_id(getattr(post, "pid", None)) == first_pid),
                    None,
                )
                if first_post is not None:
                    comments = list(first_post.comments)
                    if comments:
                        await self.datastore.save_items_in_session(
                            session,
                            [Comment.from_dto(comment) for comment in comments],
                        )
                    await register_comment_scope(first_post, "first_post")
                else:
                    fallback_first_post = SimpleNamespace(
                        pid=first_pid,
                        fid=task.fid or page.forum.fid,
                        fname=task.fname or page.forum.fname,
                    )
                    await register_comment_scope(
                        fallback_first_post,
                        "first_post",
                        allow_zero_evidence=False,
                    )

            has_more = page.page.has_more
            scope_status = "running" if has_more else "completed"
            continuation = self._continuation_from_page(
                [post.pid for post in page.objs], cursor, has_more=has_more
            )
            committed = await self.datastore.record_research_checkpoint(
                run_id=run_id,
                task_key=research_posts_task_key(task.tid, task.generation),
                page_number=page_number,
                task_type="full_scan_posts",
                scope_type="thread_replies",
                tid=task.tid,
                fid=task.fid or page.forum.fid,
                fname=task.fname or page.forum.fname,
                status="completed",
                scope_status=scope_status,
                attempts=attempts,
                has_more=has_more,
                next_page=page_number + 1 if has_more else None,
                total_pages=page.page.total_page,
                reported_count=page.page.total_count,
                reported_count_missing_reason=(
                    "not_provided_by_aiotieba_posts_page" if page.page.total_count == 0 else None
                ),
                page_item_count=len(page.objs),
                continuation_json=continuation,
                generation=task.generation,
                session=session,
            )
            if committed and not has_more:
                await self.datastore.remove_pending_thread_scan_in_session(session, task.tid)

        await self._enqueue_missing_web_targets_after_commit(
            run_id,
            task_type="posts",
            tid=task.tid,
            page_number=page_number,
            thread_ids=(task.tid,) if first_pid > 0 else (),
            post_ids=tuple(post.pid for post in posts),
        )
        if not committed:
            return False, []

        if not task.backfill:
            for post in posts:
                await self.datastore.push_object_event("post", post)
        for child_task in child_tasks:
            if self._stop_requested():
                break
            try:
                self.router.put_nowait(Task(priority=parent_priority, content=child_task))
            except asyncio.QueueFull:
                continue
        return True, child_tasks

    async def handle(self, task_content: FullScanPostsTask):
        """处理扫描单个主题贴内容的任务。

        主要流程：
        从第一页逐页扫描并处理主题贴的回复内容，直至has_more为False。
        任务完成后，移除主题贴的 pending_scan 标记。

        Args:
            task_content: ScanPostsTask实例，包含主题贴tid和每页条目数量。
        """
        tid = task_content.tid
        rn = task_content.rn
        pn = task_content.start_page
        backfill = task_content.backfill
        run_id = task_content.run_id or getattr(self.container, "research_run_id", None)
        generation = task_content.generation
        logical_key = research_posts_task_key(tid)
        task_key = research_posts_task_key(tid, generation)
        cursor: dict[str, object] = {}
        scope_managed = run_id is not None and hasattr(self.datastore, "get_research_task_scope_state")

        if scope_managed:
            current_scope = await self.datastore.get_research_task_scope_state(run_id, logical_key)
            if current_scope is None or current_scope[0] != generation:
                return
            if current_scope[1] == "completed":
                await self._finalize_thread(tid, run_id=run_id, generation=generation)
                return

        if run_id and hasattr(self.datastore, "get_research_resume_state"):
            resume_page, _scope_status, saved_cursor = await self.datastore.get_research_resume_state(run_id, task_key)
            if resume_page is None:
                if not scope_managed or current_scope[1] == "completed":
                    await self._finalize_thread(tid, run_id=run_id if scope_managed else None, generation=generation)
                    return
                pn = 1
            else:
                pn = resume_page
                cursor = saved_cursor or {}

        lock_key = (
            f"tieba:scrapper:lock:run:{run_id}:thread_replies:{tid}:g{generation}"
            if scope_managed
            else f"tieba:scrapper:lock:tid:{tid}"
        )

        if not await self.try_acquire_lock(lock_key):
            self.log.debug("Could not acquire lock for tid={}. Another task may be processing it. Skipping.", tid)
            return

        try:
            while True:
                if self._stop_requested():
                    return
                fetched = await self.get_posts(
                    tid,
                    pn,
                    rn=rn,
                    with_comments=True,
                    comment_sort_by_agree=False,
                    comment_rn=10,
                )
                page = self._dto(fetched.response)

                self._validate_terminal_metadata("post detail", pn, page.page.has_more, page.page.total_page)
                if not page.objs:
                    if not self._is_confirmed_empty(
                        page.page,
                        pn,
                        raw_page=getattr(getattr(fetched.response, "raw", None), "page", None),
                    ):
                        raise PageIntegrityError(f"Unexpected empty post page for tid={tid} pn={pn}.")
                    await self._record_validated_research_page("posts", pn, page)
                    cursor = self._continuation_from_page([], cursor, has_more=False)
                    if scope_managed:
                        committed, _children = await self._commit_research_posts_page(
                            task_content,
                            page,
                            fetched.response,
                            page_number=pn,
                            cursor=cursor,
                            attempts=fetched.attempts,
                        )
                        if not committed:
                            return
                    else:
                        await self._persist_research_page(fetched.response)
                        await self._record_research_checkpoint(
                            run_id=run_id,
                            task_key=task_key,
                            page_number=pn,
                            task_type="full_scan_posts",
                            scope_type="thread_replies",
                            tid=tid,
                            status="completed",
                            scope_status="completed",
                            attempts=fetched.attempts,
                            has_more=False,
                            total_pages=page.page.total_page,
                            reported_count=page.page.total_count,
                            reported_count_missing_reason=(
                                "not_provided_by_aiotieba_posts_page" if page.page.total_count == 0 else None
                            ),
                            page_item_count=0,
                            continuation_json=cursor,
                        )
                    break

                self._validate_repeated_page(
                    [post.pid for post in page.objs],
                    cursor,
                    has_more=page.page.has_more,
                    scope=f"posts tid={tid}",
                )
                await self._record_validated_research_page("posts", pn, page)
                cursor = self._continuation_from_page(
                    [post.pid for post in page.objs], cursor, has_more=page.page.has_more
                )
                if scope_managed:
                    committed, _children = await self._commit_research_posts_page(
                        task_content,
                        page,
                        fetched.response,
                        page_number=pn,
                        cursor=cursor,
                        attempts=fetched.attempts,
                    )
                    if not committed:
                        return
                else:
                    await self._persist_research_page(fetched.response)
                    await self._process_posts_on_page(tid, page, backfill, research_response=fetched.response)
                    await self._record_research_checkpoint(
                        run_id=run_id,
                        task_key=task_key,
                        page_number=pn,
                        task_type="full_scan_posts",
                        scope_type="thread_replies",
                        tid=tid,
                        status="completed",
                        scope_status="running" if page.page.has_more else "completed",
                        attempts=fetched.attempts,
                        has_more=page.page.has_more,
                        next_page=pn + 1 if page.page.has_more else None,
                        total_pages=page.page.total_page,
                        reported_count=page.page.total_count,
                        reported_count_missing_reason=(
                            "not_provided_by_aiotieba_posts_page" if page.page.total_count == 0 else None
                        ),
                        page_item_count=len(page.objs),
                        continuation_json=cursor,
                    )

                if not page.page.has_more:
                    self.log.debug("All pages processed for tid={}. Task finished.", tid)
                    break
                pn += 1

            # 任务完成后：移除 pending_scan 标记
            await self._finalize_thread(tid, run_id=run_id if scope_managed else None, generation=generation)

        except Exception as e:
            await self._record_detail_failure(
                run_id=run_id,
                task_key=task_key,
                page_number=pn,
                task_type="full_scan_posts",
                scope_type="thread_replies",
                tid=tid,
                error=e,
                continuation_json=cursor,
                generation=generation if scope_managed else None,
            )
            error_detail = str(e) if isinstance(e, PageIntegrityError) else type(e).__name__
            self.log.error("Failed to process post pages for tid={} pn={} error={}", tid, pn, error_detail)
        finally:
            await self.release_lock(lock_key)

    async def _finalize_thread(
        self, tid: int, *, run_id: str | None = None, generation: int | None = None
    ) -> None:
        """移除 pending_scan 标记，表示该 thread 的全量扫描已完成。"""
        if run_id is None or generation is None:
            await self.datastore.remove_pending_thread_scan(tid)
        else:
            await self.datastore.remove_pending_thread_scan(tid, run_id=run_id, generation=generation)
        self.log.debug("Removed pending_scan marker for tid={}.", tid)

    async def _process_posts_on_page(
        self, tid: int, posts_page: PostsDTO, backfill: bool, *, research_response: Any = None
    ):
        """处理单个页面上的所有回复。

        主要流程：
        1. 转换为数据库模型并保存
        2. 非回溯模式下推送到消费者队列
        3. 如果携带的楼中楼超过10条，生成楼中楼全量扫描任务

        Args:
            tid: 主题贴tid
            posts_page: 该页面的回复数据
        """
        page_posts = [p for p in posts_page.objs if p.floor != 1]
        raw_thread = getattr(getattr(research_response, "raw", None), "thread", None)
        first_pid = _page_first_post_pid(posts_page, raw_thread, tid)
        if first_pid <= 0 and self.research_enabled and hasattr(self.datastore, "get_research_first_post_pids"):
            stored_first_pids = await self.datastore.get_research_first_post_pids(tid)
            if len(stored_first_pids) == 1:
                first_pid = _positive_int_id(next(iter(stored_first_pids))) or 0

        await self.ensure_users(page_posts)

        posts = [
            post
            for post in page_posts
            if first_pid <= 0 or _positive_int_id(getattr(post, "pid", None)) != first_pid
        ]

        post_models = [Post.from_dto(p) for p in posts]
        await self.datastore.save_items(post_models)
        await self._enqueue_missing_web_targets(
            getattr(self.container, "research_run_id", None),
            thread_ids=(tid,) if first_pid > 0 else (),
            post_ids=tuple(post.pid for post in posts),
        )

        priority = Priority.BACKFILL_POSTS if backfill else Priority.LOW

        for post in posts:
            SCRAPED_ITEMS.labels(type="post", forum=post.fname).inc(1)
            if not backfill:
                await self.datastore.push_object_event("post", post)
                self.log.debug("Pushed post pid={} to object stream.", post.pid)

            await self.ensure_users(post.comments)

            comment_models = [Comment.from_dto(c) for c in post.comments]
            await self.datastore.save_items(comment_models)

            if self.research_enabled or post.reply_num > 10 or len(post.comments) != post.reply_num:
                await self.datastore.add_pending_comment_scan(
                    tid=tid,
                    pid=post.pid,
                    fid=post.fid,
                    fname=post.fname,
                    backfill=backfill,
                    task_kind="full",
                )
                comment_task = FullScanCommentsTask(
                    tid=tid,
                    pid=post.pid,
                    backfill=backfill,
                    parent_kind="reply_post",
                )
                if not self._stop_requested():
                    await self._enqueue_task_unless_stopping(Task(priority=priority, content=comment_task))
                    self.log.info(
                        "[{}吧] Scheduled FullScanCommentsTask for pid={} in tid={}",
                        post.fname,
                        post.pid,
                        tid,
                    )

        if first_pid > 0:
            for post in page_posts:
                if _positive_int_id(getattr(post, "pid", None)) != first_pid:
                    continue
                comments = list(post.comments)
                if comments:
                    await self.ensure_users(comments)
                    await self.datastore.save_items([Comment.from_dto(comment) for comment in comments])

        if self.research_enabled and first_pid > 0:
            fid = int(getattr(raw_thread, "fid", 0) or 0)
            fname = str(getattr(raw_thread, "fname", "") or "")
            if fid <= 0 and posts_page.forum.fid > 0:
                fid = posts_page.forum.fid
            if not fname:
                fname = posts_page.forum.fname
            if fid > 0 and fname:
                await self.datastore.add_pending_comment_scan(
                    tid=tid,
                    pid=first_pid,
                    fid=fid,
                    fname=fname,
                    backfill=backfill,
                    task_kind="full",
                )
                await self._enqueue_task_unless_stopping(
                    Task(
                        priority=priority,
                        content=FullScanCommentsTask(
                            tid=tid,
                            pid=first_pid,
                            backfill=backfill,
                            parent_kind="first_post",
                        ),
                    )
                )


class IncrementalScanPostsTaskHandler(TaskHandler):
    """处理增量扫描单个主题贴内容的任务处理器。

    负责处理IncrementalScanPostsTask类型的任务，增量扫描指定主题贴的回复内容。
    """

    async def handle(self, task_content: IncrementalScanPostsTask):
        """处理增量扫描单个主题贴内容的任务。

        主要流程：
        1. 通过pn=0xFFFF获取最新一页数据
        2. 从最新一页的总页数开始，逐页倒序扫描主题贴的所有回复内容
        3. 任务完成后，更新主题贴的元数据到数据库。
        4. 如果未找到新内容且 DeepScan 已启用，触发深度扫描。

        Args:
            task_content: IncrementalScanPostsTask实例，包含主题贴tid和存储的元数据。
        """
        tid = task_content.tid
        stored_last_time = task_content.stored_last_time
        stored_reply_num = task_content.stored_reply_num
        rn = task_content.rn
        backfill = task_content.backfill

        lock_key = f"tieba:scrapper:lock:tid:{tid}"

        if not await self.try_acquire_lock(lock_key):
            self.log.debug("Could not acquire lock for tid={}. Another task may be processing it. Skipping.", tid)
            return

        try:
            if self._stop_requested():
                return
            latest_fetched = await self.get_posts(
                tid,
                pn=0xFFFF,
                rn=rn,
                sort=PostSortType.DESC,
                with_comments=True,
                comment_sort_by_agree=False,
                comment_rn=10,
            )
            latest_page = self._dto(latest_fetched.response)
            await self._record_validated_research_page("posts", latest_page.page.current_page, latest_page)
            await self._persist_research_page(latest_fetched.response)

            if not latest_page or not latest_page.objs:
                self.log.debug("No posts found for tid={}. Task finished.", tid)
                return

            start_pn = latest_page.page.total_page

            self.log.debug("Incremental scan for tid={}. Starting from page {}.", tid, start_pn)

            found_new_content = await self._scan_thread_pages(
                tid, start_pn, rn, stored_last_time, latest_page, backfill
            )

            if self._stop_requested():
                return

            self.log.debug("Finished incremental scan for tid={}.", tid)

            # 任务完成后更新元数据
            if task_content.target_last_time and task_content.target_reply_num is not None:
                await self.datastore.update_thread_metadata(
                    tid, task_content.target_last_time, task_content.target_reply_num
                )
                self.log.debug("Updated thread metadata for tid={} after incremental scan.", tid)

            if not found_new_content and not backfill and self.container.config.deep_scan_enabled:
                expected_new = 1
                if task_content.target_reply_num is not None:
                    expected_new = max(1, task_content.target_reply_num - stored_reply_num)

                self.log.info(
                    "No new content found for tid={} during incremental scan. Triggering DeepScan (expected_new={}).",
                    tid,
                    expected_new,
                )
                deep_task = DeepScanTask(
                    tid=tid,
                    depth=self.container.config.deep_scan_depth,
                    total_pages=start_pn,
                    expected_new_comments=expected_new,
                )
                await self._enqueue_task_unless_stopping(Task(priority=Priority.BACKGROUND, content=deep_task))
        except UnretriableApiError as e:
            if e.code == 4:
                self.log.warning(
                    "Thread tid={} may have been deleted (err_code=4). Saving thread metadata from previous crawl.", tid
                )
                if task_content.target_last_time and task_content.target_reply_num is not None:
                    await self.datastore.update_thread_metadata(
                        tid, task_content.target_last_time, task_content.target_reply_num
                    )
                    self.log.debug("Updated thread metadata for tid={} after deletion detected.", tid)
            else:
                self.log.exception("Failed to process IncrementalScanPostsTask for tid={}: {}", tid, e)
        except Exception as e:
            self.log.exception("Failed to process IncrementalScanPostsTask for tid={}: {}", tid, e)
        finally:
            await self.release_lock(lock_key)

    async def _scan_thread_pages(
        self,
        tid: int,
        start_pn: int,
        rn: int,
        stored_last_time: datetime,
        initial_page_data: PostsDTO,
        backfill: bool,
    ) -> bool:
        """倒序扫描主题贴回复页面。

        从指定的起始页开始，逐页倒序扫描主题贴的所有回复内容。
        默认扫描一次最新一页数据和热门序第一页数据。

        Args:
            tid: 主题贴tid
            start_pn: 扫描起始页码
            rn: 每页条目数量
            stored_last_time: 数据库中存储的最后回复时间戳（用于判断停止扫描）
            initial_page_data: 最新一页的数据
            backfill: 是否为回溯任务

        Returns:
            bool: 是否在扫描过程中发现了任何新内容（新回复或新楼中楼更新）
        """
        found_new_content = False

        for pn in range(start_pn, 0, -1):
            self.log.debug("Scanning tid={}, Posts page {}.", tid, pn)

            if pn == start_pn and initial_page_data:
                posts_page = initial_page_data
            else:
                if self._stop_requested():
                    return found_new_content
                fetched = await self.get_posts(
                    tid,
                    pn,
                    rn=rn,
                    with_comments=True,
                    comment_sort_by_agree=False,
                    comment_rn=10,
                )
                posts_page = self._dto(fetched.response)
                await self._record_validated_research_page("posts", pn, posts_page)
                await self._persist_research_page(fetched.response)

            if not posts_page or not posts_page.objs:
                self.log.warning("No posts found on tid={}, pn={}. Skipping page.", tid, pn)
                continue

            has_more, page_found_new = await self._process_posts_on_page(stored_last_time, posts_page, backfill)

            if page_found_new:
                found_new_content = True

            if not has_more:
                self.log.debug("No new posts found on tid={}, pn={}. Stopping scan.", tid, pn)
                break

        if not found_new_content and not self._stop_requested():
            hot_fetched = await self.get_posts(
                tid,
                pn=1,
                rn=rn,
                sort=PostSortType.HOT,
                with_comments=True,
                comment_sort_by_agree=False,
                comment_rn=10,
            )
            hot_page = self._dto(hot_fetched.response)
            await self._record_validated_research_page("posts", hot_page.page.current_page, hot_page)
            await self._persist_research_page(hot_fetched.response)

            if hot_page and hot_page.objs:
                self.log.debug("Hot posts for tid={} found on page 1. Processing hot posts.", tid)
                _, hot_found_new = await self._process_posts_on_page(stored_last_time, hot_page, backfill)
                if hot_found_new:
                    found_new_content = True

        return found_new_content

    async def _process_posts_on_page(
        self, stored_last_time: datetime, posts_page: PostsDTO, backfill: bool
    ) -> tuple[bool, bool]:
        """处理单个页面上的所有回复。

        主要流程：
        1. 过滤出新回复和旧回复
        2. 对新回复进行处理：保存到数据库、推送到消费者队列、生成评论扫描任务
        3. 对旧回复进行处理：检查是否有更新并生成增量扫描任务
        4. 处理所有回复附带的楼中楼
        5. 判断是否还有新回复需要继续扫描。

        Args:
            stored_last_time: 数据库中存储的最后回复时间戳（用于判断停止扫描）
            posts_page: 该页面的回复数据
            backfill: 是否为回溯任务

        Returns:
            tuple[bool, bool]: (是否还有新回复需要继续扫描, 是否在本页发现了新内容)
        """
        posts = [p for p in posts_page.objs if p.floor != 1]
        all_pids_on_page = [p.pid for p in posts]
        new_pids = await self.datastore.filter_new_ids("post", all_pids_on_page)
        old_pids = set(all_pids_on_page) - new_pids

        tid = posts_page.objs[0].tid if posts_page.objs else None
        current_page = posts_page.page.current_page

        await self._enqueue_missing_web_targets(
            getattr(self.container, "research_run_id", None),
            thread_ids=(tid,) if tid is not None else (),
            post_ids=tuple(post.pid for post in posts),
        )

        found_new_content = bool(new_pids)

        if not new_pids:
            if tid is not None:
                self.log.debug(
                    "No new posts on tid={}, pn={}.",
                    tid,
                    current_page,
                )
            # 即使没有新回复，也检查旧回复的楼中楼更新
            old_posts_had_updates = await self._process_old_posts(old_pids, posts, backfill)
            if old_posts_had_updates:
                found_new_content = True
            return False, found_new_content

        if tid is not None:
            self.log.debug(
                "tid={}, pn={}: Found {} new posts.",
                tid,
                current_page,
                len(new_pids),
            )

        await self._process_new_posts(new_pids, posts, backfill)

        old_posts_had_updates = await self._process_old_posts(old_pids, posts, backfill)
        if old_posts_had_updates:
            found_new_content = True

        attached_new_comments = await self._process_attached_comments(posts)
        if attached_new_comments > 0:
            found_new_content = True

        if any(p.create_time < stored_last_time for p in posts_page.objs):
            return False, found_new_content

        return True, found_new_content

    async def _process_new_posts(self, new_pids: set[int], posts: list[PostDTO], backfill: bool):
        """处理新发现的回复。

        主要流程：
        1. 转换为数据库模型并保存
        2. 非回溯模式下推送到消费者队列
        3. 如果携带的楼中楼超过10条，生成楼中楼全量扫描任务

        Args:
            new_pids: 新回复的pid集合
            posts_page: 该页面的回复数据
        """
        if not new_pids:
            return

        new_posts = [p for p in posts if p.pid in new_pids]

        await self.ensure_users(new_posts)

        new_post_models = [Post.from_dto(p) for p in new_posts]
        await self.datastore.save_items(new_post_models)
        self.log.debug("Saved {} new posts to DB.", len(new_posts))

        priority = Priority.BACKFILL_POSTS if backfill else Priority.LOW

        for post in new_posts:
            SCRAPED_ITEMS.labels(type="post", forum=post.fname).inc(1)
            if not backfill:
                await self.datastore.push_object_event("post", post)
                self.log.debug("Pushed new post pid={} to ID queue or object stream.", post.pid)

            if self.research_enabled or post.reply_num > 10 or len(post.comments) != post.reply_num:
                await self.datastore.add_pending_comment_scan(
                    tid=post.tid,
                    pid=post.pid,
                    fid=post.fid,
                    fname=post.fname,
                    backfill=backfill,
                    task_kind="full",
                )
                comment_task = FullScanCommentsTask(
                    tid=post.tid,
                    pid=post.pid,
                    backfill=backfill,
                    parent_kind="reply_post",
                )
                if not self._stop_requested():
                    await self._enqueue_task_unless_stopping(Task(priority=priority, content=comment_task))
                    self.log.info(
                        "[{}吧] Scheduled FullScanCommentsTask for new pid={} in tid={}",
                        post.fname,
                        post.pid,
                        post.tid,
                    )

    async def _process_old_posts(self, old_pids: set[int], posts: list[PostDTO], backfill: bool) -> bool:
        """处理已存在的回复，检查是否有更新。

        主要流程：
        1. 对比已存储的回复的reply_num，检查楼中楼是否有更新
        2. 如果有更新，生成楼中楼增量扫描任务

        Args:
            old_pids: 已存在的回复的pid集合
            posts: 该页面的回复数据
            backfill: 是否为回溯任务

        Returns:
            bool: 是否发现了楼中楼更新
        """
        if not old_pids:
            return False

        old_posts = [p for p in posts if p.pid in old_pids]
        stored_posts = await self.datastore.get_posts_by_pids(old_pids)
        stored_posts_map = {p.pid: p for p in stored_posts}

        posts_to_update = []

        priority = Priority.BACKFILL_POSTS if backfill else Priority.MEDIUM

        for post_data in old_posts:
            stored_post = stored_posts_map.get(post_data.pid)
            if stored_post and post_data.reply_num > stored_post.reply_num:
                self.log.debug(
                    "Post pid={} has updates. Reply count: {} -> {}",
                    post_data.pid,
                    stored_post.reply_num,
                    post_data.reply_num,
                )
                posts_to_update.append(post_data)

                await self.datastore.add_pending_comment_scan(
                    tid=post_data.tid,
                    pid=post_data.pid,
                    fid=post_data.fid,
                    fname=post_data.fname,
                    backfill=backfill,
                    task_kind="full" if self.research_enabled else "incremental",
                    refresh_research_run_id=(
                        getattr(self.container, "research_run_id", None) if self.research_enabled else None
                    ),
                )
                if self.research_enabled:
                    update_task_content = FullScanCommentsTask(
                        tid=post_data.tid,
                        pid=post_data.pid,
                        backfill=backfill,
                        parent_kind="reply_post",
                    )
                else:
                    update_task_content = IncrementalScanCommentsTask(
                        tid=post_data.tid,
                        pid=post_data.pid,
                        backfill=backfill,
                    )
                if not self._stop_requested():
                    await self._enqueue_task_unless_stopping(Task(priority=priority, content=update_task_content))
                    self.log.info(
                        "[{}吧] Scheduled IncrementalScanCommentsTask for updated pid={}",
                        post_data.fname,
                        post_data.pid,
                    )

        if posts_to_update:
            post_models = [Post.from_dto(p) for p in posts_to_update]
            await self.datastore.save_items(post_models, upsert=True)

        return bool(posts_to_update)

    async def _process_attached_comments(self, posts: list[PostDTO]) -> int:
        """处理回复附带的楼中楼。

        对每个回复的楼中楼进行去重处理，保存新楼中楼到数据库。

        Args:
            posts: 包含回复数据的Posts对象。

        Returns:
            int: 发现的新楼中楼数量
        """
        total_new_comments = 0

        for post in posts:
            new_comment_ids = await self.datastore.filter_new_ids("comment", [c.cid for c in post.comments])

            if not new_comment_ids:
                self.log.debug("No new comments for post pid={}.", post.pid)
                continue

            self.log.debug("Post pid={}: Found {} new comments.", post.pid, len(new_comment_ids))
            new_comments = [c for c in post.comments if c.cid in new_comment_ids]

            await self.ensure_users(new_comments)

            new_comment_models = [Comment.from_dto(c) for c in new_comments]
            await self.datastore.save_items(new_comment_models)

            total_new_comments += len(new_comments)

        return total_new_comments


class FullScanCommentsTaskHandler(TaskHandler):
    """处理全量扫描单个回复下的楼中楼的任务处理器。

    负责处理ScanCommentsTask类型的任务，全量扫描指定回复下的所有楼中楼。
    """

    async def _commit_research_comments_page(
        self,
        task: FullScanCommentsTask,
        page: CommentsDTO,
        response: Any,
        *,
        page_number: int,
        cursor: dict[str, object],
        attempts: int,
    ) -> bool:
        from ..research.persistence import persist_research_page

        run_id = task.run_id or getattr(self.container, "research_run_id", None)
        parent_logical_key = research_posts_task_key(task.tid)
        logical_key = research_comments_task_key(task.tid, task.pid)
        await self.ensure_users(page.objs)
        await self._prepare_research_page(response)

        async with self.datastore.async_sessionmaker() as session, session.begin():
            parent = await self.datastore.get_research_task_scope_for_update(
                session, run_id, parent_logical_key
            )
            if parent is None or (
                task.parent_generation is not None and parent.generation != task.parent_generation
            ):
                return False
            scope = await self.datastore.get_research_task_scope_for_update(session, run_id, logical_key)
            if (
                scope is None
                or scope.generation != task.generation
                or scope.parent_generation != parent.generation
            ):
                return False

            await persist_research_page(
                self.datastore.async_sessionmaker,
                response,
                session=session,
                run_id=run_id,
            )
            await self.datastore.save_items_in_session(
                session,
                [Comment.from_dto(comment) for comment in page.objs],
            )
            has_more = page.page.has_more
            scope_status = "running" if has_more else "completed"
            continuation = self._continuation_from_page(
                [comment.cid for comment in page.objs], cursor, has_more=has_more
            )
            committed = await self.datastore.record_research_checkpoint(
                run_id=run_id,
                task_key=research_comments_task_key(task.tid, task.pid, task.generation),
                page_number=page_number,
                task_type="full_scan_comments",
                scope_type="post_comments",
                tid=task.tid,
                parent_pid=task.pid,
                parent_kind=task.parent_kind,
                fid=task.fid or page.forum.fid,
                fname=task.fname or page.forum.fname,
                status="completed",
                scope_status=scope_status,
                attempts=attempts,
                has_more=has_more,
                next_page=page_number + 1 if has_more else None,
                total_pages=page.page.total_page,
                reported_count=page.page.total_count,
                reported_count_missing_reason=(
                    "zero_presence_unverified_in_parsed_object" if page.page.total_count == 0 else None
                ),
                page_item_count=len(page.objs),
                continuation_json=continuation,
                generation=task.generation,
                session=session,
            )
            if committed and not has_more:
                await self.datastore.remove_pending_comment_scan_in_session(
                    session, task.tid, task.pid, task_kind="full"
                )

        await self._enqueue_missing_web_targets_after_commit(
            run_id,
            task_type="comments",
            tid=task.tid,
            parent_pid=task.pid,
            page_number=page_number,
            comment_ids=tuple(comment.cid for comment in page.objs),
        )
        return committed

    async def _revalidate_failed_comment_page_from_parent_zero(
        self,
        task: FullScanCommentsTask,
        error: Exception,
        *,
        run_id: str,
        task_key: str,
    ) -> bool:
        if (
            type(error) is not PageIntegrityError
            or str(error) != "Comment detail empty response is missing or has zero page identity for page 1."
            or not getattr(self.container, "revalidate_failed_comment_page_zero", False)
            or task.parent_generation is None
        ):
            return False
        get_hint = getattr(self.datastore, "get_research_comment_parent_page_hint", None)
        complete = getattr(self.datastore, "complete_failed_comment_scope_from_parent_zero", None)
        if get_hint is None or complete is None:
            return False
        hint = await get_hint(
            run_id,
            task_key,
            task.tid,
            task.pid,
            task.generation,
            task.parent_generation,
        )
        if hint is None:
            return False
        parent_floor, parent_rn = hint
        if type(parent_floor) is not int or parent_floor < 1 or type(parent_rn) is not int or parent_rn < 1:
            return False
        parent_page = (parent_floor - 1) // parent_rn + 1
        fetched = await self.get_posts(task.tid, parent_page, rn=parent_rn, with_comments=False)
        page = self._dto(fetched.response)
        matches = [post for post in page.objs if post.pid == task.pid]
        if len(matches) != 1:
            return False
        evidence = _explicit_zero_evidence_for_post(
            fetched.response,
            page,
            matches[0],
            task,
            parent_page,
        )
        if evidence is None:
            return False
        observed_at = getattr(fetched.response, "observed_at", None)
        isoformat = getattr(observed_at, "isoformat", None)
        if isoformat is None:
            return False
        source_observed_at = isoformat()
        if not isinstance(source_observed_at, str) or not source_observed_at:
            return False
        return await complete(
            run_id=run_id,
            task_key=task_key,
            tid=task.tid,
            parent_pid=task.pid,
            generation=task.generation,
            parent_generation=task.parent_generation,
            fid=matches[0].fid,
            parent_floor=parent_floor,
            parent_rn=parent_rn,
            parent_page=parent_page,
            source_observed_at=source_observed_at,
            current_failure={"error_type": type(error).__name__, "reason": str(error)},
            evidence=evidence,
        )

    async def handle(self, task_content: FullScanCommentsTask):
        tid = task_content.tid
        pid = task_content.pid
        backfill = task_content.backfill
        parent_kind = task_content.parent_kind
        run_id = task_content.run_id or getattr(self.container, "research_run_id", None)
        generation = task_content.generation
        logical_key = research_comments_task_key(tid, pid)
        task_key = research_comments_task_key(tid, pid, generation)
        pn = task_content.start_page
        cursor: dict[str, object] = {}
        scope_managed = run_id is not None and hasattr(self.datastore, "get_research_task_scope_state")

        if self.research_enabled and parent_kind is None and hasattr(self.datastore, "get_research_first_post_pids"):
            first_post_pids = await self.datastore.get_research_first_post_pids(tid)
            parent_kind = "first_post" if pid in first_post_pids else "reply_post"
        task_content = dataclasses.replace(task_content, parent_kind=parent_kind)
        if scope_managed:
            current_scope = await self.datastore.get_research_task_scope_state(run_id, logical_key)
            parent_scope = await self.datastore.get_research_task_scope_state(
                run_id, research_posts_task_key(tid)
            )
            if (
                current_scope is None
                or current_scope[0] != generation
                or parent_scope is None
                or current_scope[2] != parent_scope[0]
                or (task_content.parent_generation is not None and task_content.parent_generation != parent_scope[0])
            ):
                return
            task_content = dataclasses.replace(task_content, parent_generation=parent_scope[0])
            if current_scope[1] == "completed":
                await self._finalize_comment_scan(tid, pid, run_id=run_id, generation=generation)
                return
        if run_id and hasattr(self.datastore, "get_research_resume_state"):
            resume_page, _scope_status, saved_cursor = await self.datastore.get_research_resume_state(run_id, task_key)
            if resume_page is None:
                if not scope_managed or current_scope[1] == "completed":
                    await self._finalize_comment_scan(
                        tid,
                        pid,
                        run_id=run_id if scope_managed else None,
                        generation=generation if scope_managed else None,
                    )
                    return
                pn = 1
            else:
                pn = resume_page
                cursor = saved_cursor or {}

        lock_key = (
            f"tieba:scrapper:lock:run:{run_id}:{logical_key}:g{generation}:pg{task_content.parent_generation}"
            if scope_managed
            else f"tieba:scrapper:lock:pid:{tid}:{pid}"
        )
        if not await self.try_acquire_lock(lock_key):
            return

        try:
            if scope_managed:
                claim = getattr(self.datastore, "claim_research_comments_scope", None)
                if claim is not None:
                    if not await claim(
                        run_id=run_id,
                        logical_task_key=logical_key,
                        generation=generation,
                        parent_logical_task_key=research_posts_task_key(tid),
                        parent_generation=task_content.parent_generation,
                        tid=tid,
                        parent_pid=pid,
                        pending_only=task_content.pending_only,
                    ):
                        return
                elif task_content.pending_only:
                    current_scope = await self.datastore.get_research_task_scope_state(run_id, logical_key)
                    parent_scope = await self.datastore.get_research_task_scope_state(
                        run_id, research_posts_task_key(tid)
                    )
                    if (
                        current_scope is None
                        or current_scope[0] != generation
                        or current_scope[1] != "pending"
                        or parent_scope is None
                        or parent_scope[0] != task_content.parent_generation
                        or current_scope[2] != parent_scope[0]
                    ):
                        return
            while True:
                if self._stop_requested():
                    return
                fetched = await self.get_comments(tid, pid, pn)
                page = self._dto(fetched.response)
                self._validate_terminal_metadata(
                    "comment detail", pn, page.page.has_more, page.page.total_page
                )

                if not page.objs:
                    if not self._is_confirmed_empty(
                        page.page,
                        pn,
                        raw_page=getattr(getattr(fetched.response, "raw", None), "page", None),
                    ):
                        raise PageIntegrityError(f"Unexpected empty comment page for tid={tid} pid={pid} pn={pn}.")
                    await self._record_validated_research_page("comments", pn, page)
                    if scope_managed:
                        committed = await self._commit_research_comments_page(
                            task_content,
                            page,
                            fetched.response,
                            page_number=pn,
                            cursor={},
                            attempts=fetched.attempts,
                        )
                        if not committed:
                            return
                    else:
                        await self._persist_research_page(fetched.response)
                        await self._record_research_checkpoint(
                            run_id=run_id,
                            task_key=task_key,
                            page_number=pn,
                            task_type="full_scan_comments",
                            scope_type="post_comments",
                            tid=tid,
                            parent_pid=pid,
                            parent_kind=parent_kind,
                            fid=page.forum.fid,
                            status="completed",
                            scope_status="completed",
                            attempts=fetched.attempts,
                            has_more=False,
                            total_pages=page.page.total_page,
                            reported_count=page.page.total_count,
                            reported_count_missing_reason=(
                                "zero_presence_unverified_in_parsed_object" if page.page.total_count == 0 else None
                            ),
                            page_item_count=0,
                            continuation_json={},
                        )
                    break

                self._validate_repeated_page(
                    [comment.cid for comment in page.objs],
                    cursor,
                    has_more=page.page.has_more,
                    scope=f"comments tid={tid} parent_pid={pid}",
                )
                await self._record_validated_research_page("comments", pn, page)
                cursor = self._continuation_from_page(
                    [comment.cid for comment in page.objs], cursor, has_more=page.page.has_more
                )
                if scope_managed:
                    committed = await self._commit_research_comments_page(
                        task_content,
                        page,
                        fetched.response,
                        page_number=pn,
                        cursor=cursor,
                        attempts=fetched.attempts,
                    )
                    if not committed:
                        return
                else:
                    await self._persist_research_page(fetched.response)
                    await self._process_comments_on_page(pid, pn, page, backfill)
                    await self._record_research_checkpoint(
                        run_id=run_id,
                        task_key=task_key,
                        page_number=pn,
                        task_type="full_scan_comments",
                        scope_type="post_comments",
                        tid=tid,
                        parent_pid=pid,
                        parent_kind=parent_kind,
                        fid=page.forum.fid,
                        status="completed",
                        scope_status="running" if page.page.has_more else "completed",
                        attempts=fetched.attempts,
                        has_more=page.page.has_more,
                        next_page=pn + 1 if page.page.has_more else None,
                        total_pages=page.page.total_page,
                        reported_count=page.page.total_count,
                        reported_count_missing_reason=(
                            "zero_presence_unverified_in_parsed_object" if page.page.total_count == 0 else None
                        ),
                        page_item_count=len(page.objs),
                        continuation_json=cursor,
                    )
                if not page.page.has_more:
                    break
                if self._stop_requested():
                    return
                pn += 1
                if pn % 5 == 0:
                    await asyncio.sleep(0)

            await self._finalize_comment_scan(
                tid,
                pid,
                run_id=run_id if scope_managed else None,
                generation=generation,
            )
            self.log.debug("Finished comment scan for pid={} in tid={}.", pid, tid)
        except Exception as e:
            failure_error = e
            failure_continuation = cursor
            if scope_managed and pn == 1:
                try:
                    completed_from_parent_zero = await self._revalidate_failed_comment_page_from_parent_zero(
                        task_content,
                        e,
                        run_id=run_id,
                        task_key=task_key,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as fallback_error:
                    completed_from_parent_zero = False
                    failure_error = fallback_error
                    failure_continuation = {
                        **cursor,
                        "comment_page_identity_failure": {
                            "error_type": type(e).__name__,
                            "reason": str(e),
                        },
                    }
                    self.log.warning(
                        "Parent-page zero revalidation did not complete scope tid={} parent_pid={} error_type={}",
                        tid,
                        pid,
                        type(fallback_error).__name__,
                    )
                if completed_from_parent_zero:
                    self.log.info(
                        "Completed comment scope from fresh parent wire-zero evidence tid={} parent_pid={}",
                        tid,
                        pid,
                    )
                    return
            await self._record_detail_failure(
                run_id=run_id,
                task_key=task_key,
                page_number=pn,
                task_type="full_scan_comments",
                scope_type="post_comments",
                tid=tid,
                parent_pid=pid,
                parent_kind=parent_kind,
                error=failure_error,
                continuation_json=failure_continuation,
                generation=generation if scope_managed else None,
            )
            error_detail = (
                str(failure_error) if isinstance(failure_error, PageIntegrityError) else type(failure_error).__name__
            )
            self.log.error(
                "Failed to process comment pages for tid={} parent_pid={} pn={} error={}",
                tid,
                pid,
                pn,
                error_detail,
            )
        finally:
            await self.release_lock(lock_key)

    async def _finalize_comment_scan(
        self,
        tid: int,
        pid: int,
        *,
        run_id: str | None = None,
        generation: int | None = None,
    ) -> None:
        """移除 pending_comment_scan 标记，表示该评论扫描任务已完成。"""
        if run_id is None or generation is None:
            await self.datastore.remove_pending_comment_scan(tid, pid)
        else:
            await self.datastore.remove_pending_comment_scan(
                tid,
                pid,
                run_id=run_id,
                generation=generation,
            )
        self.log.debug("Removed pending_comment_scan marker for tid={}, pid={}", tid, pid)

    async def _scan_comment_pages(self, tid: int, pid: int, initial_page_data: CommentsDTO, backfill: bool):
        """扫描楼中楼的所有页面。

        Args:
            tid: 主题贴tid
            pid: 回复pid
            total_pages: 楼中楼总页数
            initial_page_data: 第一页楼中楼数据
        """
        total_pages = initial_page_data.page.total_page
        if total_pages < 1:
            total_pages = 1

        processed_num = 0

        for pn in range(1, total_pages + 1):
            self.log.debug("Scanning comments for pid={}, page {}/{}.", pid, pn, total_pages)

            if pn == 1:
                comments_page = initial_page_data
            else:
                comments_page = await self.get_comments(tid, pid, pn)
                await self._record_validated_research_page("comments", pn, comments_page)

            if not comments_page or not comments_page.objs:
                self.log.warning("No comments found on pid={}, pn={}. Stopping scan.", pid, pn)
                break

            await self._process_comments_on_page(pid, pn, comments_page, backfill)

            processed_num += len(comments_page.objs)

            if processed_num >= self.batch_size:
                await asyncio.sleep(0)
                processed_num = 0

    async def _process_comments_on_page(self, pid: int, pn: int, comments_page: CommentsDTO, backfill: bool):
        """处理单个页面上的所有楼中楼。

        主要流程：
        1. 对页面上的楼中楼进行去重处理，保存新楼中楼到数据库
        2. 非回溯模式下推送到消费者队列

        Args:
            pid: 回复ID。
            pn: 页码。
            comments_page: 该页面的评论数据。
        """
        all_cids_on_page = [c.cid for c in comments_page.objs]
        new_cids = await self.datastore.filter_new_ids("comment", all_cids_on_page)

        await self._enqueue_missing_web_targets(
            getattr(self.container, "research_run_id", None),
            comment_ids=tuple(all_cids_on_page),
        )

        if not new_cids:
            self.log.debug("No new comments on pid={}, pn={}.", pid, pn)
            return

        self.log.debug("pid={}, pn={}: Found {} new comments.", pid, pn, len(new_cids))

        new_comments_data = [c for c in comments_page.objs if c.cid in new_cids]

        await self.ensure_users(new_comments_data)

        new_comment_models = [Comment.from_dto(c) for c in new_comments_data]
        await self.datastore.save_items(new_comment_models)

        for comment in new_comments_data:
            SCRAPED_ITEMS.labels(type="comment", forum=comment.fname).inc(1)
            if not backfill:
                await self.datastore.push_object_event("comment", comment)
                self.log.debug("Pushed comment cid={} to ID queue or object stream.", comment.cid)


class IncrementalScanCommentsTaskHandler(TaskHandler):
    """处理增量扫描单个回复下的楼中楼的任务处理器。

    负责处理IncrementalScanCommentsTask类型的任务，增量扫描指定回复下的楼中楼。
    """

    async def handle(self, task_content: IncrementalScanCommentsTask):
        """处理增量扫描楼中楼的任务。

        主要流程：
        1. 获取指定回复的最新一页数据以确定总页数
        2. 从最新一页的总页数开始，逐页倒序扫描回复下的所有楼中楼

        Args:
            task_content: IncrementalScanCommentsTask实例，包含主题贴tid和回复pid。
        """
        tid = task_content.tid
        pid = task_content.pid
        backfill = task_content.backfill

        try:
            if self._stop_requested():
                return
            comments_fetched = await self.get_comments(tid, pid)
            comments_page = self._dto(comments_fetched.response)
            await self._record_validated_research_page(
                "comments", comments_page.page.current_page, comments_page
            )
            await self._persist_research_page(comments_fetched.response)

            if not comments_page or not comments_page.objs:
                self.log.debug("No comments found for pid={} in tid={}. Task finished.", pid, tid)
                await self._finalize_comment_scan(tid, pid)
                return

            total_pages = comments_page.page.total_page

            await self._scan_comment_pages(tid, pid, total_pages, comments_page, backfill)
            if self._stop_requested():
                return
            await self._finalize_comment_scan(tid, pid)
        except UnretriableApiError as e:
            if e.code == 4:
                self.log.warning(
                    "Post pid={} or thread tid={} may have been deleted (err_code=4). Stopping comment scan.",
                    pid,
                    tid,
                )
                await self._finalize_comment_scan(tid, pid)
            else:
                self.log.exception("Failed to process IncrementalScanCommentsTask for pid={}, tid={}: {}", pid, tid, e)
        except Exception as e:
            self.log.exception("Failed to process IncrementalScanCommentsTask for pid={}, tid={}: {}", pid, tid, e)

    async def _finalize_comment_scan(self, tid: int, pid: int) -> None:
        """移除 pending_comment_scan 标记，表示该评论扫描任务已完成。"""
        await self.datastore.remove_pending_comment_scan(tid, pid, task_kind="incremental")
        self.log.debug("Removed incremental pending_comment_scan marker for tid={}, pid={}", tid, pid)

    async def _scan_comment_pages(
        self,
        tid: int,
        pid: int,
        total_pages: int,
        initial_page_data: CommentsDTO,
        backfill: bool,
    ):
        """扫描楼中楼的所有页面。

        从最后一页开始，逐页倒序扫描指定回复下的所有楼中楼。

        Args:
            tid: 主题贴tid
            pid: 回复pid
            total_pages: 楼中楼总页数
            initial_page_data: 第一页的楼中楼数据
        """
        for pn in range(total_pages, 0, -1):
            self.log.debug("Scanning comments for pid={}, page {}/{}.", pid, pn, total_pages)

            if pn == 1:
                comments_page = initial_page_data
            else:
                if self._stop_requested():
                    return
                fetched = await self.get_comments(tid, pid, pn)
                comments_page = self._dto(fetched.response)
                await self._record_validated_research_page("comments", pn, comments_page)
                await self._persist_research_page(fetched.response)

            if not comments_page or not comments_page.objs:
                self.log.warning("No comments found on pid={}, pn={}. Stopping scan.", pid, pn)
                break

            has_more = await self._process_comments_on_page(pid, pn, comments_page, backfill)

            if not has_more:
                self.log.debug("No new comments found on pid={}, pn={}. Stopping scan.", pid, pn)
                break

    async def _process_comments_on_page(self, pid: int, pn: int, comments_page: CommentsDTO, backfill: bool) -> bool:
        """处理单个页面上的所有楼中楼。

        主要流程：
        1. 对页面上的楼中楼进行去重处理，保存新楼中楼到数据库
        2. 非回溯模式下推送到消费者队列

        Args:
            pid: 回复ID。
            pn: 页码。
            comments_page: 该页面的评论数据。
        """
        all_cids_on_page = [c.cid for c in comments_page.objs]
        new_cids = await self.datastore.filter_new_ids("comment", all_cids_on_page)
        old_cids = set(all_cids_on_page) - new_cids

        await self._enqueue_missing_web_targets(
            getattr(self.container, "research_run_id", None),
            comment_ids=tuple(all_cids_on_page),
        )

        if not new_cids:
            self.log.debug("No new comments on pid={}, pn={}.", pid, pn)
            return False

        self.log.debug("pid={}, pn={}: Found {} new comments.", pid, pn, len(new_cids))

        new_comments_data = [c for c in comments_page.objs if c.cid in new_cids]

        await self.ensure_users(new_comments_data)

        new_comment_models = [Comment.from_dto(c) for c in new_comments_data]
        await self.datastore.save_items(new_comment_models)

        for comment in new_comments_data:
            SCRAPED_ITEMS.labels(type="comment", forum=comment.fname).inc(1)
            if not backfill:
                await self.datastore.push_object_event("comment", comment)
                self.log.debug("Pushed comment cid={} to object stream.", comment.cid)

        if old_cids:
            return False

        return True


class DeepScanTaskHandler(TaskHandler):
    """深度扫描任务处理器。

    用于补偿增量扫描无法覆盖的"深处"楼中楼更新。
    扫描主题贴的前 depth 页和后 depth 页回复，检查这些回复的楼中楼更新。
    """

    async def handle(self, task_content: DeepScanTask):
        """处理深度扫描任务。

        主要流程：
        1. 扫描前 depth 页（第 1 ~ depth 页）
        2. 扫描后 depth 页（最后 depth 页）
        3. 对每个页面上的回复，检查楼中楼是否有更新
        4. 如果发现楼中楼有更新，生成 IncrementalScanCommentsTask

        Args:
            task_content: DeepScanTask 实例，包含主题贴 tid 和扫描深度。
        """
        tid = task_content.tid
        depth = task_content.depth
        expected_new_comments = task_content.expected_new_comments

        try:
            total_pages = task_content.total_pages
            self.log.info(
                "DeepScan started for tid={}, total_pages={}, depth={}.",
                tid,
                total_pages,
                depth,
            )

            pages_to_scan = self._calculate_pages_to_scan(total_pages, depth)
            if not pages_to_scan:
                self.log.debug("DeepScan: No pages to scan for tid={}.", tid)
                return

            new_comments_found = await self._scan_pages_for_comments(tid, pages_to_scan, expected_new_comments)
            self.log.info(
                "DeepScan finished for tid={}. Found {} comment updates.",
                tid,
                new_comments_found,
            )
        except UnretriableApiError as e:
            if e.code == 4:
                self.log.warning(
                    "Thread tid={} may have been deleted (err_code=4). Stopping DeepScan.",
                    tid,
                )
            else:
                self.log.exception("DeepScan failed for tid={}: {}", tid, e)
        except Exception as e:
            self.log.exception("DeepScan failed for tid={}: {}", tid, e)

    def _calculate_pages_to_scan(self, total_pages: int, depth: int) -> list[int]:
        """计算需要扫描的页码列表。

        扫描前 depth 页和后 depth 页，去除重复并排除已被增量扫描覆盖的最后一页。

        Args:
            total_pages: 主题贴总页数
            depth: 扫描深度

        Returns:
            需要扫描的页码列表
        """
        front_pages = set(range(1, min(depth + 1, total_pages + 1)))
        back_pages = {total_pages - i for i in range(1, depth + 1) if total_pages - i > 0}

        return sorted(front_pages | back_pages)

    async def _scan_pages_for_comments(self, tid: int, pages: list[int], expected_new_comments: int) -> int:
        """扫描指定页码，检查回复的楼中楼更新。

        Args:
            tid: 主题贴 tid
            pages: 要扫描的页码列表
            expected_new_comments: 期望找到的新楼中楼数量，达到后提前停止

        Returns:
            发现的楼中楼更新数量
        """
        new_comments_found = 0

        for pn in pages:
            if self._stop_requested():
                return new_comments_found
            self.log.debug("DeepScan: Scanning tid={}, pn={}.", tid, pn)

            fetched = await self.get_posts(
                tid,
                pn,
                rn=30,
                with_comments=True,
                comment_sort_by_agree=False,
                comment_rn=10,
            )
            posts_page = self._dto(fetched.response)
            await self._record_validated_research_page("posts", pn, posts_page)
            await self._persist_research_page(fetched.response)
            if not posts_page or not posts_page.objs:
                self.log.warning("DeepScan: No posts found on tid={}, pn={}.", tid, pn)
                continue

            await self._enqueue_missing_web_targets(
                getattr(self.container, "research_run_id", None),
                thread_ids=(tid,),
                post_ids=tuple(post.pid for post in posts_page.objs if post.floor != 1),
            )

            updates = await self._check_posts_for_comment_updates(posts_page.objs)
            new_comments_found += updates

            if new_comments_found >= expected_new_comments:
                self.log.info(
                    "DeepScan: Found expected {} new comments for tid={}. Stopping early.",
                    expected_new_comments,
                    tid,
                )
                break

        return new_comments_found

    async def _check_posts_for_comment_updates(self, posts: list[PostDTO]) -> int:
        """检查回复列表中的楼中楼更新。

        先处理所有附带的楼中楼（可能存在删除+新增的边缘情况），
        再根据 reply_num 判断是否需要额外的增量扫描任务。

        Args:
            posts: 回复列表

        Returns:
            发现的新楼中楼数量
        """
        posts_with_comments = [p for p in posts if p.floor != 1 and p.reply_num > 0]

        if not posts_with_comments:
            return 0

        pids = [p.pid for p in posts_with_comments]
        stored_posts = await self.datastore.get_posts_by_pids(set(pids))
        stored_posts_map = {p.pid: p for p in stored_posts}

        updates_found = 0
        posts_to_update = []

        for post in posts_with_comments:
            stored_post = stored_posts_map.get(post.pid)

            if not stored_post:
                continue

            if post.comments:
                new_comments = await self._process_attached_comments(post)
                updates_found += new_comments

            if post.reply_num > stored_post.reply_num:
                self.log.debug(
                    "DeepScan: Post pid={} has comment updates. Reply count: {} -> {}",
                    post.pid,
                    stored_post.reply_num,
                    post.reply_num,
                )

                if self.research_enabled or post.reply_num > 10 or len(post.comments) != post.reply_num:
                    await self.datastore.add_pending_comment_scan(
                        tid=post.tid,
                        pid=post.pid,
                        fid=post.fid,
                        fname=post.fname,
                        backfill=False,
                        task_kind="full" if self.research_enabled else "incremental",
                    )
                    if self.research_enabled:
                        update_task = FullScanCommentsTask(
                            tid=post.tid,
                            pid=post.pid,
                            backfill=False,
                            parent_kind="reply_post",
                        )
                    else:
                        update_task = IncrementalScanCommentsTask(
                            tid=post.tid,
                            pid=post.pid,
                            backfill=False,
                        )
                    await self._enqueue_task_unless_stopping(Task(priority=Priority.MEDIUM, content=update_task))
                    self.log.info(
                        "DeepScan: Scheduled IncrementalScanCommentsTask for pid={} in tid={}.",
                        post.pid,
                        post.tid,
                    )

                posts_to_update.append(post)

        if posts_to_update:
            post_models = [Post.from_dto(p) for p in posts_to_update]
            await self.datastore.save_items(post_models, upsert=True)

        return updates_found

    async def _process_attached_comments(self, post: PostDTO) -> int:
        """处理回复附带的楼中楼。

        对回复的楼中楼进行去重处理，保存新楼中楼到数据库。

        Args:
            post: 包含楼中楼数据的回复对象

        Returns:
            新保存的楼中楼数量
        """
        new_comment_ids = await self.datastore.filter_new_ids("comment", [c.cid for c in post.comments])

        if not new_comment_ids:
            self.log.debug("DeepScan: No new comments for post pid={}.", post.pid)
            return 0

        self.log.debug("DeepScan: Post pid={}: Found {} new comments.", post.pid, len(new_comment_ids))
        new_comments = [c for c in post.comments if c.cid in new_comment_ids]

        await self.ensure_users(new_comments)

        new_comment_models = [Comment.from_dto(c) for c in new_comments]
        await self.datastore.save_items(new_comment_models)

        for comment in new_comments:
            SCRAPED_ITEMS.labels(type="comment", forum=comment.fname).inc(1)
            await self.datastore.push_object_event("comment", comment)

        return len(new_comments)


class Worker:
    """工作器类，负责处理任务队列中的任务。

    每个 Worker 从指定通道的 ``UniquePriorityQueue`` 中消费任务。
    多通道架构下，每条通道（threads / posts / comments）拥有独立的
    Worker 组，各自消耗对应 API 的限速配额。

    Attributes:
        worker_id: 工作器的唯一标识ID。
        lane: 通道名称（threads / posts / comments）。
        queue: 该通道的任务优先队列。
        container: 依赖注入容器。
        datastore: 数据存储层实例。
        log: 日志记录器。
        handlers: 任务类型到处理器的映射字典。
    """

    def __init__(
        self,
        worker_id: int,
        queue: UniquePriorityQueue,
        container: Container,
        datastore: DataStore,
        router: QueueRouter,
        *,
        lane: str = "unknown",
    ):
        """初始化 Worker。

        Args:
            worker_id: Worker 的唯一标识 ID。
            queue: 该通道的任务优先队列。
            container: 依赖注入容器。
            datastore: 数据存储层实例。
            router: 多通道队列路由器。
            lane: 通道名称，用于日志标识。
        """
        self.worker_id = worker_id
        self.lane = lane
        self.queue = queue
        self.container = container
        self.datastore = datastore
        self.log = logger.bind(worker_id=worker_id, lane=lane)
        self._stop_event = asyncio.Event()

        # 初始化任务处理器
        self.handlers = {
            ScanThreadsTask: ThreadsTaskHandler(worker_id, container, datastore, router, stop_event=self._stop_event),
            FullScanPostsTask: FullScanPostsTaskHandler(
                worker_id, container, datastore, router, stop_event=self._stop_event
            ),
            IncrementalScanPostsTask: IncrementalScanPostsTaskHandler(
                worker_id, container, datastore, router, stop_event=self._stop_event
            ),
            FullScanCommentsTask: FullScanCommentsTaskHandler(
                worker_id, container, datastore, router, stop_event=self._stop_event
            ),
            IncrementalScanCommentsTask: IncrementalScanCommentsTaskHandler(
                worker_id, container, datastore, router, stop_event=self._stop_event
            ),
            DeepScanTask: DeepScanTaskHandler(
                worker_id, container, datastore, router, stop_event=self._stop_event
            ),
        }

    def stop_after_current(self) -> None:
        """Finish the active API page, then stop before another request or task."""
        self._stop_event.set()

    async def _get_next_task(self) -> Task | None:
        if self._stop_event.is_set():
            return None
        queue_get = asyncio.create_task(self.queue.get())
        stop_wait = asyncio.create_task(self._stop_event.wait())
        task: Task | None = None
        transferred = False
        try:
            done, _pending = await asyncio.wait(
                {queue_get, stop_wait},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if queue_get in done:
                task = queue_get.result()
                if self._stop_event.is_set():
                    return None
                transferred = True
                return task
            return None
        finally:
            if not queue_get.done():
                queue_get.cancel()
            await asyncio.gather(queue_get, return_exceptions=True)
            if not transferred and not queue_get.cancelled():
                if task is None:
                    try:
                        task = queue_get.result()
                    except BaseException:
                        task = None
                if task is not None:
                    self.queue.task_done(task)
            if not stop_wait.done():
                stop_wait.cancel()
            await asyncio.gather(stop_wait, return_exceptions=True)

    async def run(self):
        """工作器主循环，持续从队列中获取并处理任务。"""
        self.log.info("Starting ({} lane)...", self.lane)
        ACTIVE_WORKERS.inc()

        try:
            while True:
                task: Task | None = None
                try:
                    task = await self._get_next_task()
                    if task is None:
                        break
                    self.log.debug(
                        "Got task: {} with priority {}.", task.content.__class__.__name__, task.priority.name
                    )

                    await self._process_task(task)

                except asyncio.CancelledError:
                    self.log.info("Cancelled. Exiting.")
                    break
                except Exception as e:
                    WORKER_ERRORS.labels(handler_type="unknown").inc()
                    self.log.exception("An unexpected error occurred in worker loop: {}", e)
                    await asyncio.sleep(1)
                finally:
                    if task is not None:
                        self.queue.task_done(task)
        finally:
            ACTIVE_WORKERS.dec()

    async def _process_task(self, task: Task):
        """处理单个任务。

        根据任务内容的类型选择相应的处理器进行处理。
        如果遇到未知的任务类型，会记录警告日志。

        Args:
            task: 待处理的任务对象。
        """
        content = task.content
        handler = self.handlers.get(type(content))

        if handler:
            task_type = type(content).__name__
            completion = getattr(content, "completion", None)
            with TASK_DURATION.labels(type=task_type).time():
                try:
                    api_stop_guard = getattr(getattr(self.container, "semaphore", None), "api_stop_guard", None)
                    if api_stop_guard is None:
                        result = await handler.handle(content)
                    else:
                        async with api_stop_guard(self._stop_event):
                            result = await handler.handle(content)
                except asyncio.CancelledError:
                    if completion is not None and not completion.done():
                        completion.cancel()
                    raise
                except Exception as exc:
                    if completion is not None and not completion.done():
                        completion.set_exception(exc)
                    WORKER_ERRORS.labels(handler_type=task_type).inc()
                    raise
                else:
                    if completion is not None and not completion.done():
                        completion.set_result(result)
        else:
            self.log.warning("Unknown task type: {}", type(content))
