"""任务调度器模块。

该模块实现了灵活的任务调度系统，支持两种调度模式：
1. 周期性调度：持续监控论坛首页，定期生成扫描任务
2. 历史回溯调度：仅投递回溯首页任务，由 Worker 递推后续页

使用单一类实现，内部通过不同的方法承载不同模式的逻辑，便于维护与扩展。
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from typing import TYPE_CHECKING, Literal

from tiebameow.utils.logger import logger

from ..core.metrics import BACKFILL_FORUMS_COUNT, PERIODIC_FORUMS_COUNT
from ..core.models import research_thread_list_task_key
from .tasks import Priority, ScanThreadsTask, Task, ThreadPageResult

if TYPE_CHECKING:
    from collections.abc import Callable

    from tiebameow.models.orm import Forum

    from ..core import Container
    from .router import QueueRouter


class BackfillIncompleteError(RuntimeError):
    """Raised when one or more backfill list streams could not be completed."""

    def __init__(self, failures: list[str]) -> None:
        self.failures = tuple(failures)
        super().__init__(f"Backfill stopped with {len(failures)} incomplete list stream(s): {'; '.join(failures)}")


class Scheduler:
    """任务调度器主类。

    提供统一的调度接口，根据指定的模式创建相应的调度策略。
    支持周期性模式和历史回溯模式的任务调度。

    Attributes:
        router: 多通道队列路由器。
        container: 依赖注入容器。
        log: 日志记录器。
    """

    def __init__(self, router: QueueRouter, container: Container, datastore=None):
        self.router = router
        self.container = container
        self.datastore = datastore
        self.log = logger.bind(name="Scheduler")
        self._stop_event = asyncio.Event()

    def stop(self):
        """请求调度器停止生成新任务。"""
        self._stop_event.set()
        self.log.info("Scheduler stop requested.")

    @property
    def is_stopped(self) -> bool:
        """检查调度器是否已停止。"""
        return self._stop_event.is_set()

    async def run(self, mode: Literal["periodic", "backfill"] = "periodic"):
        """根据不同模式生成任务并放入队列。

        Args:
            mode: 调度模式，支持 "periodic" 和 "backfill"。
        """
        if mode == "periodic":
            await self._run_periodic()
        elif mode == "backfill":
            await self._run_backfill()
        else:
            raise ValueError(f"Unknown scheduler mode: {mode}")

    def _get_forums_for_group(self, group_name: str) -> list[Forum]:
        """动态获取分组下的贴吧列表。"""
        current_forums = self.container.forums or []
        available_forums_map = {f.fname: f for f in current_forums}

        group = next((g for g in self.container.config.groups if g.name == group_name), None)
        if not group:
            return []

        return [available_forums_map[name] for name in group.forums if name in available_forums_map]

    async def _listen_for_redis_commands(self):
        """监听 Redis 指令 Stream。"""
        if not self.container.redis_client:
            return

        stream_in = self.container.config.redis_stream_request
        stream_out = self.container.config.redis_stream_response
        group_name = "tieba_scheduler_group"
        consumer_name = "scheduler_main"

        try:
            await self.container.redis_client.xgroup_create(stream_in, group_name, id="0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                self.log.warning("Error creating stream group: {}", e)

        self.log.info("[RedisListener] Listening on Stream {} (Group: {})", stream_in, group_name)

        while not self._stop_event.is_set():
            try:
                streams = {stream_in: ">"}
                events = await self.container.redis_client.xreadgroup(
                    group_name,
                    consumer_name,
                    streams,  # type: ignore
                    count=1,
                    block=1000,
                )

                if not events:
                    continue

                for _stream, messages in events:
                    for msg_id, fields in messages:
                        try:
                            payload_str = fields.get("payload")
                            if not payload_str:
                                self.log.warning("Received message without 'payload' field: {}", fields)
                                continue

                            data = json.loads(payload_str)
                            cmd_type = data.get("type")
                            req_id = data.get("id")

                            success = False
                            msg = "Unknown command"

                            if cmd_type == "add_forum":
                                fname = data.get("fname")
                                group = data.get("group")
                                if fname:
                                    self.log.info(
                                        "[RedisListener] Processing command: add_forum(fname={}, group={})",
                                        fname,
                                        group,
                                    )
                                    success = await self.container.add_forum(fname, group)
                                    msg = "Added successfully" if success else "Failed to add forum"
                                else:
                                    msg = "fname is required"

                            # 发送回执
                            if req_id:
                                response = {"ref_id": req_id, "ok": success, "msg": msg}
                                await self.container.redis_client.xadd(stream_out, {"payload": json.dumps(response)})

                        except Exception as e:
                            self.log.error("Error processing message {} content: {}", msg_id, e)
                        finally:
                            await self.container.redis_client.xack(stream_in, group_name, msg_id)

            except Exception as e:
                self.log.error("[RedisListener] Loop error: {}", e)
                await asyncio.sleep(1)

        self.log.info("[RedisListener] Stopped.")

    async def _run_periodic(self):
        """
        周期性调度任务生成器。

        根据配置的调度间隔，持续生成首页扫描任务，支持分组调度和默认调度。
        """
        log_interval = self.container.config.scheduler_interval_seconds
        logger.info("Starting PERIODIC mode. Default Interval: {}s", log_interval)

        tasks_to_schedule = []

        for group in self.container.config.groups:
            interval = group.interval_seconds or self.container.config.scheduler_interval_seconds
            logger.info("Starting Group '{}' loop. Interval: {}s.", group.name, interval)

            tasks_to_schedule.append(
                self._run_loop(
                    lambda g_name=group.name: self._get_forums_for_group(g_name),
                    interval,
                    f"Group-{group.name}",
                )
            )

        def get_default_forums() -> list[Forum]:
            """获取未分组的默认贴吧列表。"""
            all_forums = self.container.forums or []
            current_grouped_names = {name for g in self.container.config.groups for name in g.forums}
            return [f for f in all_forums if f.fname not in current_grouped_names]

        logger.info("Starting Default loop for non-grouped/dynamic forums.")
        tasks_to_schedule.append(
            self._run_loop(get_default_forums, self.container.config.scheduler_interval_seconds, "Default")
        )

        if self.container.redis_client:
            tasks_to_schedule.append(self._listen_for_redis_commands())

        if not tasks_to_schedule:
            logger.warning("No loops scheduled. check configuration.")
            return

        await asyncio.gather(*tasks_to_schedule)

    async def _run_loop(self, forums_getter: Callable[[], list[Forum]], interval: int, task_name: str):
        """通用调度循环。

        支持优雅停止：当 stop() 被调用后，循环会在当前迭代完成后退出。
        支持队列感知：当队列深度超过阈值时，跳过本轮调度。
        """
        good_every = self.container.config.good_page_every_ticks
        queue_threshold = self.container.config.queue_depth_threshold
        skip_wait = self.container.config.skip_wait_seconds
        tick = 0

        while not self._stop_event.is_set():
            forums = forums_getter()
            PERIODIC_FORUMS_COUNT.labels(group=task_name).set(len(forums))
            if not forums:
                if task_name == "Default":
                    logger.debug("[{}] No forums to scan. Sleeping.", task_name)
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                    break
                except TimeoutError:
                    pass
                continue

            # 队列感知调度：检查队列深度
            if queue_threshold > 0:
                current_depth = self.router.get_queue("threads").qsize()
                if current_depth >= queue_threshold:
                    logger.warning(
                        "[{}] Threads queue depth {} >= threshold {}. Skipping tick #{} and waiting {}s.",
                        task_name,
                        current_depth,
                        queue_threshold,
                        tick,
                        skip_wait,
                    )
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=skip_wait)
                        break
                    except TimeoutError:
                        pass
                    continue

            logger.debug(
                "[{}] tick #{}: Generating homepage scan tasks for {} forums.",
                task_name,
                tick,
                len(forums),
            )

            await self._schedule_homepage_scans(forums, is_good=False)

            if tick % good_every == 0:
                logger.debug("[{}] Extra GOOD-section scheduling.", task_name)
                await self._schedule_homepage_scans(forums, is_good=True)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                break
            except TimeoutError:
                pass

            tick += 1

        logger.info("[{}] Scheduler loop stopped.", task_name)

    async def _run_backfill(self):
        """历史回溯调度任务生成器。"""
        max_pages = self.container.config.max_backfill_pages
        logger.info("Starting BACKFILL mode (homepage kickoff, max_pages={}).", max_pages)
        forums = self.container.forums or []

        if not forums:
            logger.warning("No forums configured in tieba.forums. Scheduler will be idle.")
            return

        BACKFILL_FORUMS_COUNT.set(len(forums))

        # Backfill uses CREATE sort while the live monitor uses REPLY sort, so
        # the monitor's page 1 cannot stand in for CREATE page 1.
        start_page = 1
        streams = [(forum, is_good) for forum in forums for is_good in (False, True)]
        pages = {(forum.fid, is_good): start_page for forum, is_good in streams}
        cursors: dict[tuple[int, bool], dict[str, object]] = {key: {} for key in pages}
        run_id = getattr(self.container, "research_run_id", None)
        if run_id and self.datastore is not None:
            saved_states = await self.datastore.get_research_list_checkpoint_states(run_id)
            for key, (next_page, _scope_status, cursor) in saved_states.items():
                if key in pages:
                    pages[key] = next_page if next_page is not None else 0
                    cursors[key] = cursor or {}
        scanned = dict.fromkeys(pages, 0)
        recent_fingerprints = {key: deque(maxlen=3) for key in pages}
        for key, cursor in cursors.items():
            for fingerprint in cursor.get("recent_page_fingerprints", []):
                recent_fingerprints[key].append(tuple(fingerprint))
        failures: list[str] = []
        limited: list[str] = []
        active = [(forum, is_good) for forum, is_good in streams if pages[(forum.fid, is_good)] != 0]

        while active and not self._stop_event.is_set():
            for forum, is_good in tuple(active):
                key = (forum.fid, is_good)
                pn = pages[key]
                if max_pages > 0 and scanned[key] >= max_pages:
                    active.remove((forum, is_good))
                    continue

                try:
                    result = await self._scan_backfill_page(forum, pn, max_pages, is_good, cursors[key])
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    failure = f"forum={forum.fname} is_good={is_good} pn={pn} error={type(exc).__name__}"
                    failures.append(failure)
                    logger.error("Backfill list stream paused: {}", failure)
                    error_code = getattr(exc, "code", None)
                    pause_status = {4011: "paused_auth", 429: "paused_rate_limit", 220034: "blocked"}.get(error_code)
                    if run_id and self.datastore is not None:
                        await self.datastore.record_research_checkpoint(
                            run_id=run_id,
                            task_key=research_thread_list_task_key(forum.fid, is_good),
                            page_number=pn,
                            task_type="thread_list",
                            scope_type="thread_list",
                            sort_mode="create",
                            fid=forum.fid,
                            fname=forum.fname,
                            is_good=is_good,
                            status=pause_status or "failed",
                            scope_status=pause_status or "incomplete",
                            next_page=pn,
                            error_type=type(exc).__name__,
                            error_code=getattr(exc, "code", None),
                            reason=type(exc).__name__,
                            continuation_json=cursors[key],
                        )
                    if run_id and self.datastore is not None and error_code not in {4011, 429, 220034}:
                        await self.datastore.set_research_run_status_if(
                            run_id,
                            {"running", "paused_rate_limit"},
                            "incomplete",
                            type(exc).__name__,
                        )
                    active.remove((forum, is_good))
                    if error_code in {429, 220034}:
                        active.clear()
                        break
                    continue

                scanned[key] += 1
                if result.current_page != pn:
                    failure = (
                        f"forum={forum.fname} is_good={is_good} requested_page={pn} "
                        f"returned_page={result.current_page}"
                    )
                    failures.append(failure)
                    logger.error("Backfill page metadata mismatch: {}", failure)
                    active.remove((forum, is_good))
                    continue
                if result.total_pages < 0 or result.total_count < 0:
                    failure = f"forum={forum.fname} is_good={is_good} pn={pn} invalid_page_metadata"
                    failures.append(failure)
                    logger.error("Backfill page metadata is invalid: {}", failure)
                    active.remove((forum, is_good))
                    continue
                if not result.tids and (result.total_count > 0 or result.has_more):
                    failure = f"forum={forum.fname} is_good={is_good} pn={pn} empty_page_with_inconsistent_metadata"
                    failures.append(failure)
                    logger.error("Backfill page is empty but metadata indicates more data: {}", failure)
                    active.remove((forum, is_good))
                    continue
                if not result.tids and pn == start_page and (result.total_pages < 1 or result.total_count != 0):
                    failure = (
                        f"forum={forum.fname} is_good={is_good} pn={pn} "
                        "empty_page_without_confirmed_empty_metadata"
                    )
                    failures.append(failure)
                    logger.error("Backfill page is empty without confirmed terminal metadata: {}", failure)
                    active.remove((forum, is_good))
                    continue
                if result.has_more and result.total_pages > 0 and pn >= result.total_pages:
                    failure = f"forum={forum.fname} is_good={is_good} pn={pn} has_more_beyond_total_pages"
                    failures.append(failure)
                    logger.error("Backfill page metadata is inconsistent: {}", failure)
                    active.remove((forum, is_good))
                    continue
                if not result.has_more and result.total_pages > 0 and pn < result.total_pages:
                    failure = f"forum={forum.fname} is_good={is_good} pn={pn} terminal_page_before_total_pages"
                    failures.append(failure)
                    logger.error("Backfill page metadata is inconsistent: {}", failure)
                    active.remove((forum, is_good))
                    continue

                fingerprint = result.tids
                if result.has_more and fingerprint and fingerprint in recent_fingerprints[key]:
                    failure = f"forum={forum.fname} is_good={is_good} pn={pn} repeated_page_fingerprint"
                    failures.append(failure)
                    logger.error("Backfill stream repeated the same page: {}", failure)
                    active.remove((forum, is_good))
                    continue
                if fingerprint:
                    recent_fingerprints[key].append(fingerprint)

                page_times = result.non_top_create_times
                previous_oldest = cursors[key].get("last_create_time")
                if previous_oldest:
                    from datetime import datetime

                    previous_oldest = datetime.fromisoformat(str(previous_oldest))
                if page_times and previous_oldest is not None and page_times[0] > previous_oldest:
                    failure = f"forum={forum.fname} is_good={is_good} pn={pn} create_time_order_reversed"
                    failures.append(failure)
                    logger.error("Backfill CREATE order moved forward across pages: {}", failure)
                    active.remove((forum, is_good))
                    continue
                cursors[key] = result.continuation_json or cursors[key]

                if result.has_more:
                    if max_pages > 0 and scanned[key] >= max_pages:
                        limit_state = f"forum={forum.fname} is_good={is_good} status=paused_limit pages={max_pages}"
                        limited.append(limit_state)
                        logger.warning("Backfill stream paused at its configured page limit: {}", limit_state)
                        if run_id and self.datastore is not None:
                            await self.datastore.mark_research_checkpoint_scope_status(
                                run_id,
                                research_thread_list_task_key(forum.fid, is_good),
                                pn,
                                "paused_limit",
                            )
                        active.remove((forum, is_good))
                    else:
                        pages[key] += 1
                else:
                    active.remove((forum, is_good))

        if active and self._stop_event.is_set():
            logger.warning(
                "Backfill stopped before completing {} list stream(s); queued detail tasks remain recoverable.",
                len(active),
            )
        logger.info(
            "Backfill page producer stopped after scanning {} pages (paused_limit_streams={}).",
            sum(scanned.values()),
            len(limited),
        )
        if failures:
            if run_id and self.datastore is not None:
                await self.datastore.set_research_run_status_if(
                    run_id,
                    {"running"},
                    "incomplete",
                    "; ".join(failures),
                )
            raise BackfillIncompleteError(failures)
        if run_id and self.datastore is not None:
            if active and self._stop_event.is_set():
                await self.datastore.set_research_run_status_if(
                    run_id, {"running", "paused_rate_limit"}, "interrupted", "scheduler_stopped"
                )
            elif limited:
                await self.datastore.set_research_run_status_if(
                    run_id, {"running"}, "paused_limit", "; ".join(limited)
                )
            # List completion is not whole-run completion: detail work may
            # still be queued. The application reconciles all checkpoint
            # scopes after its workers drain.

    async def _scan_backfill_page(
        self,
        forum: Forum,
        pn: int,
        max_pages: int,
        is_good: bool,
        continuation_json: dict[str, object] | None = None,
    ) -> ThreadPageResult:
        """Queue one page and wait for its worker acknowledgement before continuing."""
        completion = asyncio.get_running_loop().create_future()
        task_content = ScanThreadsTask(
            fid=forum.fid,
            fname=forum.fname,
            pn=pn,
            backfill=True,
            max_pages=max_pages,
            is_good=is_good,
            force=self.container.config.backfill_force_scan,
            completion=completion,
            continuation_json=continuation_json,
        )
        task = Task(priority=Priority.BACKFILL_THREADS, content=task_content)
        try:
            await self.router.put(task)
            return await completion
        finally:
            if not completion.done():
                completion.cancel()

    async def _schedule_homepage_scans(self, forums: list[Forum], *, is_good: bool = False):
        """调度首页扫描任务（周期模式）。

        Args:
            forums: 需要扫描的贴吧列表。
        """
        for forum in forums:
            task_content = ScanThreadsTask(fid=forum.fid, fname=forum.fname, pn=1, is_good=is_good)
            task = Task(priority=Priority.REALTIME, content=task_content)
            await self.router.put(task)
            section = "GOOD" if is_good else "NORMAL"
            logger.debug("Scheduled {} homepage scan for [{}吧] with priority=REALTIME", section, forum.fname)
