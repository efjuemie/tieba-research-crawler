"""Tieba爬虫应用程序主入口模块。

该模块提供三种运行模式：
1. 周期性模式(periodic): 持续监控指定论坛的最新内容
2. 回溯模式(backfill): 抓取指定论坛的历史数据（仅投递首页，由 Worker 递推）
3. 混合模式(hybrid): 周期监控首页 + 一次性触发回溯首页

统一入口 main(mode) 根据模式分支启动相应组件。
"""

import asyncio
import platform
import signal
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, Literal

from prometheus_client import start_http_server
from tiebameow.utils.logger import init_logger, logger
from tiebameow.utils.time_utils import SHANGHAI_TZ

from src.core import DataStore, initialize_application
from src.core.models import REPLY_BACKFILL_MODE
from src.core.monitor import SystemMonitor
from src.research.web_fallback.live import (
    discover_missing_web_targets,
    pending_web_run_ids,
    prune_pre_launch_ip_web_tasks,
    prune_stale_web_tasks,
    run_live_web_worker,
)
from src.research.web_fallback.tasks import reset_web_worker_failure_tasks
from src.scraper import Scheduler, Worker
from src.scraper.router import QueueRouter
from src.scraper.tasks import FullScanCommentsTask, FullScanPostsTask, IncrementalScanCommentsTask, Priority, Task

init_logger(
    service_name="TiebaScraper",
    enable_error_filelog=True,
    diagnose=False,
)


async def _cancel_and_reap_tasks(tasks: list[asyncio.Task], aggregate: asyncio.Future | None = None) -> None:
    """Cancel internal tasks and consume both individual and aggregate results."""
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    if aggregate is not None:
        if not aggregate.done():
            aggregate.cancel()
        try:
            aggregate.result()
        except BaseException:
            pass


def _min_thread_create_time(min_thread_year: int | None) -> datetime | None:
    if min_thread_year is None:
        return None
    if min_thread_year < 1990 or min_thread_year > 2100:
        raise ValueError("--min-thread-year must be between 1990 and 2100.")
    return datetime(min_thread_year, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ)


def _parse_ip_location_min_date(value: str | None) -> datetime | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped or stripped.lower() in {"none", "off"}:
        return None
    try:
        parsed = datetime.strptime(stripped, "%Y-%m-%d")
    except ValueError as error:
        raise ValueError("--ip-location-min-date must use YYYY-MM-DD format or 'none'.") from error
    return parsed.replace(tzinfo=SHANGHAI_TZ)


async def _recover_pending_tasks(
    datastore: DataStore,
    router: QueueRouter,
    responsible_fids: set[int],
    *,
    research_run_id: str | None = None,
    min_thread_create_time: datetime | None = None,
    newest_first: bool = False,
    api_continue_older: bool = False,
    older_api_enabled: asyncio.Event | None = None,
) -> None:
    """启动时恢复未完成任务并重新入队。

    研究模式只从 run 的当前 task-scope 代际恢复；普通模式继续使用旧 pending 表。
    """
    if not responsible_fids:
        return

    if research_run_id is not None:
        adopt_legacy = getattr(datastore, "adopt_legacy_research_scopes", None)
        pending_scopes = getattr(datastore, "iter_pending_research_task_scopes", None)
        if adopt_legacy is None or pending_scopes is None:
            raise RuntimeError("Research task-scope recovery is unavailable in this DataStore.")

        await adopt_legacy(research_run_id, responsible_fids)
        recovered = 0
        extra_scope_kwargs: dict[str, Any] = (
            {"min_thread_create_time": min_thread_create_time} if min_thread_create_time is not None else {}
        )
        if newest_first:
            extra_scope_kwargs["newest_first"] = True
        for scope_type in ("thread_replies", "post_comments"):
            async for scope_batch in pending_scopes(
                research_run_id,
                responsible_fids,
                batch_size=500,
                scope_type=scope_type,
                **extra_scope_kwargs,
            ):
                recovered += await _enqueue_research_scope_batch(
                    router,
                    scope_batch,
                    responsible_fids,
                    research_run_id,
                )

        if recovered:
            logger.info("Recovered current research task scopes: {}", recovered)

        if api_continue_older and min_thread_create_time is not None:
            count_pending = getattr(datastore, "count_pending_research_task_scopes", None)
            if count_pending is not None:
                while True:
                    remaining_priority = await count_pending(
                        research_run_id,
                        responsible_fids,
                        min_thread_create_time=min_thread_create_time,
                    )
                    posts_empty = router.get_queue("posts").empty()
                    comments_empty = router.get_queue("comments").empty()
                    if remaining_priority == 0 and posts_empty and comments_empty:
                        break
                    await asyncio.sleep(2.0)

            logger.info(
                "All >= {} API detail scopes completed; continuing to older (< {}) pending scopes.",
                min_thread_create_time.isoformat(),
                min_thread_create_time.isoformat(),
            )
            if older_api_enabled is not None:
                older_api_enabled.set()
            older_scope_kwargs: dict[str, Any] = {"older_than_thread_create_time": min_thread_create_time}
            if newest_first:
                older_scope_kwargs["newest_first"] = True
            older_recovered = 0
            for scope_type in ("thread_replies", "post_comments"):
                async for scope_batch in pending_scopes(
                    research_run_id,
                    responsible_fids,
                    batch_size=500,
                    scope_type=scope_type,
                    **older_scope_kwargs,
                ):
                    older_recovered += await _enqueue_research_scope_batch(
                        router,
                        scope_batch,
                        responsible_fids,
                        research_run_id,
                    )
            if older_recovered:
                logger.info("Enqueued older (< {}) research task scopes: {}", min_thread_create_time.isoformat(), older_recovered)
        return

    recovered_threads = 0
    recovered_comments = 0

    async for pending_batch in _pending_scan_batches(datastore, "thread", responsible_fids):
        for pending in pending_batch:
            task = Task(
                priority=Priority.BACKFILL_POSTS if pending.backfill else Priority.MEDIUM,
                content=FullScanPostsTask(tid=pending.tid, backfill=pending.backfill),
            )
            await router.put(task)
            recovered_threads += 1

    async for pending_batch in _pending_scan_batches(datastore, "comment", responsible_fids):
        for pending in pending_batch:
            if pending.task_kind == "incremental":
                content = IncrementalScanCommentsTask(tid=pending.tid, pid=pending.pid, backfill=pending.backfill)
                priority = Priority.BACKFILL_POSTS if pending.backfill else Priority.MEDIUM
            else:
                content = FullScanCommentsTask(tid=pending.tid, pid=pending.pid, backfill=pending.backfill)
                priority = Priority.BACKFILL_POSTS if pending.backfill else Priority.LOW

            await router.put(Task(priority=priority, content=content))
            recovered_comments += 1

    if recovered_threads or recovered_comments:
        logger.info(
            "Recovered pending tasks on startup: threads={}, comments={}",
            recovered_threads,
            recovered_comments,
        )


async def _enqueue_research_scope_batch(
    router: QueueRouter,
    scopes: list[Any],
    responsible_fids: set[int],
    research_run_id: str,
) -> int:
    """Enqueue only durable research detail scopes from the requested run."""
    enqueued = 0
    for scope in scopes:
        if scope.status == "completed" or scope.fid not in responsible_fids:
            continue
        if scope.tid is None:
            raise ValueError(f"Research task scope {scope.logical_task_key!r} has no thread ID.")

        if scope.scope_type == "thread_replies" and scope.task_type == "full_scan_posts":
            content = FullScanPostsTask(
                tid=int(scope.tid),
                rn=scope.rn,
                backfill=scope.backfill,
                run_id=research_run_id,
                generation=scope.generation,
                fid=scope.fid,
                fname=scope.fname,
            )
            priority = Priority.BACKFILL_POSTS if scope.backfill else Priority.MEDIUM
        elif scope.scope_type == "post_comments" and scope.task_type == "full_scan_comments":
            if scope.parent_pid is None:
                raise ValueError(f"Research comment scope {scope.logical_task_key!r} has no parent post ID.")
            content = FullScanCommentsTask(
                tid=int(scope.tid),
                pid=int(scope.parent_pid),
                backfill=scope.backfill,
                parent_kind=scope.parent_kind,
                run_id=research_run_id,
                generation=scope.generation,
                parent_generation=scope.parent_generation,
                fid=scope.fid,
                fname=scope.fname,
                rn=scope.rn,
            )
            priority = Priority.BACKFILL_POSTS if scope.backfill else Priority.LOW
        else:
            raise ValueError(
                "Unsupported current research task scope: "
                f"scope_type={scope.scope_type!r}, task_type={scope.task_type!r}."
            )

        await router.put(Task(priority=priority, content=content))
        enqueued += 1
    return enqueued


async def _dispatch_pending_comment_scopes(
    datastore: DataStore,
    router: QueueRouter,
    responsible_fids: set[int],
    research_run_id: str,
    *,
    producers_done: asyncio.Event | None = None,
    selected_comment_scopes: set[tuple[str, int]] | None = None,
    selected_thread_scopes: set[tuple[str, int]] | None = None,
    min_thread_create_time: datetime | None = None,
    newest_first: bool = False,
    older_api_enabled: asyncio.Event | None = None,
) -> bool:
    """Feed durable pending comment scopes into the bounded comments queue."""
    dispatched: set[tuple[str, int]] = set()
    extra_scope_kwargs: dict[str, Any] = (
        {"min_thread_create_time": min_thread_create_time} if min_thread_create_time is not None else {}
    )
    if newest_first:
        extra_scope_kwargs["newest_first"] = True

    async def read_pending() -> list[Any]:
        scopes: list[Any] = []
        passes: list[dict[str, Any]] = [extra_scope_kwargs]
        if (
            min_thread_create_time is not None
            and older_api_enabled is not None
            and older_api_enabled.is_set()
        ):
            older_kwargs = {"older_than_thread_create_time": min_thread_create_time}
            if newest_first:
                older_kwargs["newest_first"] = True
            passes.append(older_kwargs)

        for pass_kwargs in passes:
            async for batch in datastore.iter_pending_research_task_scopes(
                research_run_id,
                responsible_fids,
                batch_size=500,
                scope_type="post_comments",
                status="pending",
                **pass_kwargs,
            ):
                for scope in batch:
                    key = (scope.logical_task_key, scope.generation)
                    parent_key = (scope.parent_logical_task_key, scope.parent_generation)
                    if selected_comment_scopes is not None or selected_thread_scopes is not None:
                        if key not in (selected_comment_scopes or set()) and parent_key not in (
                            selected_thread_scopes or set()
                        ):
                            continue
                    scopes.append(scope)
        return scopes

    while True:
        pending = await read_pending()
        newly_dispatched = 0
        for scope in pending:
            key = (scope.logical_task_key, scope.generation)
            if key in dispatched:
                continue
            if scope.tid is None or scope.parent_pid is None:
                raise ValueError(f"Research comment scope {scope.logical_task_key!r} has incomplete identity.")
            content = FullScanCommentsTask(
                tid=int(scope.tid),
                pid=int(scope.parent_pid),
                backfill=scope.backfill,
                parent_kind=scope.parent_kind,
                run_id=research_run_id,
                generation=scope.generation,
                parent_generation=scope.parent_generation,
                fid=scope.fid,
                fname=scope.fname,
                rn=scope.rn,
                pending_only=True,
            )
            priority = Priority.BACKFILL_POSTS if scope.backfill else Priority.LOW
            task = Task(priority=priority, content=content)
            try:
                router.put_nowait(task)
            except asyncio.QueueFull:
                await router.put(task)
            dispatched.add(key)
            newly_dispatched += 1
        if newly_dispatched:
            logger.info("Dispatched {} durable comment scopes to API workers.", newly_dispatched)

        if producers_done is None:
            await asyncio.sleep(1)
            continue
        if not producers_done.is_set():
            try:
                await asyncio.wait_for(producers_done.wait(), timeout=1)
            except TimeoutError:
                continue
            continue

        await router.wait_idle()
        pending = await read_pending()
        undispatched = [scope for scope in pending if (scope.logical_task_key, scope.generation) not in dispatched]
        if undispatched:
            continue
        if producers_done is not None and producers_done.is_set():
            return not pending

        try:
            if producers_done is None:
                await asyncio.sleep(1)
            else:
                await asyncio.wait_for(producers_done.wait(), timeout=1)
        except TimeoutError:
            continue


async def _snapshot_pending_research_scopes(
    datastore: DataStore,
    run_id: str,
    responsible_fids: set[int],
    *,
    tid: int | None,
    limit: int,
) -> list[Any]:
    """Freeze the initial selection before workers can create child scopes."""
    scopes: list[Any] = []
    async for scope_batch in datastore.iter_pending_research_task_scopes(
        run_id,
        responsible_fids,
        batch_size=min(500, limit),
        tid=tid,
        limit=limit,
    ):
        scopes.extend(scope_batch)
    return scopes


async def _pending_scan_batches(
    datastore: DataStore,
    kind: Literal["thread", "comment"],
    fids: set[int],
) -> AsyncIterator[list[Any]]:
    """Yield bounded pending-scan batches, with compatibility for older stores."""
    if not fids:
        return
    iterator_name = "iter_pending_thread_scans" if kind == "thread" else "iter_pending_comment_scans"
    if hasattr(datastore, iterator_name):
        async for batch in getattr(datastore, iterator_name)(fids, batch_size=500):
            yield batch
        return

    loader_name = "get_all_pending_thread_scans" if kind == "thread" else "get_all_pending_comment_scans"
    pending = await getattr(datastore, loader_name)()
    scoped = [item for item in pending if item.fid in fids]
    if scoped:
        yield scoped


async def _stop_scheduler_tasks(scheduler: Scheduler, scheduler_tasks: list[asyncio.Task]) -> None:
    scheduler.stop()
    for task in scheduler_tasks:
        if not task.done():
            task.cancel()
    if scheduler_tasks:
        await asyncio.gather(*scheduler_tasks, return_exceptions=True)


async def _stop_api_workers(workers: list[Worker], worker_tasks: list[asyncio.Task]) -> None:
    for worker in workers:
        stop = getattr(worker, "stop_after_current", None)
        if stop is not None:
            stop()
    for index, task in enumerate(worker_tasks):
        worker = workers[index] if index < len(workers) else None
        if worker is None or not hasattr(worker, "stop_after_current"):
            if not task.done():
                task.cancel()
    if worker_tasks:
        await asyncio.gather(*worker_tasks, return_exceptions=True)


def _report_web_worker_exit(web_worker_task: asyncio.Task) -> None:
    try:
        summary = web_worker_task.result()
    except asyncio.CancelledError:
        logger.info("WEB fallback worker was stopped; API production continues independently.")
    except Exception as error:
        logger.warning(
            "WEB fallback worker exited with {} while API production continues.",
            type(error).__name__,
        )
    else:
        logger.info(
            "WEB fallback worker exited reason={} run_id={} target_type={} run_ids={} "
            "completed={} retryable={} blocked={} unresolved={}; API production continues.",
            getattr(summary, "stopped_reason", None),
            getattr(summary, "stopped_run_id", None),
            getattr(summary, "stopped_target_type", None),
            getattr(summary, "run_ids", "unknown"),
            getattr(summary, "completed_count", "unknown"),
            getattr(summary, "retryable_count", "unknown"),
            getattr(summary, "blocked_count", "unknown"),
            getattr(summary, "unresolved_count", "unknown"),
        )


def _cancel_recovery_task(recovery_task: asyncio.Task | None) -> None:
    if recovery_task is not None and not recovery_task.done():
        recovery_task.cancel()


async def _run_periodic_mode(
    scheduler: Scheduler,
    workers: list[Worker],
    router: QueueRouter,
    worker_tasks: list[asyncio.Task] | None = None,
    *,
    web_worker_task: asyncio.Task | None = None,
    web_stop_event: asyncio.Event | None = None,
    recovery_task: asyncio.Task | None = None,
    comment_dispatch_task: asyncio.Task | None = None,
    on_api_stopped: Any | None = None,
    on_api_interrupted: Any | None = None,
) -> None:
    """运行周期模式； cooperative stop leaves queued API scopes for recovery."""
    tasks: list[asyncio.Task] = []

    scheduler_task = asyncio.create_task(scheduler.run(mode="periodic"), name="scheduler")
    tasks.append(scheduler_task)
    if worker_tasks is None:
        worker_tasks = [asyncio.create_task(w.run(), name=f"worker-{w.lane}-{w.worker_id}") for w in workers]
    tasks.extend(worker_tasks)
    if recovery_task is not None:
        tasks.append(recovery_task)
    if comment_dispatch_task is not None:
        tasks.append(comment_dispatch_task)

    aggregate = asyncio.gather(*tasks)
    try:
        await asyncio.shield(aggregate)
        if on_api_stopped is not None:
            await on_api_stopped()
    except asyncio.CancelledError:
        logger.info("Received shutdown signal. Starting graceful shutdown...")
        await _stop_scheduler_tasks(scheduler, [scheduler_task])
        _cancel_recovery_task(recovery_task)
        if comment_dispatch_task is not None and not comment_dispatch_task.done():
            comment_dispatch_task.cancel()
            await asyncio.gather(comment_dispatch_task, return_exceptions=True)
        await _stop_api_workers(workers, worker_tasks)
        if on_api_interrupted is not None:
            await on_api_interrupted()
        raise
    except Exception:
        await _stop_scheduler_tasks(scheduler, [scheduler_task])
        _cancel_recovery_task(recovery_task)
        if comment_dispatch_task is not None and not comment_dispatch_task.done():
            comment_dispatch_task.cancel()
            await asyncio.gather(comment_dispatch_task, return_exceptions=True)
        await _stop_api_workers(workers, worker_tasks)
        if on_api_interrupted is not None:
            await on_api_interrupted()
        raise
    finally:
        await _cancel_and_reap_tasks(tasks, aggregate)


async def _run_hybrid_mode(
    scheduler: Scheduler,
    workers: list[Worker],
    router: QueueRouter,
    worker_tasks: list[asyncio.Task] | None = None,
    *,
    web_worker_task: asyncio.Task | None = None,
    web_stop_event: asyncio.Event | None = None,
    recovery_task: asyncio.Task | None = None,
    comment_dispatch_task: asyncio.Task | None = None,
    on_api_stopped: Any | None = None,
    on_api_interrupted: Any | None = None,
) -> None:
    """Run periodic and backfill producers with one cooperative drain path."""
    tasks: list[asyncio.Task] = []

    periodic_task = asyncio.create_task(scheduler.run(mode="periodic"), name="scheduler-periodic")
    tasks.append(periodic_task)

    backfill_task = asyncio.create_task(scheduler.run(mode="backfill"), name="scheduler-backfill")
    tasks.append(backfill_task)

    if worker_tasks is None:
        worker_tasks = [asyncio.create_task(w.run(), name=f"worker-{w.lane}-{w.worker_id}") for w in workers]
    tasks.extend(worker_tasks)
    if recovery_task is not None:
        tasks.append(recovery_task)
    if comment_dispatch_task is not None:
        tasks.append(comment_dispatch_task)

    aggregate = asyncio.gather(*tasks)
    try:
        await asyncio.shield(aggregate)
        if on_api_stopped is not None:
            await on_api_stopped()
    except asyncio.CancelledError:
        logger.info("Received shutdown signal in hybrid mode. Starting graceful shutdown...")
        await _stop_scheduler_tasks(scheduler, [periodic_task, backfill_task])
        _cancel_recovery_task(recovery_task)
        if comment_dispatch_task is not None and not comment_dispatch_task.done():
            comment_dispatch_task.cancel()
            await asyncio.gather(comment_dispatch_task, return_exceptions=True)
        await _stop_api_workers(workers, worker_tasks)
        if on_api_interrupted is not None:
            await on_api_interrupted()
        raise
    except Exception:
        await _stop_scheduler_tasks(scheduler, [periodic_task, backfill_task])
        _cancel_recovery_task(recovery_task)
        if comment_dispatch_task is not None and not comment_dispatch_task.done():
            comment_dispatch_task.cancel()
            await asyncio.gather(comment_dispatch_task, return_exceptions=True)
        await _stop_api_workers(workers, worker_tasks)
        if on_api_interrupted is not None:
            await on_api_interrupted()
        raise
    finally:
        await _cancel_and_reap_tasks(tasks, aggregate)


async def _run_backfill_mode(
    scheduler: Scheduler,
    workers: list[Worker],
    router: QueueRouter,
    worker_tasks: list[asyncio.Task],
    *,
    web_worker_task: asyncio.Task | None,
    web_stop_event: asyncio.Event | None = None,
    recovery_task: asyncio.Task | None = None,
    comment_dispatch_task: asyncio.Task | None = None,
    producers_done: asyncio.Event | None = None,
    on_api_stopped: Any | None,
    on_api_interrupted: Any | None = None,
) -> None:
    scheduler_task = asyncio.create_task(scheduler.run(mode="backfill"), name="scheduler")

    async def wait_for_api_drain() -> None:
        if comment_dispatch_task is None or producers_done is None:
            await scheduler_task
            if recovery_task is not None:
                await recovery_task
            await router.wait_idle()
            return

        producers: set[asyncio.Task] = {scheduler_task}
        if recovery_task is not None:
            producers.add(recovery_task)
        while producers:
            watched = {*producers, comment_dispatch_task}
            done, _pending = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
            if comment_dispatch_task in done and any(task not in done for task in producers):
                comment_dispatch_task.result()
                raise RuntimeError("Comment dispatcher exited before API producers finished.")
            for task in done:
                if task in producers:
                    task.result()
                    producers.discard(task)
        producers_done.set()
        if not await comment_dispatch_task:
            raise RuntimeError("Comment dispatcher did not drain all pending API scopes.")

    api_drain_task = asyncio.create_task(wait_for_api_drain(), name="api-backfill-drain")
    try:
        await api_drain_task
        await _stop_api_workers(workers, worker_tasks)
        if on_api_stopped is not None:
            await on_api_stopped()
    except asyncio.CancelledError:
        await _stop_scheduler_tasks(scheduler, [scheduler_task])
        if not api_drain_task.done():
            api_drain_task.cancel()
            await asyncio.gather(api_drain_task, return_exceptions=True)
        _cancel_recovery_task(recovery_task)
        if comment_dispatch_task is not None and not comment_dispatch_task.done():
            comment_dispatch_task.cancel()
            await asyncio.gather(comment_dispatch_task, return_exceptions=True)
        await _stop_api_workers(workers, worker_tasks)
        if on_api_interrupted is not None:
            await on_api_interrupted()
        raise
    except Exception:
        await _stop_scheduler_tasks(scheduler, [scheduler_task])
        if not api_drain_task.done():
            api_drain_task.cancel()
            await asyncio.gather(api_drain_task, return_exceptions=True)
        _cancel_recovery_task(recovery_task)
        if comment_dispatch_task is not None and not comment_dispatch_task.done():
            comment_dispatch_task.cancel()
            await asyncio.gather(comment_dispatch_task, return_exceptions=True)
        await _stop_api_workers(workers, worker_tasks)
        if on_api_interrupted is not None:
            await on_api_interrupted()
        raise
    finally:
        if not api_drain_task.done():
            api_drain_task.cancel()
            await asyncio.gather(api_drain_task, return_exceptions=True)
        if recovery_task is not None and not recovery_task.done():
            recovery_task.cancel()
            await asyncio.gather(recovery_task, return_exceptions=True)
        if comment_dispatch_task is not None and not comment_dispatch_task.done():
            comment_dispatch_task.cancel()
            await asyncio.gather(comment_dispatch_task, return_exceptions=True)
        if not scheduler_task.done():
            scheduler_task.cancel()
        await asyncio.gather(scheduler_task, return_exceptions=True)


async def _prepare_research_run(
    container,
    datastore: DataStore,
    mode: Literal["periodic", "backfill", "hybrid"],
    *,
    backfill_sort: Literal["create", "reply"] = "create",
    resume: bool,
    run_id: str | None,
) -> str | None:
    config = container.config
    if backfill_sort not in {"create", "reply"}:
        raise ValueError(f"Unsupported backfill list sort: {backfill_sort}")
    if backfill_sort == "reply" and mode != "backfill":
        raise ValueError("--backfill-sort reply can only be used with --mode backfill.")
    if backfill_sort == "reply" and not getattr(config, "research_enabled", False):
        raise ValueError("--backfill-sort reply requires research.enabled=true.")
    if not getattr(config, "research_enabled", False):
        if resume or run_id is not None:
            raise ValueError("--resume and --run-id require research.enabled=true.")
        return None
    if run_id is not None and not resume:
        raise ValueError("--run-id requires --resume; omit it to start a new research run.")

    forums = [(int(forum.fid), str(forum.fname)) for forum in (container.forums or [])]
    if mode in {"backfill", "hybrid"} and not forums:
        raise ValueError(f"Research {mode} requires at least one initialized forum.")

    active_run_id = await datastore.start_research_run(
        mode=REPLY_BACKFILL_MODE if backfill_sort == "reply" else mode,
        forums=forums,
        max_backfill_pages=config.max_backfill_pages,
        resume=resume,
        run_id=run_id,
    )
    container.research_run_id = active_run_id
    research_run_cache = getattr(container, "research_run_cache", None)
    if research_run_cache is not None:
        research_run_cache.clear()
    logger.info("Research run started: {}", active_run_id)

    await _prepare_research_auth(container, datastore, active_run_id)
    return active_run_id


async def _prepare_research_auth(container, datastore: DataStore, active_run_id: str) -> None:
    config = container.config

    if not getattr(config, "fetch_user_forum_info", False):
        auth_ready = getattr(container, "research_auth_ready", None)
        if auth_ready is not None:
            auth_ready.set()
        return

    auth_ready = getattr(container, "research_auth_ready", None)
    if auth_ready is not None:
        auth_ready.clear()
    from src.core.auth import ResearchAuthError

    try:
        await container.validate_research_auth()
    except ResearchAuthError as error:
        await datastore.set_research_run_status(active_run_id, error.status, error.reason)
        if error.status != "paused_auth":
            raise

        account_file = config.research_account_file
        logger.warning(
            "Research run paused for local authentication. Complete Baidu QR login with "
            "`python -m src.research login --browser --account-file {}`, then this process will resume.",
            account_file,
        )
        try:
            await container.wait_for_initial_auth()
        except ResearchAuthError as wait_error:
            await datastore.set_research_run_status(active_run_id, wait_error.status, wait_error.reason)
            raise
        await datastore.resume_research_run_if_status(active_run_id, "paused_auth")


async def drain_pending(
    *,
    run_id: str,
    tid: int | None = None,
    limit: int = 10,
    max_seconds: int = 120,
) -> int:
    """Drain existing detail scopes for one run without scheduling thread-list requests.

    ``limit`` applies to the initial set of durable scopes. Child comment scopes
    created by a selected thread scope remain part of that work. A zero
    ``max_seconds`` disables the deadline.
    """
    if not run_id:
        raise ValueError("drain-pending requires an existing --run-id.")
    if tid is not None and tid <= 0:
        raise ValueError("--tid must be a positive integer.")
    if limit < 1:
        raise ValueError("--limit must be a positive integer.")
    if max_seconds < 0:
        raise ValueError("--max-seconds must be zero or a positive integer.")

    container, router = await initialize_application(mode="backfill")
    shutdown_event = asyncio.Event()
    restore_signal_handlers = _install_shutdown_signal_handlers(shutdown_event)
    datastore: DataStore | None = None
    workers: list[Worker] = []
    worker_tasks: list[asyncio.Task] = []
    web_worker_task: asyncio.Task | None = None
    web_stop_event: asyncio.Event | None = None
    api_done_event: asyncio.Event | None = None
    web_run_ids: tuple[str, ...] = ()
    active_run_id: str | None = None
    api_drain_task: asyncio.Task | None = None
    api_monitor_task: asyncio.Task | None = None
    comment_dispatch_task: asyncio.Task | None = None
    comment_producers_done: asyncio.Event | None = None
    web_discovery_task: asyncio.Task | None = None
    web_finished = False
    web_summary: Any | None = None
    cancelled = False
    initial_run_status: str | None = None
    web_discovered_count = 0
    web_lifecycle_error = False
    comment_dispatch_complete = True

    async def start_web_worker(current_run_id: str) -> None:
        nonlocal web_worker_task, web_stop_event, api_done_event, web_run_ids, web_discovery_task
        responsible_forums = tuple((int(forum.fid), str(forum.fname)) for forum in (container.forums or []))
        web_run_ids = await pending_web_run_ids(
            datastore.async_sessionmaker,
            current_run_id,
            responsible_forums=responsible_forums,
        )
        web_stop_event = asyncio.Event()
        api_done_event = asyncio.Event()
        web_config = container.config.pydantic_config.web_fallback
        web_worker_task = asyncio.create_task(
            run_live_web_worker(
                datastore.async_sessionmaker,
                web_run_ids,
                stop_event=web_stop_event,
                api_done_event=api_done_event,
                browser_profile=web_config.browser_profile,
                browser_config=web_config,
            ),
            name="web-fallback-worker",
        )
        web_worker_task.add_done_callback(_report_web_worker_exit)
        web_discovery_task = asyncio.create_task(
            discover_missing_web_targets(datastore.async_sessionmaker, web_run_ids),
            name="web-startup-discovery",
        )

    async def finish_web_after_api_stopped() -> Any | None:
        nonlocal web_finished, web_summary, cancelled, web_discovered_count
        if workers:
            await _stop_api_workers(workers, worker_tasks)
        if web_worker_task is None or web_stop_event is None or api_done_event is None or web_finished:
            return web_summary

        async def complete_api_then_web_drain() -> Any | None:
            nonlocal web_finished, web_summary, web_discovered_count, web_lifecycle_error
            discovery_error: Exception | None = None
            web_error: Exception | None = None
            if web_discovery_task is not None:
                try:
                    web_discovered_count = await asyncio.shield(web_discovery_task)
                except Exception as error:
                    discovery_error = error
                    logger.exception("WEB startup discovery failed: {}", error)
            try:
                await discover_missing_web_targets(datastore.async_sessionmaker, web_run_ids)
            except Exception as error:
                discovery_error = discovery_error or error
                logger.exception("Could not rescan stored API metadata before WEB shutdown: {}", error)

            api_done_event.set()
            try:
                web_summary = await asyncio.shield(web_worker_task)
            except asyncio.CancelledError:
                if not web_worker_task.cancelled():
                    raise
                web_finished = True
            except Exception as error:
                web_error = error
                logger.warning("WEB fallback worker failed after API completion: {}", type(error).__name__)
                web_finished = True
            else:
                web_finished = True

            unresolved_count = getattr(web_summary, "unresolved_count", 0)
            if (
                unresolved_count
                or discovery_error is not None
                or web_error is not None
                or web_summary is None
                or not comment_dispatch_complete
            ):
                web_lifecycle_error = True
                await datastore.set_research_run_status_if(
                    run_id,
                    {"running", "completed", "incomplete"},
                    "incomplete",
                    "web_fallback_unresolved_tasks"
                    if unresolved_count
                    else "web_fallback_worker_error"
                    if web_error is not None or web_summary is None
                    else "pending_comment_scopes_not_drained"
                    if not comment_dispatch_complete
                    else "web_fallback_discovery_error",
                )
            else:
                if (
                    initial_run_status == "completed"
                    and comment_dispatch_complete
                    and web_discovered_count == 0
                    and not getattr(web_summary, "collected_count", 0)
                    and not getattr(web_summary, "completed_count", 0)
                    and not getattr(web_summary, "partial_count", 0)
                ):
                    return web_summary
                status = await datastore.reconcile_research_run_status_after_drain(run_id)
                logger.info("drain-pending finished; run status is {}.", status)
            return web_summary

        finish_task = asyncio.create_task(
            complete_api_then_web_drain(),
            name="web-api-drain-finish",
        )
        shutdown_wait = asyncio.create_task(shutdown_event.wait(), name="drain-web-stop-wait")
        try:
            done, _pending = await asyncio.wait(
                {finish_task, shutdown_wait},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if shutdown_wait in done:
                cancelled = True
                web_stop_event.set()
                finish_task.cancel()
                await asyncio.gather(finish_task, return_exceptions=True)
                await stop_web_for_interruption()
                return web_summary
            return await finish_task
        except asyncio.CancelledError:
            web_stop_event.set()
            if not finish_task.done():
                finish_task.cancel()
                await asyncio.gather(finish_task, return_exceptions=True)
            raise
        finally:
            if not shutdown_wait.done():
                shutdown_wait.cancel()
            await asyncio.gather(shutdown_wait, return_exceptions=True)

    async def stop_web_for_interruption() -> Any | None:
        nonlocal web_finished, web_summary
        if web_discovery_task is not None and not web_discovery_task.done():
            web_discovery_task.cancel()
            await asyncio.gather(web_discovery_task, return_exceptions=True)
        if web_worker_task is None or web_stop_event is None or web_finished:
            return web_summary
        web_stop_event.set()
        try:
            web_summary = await asyncio.shield(web_worker_task)
        except asyncio.CancelledError:
            if not web_worker_task.cancelled():
                raise
        except Exception as error:
            logger.warning("WEB fallback worker stopped with {} during interruption.", type(error).__name__)
        finally:
            web_finished = web_worker_task.done()
        return web_summary

    try:
        datastore = DataStore(container)
        if not getattr(container.config, "research_enabled", False):
            raise ValueError("drain-pending requires research.enabled=true.")
        forums = [(int(forum.fid), str(forum.fname)) for forum in (container.forums or [])]
        if not forums:
            raise ValueError("Research backfill requires at least one initialized forum.")
        run_status = await datastore.load_research_run_for_drain(run_id, forums)
        initial_run_status = run_status
        responsible_fids = {int(forum.fid) for forum in (container.forums or [])}
        selected = await _snapshot_pending_research_scopes(
            datastore,
            run_id,
            responsible_fids,
            tid=tid,
            limit=limit,
        )

        if selected:
            if not await datastore.resume_research_run_for_drain(run_id, run_status):
                raise RuntimeError(f"Research run {run_id} could not be activated from status {run_status!r}.")
            active_run_id = run_id
            container.research_run_id = active_run_id
            research_run_cache = getattr(container, "research_run_cache", None)
            if research_run_cache is not None:
                research_run_cache.clear()
            activation_reason = "drain_reopened_completed_run" if run_status == "completed" else None
            logger.info(
                "Research run selected for drain: {} (previous status={}, reason={}).",
                active_run_id,
                run_status,
                activation_reason,
            )
            await _prepare_research_auth(container, datastore, active_run_id)
        else:
            active_run_id = run_id
            container.research_run_id = active_run_id
            logger.info("No pending API detail scopes selected; draining stored WEB metadata for run {}.", run_id)

        await start_web_worker(active_run_id)

        if selected:
            worker_id = 0
            for lane, count in (
                ("posts", container.config.worker_posts),
                ("comments", container.config.worker_comments),
            ):
                for _ in range(count):
                    workers.append(
                        Worker(
                            worker_id,
                            router.get_queue(lane),
                            container,
                            datastore,
                            router,
                            lane=lane,
                        )
                    )
                    worker_id += 1
            worker_tasks = [
                asyncio.create_task(worker.run(), name=f"worker-{worker.lane}-{worker.worker_id}")
                for worker in workers
            ]
            comment_producers_done = asyncio.Event()
            selected_comment_scopes = {
                (scope.logical_task_key, scope.generation)
                for scope in selected
                if scope.scope_type == "post_comments"
            }
            selected_thread_scopes = {
                (scope.logical_task_key, scope.generation)
                for scope in selected
                if scope.scope_type == "thread_replies"
            }
            comment_dispatch_task = asyncio.create_task(
                _dispatch_pending_comment_scopes(
                    datastore,
                    router,
                    responsible_fids,
                    active_run_id,
                    producers_done=comment_producers_done,
                    selected_comment_scopes=selected_comment_scopes,
                    selected_thread_scopes=selected_thread_scopes,
                ),
                name="api-comment-scope-dispatcher",
            )
            logger.info(
                "Draining research run {}: initial_scopes={}, tid={}, limit={}, max_seconds={}",
                active_run_id,
                len(selected),
                tid,
                limit,
                max_seconds,
            )

            async def enqueue_and_wait_idle() -> bool:
                nonlocal comment_dispatch_complete

                async def enqueue_initial_scopes() -> None:
                    await _enqueue_research_scope_batch(router, selected, responsible_fids, active_run_id)
                    comment_producers_done.set()

                async def supervise_dispatch() -> None:
                    nonlocal comment_dispatch_complete
                    enqueue_task = asyncio.create_task(enqueue_initial_scopes(), name="drain-initial-scopes")
                    try:
                        while not enqueue_task.done():
                            done, _pending = await asyncio.wait(
                                {enqueue_task, comment_dispatch_task},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if comment_dispatch_task in done and not enqueue_task.done():
                                comment_dispatch_task.result()
                                raise RuntimeError("Comment dispatcher exited before drain producers finished.")
                        enqueue_task.result()
                        comment_dispatch_complete = await comment_dispatch_task
                    finally:
                        if not enqueue_task.done():
                            enqueue_task.cancel()
                            await asyncio.gather(enqueue_task, return_exceptions=True)

                try:
                    if max_seconds == 0:
                        await supervise_dispatch()
                    else:
                        async with asyncio.timeout(max_seconds):
                            await supervise_dispatch()
                except TimeoutError:
                    return False
                return True

            api_drain_task = asyncio.create_task(enqueue_and_wait_idle(), name="api-detail-drain")

            api_monitor_task = api_drain_task
            cancelled = await _await_mode_or_shutdown(api_drain_task, shutdown_event)
            if not cancelled:
                api_drained = await api_drain_task
                if not api_drained:
                    if comment_dispatch_task is not None and not comment_dispatch_task.done():
                        comment_dispatch_task.cancel()
                        await asyncio.gather(comment_dispatch_task, return_exceptions=True)
                    await datastore.set_research_run_status_if(
                        active_run_id,
                        {"running", "incomplete"},
                        "paused_limit",
                        "drain_max_seconds_reached",
                    )
                    logger.warning("drain-pending reached its {} second API detail deadline.", max_seconds)

        if cancelled:
            if api_drain_task is not None and not api_drain_task.done():
                api_drain_task.cancel()
                await asyncio.gather(api_drain_task, return_exceptions=True)
            if comment_dispatch_task is not None and not comment_dispatch_task.done():
                comment_dispatch_task.cancel()
                await asyncio.gather(comment_dispatch_task, return_exceptions=True)
            if workers:
                await _stop_api_workers(workers, worker_tasks)
            await stop_web_for_interruption()
        else:
            web_summary = await finish_web_after_api_stopped()
        unresolved_count = getattr(web_summary, "unresolved_count", 0)
        if cancelled:
            for pending_run_id in web_run_ids:
                await datastore.set_research_run_status_if(
                    pending_run_id,
                    {"running", "completed", "incomplete", "paused_limit"},
                    "interrupted",
                    "drain_cancelled_after_current_api_page",
                )
        elif selected and not unresolved_count and not web_lifecycle_error:
            if comment_dispatch_complete:
                status = await datastore.reconcile_research_run_status_after_drain(active_run_id)
                logger.info("drain-pending finished; run status is {}.", status)
            else:
                await datastore.set_research_run_status_if(
                    active_run_id,
                    {"running", "completed", "incomplete"},
                    "incomplete",
                    "pending_comment_scopes_not_drained",
                )

        return len(selected)
    except asyncio.CancelledError:
        cancelled = True
        if api_drain_task is not None and not api_drain_task.done():
            api_drain_task.cancel()
            await asyncio.gather(api_drain_task, return_exceptions=True)
        if api_monitor_task is not None and not api_monitor_task.done():
            api_monitor_task.cancel()
            await asyncio.gather(api_monitor_task, return_exceptions=True)
        if comment_dispatch_task is not None and not comment_dispatch_task.done():
            comment_dispatch_task.cancel()
            await asyncio.gather(comment_dispatch_task, return_exceptions=True)
        if datastore is not None:
            if workers:
                await _stop_api_workers(workers, worker_tasks)
            await stop_web_for_interruption()
            for pending_run_id in web_run_ids or ((active_run_id,) if active_run_id else ()):
                await datastore.set_research_run_status_if(
                    pending_run_id,
                    {"running", "completed", "incomplete", "paused_limit"},
                    "interrupted",
                    "drain_cancelled_after_current_api_page",
                )
        raise
    except Exception as error:
        if datastore is not None:
            if api_monitor_task is not None and not api_monitor_task.done():
                api_monitor_task.cancel()
                await asyncio.gather(api_monitor_task, return_exceptions=True)
            if comment_dispatch_task is not None and not comment_dispatch_task.done():
                comment_dispatch_task.cancel()
                await asyncio.gather(comment_dispatch_task, return_exceptions=True)
            if workers:
                await _stop_api_workers(workers, worker_tasks)
            try:
                await stop_web_for_interruption()
            except Exception:
                logger.exception("WEB fallback could not stop cleanly after drain-pending failure.")
            if active_run_id is not None:
                await datastore.set_research_run_status_if(
                    active_run_id,
                    {"running"},
                    "incomplete",
                    f"drain_failed_{type(error).__name__}",
                )
        raise
    finally:
        if api_drain_task is not None and not api_drain_task.done():
            api_drain_task.cancel()
            await asyncio.gather(api_drain_task, return_exceptions=True)
        if api_monitor_task is not None and not api_monitor_task.done():
            api_monitor_task.cancel()
            await asyncio.gather(api_monitor_task, return_exceptions=True)
        if comment_dispatch_task is not None and not comment_dispatch_task.done():
            comment_dispatch_task.cancel()
            await asyncio.gather(comment_dispatch_task, return_exceptions=True)
        if workers:
            await _stop_api_workers(workers, worker_tasks)
        if web_worker_task is not None and not web_finished:
            try:
                await stop_web_for_interruption()
            except Exception:
                logger.exception("WEB fallback stop did not finish cleanly; durable tasks remain recoverable.")
        restore_signal_handlers()
        if datastore is not None:
            try:
                await datastore.close()
            except Exception:
                pass
        await container.teardown()


def _install_shutdown_signal_handlers(shutdown_event: asyncio.Event):
    loop = asyncio.get_running_loop()
    previous_handlers: dict[signal.Signals, Any] = {}
    installed: list[signal.Signals] = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous_handlers[signum] = signal.getsignal(signum)
            loop.add_signal_handler(signum, shutdown_event.set)
            installed.append(signum)
        except (NotImplementedError, RuntimeError, ValueError):
            continue

    def restore() -> None:
        for signum in installed:
            loop.remove_signal_handler(signum)
            try:
                signal.signal(signum, previous_handlers[signum])
            except (OSError, ValueError):
                pass

    return restore


async def _await_mode_or_shutdown(mode_task: asyncio.Task, shutdown_event: asyncio.Event) -> bool:
    signal_task = asyncio.create_task(shutdown_event.wait(), name="cooperative-shutdown-wait")
    try:
        done, _pending = await asyncio.wait(
            {mode_task, signal_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
    except asyncio.CancelledError:
        if not mode_task.done():
            mode_task.cancel()
        await asyncio.gather(mode_task, return_exceptions=True)
        raise
    finally:
        if not signal_task.done():
            signal_task.cancel()
        await asyncio.gather(signal_task, return_exceptions=True)

    if signal_task in done or shutdown_event.is_set():
        if not mode_task.done():
            mode_task.cancel()
        try:
            await mode_task
        except asyncio.CancelledError:
            pass
        return True
    await mode_task
    return False


async def main(
    mode: Literal["periodic", "backfill", "hybrid"] = "periodic",
    *,
    backfill_sort: Literal["create", "reply"] | None = None,
    resume: bool = False,
    run_id: str | None = None,
    revalidate_failed_comment_page_zero: bool = False,
    reset_web_worker_failures: bool = False,
    min_thread_year: int | None = None,
    newest_first: bool = False,
    ip_location_min_date: str | None = None,
    api_continue_older: bool = False,
    prioritize_web_comments: bool | None = None,
):
    """统一入口，根据模式启动应用。

    - periodic: 周期性调度器、API workers 和 WEB fallback worker 并行运行。
    - backfill: 回溯调度器、API workers 和 WEB fallback worker 并行运行。
    - hybrid: 周期调度器常驻 + 回溯调度器跑一轮 + 多 worker。
    """
    selected_backfill_sort = backfill_sort or "create"
    if backfill_sort is not None and mode != "backfill":
        raise ValueError("--backfill-sort can only be used with --mode backfill.")
    if revalidate_failed_comment_page_zero and not resume:
        raise ValueError("--revalidate-failed-comment-page-zero requires --resume.")
    if reset_web_worker_failures and not resume:
        raise ValueError("--reset-web-worker-failures requires --resume.")
    if api_continue_older and min_thread_year is None:
        raise ValueError("--api-continue-older requires --min-thread-year.")
    min_thread_create_time = _min_thread_create_time(min_thread_year)
    explicit_min_ip_create_time = (
        _parse_ip_location_min_date(ip_location_min_date)
        if ip_location_min_date is not None
        else None
    )
    container, router = await initialize_application(mode=mode)
    container.revalidate_failed_comment_page_zero = revalidate_failed_comment_page_zero
    container.min_thread_create_time = min_thread_create_time
    pydantic_cfg = getattr(container.config, "pydantic_config", None)
    web_cfg = getattr(pydantic_cfg, "web_fallback", None)
    if ip_location_min_date is not None:
        min_ip_create_time = explicit_min_ip_create_time
    else:
        min_ip_create_time = getattr(web_cfg, "ip_location_min_time", None)
    web_prioritize_comments = (
        prioritize_web_comments
        if prioritize_web_comments is not None
        else bool(getattr(web_cfg, "prioritize_comments", False))
    )
    container.min_ip_create_time = min_ip_create_time
    if min_thread_create_time is not None:
        logger.info(
            "Priority min-thread-year filter enabled: only threads created on/after {} are scanned in detail{}.",
            min_thread_create_time.isoformat(),
            " first (API continues older afterwards)" if api_continue_older else "",
        )
    if min_ip_create_time is not None:
        logger.info(
            "WEB ip_location_text cutoff enabled: skipping WEB enrichment for content created before {}.",
            min_ip_create_time.isoformat(),
        )
    if web_prioritize_comments:
        logger.info("WEB comment prioritization enabled: comment tasks run before thread/post IP tasks.")
    if newest_first:
        logger.info(
            "Newest-first ordering enabled: WEB tasks (thread->post->comment per tid DESC) "
            "and pending scope recovery run newest-first."
        )

    shutdown_event = asyncio.Event()
    restore_signal_handlers = _install_shutdown_signal_handlers(shutdown_event)
    monitor_task: asyncio.Task | None = None
    tasks: list[asyncio.Task] = []
    datastore: DataStore | None = None
    mark_interrupted_after_cancel = False
    research_run_id: str | None = None
    scheduler: Scheduler | None = None
    workers: list[Worker] = []
    worker_tasks: list[asyncio.Task] = []
    mode_task: asyncio.Task | None = None
    recovery_task: asyncio.Task | None = None
    comment_dispatch_task: asyncio.Task | None = None
    producers_done: asyncio.Event | None = None
    web_worker_task: asyncio.Task | None = None
    web_stop_event: asyncio.Event | None = None
    api_done_event: asyncio.Event | None = None
    web_discovery_task: asyncio.Task | None = None
    web_run_ids: tuple[str, ...] = ()
    web_finished = False
    web_summary: Any | None = None

    async def stop_web_for_interruption() -> Any | None:
        nonlocal web_finished, web_summary
        if web_discovery_task is not None and not web_discovery_task.done():
            web_discovery_task.cancel()
            await asyncio.gather(web_discovery_task, return_exceptions=True)
        if web_worker_task is None or web_stop_event is None or web_finished:
            return web_summary
        web_stop_event.set()
        try:
            web_summary = await asyncio.shield(web_worker_task)
        except asyncio.CancelledError:
            if not web_worker_task.cancelled():
                raise
        except Exception as error:
            logger.warning("WEB fallback worker stopped with {} during interruption.", type(error).__name__)
        finally:
            web_finished = web_worker_task.done()
        return web_summary

    async def finish_web_after_api_stopped() -> Any | None:
        nonlocal web_finished, web_summary
        if datastore is None:
            return web_summary
        if shutdown_event.is_set():
            return await stop_web_for_interruption()
        if web_worker_task is None or web_stop_event is None or api_done_event is None:
            if research_run_id is not None:
                dispatch_complete = (
                    comment_dispatch_task is None
                    or not comment_dispatch_task.done()
                    or comment_dispatch_task.result()
                )
                if dispatch_complete:
                    await datastore.reconcile_research_run_status_after_drain(research_run_id)
                else:
                    await datastore.set_research_run_status_if(
                        research_run_id,
                        {"running", "completed", "incomplete"},
                        "incomplete",
                        "pending_comment_scopes_not_drained",
                    )
            return web_summary
        if web_finished:
            return web_summary

        discovery_error: Exception | None = None
        web_error: Exception | None = None
        if web_discovery_task is not None:
            try:
                created = await asyncio.shield(web_discovery_task)
                if created:
                    logger.info("Queued {} missing metadata targets during API production.", created)
            except Exception as error:
                discovery_error = error
                logger.exception("WEB startup discovery failed: {}", error)
        try:
            created = await discover_missing_web_targets(
                datastore.async_sessionmaker,
                web_run_ids,
                **extra_web_filter_kwargs,
            )
            if created:
                logger.info("Queued {} missing metadata targets after API producers stopped.", created)
        except Exception as error:
            discovery_error = discovery_error or error
            logger.exception("Could not rescan stored API metadata before WEB shutdown: {}", error)

        pending_run_ids = web_run_ids or ((research_run_id,) if research_run_id is not None else ())
        for pending_run_id in pending_run_ids:
            await datastore.reconcile_research_run_status_after_drain(pending_run_id)

        api_done_event.set()
        try:
            web_summary = await asyncio.shield(web_worker_task)
        except asyncio.CancelledError:
            if not web_worker_task.cancelled():
                raise
            web_finished = True
        except Exception as error:
            web_error = error
            logger.warning("WEB fallback worker failed after API completion: {}", type(error).__name__)
            web_finished = True
        else:
            web_finished = True
        unresolved_count = getattr(web_summary, "unresolved_count", 0)
        dispatch_complete = (
            comment_dispatch_task is None
            or not comment_dispatch_task.done()
            or comment_dispatch_task.result()
        )
        if (
            unresolved_count
            or discovery_error is not None
            or web_error is not None
            or web_summary is None
            or not dispatch_complete
        ):
            for pending_run_id in pending_run_ids:
                await datastore.set_research_run_status_if(
                    pending_run_id,
                    {"running", "completed", "incomplete"},
                    "incomplete",
                    "web_fallback_unresolved_tasks"
                    if unresolved_count
                    else "web_fallback_worker_error"
                    if web_error is not None or web_summary is None
                    else "pending_comment_scopes_not_drained"
                    if not dispatch_complete
                    else "web_fallback_discovery_error",
                )
        else:
            for pending_run_id in pending_run_ids:
                await datastore.reconcile_research_run_status_after_drain(pending_run_id)
        return web_summary

    try:
        if container.config.metrics_enabled:
            start_http_server(container.config.metrics_port)
            logger.info("Prometheus metrics server started on port {}", container.config.metrics_port)

            monitor = SystemMonitor(container)
            monitor_task = asyncio.create_task(monitor.run(), name="system-monitor")

        logger.info("Starting application in {} mode.", mode)

        datastore = DataStore(container)
        scheduler_options = {"backfill_sort": "reply"} if selected_backfill_sort == "reply" else {}
        scheduler = Scheduler(
            router=router,
            container=container,
            datastore=datastore,
            **scheduler_options,
        )
        research_run_id = await _prepare_research_run(
            container,
            datastore,
            mode,
            backfill_sort=selected_backfill_sort,
            resume=resume,
            run_id=run_id,
        )

        # 按通道创建 Worker 组
        worker_id = 0
        lane_counts: dict[Literal["threads", "posts", "comments"], int] = {
            "threads": container.config.worker_threads,
            "posts": container.config.worker_posts,
            "comments": container.config.worker_comments,
        }
        for lane_name, count in lane_counts.items():
            queue = router.get_queue(lane_name)
            for _ in range(count):
                w = Worker(worker_id, queue, container, datastore, router, lane=lane_name)
                workers.append(w)
                worker_id += 1

        logger.info(
            "Created {} workers: threads={}, posts={}, comments={}",
            len(workers),
            lane_counts["threads"],
            lane_counts["posts"],
            lane_counts["comments"],
        )

        worker_tasks = [
            asyncio.create_task(w.run(), name=f"worker-{w.lane}-{w.worker_id}") for w in workers
        ]
        tasks.extend(worker_tasks)
        responsible_fids = {forum.fid for forum in (container.forums or [])}
        extra_filter_kwargs: dict[str, Any] = (
            {"min_thread_create_time": min_thread_create_time} if min_thread_create_time is not None else {}
        )
        extra_order_kwargs = dict(extra_filter_kwargs)
        if newest_first:
            extra_order_kwargs["newest_first"] = True
        older_api_enabled = (
            asyncio.Event()
            if (research_run_id is not None and api_continue_older and min_thread_create_time is not None)
            else None
        )
        recovery_kwargs = dict(extra_order_kwargs)
        if older_api_enabled is not None:
            recovery_kwargs["api_continue_older"] = True
            recovery_kwargs["older_api_enabled"] = older_api_enabled
        dispatch_kwargs = dict(extra_order_kwargs)
        if older_api_enabled is not None:
            dispatch_kwargs["older_api_enabled"] = older_api_enabled
        extra_web_filter_kwargs: dict[str, Any] = (
            {} if (api_continue_older and min_thread_create_time is not None) else dict(extra_filter_kwargs)
        )
        if min_ip_create_time is not None:
            extra_web_filter_kwargs["min_ip_create_time"] = min_ip_create_time
        extra_web_worker_kwargs = dict(extra_web_filter_kwargs)
        if api_continue_older and min_thread_create_time is not None:
            extra_web_worker_kwargs["priority_min_thread_create_time"] = min_thread_create_time
        if web_prioritize_comments:
            extra_web_worker_kwargs["prioritize_comments"] = True
        if newest_first:
            extra_web_worker_kwargs["newest_first"] = True
        recovery_task = asyncio.create_task(
            _recover_pending_tasks(
                datastore,
                router,
                responsible_fids=responsible_fids,
                research_run_id=research_run_id,
                **recovery_kwargs,
            ),
            name="api-pending-recovery",
        )
        tasks.append(recovery_task)

        if research_run_id is not None:
            producers_done = asyncio.Event() if mode == "backfill" else None
            comment_dispatch_task = asyncio.create_task(
                _dispatch_pending_comment_scopes(
                    datastore,
                    router,
                    responsible_fids,
                    research_run_id,
                    producers_done=producers_done,
                    **dispatch_kwargs,
                ),
                name="api-comment-scope-dispatcher",
            )
            tasks.append(comment_dispatch_task)

        if research_run_id is not None and hasattr(datastore, "async_sessionmaker"):
            responsible_forums = tuple((int(forum.fid), str(forum.fname)) for forum in (container.forums or []))
            web_run_ids = await pending_web_run_ids(
                datastore.async_sessionmaker,
                research_run_id,
                responsible_forums=responsible_forums,
            )
            if web_run_ids:
                prune_scope_kwargs = (
                    {"min_thread_create_time": min_thread_create_time}
                    if (min_thread_create_time is not None and not api_continue_older)
                    else {}
                )
                if min_ip_create_time is not None:
                    pruned = await prune_pre_launch_ip_web_tasks(
                        datastore.async_sessionmaker,
                        web_run_ids,
                        min_ip_create_time=min_ip_create_time,
                        **prune_scope_kwargs,
                    )
                    if pruned["marked_not_applicable"] or pruned["annotated_mixed"]:
                        logger.info(
                            "Pruned pre-launch IP WEB tasks before worker start (cutoff={}): "
                            "marked_not_applicable={}, annotated_mixed={}",
                            min_ip_create_time.isoformat(),
                            pruned["marked_not_applicable"],
                            pruned["annotated_mixed"],
                        )
                stale_pruned = await prune_stale_web_tasks(
                    datastore.async_sessionmaker,
                    web_run_ids,
                    **prune_scope_kwargs,
                )
                if stale_pruned:
                    logger.info(
                        "Pruned stale WEB tasks (target_observed_at_changed) before worker start: {}",
                        stale_pruned,
                    )
                if reset_web_worker_failures:
                    reset_count = await reset_web_worker_failure_tasks(
                        datastore.async_sessionmaker,
                        web_run_ids,
                    )
                    if reset_count:
                        logger.info(
                            "Reset {} WEB tasks failed by worker/browser crash back to "
                            "pending_web (attempts=0) before worker start.",
                            reset_count,
                        )
            web_stop_event = asyncio.Event()
            api_done_event = asyncio.Event()
            web_config = container.config.pydantic_config.web_fallback
            web_worker_task = asyncio.create_task(
                run_live_web_worker(
                    datastore.async_sessionmaker,
                    web_run_ids,
                    stop_event=web_stop_event,
                    api_done_event=api_done_event,
                    browser_profile=web_config.browser_profile,
                    browser_config=web_config,
                    **extra_web_worker_kwargs,
                ),
                name="web-fallback-worker",
            )
            web_worker_task.add_done_callback(_report_web_worker_exit)
            tasks.append(web_worker_task)
            web_discovery_task = asyncio.create_task(
                discover_missing_web_targets(
                    datastore.async_sessionmaker,
                    web_run_ids,
                    **extra_web_filter_kwargs,
                ),
                name="web-startup-discovery",
            )
            tasks.append(web_discovery_task)

        if mode == "periodic":
            mode_task = asyncio.create_task(
                _run_periodic_mode(
                    scheduler,
                    workers,
                    router,
                    worker_tasks=worker_tasks,
                    web_worker_task=web_worker_task,
                    web_stop_event=web_stop_event,
                    recovery_task=recovery_task,
                    comment_dispatch_task=comment_dispatch_task,
                    on_api_stopped=finish_web_after_api_stopped,
                    on_api_interrupted=stop_web_for_interruption,
                ),
                name="periodic-mode",
            )
        elif mode == "backfill":
            mode_task = asyncio.create_task(
                _run_backfill_mode(
                    scheduler,
                    workers,
                    router,
                    worker_tasks,
                    web_worker_task=web_worker_task,
                    web_stop_event=web_stop_event,
                    recovery_task=recovery_task,
                    comment_dispatch_task=comment_dispatch_task,
                    producers_done=producers_done,
                    on_api_stopped=finish_web_after_api_stopped,
                    on_api_interrupted=stop_web_for_interruption,
                ),
                name="backfill-mode",
            )
        else:  # hybrid
            mode_task = asyncio.create_task(
                _run_hybrid_mode(
                    scheduler,
                    workers,
                    router,
                    worker_tasks=worker_tasks,
                    web_worker_task=web_worker_task,
                    web_stop_event=web_stop_event,
                    recovery_task=recovery_task,
                    comment_dispatch_task=comment_dispatch_task,
                    on_api_stopped=finish_web_after_api_stopped,
                    on_api_interrupted=stop_web_for_interruption,
                ),
                name="hybrid-mode",
            )
        tasks.append(mode_task)
        if await _await_mode_or_shutdown(mode_task, shutdown_event):
            mark_interrupted_after_cancel = research_run_id is not None

    except asyncio.CancelledError:
        logger.info("Received cancellation in {} mode.", mode)
        shutdown_event.set()
        mark_interrupted_after_cancel = research_run_id is not None
        if mode_task is not None and not mode_task.done():
            mode_task.cancel()
            await asyncio.gather(mode_task, return_exceptions=True)
        raise

    except Exception as e:
        logger.exception("Application failed to start or run: {}", e)
        if datastore is not None and research_run_id is not None:
            try:
                await datastore.set_research_run_status_if(
                    research_run_id,
                    {"running"},
                    "incomplete",
                    f"application_failed_{type(e).__name__}",
                )
            except Exception:
                logger.exception("Failed to persist incomplete state for the failed research run.")
        raise
    finally:
        if mode_task is not None and not mode_task.done():
            mode_task.cancel()
            await asyncio.gather(mode_task, return_exceptions=True)

        if scheduler is not None:
            scheduler.stop()
        if workers:
            await _stop_api_workers(workers, worker_tasks)
        if recovery_task is not None and not recovery_task.done():
            recovery_task.cancel()
            await asyncio.gather(recovery_task, return_exceptions=True)
        if web_worker_task is not None and not web_finished:
            try:
                await stop_web_for_interruption()
            except Exception:
                logger.exception("WEB fallback stop did not finish cleanly; durable tasks remain recoverable.")

        if monitor_task and not monitor_task.done():
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if mark_interrupted_after_cancel and datastore is not None and research_run_id is not None:
            try:
                await datastore.set_research_run_status_if(
                    research_run_id,
                    {"running", "completed", "incomplete"},
                    "interrupted",
                    "cooperative_shutdown_after_current_api_page",
                )
            except Exception:
                logger.exception("Failed to persist interrupted state for the cancelled research run.")

        restore_signal_handlers()

        logger.info("Shutting down application...")

        if datastore is not None:
            try:
                await datastore.close()
            except Exception:
                pass

        await container.teardown()


def get_loop_factory():
    if platform.system() != "Windows":
        try:
            import uvloop  # type: ignore

            return uvloop.new_event_loop

        except ImportError:
            logger.warning("uvloop not installed; using default asyncio event loop.")

        except Exception as e:
            logger.warning("Failed to set up uvloop; using default asyncio event loop. Error: {}", e)

    else:
        logger.info("Running on Windows, using the default ProactorEventLoop.")

    return None


if __name__ == "__main__":
    import argparse

    def positive_int(value: str) -> int:
        try:
            parsed = int(value)
        except ValueError as error:
            raise argparse.ArgumentTypeError("must be an integer") from error
        if parsed <= 0:
            raise argparse.ArgumentTypeError("must be positive")
        return parsed

    def nonnegative_int(value: str) -> int:
        try:
            parsed = int(value)
        except ValueError as error:
            raise argparse.ArgumentTypeError("must be an integer") from error
        if parsed < 0:
            raise argparse.ArgumentTypeError("must be zero or positive")
        return parsed

    parser = argparse.ArgumentParser(description="Tieba Scraper Application")
    parser.add_argument("command", nargs="?", choices=["drain-pending"])
    parser.add_argument(
        "--mode",
        choices=["periodic", "backfill", "hybrid"],
        help=(
            "Running mode: 'periodic' for continuous monitoring, 'backfill' for historical data, "
            "or 'hybrid' for both."
        ),
    )
    parser.add_argument(
        "--backfill-sort",
        choices=["create", "reply"],
        help="backfill list order (default: create); reply uses the experimental API rn=100 stream",
    )
    parser.add_argument("--resume", action="store_true", help="resume the latest compatible research run")
    parser.add_argument("--run-id", help="resume this exact run with --resume or drain-pending")
    parser.add_argument(
        "--revalidate-failed-comment-page-zero",
        action="store_true",
        help="on resume, recheck one stored parent page after a page-one comment identity failure",
    )
    parser.add_argument(
        "--reset-web-worker-failures",
        action="store_true",
        help=(
            "on resume, reset failed_retryable/failed_final WEB tasks whose last error was a "
            "worker/browser crash back to pending_web with attempts=0"
        ),
    )
    parser.add_argument(
        "--min-thread-year",
        type=positive_int,
        help=(
            "prioritize full/web scans only for threads created on/after YYYY-01-01 (Asia/Shanghai); "
            "older threads remain deferred"
        ),
    )
    parser.add_argument(
        "--newest-first",
        action="store_true",
        help=(
            "lease WEB tasks newest-first per thread (thread -> post -> comment) "
            "and recover pending scopes newest-first"
        ),
    )
    parser.add_argument(
        "--ip-location-min-date",
        help=(
            "skip WEB ip_location_text enrichment for content created before YYYY-MM-DD "
            "(Asia/Shanghai); defaults to config (2022-04-28); pass 'none' to disable"
        ),
    )
    parser.add_argument(
        "--api-continue-older",
        action="store_true",
        help=(
            "after finishing >= --min-thread-year API scopes, continue draining older pending scopes "
            "while WEB still prioritizes >= --min-thread-year first"
        ),
    )
    parser.add_argument(
        "--prioritize-web-comments",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="prioritize WEB comment tasks ahead of thread/post IP-only tasks (defaults to config)",
    )
    parser.add_argument("--tid", type=positive_int, help="limit drain-pending to this thread ID")
    parser.add_argument(
        "--limit",
        type=positive_int,
        help="drain-pending maximum initial detail scopes (default: 10; discovered child scopes are included)",
    )
    parser.add_argument(
        "--max-seconds",
        type=nonnegative_int,
        help="drain-pending detail-work deadline in seconds (default: 120; 0 means no deadline)",
    )
    args = parser.parse_args()
    if args.command == "drain-pending":
        if not args.run_id:
            parser.error("drain-pending requires --run-id for an existing research run")
        if args.resume:
            parser.error("drain-pending resumes the exact run named by --run-id; omit --resume")
        if args.mode is not None:
            parser.error("--mode cannot be combined with drain-pending")
        if args.backfill_sort is not None:
            parser.error("--backfill-sort cannot be combined with drain-pending")
        if args.revalidate_failed_comment_page_zero:
            parser.error("--revalidate-failed-comment-page-zero cannot be combined with drain-pending")
        if args.reset_web_worker_failures:
            parser.error("--reset-web-worker-failures cannot be combined with drain-pending")
        if args.min_thread_year is not None:
            parser.error("--min-thread-year cannot be combined with drain-pending")
        if args.newest_first:
            parser.error("--newest-first cannot be combined with drain-pending")
        if args.ip_location_min_date is not None:
            parser.error("--ip-location-min-date cannot be combined with drain-pending")
        if args.api_continue_older:
            parser.error("--api-continue-older cannot be combined with drain-pending")
        if args.prioritize_web_comments is not None:
            parser.error("--prioritize-web-comments cannot be combined with drain-pending")
    else:
        if args.run_id and not args.resume:
            parser.error("--run-id requires --resume")
        if args.backfill_sort is not None and args.mode != "backfill":
            parser.error("--backfill-sort can only be used with --mode backfill")
        if args.revalidate_failed_comment_page_zero and not args.resume:
            parser.error("--revalidate-failed-comment-page-zero requires --resume")
        if args.reset_web_worker_failures and not args.resume:
            parser.error("--reset-web-worker-failures requires --resume")
        if args.min_thread_year is not None and not 1990 <= args.min_thread_year <= 2100:
            parser.error("--min-thread-year must be between 1990 and 2100")
        if args.api_continue_older and args.min_thread_year is None:
            parser.error("--api-continue-older requires --min-thread-year")
        if args.ip_location_min_date is not None:
            try:
                _parse_ip_location_min_date(args.ip_location_min_date)
            except ValueError as error:
                parser.error(str(error))
        if args.tid is not None or args.limit is not None or args.max_seconds is not None:
            parser.error("--tid, --limit, and --max-seconds are only valid with drain-pending")

    loop_factory = get_loop_factory()

    try:
        if args.command == "drain-pending":
            coroutine = drain_pending(
                run_id=args.run_id,
                tid=args.tid,
                limit=args.limit if args.limit is not None else 10,
                max_seconds=args.max_seconds if args.max_seconds is not None else 120,
            )
        else:
            coroutine = main(
                args.mode or "periodic",
                backfill_sort=args.backfill_sort,
                resume=args.resume,
                run_id=args.run_id,
                revalidate_failed_comment_page_zero=args.revalidate_failed_comment_page_zero,
                reset_web_worker_failures=args.reset_web_worker_failures,
                min_thread_year=args.min_thread_year,
                newest_first=args.newest_first,
                ip_location_min_date=args.ip_location_min_date,
                api_continue_older=args.api_continue_older,
                prioritize_web_comments=args.prioritize_web_comments,
            )
        if loop_factory is not None:
            with asyncio.Runner(loop_factory=loop_factory) as runner:
                runner.run(coroutine)
        else:
            asyncio.run(coroutine)
    except KeyboardInterrupt:
        logger.info("Application stopped by user.")
