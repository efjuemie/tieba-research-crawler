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
from typing import Any, Literal

from prometheus_client import start_http_server
from tiebameow.utils.logger import init_logger, logger

from src.core import DataStore, initialize_application
from src.core.models import REPLY_BACKFILL_MODE
from src.core.monitor import SystemMonitor
from src.research.web_fallback.live import discover_missing_web_targets, pending_web_run_ids, run_live_web_worker
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


async def _recover_pending_tasks(
    datastore: DataStore,
    router: QueueRouter,
    responsible_fids: set[int],
    *,
    research_run_id: str | None = None,
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
        async for scope_batch in pending_scopes(research_run_id, responsible_fids, batch_size=500):
            recovered += await _enqueue_research_scope_batch(
                router,
                scope_batch,
                responsible_fids,
                research_run_id,
            )

        if recovered:
            logger.info("Recovered current research task scopes: {}", recovered)
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
            "WEB fallback worker exited with reason={} unresolved={}; API production continues.",
            getattr(summary, "stopped_reason", None),
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

    aggregate = asyncio.gather(*tasks)
    try:
        await asyncio.shield(aggregate)
        if on_api_stopped is not None:
            await on_api_stopped()
    except asyncio.CancelledError:
        logger.info("Received shutdown signal. Starting graceful shutdown...")
        await _stop_scheduler_tasks(scheduler, [scheduler_task])
        _cancel_recovery_task(recovery_task)
        await _stop_api_workers(workers, worker_tasks)
        if on_api_interrupted is not None:
            await on_api_interrupted()
        raise
    except Exception:
        await _stop_scheduler_tasks(scheduler, [scheduler_task])
        _cancel_recovery_task(recovery_task)
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

    aggregate = asyncio.gather(*tasks)
    try:
        await asyncio.shield(aggregate)
        if on_api_stopped is not None:
            await on_api_stopped()
    except asyncio.CancelledError:
        logger.info("Received shutdown signal in hybrid mode. Starting graceful shutdown...")
        await _stop_scheduler_tasks(scheduler, [periodic_task, backfill_task])
        _cancel_recovery_task(recovery_task)
        await _stop_api_workers(workers, worker_tasks)
        if on_api_interrupted is not None:
            await on_api_interrupted()
        raise
    except Exception:
        await _stop_scheduler_tasks(scheduler, [periodic_task, backfill_task])
        _cancel_recovery_task(recovery_task)
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
    on_api_stopped: Any | None,
    on_api_interrupted: Any | None = None,
) -> None:
    scheduler_task = asyncio.create_task(scheduler.run(mode="backfill"), name="scheduler")

    async def wait_for_api_drain() -> None:
        await scheduler_task
        if recovery_task is not None:
            await recovery_task
        await router.wait_idle()

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
    web_discovery_task: asyncio.Task | None = None
    web_finished = False
    web_summary: Any | None = None
    cancelled = False
    initial_run_status: str | None = None
    web_discovered_count = 0
    web_lifecycle_error = False

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
            if unresolved_count or discovery_error is not None or web_error is not None or web_summary is None:
                web_lifecycle_error = True
                await datastore.set_research_run_status_if(
                    run_id,
                    {"running", "completed", "incomplete"},
                    "incomplete",
                    "web_fallback_unresolved_tasks"
                    if unresolved_count
                    else "web_fallback_worker_error"
                    if web_error is not None or web_summary is None
                    else "web_fallback_discovery_error",
                )
            else:
                if (
                    initial_run_status == "completed"
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
            logger.info(
                "Draining research run {}: initial_scopes={}, tid={}, limit={}, max_seconds={}",
                active_run_id,
                len(selected),
                tid,
                limit,
                max_seconds,
            )

            async def enqueue_and_wait_idle() -> bool:
                await _enqueue_research_scope_batch(router, selected, responsible_fids, active_run_id)
                try:
                    if max_seconds == 0:
                        await router.wait_idle()
                    else:
                        async with asyncio.timeout(max_seconds):
                            await router.wait_idle()
                except TimeoutError:
                    return False
                return True

            api_drain_task = asyncio.create_task(enqueue_and_wait_idle(), name="api-detail-drain")

            api_monitor_task = api_drain_task
            cancelled = await _await_mode_or_shutdown(api_drain_task, shutdown_event)
            if not cancelled:
                api_drained = await api_drain_task
                if not api_drained:
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
            status = await datastore.reconcile_research_run_status_after_drain(active_run_id)
            logger.info("drain-pending finished; run status is {}.", status)

        return len(selected)
    except asyncio.CancelledError:
        cancelled = True
        if api_drain_task is not None and not api_drain_task.done():
            api_drain_task.cancel()
            await asyncio.gather(api_drain_task, return_exceptions=True)
        if api_monitor_task is not None and not api_monitor_task.done():
            api_monitor_task.cancel()
            await asyncio.gather(api_monitor_task, return_exceptions=True)
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
):
    """统一入口，根据模式启动应用。

    - periodic: 周期性调度器、API workers 和 WEB fallback worker 并行运行。
    - backfill: 回溯调度器、API workers 和 WEB fallback worker 并行运行。
    - hybrid: 周期调度器常驻 + 回溯调度器跑一轮 + 多 worker。
    """
    selected_backfill_sort = backfill_sort or "create"
    if backfill_sort is not None and mode != "backfill":
        raise ValueError("--backfill-sort can only be used with --mode backfill.")
    container, router = await initialize_application(mode=mode)

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
                await datastore.reconcile_research_run_status_after_drain(research_run_id)
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
            created = await discover_missing_web_targets(datastore.async_sessionmaker, web_run_ids)
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
        if unresolved_count or discovery_error is not None or web_error is not None or web_summary is None:
            for pending_run_id in pending_run_ids:
                await datastore.set_research_run_status_if(
                    pending_run_id,
                    {"running", "completed", "incomplete"},
                    "incomplete",
                    "web_fallback_unresolved_tasks"
                    if unresolved_count
                    else "web_fallback_worker_error"
                    if web_error is not None or web_summary is None
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
        recovery_task = asyncio.create_task(
            _recover_pending_tasks(
                datastore,
                router,
                responsible_fids=responsible_fids,
                research_run_id=research_run_id,
            ),
            name="api-pending-recovery",
        )
        tasks.append(recovery_task)

        if research_run_id is not None and hasattr(datastore, "async_sessionmaker"):
            responsible_forums = tuple((int(forum.fid), str(forum.fname)) for forum in (container.forums or []))
            web_run_ids = await pending_web_run_ids(
                datastore.async_sessionmaker,
                research_run_id,
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
            tasks.append(web_worker_task)
            web_discovery_task = asyncio.create_task(
                discover_missing_web_targets(datastore.async_sessionmaker, web_run_ids),
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
    else:
        if args.run_id and not args.resume:
            parser.error("--run-id requires --resume")
        if args.backfill_sort is not None and args.mode != "backfill":
            parser.error("--backfill-sort can only be used with --mode backfill")
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
            )
        if loop_factory is not None:
            with asyncio.Runner(loop_factory=loop_factory) as runner:
                runner.run(coroutine)
        else:
            asyncio.run(coroutine)
    except KeyboardInterrupt:
        logger.info("Application stopped by user.")
