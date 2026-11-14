"""Tieba爬虫应用程序主入口模块。

该模块提供三种运行模式：
1. 周期性模式(periodic): 持续监控指定论坛的最新内容
2. 回溯模式(backfill): 抓取指定论坛的历史数据（仅投递首页，由 Worker 递推）
3. 混合模式(hybrid): 周期监控首页 + 一次性触发回溯首页

统一入口 main(mode) 根据模式分支启动相应组件。
"""

import asyncio
import platform
from collections.abc import AsyncIterator
from typing import Any, Literal

from prometheus_client import start_http_server
from tiebameow.utils.logger import init_logger, logger

from src.core import DataStore, initialize_application
from src.core.monitor import SystemMonitor
from src.scraper import Scheduler, Worker
from src.scraper.router import QueueRouter
from src.scraper.tasks import FullScanCommentsTask, FullScanPostsTask, IncrementalScanCommentsTask, Priority, Task

init_logger(
    service_name="TiebaScraper",
    enable_error_filelog=True,
    diagnose=False,
)

GRACEFUL_SHUTDOWN_TIMEOUT = 60


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
            for scope in scope_batch:
                if scope.status == "completed":
                    continue
                if scope.fid not in responsible_fids:
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
                recovered += 1

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


async def _run_periodic_mode(
    scheduler: Scheduler,
    workers: list[Worker],
    router: QueueRouter,
    worker_tasks: list[asyncio.Task] | None = None,
) -> None:
    """运行周期模式，支持优雅退出。

    优雅退出流程：
    1. 收到取消信号后，停止调度器（不再生成新任务）
    2. 等待所有通道队列清空（带超时）
    3. 取消所有 Worker
    """
    tasks: list[asyncio.Task] = []

    scheduler_task = asyncio.create_task(scheduler.run(mode="periodic"), name="scheduler")
    tasks.append(scheduler_task)
    if worker_tasks is None:
        worker_tasks = [asyncio.create_task(w.run(), name=f"worker-{w.lane}-{w.worker_id}") for w in workers]
    tasks.extend(worker_tasks)

    aggregate = asyncio.gather(*tasks)
    try:
        await asyncio.shield(aggregate)
    except asyncio.CancelledError:
        logger.info("Received shutdown signal. Starting graceful shutdown...")

        scheduler.stop()

        if not scheduler_task.done():
            try:
                await asyncio.wait_for(scheduler_task, timeout=5)
            except TimeoutError:
                scheduler_task.cancel()
                try:
                    await scheduler_task
                except asyncio.CancelledError:
                    pass

        logger.info(
            "Waiting for all lane tasks to complete (timeout={}s, queued_now={})...",
            GRACEFUL_SHUTDOWN_TIMEOUT,
            router.total_qsize(),
        )
        try:
            join_tasks = [q.join() for q in router.lanes.values()]
            await asyncio.wait_for(asyncio.gather(*join_tasks), timeout=GRACEFUL_SHUTDOWN_TIMEOUT)
            logger.info("All lane tasks completed.")
        except TimeoutError:
            remaining = router.total_qsize()
            logger.warning("Graceful shutdown timeout. queued_now={} across all lanes.", remaining)

        worker_tasks = [t for t in tasks if t.get_name().startswith("worker-")]
        for t in worker_tasks:
            if not t.done():
                t.cancel()
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)

        logger.info("Graceful shutdown completed.")
        raise
    except Exception:
        scheduler.stop()
        raise
    finally:
        await _cancel_and_reap_tasks(tasks, aggregate)


async def _run_hybrid_mode(
    scheduler: Scheduler,
    workers: list[Worker],
    router: QueueRouter,
    worker_tasks: list[asyncio.Task] | None = None,
    *,
    datastore: DataStore | None = None,
    research_run_id: str | None = None,
) -> None:
    """运行混合模式，支持优雅退出。

    混合模式同时运行周期调度器和回溯调度器。
    优雅退出流程：
    1. 收到取消信号后，停止周期调度器（不再生成新任务）
    2. 等待回溯调度器完成（如果还在运行）
    3. 等待所有通道队列清空（带超时）
    4. 取消所有 Worker
    """
    tasks: list[asyncio.Task] = []

    periodic_task = asyncio.create_task(scheduler.run(mode="periodic"), name="scheduler-periodic")
    tasks.append(periodic_task)

    backfill_task = asyncio.create_task(scheduler.run(mode="backfill"), name="scheduler-backfill")
    tasks.append(backfill_task)

    if worker_tasks is None:
        worker_tasks = [asyncio.create_task(w.run(), name=f"worker-{w.lane}-{w.worker_id}") for w in workers]
    tasks.extend(worker_tasks)

    aggregate = asyncio.gather(*tasks)
    try:
        await asyncio.shield(aggregate)
    except asyncio.CancelledError:
        logger.info("Received shutdown signal in hybrid mode. Starting graceful shutdown...")

        scheduler.stop()

        if not periodic_task.done():
            try:
                await asyncio.wait_for(periodic_task, timeout=5)
            except TimeoutError:
                periodic_task.cancel()
                try:
                    await periodic_task
                except asyncio.CancelledError:
                    pass

        backfill_error: Exception | None = None
        backfill_cancelled = False
        if not backfill_task.done():
            logger.info("Waiting for backfill scheduler to complete...")
            try:
                await asyncio.wait_for(asyncio.shield(backfill_task), timeout=10)
            except TimeoutError:
                logger.warning("Backfill scheduler timeout, cancelling...")
                backfill_cancelled = True
                backfill_task.cancel()
                await asyncio.gather(backfill_task, return_exceptions=True)
            except asyncio.CancelledError:
                backfill_cancelled = True
            except Exception as error:
                backfill_error = error

        if backfill_task.done() and not backfill_task.cancelled() and backfill_error is None:
            backfill_error = backfill_task.exception()
        elif backfill_task.cancelled():
            backfill_cancelled = True

        logger.info(
            "Waiting for all lane tasks to complete (timeout={}s, queued_now={})...",
            GRACEFUL_SHUTDOWN_TIMEOUT,
            router.total_qsize(),
        )
        queue_drained = False
        try:
            await asyncio.wait_for(router.wait_idle(), timeout=GRACEFUL_SHUTDOWN_TIMEOUT)
            queue_drained = True
            logger.info("All lane tasks completed.")
        except TimeoutError:
            remaining = router.total_qsize()
            logger.warning("Graceful shutdown timeout. queued_now={} across all lanes.", remaining)

        if datastore is not None and research_run_id is not None:
            if not queue_drained or backfill_cancelled:
                await datastore.set_research_run_status_if(
                    research_run_id,
                    {"running", "completed", "incomplete"},
                    "interrupted",
                    "hybrid_shutdown_before_backfill_drain",
                )
            elif backfill_error is None:
                await datastore.reconcile_research_run_status_after_drain(research_run_id)

        worker_tasks = [t for t in tasks if t.get_name().startswith("worker-")]
        for t in worker_tasks:
            if not t.done():
                t.cancel()
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)

        logger.info("Graceful shutdown completed.")
        if backfill_error is not None:
            raise backfill_error from None
        raise
    except Exception:
        scheduler.stop()
        raise
    finally:
        await _cancel_and_reap_tasks(tasks, aggregate)


async def _prepare_research_run(
    container,
    datastore: DataStore,
    mode: Literal["periodic", "backfill", "hybrid"],
    *,
    resume: bool,
    run_id: str | None,
) -> str | None:
    config = container.config
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
        mode=mode,
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

    if not getattr(config, "fetch_user_forum_info", False):
        auth_ready = getattr(container, "research_auth_ready", None)
        if auth_ready is not None:
            auth_ready.set()
        return active_run_id

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
    return active_run_id


async def main(
    mode: Literal["periodic", "backfill", "hybrid"] = "periodic",
    *,
    resume: bool = False,
    run_id: str | None = None,
):
    """统一入口，根据模式启动应用。

    - periodic: 周期性调度器 + 多 worker，常驻运行，支持优雅退出。
    - backfill: 一次性回溯调度器 + 多 worker；等待队列清空后优雅退出。
    - hybrid: 周期调度器常驻 + 回溯调度器跑一轮 + 多 worker。
    """
    container, router = await initialize_application(mode=mode)

    monitor_task: asyncio.Task | None = None
    if container.config.metrics_enabled:
        start_http_server(container.config.metrics_port)
        logger.info("Prometheus metrics server started on port {}", container.config.metrics_port)

        monitor = SystemMonitor(container)
        monitor_task = asyncio.create_task(monitor.run(), name="system-monitor")

    tasks: list[asyncio.Task] = []
    datastore: DataStore | None = None
    mark_interrupted_after_cancel = False
    research_run_id: str | None = None
    try:
        logger.info("Starting application in {} mode.", mode)

        datastore = DataStore(container)
        scheduler = Scheduler(router=router, container=container, datastore=datastore)
        research_run_id = await _prepare_research_run(container, datastore, mode, resume=resume, run_id=run_id)

        # 按通道创建 Worker 组
        workers: list[Worker] = []
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
        await _recover_pending_tasks(
            datastore,
            router,
            responsible_fids=responsible_fids,
            research_run_id=research_run_id,
        )

        if mode == "periodic":
            await _run_periodic_mode(scheduler, workers, router, worker_tasks=worker_tasks)

        elif mode == "backfill":
            scheduler_task = asyncio.create_task(scheduler.run(mode="backfill"), name="scheduler")
            tasks.append(scheduler_task)

            await scheduler_task
            await router.wait_idle()
            if research_run_id is not None:
                await datastore.reconcile_research_run_status_after_drain(research_run_id)

        else:  # hybrid
            await _run_hybrid_mode(
                scheduler,
                workers,
                router,
                worker_tasks=worker_tasks,
                datastore=datastore,
                research_run_id=research_run_id,
            )

    except asyncio.CancelledError:
        logger.info("Received cancellation in {} mode.", mode)
        mark_interrupted_after_cancel = research_run_id is not None
        raise

    except Exception as e:
        logger.exception("Application failed to start or run: {}", e)
        raise
    finally:
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
                    {"running"},
                    "interrupted",
                    "main_cancelled_before_completion",
                )
            except Exception:
                logger.exception("Failed to persist interrupted state for the cancelled research run.")

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

    parser = argparse.ArgumentParser(description="Tieba Scraper Application")
    parser.add_argument(
        "--mode",
        choices=["periodic", "backfill", "hybrid"],
        default="periodic",
        help=("Running mode: 'periodic' for continuous monitoring, 'backfill' for historical data, 'hybrid' for both."),
    )
    parser.add_argument("--resume", action="store_true", help="resume the latest compatible research run")
    parser.add_argument("--run-id", help="resume this exact research run; requires --resume")
    args = parser.parse_args()
    if args.run_id and not args.resume:
        parser.error("--run-id requires --resume")

    loop_factory = get_loop_factory()

    try:
        if loop_factory is not None:
            with asyncio.Runner(loop_factory=loop_factory) as runner:
                runner.run(main(args.mode, resume=args.resume, run_id=args.run_id))
        else:
            asyncio.run(main(args.mode, resume=args.resume, run_id=args.run_id))
    except KeyboardInterrupt:
        logger.info("Application stopped by user.")
