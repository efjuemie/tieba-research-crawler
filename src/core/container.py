"""依赖注入容器模块。

该模块实现了应用程序的依赖注入容器，负责统一管理和初始化
各种外部资源和服务，包括数据库连接、Redis客户端、aiotieba客户端等。
"""

from __future__ import annotations

import asyncio
import itertools
from pathlib import Path
from typing import TYPE_CHECKING

import redis.asyncio as redis
import tomlkit
from aiolimiter import AsyncLimiter
from aiotieba.config import ProxyConfig as AiotiebaProxyConfig
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from tiebameow.client import Client
from tiebameow.models.orm import Forum
from tiebameow.utils.logger import logger

from ..research.client import ResearchClient, ResearchRunCache
from .auth import (
    GateAwareSemaphore,
    account_file_fingerprint,
    verify_research_credentials,
    wait_for_account_file_update,
)
from .lock import LockManager, MemoryLockManager, RedisLockManager
from .ws_server import WebSocketServer

if TYPE_CHECKING:
    from collections.abc import Iterator

    from .config import Config


class Container:
    """依赖注入容器。

    负责管理应用程序的所有外部依赖，包括数据库连接、Redis客户端、
    aiotieba客户端、aiolimiter限流器等。提供统一的资源初始化和清理接口。

    Attributes:
        config (Config): 应用程序配置对象
        limiter (AsyncLimiter): 漏桶算法实现的异步限流器（全局，向后兼容）
        threads_limiter (AsyncLimiter): get_threads 接口专用限流器
        posts_limiter (AsyncLimiter): get_posts 接口专用限流器
        comments_limiter (AsyncLimiter): get_comments 接口专用限流器
        db_engine (AsyncEngine): SQLAlchemy异步数据库引擎
        redis_client (redis.Redis): Redis异步客户端
        tb_client (Client): tiebameow异步贴吧客户端（主客户端，向后兼容）
        tb_clients (list[Client]): 多代理模式下的所有客户端列表
        forums (list[Forum]): 贴吧信息缓存
        async_sessionmaker: 异步数据库会话工厂
    """

    def __init__(self, config: Config):
        """初始化容器。

        Args:
            config: 应用程序的配置对象，包含数据库、Redis、贴吧等配置信息。
        """
        self.config = config

        self.semaphore: GateAwareSemaphore | None = None
        self._request_semaphore: asyncio.Semaphore | None = None
        # 分接口限流器（多代理模式下，每个代理有独立的限流器组）
        self.threads_limiter: AsyncLimiter | None = None
        self.posts_limiter: AsyncLimiter | None = None
        self.comments_limiter: AsyncLimiter | None = None
        # 多代理支持：每个代理有独立的限流器组
        self._threads_limiters: list[AsyncLimiter] = []
        self._posts_limiters: list[AsyncLimiter] = []
        self._comments_limiters: list[AsyncLimiter] = []
        self._client_cycle: Iterator[int] | None = None
        self.db_engine: AsyncEngine | None = None
        self.redis_client: redis.Redis | None = None
        self.tb_client: Client | None = None
        self.tb_clients: list[Client] = []
        self.research_run_cache = ResearchRunCache()
        self.research_run_id: str | None = None
        self.research_auth_ready = asyncio.Event()
        self.research_auth_ready.set()
        # Compatibility alias; this gate is auth-only. New request paths should
        # call wait_for_research_ready() to include both auth and cooldown gates.
        self.research_ready = self.research_auth_ready
        self.research_network_ready = asyncio.Event()
        self.research_network_ready.set()
        self._research_rate_limit_deadline = 0.0
        self._research_rate_limit_lock = asyncio.Lock()
        self._research_empty_response_streak = 0
        self._research_empty_response_lock = asyncio.Lock()
        self._auth_refresh_lock = asyncio.Lock()
        self._research_account_file_state: tuple[int, int, int, int] | None = None
        self._research_credentials_loaded = False
        self.forums: list[Forum] | None = None
        self.ws_server: WebSocketServer | None = None
        self.lock_manager: LockManager = MemoryLockManager()
        self._config_lock = asyncio.Lock()

    def get_next_client_index(self) -> int:
        """获取下一个客户端索引（轮询）。

        Returns:
            int: 下一个要使用的客户端索引。
        """
        if self._client_cycle is None:
            return 0
        return next(self._client_cycle)

    def get_client(self, index: int | None = None) -> Client:
        """获取指定索引或轮询获取的客户端。

        Args:
            index: 客户端索引，None 则轮询获取。

        Returns:
            Client: tiebameow 客户端实例。
        """
        if not self.tb_clients:
            assert self.tb_client is not None
            return self.tb_client

        idx = index if index is not None else self.get_next_client_index()
        return self.tb_clients[idx % len(self.tb_clients)]

    def get_limiters(self, index: int | None = None) -> tuple[AsyncLimiter, AsyncLimiter, AsyncLimiter]:
        """获取指定索引或轮询获取的限流器组。

        Args:
            index: 限流器组索引，None 则使用全局限流器。

        Returns:
            tuple: (threads_limiter, posts_limiter, comments_limiter)
        """
        if index is not None and self._threads_limiters:
            idx = index % len(self._threads_limiters)
            return (
                self._threads_limiters[idx],
                self._posts_limiters[idx],
                self._comments_limiters[idx],
            )
        # 使用全局限流器
        assert self.threads_limiter is not None
        assert self.posts_limiter is not None
        assert self.comments_limiter is not None
        return (self.threads_limiter, self.posts_limiter, self.comments_limiter)

    async def add_forum(self, fname: str, group_name: str | None = None) -> bool:
        """动态添加监控贴吧。

        Args:
            fname: 贴吧名称。
            group_name: 分组名称。如果指定，将添加到该分组；否则添加到默认列表。

        Returns:
            bool: 是否成功添加（已存在也返回 True）。
        """
        async with self._config_lock:
            assert self.tb_client is not None
            assert self.async_sessionmaker is not None

        # 1. 检查是否存在
        current = self.forums or []
        existing_forum = next((f for f in current if f.fname == fname), None)

        if existing_forum:
            # 已存在，仅更新分组配置
            fid = existing_forum.fid
            logger.debug("Forum [{}](fid={}) already monitored.", fname, fid)
        else:
            # 不存在，需获取 fid 并存入 DB
            fid = await self.tb_client.get_fid(fname)
            if not fid:
                logger.warning("Forum [{}吧] not found on Tieba.", fname)
                return False

            async with self.async_sessionmaker() as session:
                try:
                    res = await session.execute(select(Forum).where(Forum.fname == fname))
                    exist = res.scalar_one_or_none()
                    if exist is None:
                        session.add(Forum(fid=fid, fname=fname))
                        await session.commit()
                        logger.info("Added new forum to DB: [{}](fid={})", fname, fid)
                    else:
                        fid = exist.fid
                except Exception:
                    await session.rollback()
                    raise

            # 更新 self.forums
            new_forum = Forum(fid=fid, fname=fname)
            current = list(current)
            current.append(new_forum)
            self.forums = current
            logger.info("Added [{}吧] to active monitor list.", fname)

            # 2. 更新分组配置
            target_group_found = False
            if group_name:
                found_group = next((g for g in self.config.groups if g.name == group_name), None)
                if found_group:
                    target_group_found = True
                    if fname not in found_group.forums:
                        found_group.forums.append(fname)
                        logger.info("Added [{}吧] to group '{}'.", fname, group_name)
                    else:
                        logger.debug("[{}吧] already in group '{}'.", fname, group_name)
                else:
                    logger.warning("Group '{}' not found. [{}吧] will be processed by Default loop.", group_name, fname)

            # 3. 持久化到 config.toml
            def _update_config_file():
                try:
                    config_path = Path(__file__).resolve().parent.parent.parent / "config.toml"
                    if config_path.exists():
                        with config_path.open("r", encoding="utf-8") as f:
                            doc = tomlkit.load(f)

                        tieba_section = doc.get("tieba")
                        if tieba_section:
                            # 确定添加目标
                            added_to_doc = False
                            if target_group_found and group_name:
                                groups = tieba_section.get("groups")
                                if groups:
                                    for g in groups:
                                        if g.get("name") == group_name:
                                            forums_list = g.get("forums")
                                            if forums_list is not None and fname not in forums_list:
                                                forums_list.append(fname)
                                                added_to_doc = True
                                            break

                            if not added_to_doc and not target_group_found:
                                forums_list = tieba_section.get("forums")
                                if forums_list is not None and fname not in forums_list:
                                    forums_list.append(fname)
                                    added_to_doc = True

                            if added_to_doc:
                                with config_path.open("w", encoding="utf-8") as f:
                                    tomlkit.dump(doc, f)
                                logger.info("Persisted [{}吧] to config.toml", fname)

                except Exception as e:
                    logger.error("Failed to update config.toml: {}", e)

            await asyncio.to_thread(_update_config_file)

        return True

    async def setup(self):
        """异步初始化容器资源。

        依次初始化以下资源：
        1. AsyncLimiter - 用于aiotieba请求限流
        2. Semaphore - 用于控制并发请求数
        3. Client - 带速率与并发限制的异步贴吧客户端
        4. PostgreSQL异步数据库引擎和会话工厂
        5. Redis异步客户端连接

        如果任何步骤失败，会自动调用teardown()清理已初始化的资源。

        Raises:
            Exception: 当资源初始化失败时抛出异常。
        """
        logger.info("Initializing container resources...")
        try:
            client_type: type[Client] = ResearchClient if self.config.research_enabled else Client
            research_client_kwargs: dict[str, object] = {}
            research_request_interval = 0.0
            if self.config.research_enabled:
                research_client_kwargs["research_run_cache"] = self.research_run_cache
                global_rps = float(self.config.global_rps)
                if global_rps <= 0:
                    raise ValueError("Research global_rps must be positive.")
                research_request_interval = 1 / global_rps
            if self.config.research_enabled and self.config.fetch_user_forum_info:
                from ..research.credentials import load_bduss

                self._research_account_file_state = account_file_fingerprint(self.config.research_account_file)
                try:
                    research_client_kwargs["BDUSS"] = load_bduss(self.config.research_account_file)
                    self._research_credentials_loaded = True
                except (OSError, ValueError):
                    self._research_credentials_loaded = False

            # 全局限流
            self._request_semaphore = asyncio.Semaphore(self.config.concurrency_limit)
            self.semaphore = GateAwareSemaphore(
                self._request_semaphore,
                self.wait_for_research_ready,
                self._research_request_gates_open,
                self.wait_for_research_network_ready,
                self._research_network_gates_open,
                minimum_interval_seconds=research_request_interval,
            )
            logger.info("Global concurrency limit set to {}.", self.config.concurrency_limit)

            # 分接口限流器
            self.threads_limiter = AsyncLimiter(1, time_period=1 / self.config.threads_rps)
            self.posts_limiter = AsyncLimiter(1, time_period=1 / self.config.posts_rps)
            self.comments_limiter = AsyncLimiter(1, time_period=1 / self.config.comments_rps)
            logger.info(
                "Per-API limiters initialized: threads={} RPS, posts={} RPS, comments={} RPS.",
                self.config.threads_rps,
                self.config.posts_rps,
                self.config.comments_rps,
            )

            # 多代理模式：创建多个客户端和限流器
            proxy_urls = self.config.proxy_urls
            if len(proxy_urls) > 1:
                logger.info("Multi-proxy mode enabled with {} proxies.", len(proxy_urls))
                for i, url in enumerate(proxy_urls):
                    # 每个代理有独立的限流器组
                    self._threads_limiters.append(AsyncLimiter(1, time_period=1 / self.config.threads_rps))
                    self._posts_limiters.append(AsyncLimiter(1, time_period=1 / self.config.posts_rps))
                    self._comments_limiters.append(AsyncLimiter(1, time_period=1 / self.config.comments_rps))

                    proxy_config = AiotiebaProxyConfig(url=url)
                    client = await client_type(
                        proxy=proxy_config,
                        semaphore=self.semaphore,
                        cooldown_429=self.config.cooldown_seconds_429,
                        retry_attempts=1 if self.config.research_enabled else 5,
                        **research_client_kwargs,
                    ).__aenter__()
                    self.tb_clients.append(client)
                    logger.info("Tieba Client #{} started with proxy: {}", i, url.split("@")[-1])

                # 设置主客户端为第一个（向后兼容）
                self.tb_client = self.tb_clients[0]
                # 设置轮询迭代器
                self._client_cycle = itertools.cycle(range(len(self.tb_clients)))
                logger.info(
                    "Multi-proxy clients initialized. Total throughput: threads={} RPS, posts={} RPS, comments={} RPS.",
                    self.config.threads_rps * len(self.tb_clients),
                    self.config.posts_rps * len(self.tb_clients),
                    self.config.comments_rps * len(self.tb_clients),
                )
            else:
                # 单代理或无代理模式
                proxy_url = self.config.proxy_url
                proxy_config: AiotiebaProxyConfig | bool = False
                if proxy_url:
                    proxy_config = AiotiebaProxyConfig(url=proxy_url)
                    logger.info("Proxy enabled: {}", proxy_url.split("@")[-1])

                self.tb_client = await client_type(
                    proxy=proxy_config,
                    semaphore=self.semaphore,
                    cooldown_429=self.config.cooldown_seconds_429,
                    retry_attempts=1 if self.config.research_enabled else 5,
                    **research_client_kwargs,
                ).__aenter__()
                logger.info("Tieba Client started.")

            self.db_engine = create_async_engine(self.config.database_url, echo=False)
            self.async_sessionmaker = async_sessionmaker(
                bind=self.db_engine, class_=AsyncSession, expire_on_commit=False
            )
            logger.info("PostgreSQL AsyncEngine created.")

            if self.config.consumer_transport == "redis" or self.config.cache_backend == "redis":
                try:
                    self.redis_client = await redis.from_url(self.config.redis_url, decode_responses=True)
                    await self.redis_client.ping()  # type: ignore
                    self.lock_manager = RedisLockManager(self.redis_client)
                    logger.info("Redis client connected successfully.")
                except Exception as e:
                    if self.config.consumer_transport == "redis" or self.config.cache_backend == "redis":
                        raise
                    logger.warning("Redis connection failed: {}. Redis commands will be unavailable.", e)
                    self.redis_client = None

            if self.config.consumer_transport == "websocket":
                # 启动内置 WS 服务
                self.ws_server = WebSocketServer(self.config.websocket_url, token=self.config.websocket_token)

                async def _ws_add_forum(fname: str, group: str | None = None) -> bool:
                    return await self.add_forum(fname, group)

                async def _remove_forum(fname: str) -> bool:
                    current = self.forums or []
                    self.forums = [f for f in current if f.fname != fname]
                    return True

                if self.config.mode == "periodic":
                    self.ws_server.on_add_forum(_ws_add_forum)
                    self.ws_server.on_remove_forum(_remove_forum)
                else:
                    logger.warning("WebSocket dynamic forum management is only supported in 'periodic' mode.")
                await self.ws_server.start()
                logger.info("WebSocket server started: %s", self.ws_server.get_ws_url())

            elif self.config.consumer_transport == "websocket":
                logger.warning("WebSocket consumer transport selected but WebSocket server is disabled in config.")
                logger.warning("Nothing will be sent to consumers.")

            logger.info("Container resources initialized successfully.")

        except Exception as e:
            logger.exception("Failed to initialize container resources: {}", e)
            await self.teardown()
            raise

    async def validate_research_auth(self) -> None:
        """Validate the explicitly configured local account without holding DB resources."""
        if not self.config.research_enabled or not self.config.fetch_user_forum_info:
            self.research_auth_ready.set()
            return
        if not self._research_credentials_loaded:
            from .auth import ResearchAuthError

            raise ResearchAuthError("paused_auth", "account_file_missing_or_invalid")
        await self._verify_research_auth()
        self.research_auth_ready.set()

    async def wait_for_initial_auth(self) -> None:
        """Wait for a changed account file, then validate it before resuming work."""
        if not self.config.research_enabled or not self.config.fetch_user_forum_info:
            self.research_auth_ready.set()
            return

        fingerprint = self._research_account_file_state
        while True:
            bduss, fingerprint = await wait_for_account_file_update(self.config.research_account_file, fingerprint)
            self._research_account_file_state = fingerprint
            await self._replace_research_accounts(bduss)
            self._research_credentials_loaded = True
            try:
                await self._verify_research_auth()
            except Exception as error:
                from .auth import ResearchAuthError

                if isinstance(error, ResearchAuthError) and error.status == "paused_auth":
                    continue
                raise
            self.research_auth_ready.set()
            return

    async def wait_for_auth_refresh(self, verify_callback, *, run_id: str | None = None):
        """Wait for an updated account file and retry the triggering API request."""
        if not self.config.research_enabled or not self.config.fetch_user_forum_info:
            return await verify_callback()

        from tiebameow.client.tieba_client import RetriableApiError

        from .auth import ResearchAuthError

        async with self._auth_refresh_lock:
            fingerprint = self._research_account_file_state
            while True:
                if not self.research_auth_ready.is_set():
                    bduss, fingerprint = await wait_for_account_file_update(
                        self.config.research_account_file,
                        fingerprint,
                    )
                    self._research_account_file_state = fingerprint
                    await self._replace_research_accounts(bduss)
                    self._research_credentials_loaded = True
                    try:
                        await self._verify_research_auth()
                    except ResearchAuthError as error:
                        if error.status == "paused_auth":
                            continue
                        raise
                    self.research_auth_ready.set()

                retry_limit = self.config.research_max_api_retries
                auth_rejected = False
                for attempt in range(1, retry_limit + 1):
                    try:
                        return await verify_callback()
                    except RetriableApiError as error:
                        if error.code == 4011:
                            self.research_auth_ready.clear()
                            fingerprint = self._research_account_file_state
                            auth_rejected = True
                            break
                        if error.code in {429, 220034} and attempt < retry_limit:
                            cooldown_seconds = self.config.cooldown_seconds_429
                            logger.warning(
                                "Research API auth refresh returned frequency limit code {}; "
                                "cooling down for {} seconds.",
                                error.code,
                                cooldown_seconds,
                            )
                            await self.wait_for_rate_limit(
                                cooldown_seconds,
                                run_id=run_id,
                            )
                            logger.info("Research API auth refresh cooldown ended; retrying at the configured rate.")
                            continue
                        raise
                if auth_rejected:
                    continue

    async def wait_for_research_ready(self) -> None:
        """Wait until auth/network gates are open and shared cooldown expires."""
        loop = asyncio.get_running_loop()
        while True:
            await self.research_auth_ready.wait()
            await self.research_network_ready.wait()
            async with self._research_rate_limit_lock:
                remaining = self._research_rate_limit_deadline - loop.time()
                if remaining <= 0 and self.research_auth_ready.is_set() and self.research_network_ready.is_set():
                    return
            await asyncio.sleep(max(remaining, 0))

    async def wait_for_research_network_ready(self) -> None:
        """Wait for cooldown/network block without requiring valid login state."""
        loop = asyncio.get_running_loop()
        while True:
            await self.research_network_ready.wait()
            async with self._research_rate_limit_lock:
                remaining = self._research_rate_limit_deadline - loop.time()
                if remaining <= 0 and self.research_network_ready.is_set():
                    return
            await asyncio.sleep(max(remaining, 0))

    async def wait_for_rate_limit(self, cooldown_seconds: float, *, run_id: str | None = None) -> None:
        """Extend and await the shared monotonic request cooldown deadline."""
        del run_id  # Status transitions belong to the caller; cooldown never reopens them.
        loop = asyncio.get_running_loop()
        async with self._research_rate_limit_lock:
            self._research_rate_limit_deadline = max(
                self._research_rate_limit_deadline,
                loop.time() + max(0.0, cooldown_seconds),
            )
        while True:
            async with self._research_rate_limit_lock:
                remaining = self._research_rate_limit_deadline - loop.time()
            if remaining <= 0:
                return
            await asyncio.sleep(remaining)

    async def record_research_api_response(self, *, suspicious_empty: bool) -> bool:
        """Track suspicious empty API envelopes across all research workers."""
        async with self._research_empty_response_lock:
            if not suspicious_empty:
                self._research_empty_response_streak = 0
                return False
            self._research_empty_response_streak += 1
            if self._research_empty_response_streak < 3:
                return False
            self._research_empty_response_streak = 0
            return True

    async def block_research_network(self) -> None:
        """Stop new research API requests until a new explicitly resumed process."""
        self.research_network_ready.clear()

    async def _research_request_gates_open(self) -> bool:
        if not self.research_auth_ready.is_set() or not self.research_network_ready.is_set():
            return False
        async with self._research_rate_limit_lock:
            return asyncio.get_running_loop().time() >= self._research_rate_limit_deadline

    async def _research_network_gates_open(self) -> bool:
        if not self.research_network_ready.is_set():
            return False
        async with self._research_rate_limit_lock:
            return asyncio.get_running_loop().time() >= self._research_rate_limit_deadline

    async def _verify_research_auth(self) -> None:
        if self.tb_client is None:
            from .auth import ResearchAuthError

            raise ResearchAuthError("incomplete", "account_validation_client_unavailable")
        if isinstance(self.semaphore, GateAwareSemaphore):
            async with self.semaphore.bypass_auth_gate():
                await self._verify_research_client()
        else:
            await self._verify_research_client()

    async def _verify_research_client(self) -> None:
        if self.tb_client is None:
            from .auth import ResearchAuthError

            raise ResearchAuthError("incomplete", "account_validation_client_unavailable")
        await verify_research_credentials(
            self.tb_client,
            self.threads_limiter,
            max_attempts=self.config.research_max_api_retries,
            retry_wait_seconds=self.config.research_retry_wait_seconds,
        )

    async def _replace_research_accounts(self, bduss: str) -> None:
        from aiotieba.core.account import Account

        if self._request_semaphore is None:
            raise RuntimeError("Cannot update research credentials before clients are initialized.")

        acquired = 0
        try:
            for _ in range(self.config.concurrency_limit):
                await self._request_semaphore.acquire()
                acquired += 1
            clients = self.tb_clients or ([self.tb_client] if self.tb_client is not None else [])
            for client in clients:
                if not isinstance(client, ResearchClient):
                    raise RuntimeError("Research account update requires ResearchClient instances.")
                client.account = Account(BDUSS=bduss)
                client.research_run_cache.clear()
        finally:
            for _ in range(acquired):
                self._request_semaphore.release()

    async def teardown(self):
        """异步关闭并清理所有资源。

        按相反顺序安全关闭所有已初始化的资源：
        1. Redis客户端连接
        2. PostgreSQL数据库引擎
        3. tiebameow.client.Client客户端

        该方法是幂等的，可以安全地多次调用。
        """
        logger.info("Tearing down container resources...")

        if self.redis_client:
            await self.redis_client.close()
            logger.info("Redis client closed.")
        if self.db_engine:
            await self.db_engine.dispose()
            logger.info("PostgreSQL AsyncEngine disposed.")

        # 关闭所有 tieba 客户端
        if self.tb_clients:
            for i, client in enumerate(self.tb_clients):
                await client.__aexit__()
                logger.info("Tieba Client #{} closed.", i)
        elif self.tb_client:
            await self.tb_client.__aexit__()
            logger.info("Tieba Client closed.")
        if self.ws_server:
            try:
                await self.ws_server.stop()
            except Exception:
                pass
            self.ws_server = None

        logger.info("Container resources torn down successfully.")
