# scrapecore/agent/agent.py

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Optional

import redis.asyncio as aioredis

from scrapecore.http.base import HttpBackend, HttpBackendError, NetworkError  # ← new
from scrapecore.http.aiohttp_backend import AiohttpBackend                    # ← new
from scrapecore.models.task import TaskEnvelope
from scrapecore.models.result import ResultEnvelope
from scrapecore.queue.task_queue import TaskQueue
from scrapecore.queue.result_queue import ResultQueue

logger = logging.getLogger(__name__)

ParserFn = Callable[[Any, dict], Any]


class Agent:
    """
    Remote worker process. Polls the task queue and executes scrape tasks.

    Args:
        agent_id:            Unique name for this agent instance.
        redis:               Connected async Redis client.
        parser_registry:     Dict mapping parser_key strings to callable parsers.
        namespace:           Must match the namespace used by the coordinator.
        num_workers:         Number of concurrent worker coroutines.
        requests_per_second: Rate limit applied per domain across all workers.
        claim_timeout:       Seconds a worker blocks on an empty queue before looping.
        heartbeat_interval:  Seconds between heartbeat writes to Redis.
        task_timeout:        Seconds before an HTTP request is abandoned.
                             Ignored if a custom http_backend is provided.
        proxy:               Proxy URL forwarded to every backend request.
                             Ignored if a custom http_backend is provided.
        http_backend:        HTTP backend to use for all requests.
                             Defaults to AiohttpBackend(timeout=task_timeout).
                             Pass a CurlCffiBackend instance for bot-protected sites.
    """

    def __init__(
        self,
        agent_id: str,
        redis: aioredis.Redis,
        parser_registry: dict[str, ParserFn],
        namespace: str = "default",
        num_workers: int = 2,
        requests_per_second: float = 1.0,
        claim_timeout: float = 5.0,
        heartbeat_interval: float = 10.0,
        task_timeout: int = 30,
        proxy: Optional[str] = None,
        http_backend: Optional[HttpBackend] = None,   # ← new
    ) -> None:
        self.agent_id = agent_id
        self._redis = redis
        self._registry = parser_registry
        self._namespace = namespace
        self._num_workers = num_workers
        self._requests_per_second = requests_per_second
        self._claim_timeout = claim_timeout
        self._heartbeat_interval = heartbeat_interval
        self._task_timeout = task_timeout
        self._proxy = proxy

        # ← new: default to AiohttpBackend so existing call sites need no change
        self._backend: HttpBackend = (
            http_backend if http_backend is not None
            else AiohttpBackend(timeout=task_timeout)
        )

        self._task_queue = TaskQueue(redis, namespace)
        self._result_queue = ResultQueue(redis, namespace)

        self._active_tasks: set[str] = set()
        self._stop_event = asyncio.Event()

        self._rate_lock: dict[str, asyncio.Lock] = {}
        self._last_request: dict[str, float] = {}
        self._min_delay = 1.0 / (requests_per_second / num_workers)

        self.tasks_completed = 0
        self.tasks_failed = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the agent. Blocks until stop() is called."""
        logger.info(f"Agent {self.agent_id!r} starting ({self._num_workers} workers)")

        # ← removed: aiohttp.ClientSession creation — backend owns its session now

        try:
            async with asyncio.TaskGroup() as tg:
                for i in range(self._num_workers):
                    tg.create_task(self._worker_loop(worker_index=i))
                tg.create_task(self._heartbeat_loop())
        finally:
            # ← new: always close the backend even if a worker raises
            await self._backend.close()

        logger.info(f"Agent {self.agent_id!r} shut down cleanly")

    async def stop(self) -> None:
        logger.info(f"Agent {self.agent_id!r} stop requested")
        self._stop_event.set()

    # ── Worker loop ───────────────────────────────────────────────────────────

    async def _worker_loop(self, worker_index: int) -> None:
        worker_id = f"{self.agent_id}:worker-{worker_index}"
        logger.debug(f"Worker {worker_id} started")

        while not self._stop_event.is_set() or self._active_tasks:
            claimed = await self._task_queue.claim(timeout=self._claim_timeout)
            if claimed is None:
                continue

            task_id, raw_payload = claimed
            envelope = TaskEnvelope.from_dict(raw_payload)
            self._active_tasks.add(task_id)

            try:
                result = await self._execute(envelope)
            except Exception as e:
                logger.error(f"Unhandled error in worker {worker_id}: {e}", exc_info=True)
                result = ResultEnvelope.failure(
                    task_id=envelope.task_id,
                    job_id=envelope.job_id,
                    agent_id=self.agent_id,
                    retry_count=envelope.retry_count,
                    max_retries=envelope.max_retries,
                    error=f"Unhandled: {type(e).__name__}: {e}",
                )

            await self._result_queue.push(result)

            if result.status == "completed":
                await self._task_queue.acknowledge(task_id)
                self.tasks_completed += 1
            else:
                cancelled = await self._redis.sismember(
                    f"scrapecore:{self._namespace}:cancelled_jobs", envelope.job_id
                )
                if cancelled or not envelope.should_retry():
                    await self._task_queue.acknowledge(task_id)
                else:
                    retried = envelope.increment_retry()
                    await self._task_queue.reject(task_id, retried.to_dict(), retried.priority)
                self.tasks_failed += 1

            self._active_tasks.discard(task_id)

        logger.debug(f"Worker {worker_id} exited")

    # ── Task execution ────────────────────────────────────────────────────────

    async def _execute(self, envelope: TaskEnvelope) -> ResultEnvelope:
        """
        Resolve the parser, make the HTTP request, call the parser, return result.
        """
        payload  = envelope.payload
        url      = payload["url"]
        method   = payload.get("method", "GET")
        headers  = payload.get("headers", {})
        params   = payload.get("params")
        body     = payload.get("body")
        metadata = payload.get("metadata", {})

        parser_fn = self._registry.get(envelope.parser_key)
        if parser_fn is None:
            raise ValueError(
                f"No parser registered for key {envelope.parser_key!r}. "
                f"Registered: {list(self._registry.keys())}"
            )

        await self._rate_limit(url)

        # ← replaced: aiohttp request block → backend call
        try:
            response = await self._backend.request(
                method,
                url,
                headers=headers,
                params=params,
                body=body,
                proxy=self._proxy,
            )
        except HttpBackendError as e:
            # ← restored: 429 back-off lives here, not in the backend
            if e.status == 429:
                logger.warning(f"429 from {url} — backing off 60s")
                await asyncio.sleep(60)
            raise RuntimeError(f"HTTP {e.status} from {e.url}: {e.message}")

        except NetworkError as e:
            raise RuntimeError(str(e))

        # ← updated: parse text → JSON ourselves using response.content_type
        if "application/json" in response.content_type:
            raw = json.loads(response.text)
        else:
            raw = response.text

        loop = asyncio.get_running_loop()
        if asyncio.iscoroutinefunction(parser_fn):
            parsed = await parser_fn(raw, metadata)
        else:
            parsed = await loop.run_in_executor(None, parser_fn, raw, metadata)

        if not isinstance(parsed, dict):
            raise TypeError(
                f"Parser {envelope.parser_key!r} must return a dict, "
                f"got {type(parsed).__name__}"
            )

        stage_output = parsed.pop("__stage_output__", None)

        return ResultEnvelope.success(
            task_id=envelope.task_id,
            job_id=envelope.job_id,
            agent_id=self.agent_id,
            retry_count=envelope.retry_count,
            max_retries=envelope.max_retries,
            output=parsed,
            stage_output=stage_output,
        )

    # ── Rate limiting ─────────────────────────────────────────────────────────

    async def _rate_limit(self, url: str) -> None:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc

        if domain not in self._rate_lock:
            self._rate_lock[domain] = asyncio.Lock()

        async with self._rate_lock[domain]:
            now  = time.monotonic()
            wait = self._min_delay - (now - self._last_request.get(domain, 0.0))
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request[domain] = time.monotonic()

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        key    = f"scrapecore:{self._namespace}:heartbeat:{self.agent_id}"
        expiry = int(self._heartbeat_interval * 3)

        while not self._stop_event.is_set():
            await self._redis.set(key, str(time.time()), ex=expiry)
            logger.debug(f"Heartbeat written for agent {self.agent_id!r}")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._heartbeat_interval,
                )
            except asyncio.TimeoutError:
                pass

        await self._redis.delete(key)
        logger.debug(f"Heartbeat key deleted for agent {self.agent_id!r}")

    # ── Introspection ─────────────────────────────────────────────────────────

    def get_statistics(self) -> dict[str, Any]:
        return {
            "agent_id":        self.agent_id,
            "tasks_completed": self.tasks_completed,
            "tasks_failed":    self.tasks_failed,
            "active_tasks":    len(self._active_tasks),
        }

    def __repr__(self) -> str:
        return (
            f"Agent(id={self.agent_id!r}, "
            f"workers={self._num_workers}, "
            f"completed={self.tasks_completed}, "
            f"failed={self.tasks_failed})"
        )