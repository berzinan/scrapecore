# scrapecore/agent/agent.py
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Optional
import redis.asyncio as aioredis

from scrapecore.http.base import HttpBackend, HttpBackendError, NetworkError
from scrapecore.http.aiohttp_backend import AiohttpBackend
from scrapecore.models.task import TaskEnvelope
from scrapecore.models.result import ResultEnvelope
from scrapecore.queue.task_queue import TaskQueue
from scrapecore.queue.result_queue import ResultQueue
from scrapecore.plugins.auth import BaseAuth

logger = logging.getLogger(__name__)
ParserFn = Callable[[Any, dict], Any]

# Backoff sequence (seconds) for 429 / 5xx retries within a single task execution.
# After all attempts exhaust, the failure surfaces to the queue-level retry
# mechanism (TaskEnvelope.max_retries).
_HTTP_RETRY_BACKOFF: tuple[int, ...] = (10, 20, 40, 80, 160)


class Agent:
    """
    Remote worker process. Polls the task queue and executes scrape tasks.

    One Agent instance corresponds to one account/session. Throughput scaling
    is achieved by running multiple Agent instances with distinct credentials,
    not by adding workers within a single instance. The run loop is a single
    sequential coroutine: claim → rate-limit → execute → push result.

    Args:
        agent_id:           Unique name for this agent instance.
        redis:              Connected async Redis client.
        parser_registry:    Dict mapping parser_key strings to callable parsers.
        namespace:          Must match the namespace used by the coordinator.
        rate_limit_config:  Dict mapping parser_key → requests_per_second.
                            Each key gets its own independent rate bucket so
                            pipeline stages with different server-side limits
                            can be tuned independently.
                            Example:
                                {
                                    "autopiter.parse_searchdetails": 0.15,
                                    "autopiter.parse_getcosts":      0.03,
                                    "autopiter.parse_appraise":      0.03,
                                }
                            Parser keys absent from this dict fall back to
                            1.0 RPS, domain-keyed (conservative default).
        claim_timeout:      Seconds the run loop blocks on an empty queue.
        heartbeat_interval: Seconds between heartbeat writes to Redis.
        task_timeout:       Total HTTP timeout in seconds.
                            Ignored if a custom http_backend is provided.
        proxy:              Proxy URL forwarded to every backend request.
                            Ignored if a custom http_backend is provided.
        http_backend:       HTTP backend. Defaults to AiohttpBackend.
        auth_provider:      Authentication provider injected before every request.
    """

    def __init__(
        self,
        agent_id: str,
        redis: aioredis.Redis,
        parser_registry: dict[str, ParserFn],
        namespace: str = "default",
        rate_limit_config: dict[str, float] | None = None,
        claim_timeout: float = 5.0,
        heartbeat_interval: float = 10.0,
        task_timeout: int = 30,
        proxy: Optional[str] = None,
        http_backend: Optional[HttpBackend] = None,
        auth_provider: BaseAuth | None = None,
    ) -> None:
        self.agent_id = agent_id
        self._redis = redis
        self._registry = parser_registry
        self._namespace = namespace
        self._rate_limit_config: dict[str, float] = rate_limit_config or {}
        self._claim_timeout = claim_timeout
        self._heartbeat_interval = heartbeat_interval
        self._task_timeout = task_timeout
        self._proxy = proxy

        self._backend: HttpBackend = (
            http_backend if http_backend is not None
            else AiohttpBackend(timeout=task_timeout)
        )

        self._auth_provider = auth_provider

        self._task_queue = TaskQueue(redis, namespace)
        self._result_queue = ResultQueue(redis, namespace)

        self._stop_event = asyncio.Event()

        self._rate_lock: dict[str, asyncio.Lock] = {}
        self._last_request: dict[str, float] = {}

        self.tasks_completed = 0
        self.tasks_failed = 0

        self._stage_counts: dict[str, int] = {}
        self._stage_started_at: Optional[float] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the agent. Blocks until stop() is called."""
        logger.info(f"Agent {self.agent_id!r} starting")
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._run_loop())
                tg.create_task(self._heartbeat_loop())
        finally:
            await self._backend.close()
        logger.info(f"Agent {self.agent_id!r} shut down cleanly")

    async def stop(self) -> None:
        """Stop the agent."""
        logger.info(f"Agent {self.agent_id!r} stop requested")
        self._stop_event.set()

    # ── Run loop ──────────────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        """
        Single sequential execution loop: claim → execute → push.

        One task is in flight at a time per agent. Multiple accounts →
        multiple agents → multiple parallel executions, each with its own
        session and rate limiter state.
        """
        logger.debug(f"Agent {self.agent_id!r} run loop started")

        while not self._stop_event.is_set():
            claimed = await self._task_queue.claim(timeout=self._claim_timeout)
            if claimed is None:
                continue

            task_id, raw_payload = claimed
            envelope = TaskEnvelope.from_dict(raw_payload)

            try:
                result = await self._execute(envelope)
            except Exception as e:
                logger.error(
                    f"Unhandled error in agent {self.agent_id!r}: {e}",
                    exc_info=True,
                )
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

        logger.debug(f"Agent {self.agent_id!r} run loop exited")

    # ── Task execution ────────────────────────────────────────────────────────

    async def _execute(self, envelope: TaskEnvelope) -> ResultEnvelope:
        """
        Resolve parser → rate-limit → HTTP (with backoff retry) → parse → return.

        Auth headers are re-applied on every retry attempt so a session refresh
        that fires between attempts is reflected immediately.
        """
        payload      = envelope.payload
        url          = payload["url"]
        method       = payload.get("method", "GET")
        task_headers = dict(payload.get("headers", {}))
        params       = payload.get("params")
        body         = payload.get("body")
        metadata     = payload.get("metadata", {})

        parser_fn = self._registry.get(envelope.parser_key)
        if parser_fn is None:
            raise ValueError(
                f"No parser registered for key {envelope.parser_key!r}. "
                f"Registered: {list(self._registry.keys())}"
            )

        await self._rate_limit(url, envelope.parser_key)

        if self._stage_started_at is None:
            self._stage_started_at = time.monotonic()
        self._stage_counts[envelope.parser_key] = (
            self._stage_counts.get(envelope.parser_key, 0) + 1
        )
        elapsed = time.monotonic() - self._stage_started_at
        if self._stage_counts[envelope.parser_key] % 25 == 0:
            logger.info(
                f"[stage_stats] elapsed={elapsed:6.1f}s "
                f"counts={dict(self._stage_counts)}"
            )

        # HTTP request with per-attempt auth refresh and backoff on 429 / 5xx.
        # Non-retryable status codes (other 4xx) raise immediately.
        response = None
        for attempt, backoff_secs in enumerate(_HTTP_RETRY_BACKOFF):
            request_headers = dict(task_headers)  # fresh copy — auth may have changed
            try:
                if self._auth_provider:
                    request_headers = await self._auth_provider.prepare_request(
                        request_headers, url
                    )
                response = await self._backend.request(
                    method, url,
                    headers=request_headers,
                    params=params,
                    body=body,
                    proxy=self._proxy,
                )
                if self._auth_provider:
                    await self._auth_provider.handle_response(
                        response.status, response.headers, url
                    )
                break  # success — exit retry loop

            except HttpBackendError as e:
                if self._auth_provider:
                    await self._auth_provider.handle_response(e.status, e.headers, url)
                if e.status == 429 or e.status >= 500:
                    if attempt == len(_HTTP_RETRY_BACKOFF) - 1:
                        raise RuntimeError(
                            f"HTTP {e.status} after {len(_HTTP_RETRY_BACKOFF)} retries "
                            f"(parser={envelope.parser_key}): {e.url}"
                        )
                    logger.warning(
                        f"HTTP {e.status} — attempt {attempt + 1}/{len(_HTTP_RETRY_BACKOFF)} "
                        f"(parser={envelope.parser_key}) — retrying in {backoff_secs}s"
                    )
                    await asyncio.sleep(backoff_secs)
                else:
                    raise RuntimeError(f"HTTP {e.status} from {e.url}: {e.message}")

            except NetworkError as e:
                raise RuntimeError(str(e))

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

    async def _rate_limit(self, url: str, parser_key: str = "") -> None:
        """
        Enforce per-parser-key rate limiting.

        Each parser key in rate_limit_config gets its own Lock and timestamp,
        so searchdetails (0.15 RPS) and appraise (0.03 RPS) run against
        completely independent buckets and do not block each other.
        """
        from urllib.parse import urlparse

        if parser_key and parser_key in self._rate_limit_config:
            bucket    = parser_key
            min_delay = 1.0 / self._rate_limit_config[parser_key]
        else:
            bucket    = urlparse(url).netloc
            min_delay = 1.0  # conservative fallback for unconfigured keys

        if bucket not in self._rate_lock:
            self._rate_lock[bucket] = asyncio.Lock()

        async with self._rate_lock[bucket]:
            now  = time.monotonic()
            wait = min_delay - (now - self._last_request.get(bucket, 0.0))
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request[bucket] = time.monotonic()

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
        }

    def __repr__(self) -> str:
        return (
            f"Agent(id={self.agent_id!r}, "
            f"completed={self.tasks_completed}, "
            f"failed={self.tasks_failed})"
        )