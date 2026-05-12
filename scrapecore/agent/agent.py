"""
scrapecore/agent/agent.py

Agent — the remote worker process.

An agent is started on each machine that participates in the scraping network.
It connects to Redis, registers its parser functions, and enters a polling loop.

Lifecycle:
    1. Consumer calls Agent(...) with a parser registry and config
    2. Consumer calls await agent.start() — this blocks until stop() is called
    3. Internally: N worker coroutines run concurrently, each polling the task queue
    4. Each worker: claim → execute → push result → repeat
    5. A heartbeat coroutine runs alongside, writing a timestamp to Redis every N seconds
    6. Consumer calls await agent.stop() to drain in-progress tasks and shut down

Parser registry format:
    {
        "autopiter.parse_appraise":    parse_appraise,
        "autopiter.parse_searchdetails": parse_searchdetails,
    }

The key must match the parser_key field in TaskEnvelope exactly.
The value is any async or sync callable with the signature:
    parser(raw_response: Any, metadata: dict) -> dict

Note on sync vs async parsers:
    The existing parsers in gng_pricing are synchronous functions.
    The agent runs them in a thread executor to avoid blocking the event loop.
    This means no changes are needed to existing parser code.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Optional

import aiohttp
import redis.asyncio as aioredis

from scrapecore.models.task import TaskEnvelope
from scrapecore.models.result import ResultEnvelope
from scrapecore.queue.task_queue import TaskQueue
from scrapecore.queue.result_queue import ResultQueue

logger = logging.getLogger(__name__)

# Type alias for parser functions registered by the consumer
ParserFn = Callable[[Any, dict], Any]


class Agent:
    """
    Remote worker process. Polls the task queue and executes scrape tasks.

    Args:
        agent_id:          Unique name for this agent instance. Used in heartbeats
                           and result envelopes. E.g. "machine-01".
        redis:             Connected async Redis client.
        parser_registry:   Dict mapping parser_key strings to callable parsers.
        namespace:         Must match the namespace used by the coordinator.
        num_workers:       Number of concurrent worker coroutines.
        requests_per_second: Rate limit applied per domain across all workers
                           on this agent. Shared with the distributed limiter.
        claim_timeout:     Seconds a worker blocks on an empty queue before
                           looping. Lower = faster shutdown response.
        heartbeat_interval: Seconds between heartbeat writes to Redis.
        task_timeout:      Seconds before an HTTP request is abandoned.
        proxy:             #TODO: add docstring desc
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

        self._task_queue = TaskQueue(redis, namespace)
        self._result_queue = ResultQueue(redis, namespace)

        # Tracks task_ids currently held by this agent
        self._active_tasks: set[str] = set()
        self._stop_event = asyncio.Event()

        # aiohttp session — created on start, shared across all workers
        self._session: Optional[aiohttp.ClientSession] = None

        # Proxy
        self._proxy = proxy

        # Per-domain rate limiting — maps domain → last request time
        self._rate_lock: dict[str, asyncio.Lock] = {}
        self._last_request: dict[str, float] = {}
        self._min_delay = 1.0 / (requests_per_second / num_workers)

        # Statistics
        self.tasks_completed = 0
        self.tasks_failed = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start the agent. Blocks until stop() is called.

        Creates an aiohttp session, spawns worker coroutines and the heartbeat
        loop, then waits for the stop event.
        """
        logger.info(f"Agent {self.agent_id!r} starting ({self._num_workers} workers)")

        timeout = aiohttp.ClientTimeout(total=self._task_timeout)
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(force_close=True),
            timeout=timeout,
            headers={"User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )},
        )

        async with asyncio.TaskGroup() as tg:
            for i in range(self._num_workers):
                tg.create_task(self._worker_loop(worker_index=i))
            tg.create_task(self._heartbeat_loop())

        await self._session.close()
        logger.info(f"Agent {self.agent_id!r} shut down cleanly")

    async def stop(self) -> None:
        """
        Signal the agent to stop after finishing in-progress tasks.
        """
        logger.info(f"Agent {self.agent_id!r} stop requested")
        self._stop_event.set()

    # ── Worker loop ───────────────────────────────────────────────────────────

    async def _worker_loop(self, worker_index: int) -> None:
        """
        Main loop for a single worker coroutine.

        Runs until the stop event is set AND there are no active tasks
        on this agent (clean drain).
        """
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
                # Catch-all so a bug in _execute never kills the worker loop
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
                if envelope.should_retry():
                    retried = envelope.increment_retry()
                    await self._task_queue.reject(task_id, retried.to_dict(), retried.priority)
                else:
                    await self._task_queue.acknowledge(task_id)
                self.tasks_failed += 1

            self._active_tasks.discard(task_id)

        logger.debug(f"Worker {worker_id} exited")

    # ── Task execution ────────────────────────────────────────────────────────

    async def _execute(self, envelope: TaskEnvelope) -> ResultEnvelope:
        """
        Resolve the parser, make the HTTP request, call the parser, return result.

        The payload dict is expected to contain:
            url:      str  — target URL
            method:   str  — HTTP method, default "GET"
            headers:  dict — extra headers
            params:   dict — query parameters (appended to URL)
            body:     dict — JSON body for POST requests
            metadata: dict — passed through to the parser unchanged

        The parser receives (raw_response, metadata) and returns a dict.
        Sync parsers are run in a thread executor to avoid blocking the loop.
        """
        payload = envelope.payload
        url = payload["url"]
        method = payload.get("method", "GET")
        headers = payload.get("headers", {})
        params = payload.get("params")
        body = payload.get("body")
        metadata = payload.get("metadata", {})

        parser_fn = self._registry.get(envelope.parser_key)
        if parser_fn is None:
            raise ValueError(
                f"No parser registered for key {envelope.parser_key!r}. "
                f"Registered: {list(self._registry.keys())}"
            )

        # Rate limiting
        await self._rate_limit(url)

        try:
            async with self._session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=body,
                proxy=self._proxy,
            ) as response:
                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "")

                if "application/json" in content_type:
                    raw = await response.json()
                else:
                    raw = await response.text()

        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                logger.warning(f"429 from {url} — backing off 60s")
                await asyncio.sleep(60)
            raise RuntimeError(f"HTTP {e.status} from {url}: {e.message}")

        except aiohttp.ClientError as e:
            raise RuntimeError(f"Network error fetching {url}: {e}")

        # Call the parser — run sync functions in a thread so they don't
        # block the event loop while parsing large HTML or JSON responses.
        loop = asyncio.get_running_loop()
        if asyncio.iscoroutinefunction(parser_fn):
            parsed = await parser_fn(raw, metadata)
        else:
            parsed = await loop.run_in_executor(None, parser_fn, raw, metadata)

        # parsed must be a dict — the serialization contract requires it.
        # If the consumer's parser returns a dataclass or custom object,
        # it should call .to_dict() itself before returning.
        if not isinstance(parsed, dict):
            raise TypeError(
                f"Parser {envelope.parser_key!r} must return a dict, "
                f"got {type(parsed).__name__}"
            )

        # Separate top-level output from any stage_output the parser signals
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
        """
        Per-domain rate limiting using an asyncio lock per domain.
        Serialises requests to the same domain and enforces min_delay between them.
        This is the local (per-agent) limiter.
        The distributed limiter (coordinator-side) is built in the next step.
        """
        from urllib.parse import urlparse
        domain = urlparse(url).netloc

        if domain not in self._rate_lock:
            self._rate_lock[domain] = asyncio.Lock()

        async with self._rate_lock[domain]:
            now = time.monotonic()
            wait = self._min_delay - (now - self._last_request.get(domain, 0.0))
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request[domain] = time.monotonic()

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """
        Writes a timestamp to Redis every heartbeat_interval seconds.

        Key: scrapecore:{namespace}:heartbeat:{agent_id}
        Value: current unix timestamp as a string
        Expiry: heartbeat_interval * 3 — auto-expires if agent dies

        The coordinator checks these keys to determine which agents are alive.
        An agent with no heartbeat key is considered dead.
        """
        key = f"scrapecore:{self._namespace}:heartbeat:{self.agent_id}"
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

        # Delete heartbeat key on clean shutdown so coordinator knows immediately
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