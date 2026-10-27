# scrapecore/agent/agent.py
from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from typing import Any, Optional

from scrapecore.http.base import HttpBackend, HttpBackendError, NetworkError, ScrapeFailedError
from scrapecore.http.aiohttp_backend import AiohttpBackend
from scrapecore.models.task import TaskEnvelope
from scrapecore.models.result import ResultEnvelope
from scrapecore.plugins.auth import BaseAuth
from scrapecore.transport.base import FATAL_TRANSPORT_ERRORS, AgentTransport, TaskNotHeld

logger = logging.getLogger(__name__)

try:
    _SCRAPECORE_VERSION = _pkg_version("scrapecore")
except PackageNotFoundError: # running from a source tree, not installed
    _SCRAPECORE_VERSION = "unknown"

# Backoff sequence (seconds) for 429 / 5xx retries within a single task execution.
# After all attempts exhaust, the failure surfaces to the queue-level retry
# mechanism (TaskEnvelope.max_retries).
_HTTP_RETRY_BACKOFF: tuple[int, ...] = (10, 20, 40, 80, 160)

# Backoff (seconds) when the transport is unreachable, e.g. the laptop is
# offline or the gateway is restarting. Doubles up to the cap; resets on success.
_TRANSPORT_BACKOFF_START = 1.0
_TRANSPORT_BACKOFF_MAX = 30.0


class Agent:
    """
    Remote worker process. Polls the task queue and fetches each task's URL.

    Agents do not parse. Successful fetch is returned as a "fetched"
    ResultEnvelope carrying the raw response.
    Coordinator runs the parser. Agents therefore no site-specific code beyond auth.

    1 Agent instance == 1 account/session.
    Throughput scaling is achieved by running multiple Agent instances.
    The run loop is a single sequential coroutine:
    claim → rate-limit → fetch → submit result.

    The agent talks to the server only through its transport, never to
    Redis directly, so the same agent runs in-process against Redis
    (RedisTransport) or remotely through a gateway.

    Args:
        agent_id:           Unique name for this agent instance.
        transport:          AgentTransport used to claim tasks, submit
                            results and report presence, e.g.
                            RedisTransport(redis, namespace).
        rate_limit_config:  Dict mapping a task's parser_key → requests_per_second.
                            Each key gets its own independent rate bucket.
                            Example:
                                {
                                    "mysite.endpoint1": 0.15,
                                    "mysite.endpoint2": 0.03,
                                    "mysite.endpoint3": 0.03,
                                }
                            Parser keys absent from this dict fall back to
                            1.0 RPS, domain-keyed.
        claim_timeout:      Seconds the run loop blocks on an empty queue.
        heartbeat_interval: Seconds between heartbeats sent via the transport.
        task_timeout:       Total HTTP timeout in seconds.
                            Ignored if custom http_backend is provided.
        proxy:              Proxy URL forwarded to every backend request.
                            Ignored if a custom http_backend is provided.
        http_backend:       HTTP backend. Defaults to AiohttpBackend.
        auth_provider:      Authentication provider injected before every request.
    """

    def __init__(
        self,
        agent_id: str,
        transport: AgentTransport,
        rate_limit_config: dict[str, float] | None = None,
        claim_timeout: float = 5.0,
        heartbeat_interval: float = 10.0,
        task_timeout: int = 30,
        proxy: Optional[str] = None,
        http_backend: Optional[HttpBackend] = None,
        auth_provider: BaseAuth | None = None,
    ) -> None:
        self.agent_id = agent_id
        self._transport = transport
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

        self._stop_event = asyncio.Event()

        self._rate_lock: dict[str, asyncio.Lock] = {}
        self._last_request: dict[str, float] = {}

        self.tasks_completed = 0
        self.tasks_failed = 0

        self._host = socket.gethostname()
        self._started_at = time.time()
        self._current_parser_key: Optional[str] = None

        self._fatal_error: Optional[BaseException] = None

        self._stage_counts: dict[str, int] = {}
        self._stage_started_at: Optional[float] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start the agent. Blocks until stop() is called.

        Transient transport failures (server unreachable) are retried with
        backoff and never stop the agent.

        :raises AuthenticationFailed, UpgradeRequired: if the server rejects
                this agent. The agent stops, since retrying cannot help.
        """
        logger.info(f"Agent {self.agent_id!r} starting")
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._run_loop())
                tg.create_task(self._heartbeat_loop())
        finally:
            await self._backend.close()
            close_transport = getattr(self._transport, "close", None)
            if close_transport is not None:
                await close_transport()
        if self._fatal_error is not None:
            logger.critical(f"Agent {self.agent_id!r} stopped: {self._fatal_error}")
            raise self._fatal_error
        logger.info(f"Agent {self.agent_id!r} shut down cleanly")

    def _stop_fatally(self, error: BaseException) -> None:
        """Record a non-retriable transport error and stop every loop."""
        if self._fatal_error is None:
            self._fatal_error = error
        self._stop_event.set()

    async def _wait(self, seconds: float) -> None:
        """Sleep, waking early on stop()."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def stop(self) -> None:
        """Stop the agent."""
        logger.info(f"Agent {self.agent_id!r} stop requested")
        self._stop_event.set()

    # ── Run loop ──────────────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        """
        Single sequential execution loop: claim → execute → submit.

        One task is in flight at a time per agent.
        """
        logger.debug(f"Agent {self.agent_id!r} run loop started")
        backoff = _TRANSPORT_BACKOFF_START

        while not self._stop_event.is_set():
            try:
                envelope = await self._transport.claim(timeout=self._claim_timeout)
            except FATAL_TRANSPORT_ERRORS as e:
                self._stop_fatally(e)
                break
            except Exception as e:
                logger.warning(
                    f"Agent {self.agent_id!r} could not claim a task ({e}); "
                    f"retrying in {backoff:.0f}s"
                )
                await self._wait(backoff)
                backoff = min(backoff * 2, _TRANSPORT_BACKOFF_MAX)
                continue
            backoff = _TRANSPORT_BACKOFF_START
            if envelope is None:
                continue

            try:
                result = await self._execute(envelope)
            except ScrapeFailedError as e:
                # Expected task failure - no "Unhandled" prefix.
                logger.warning(
                    f"Task {envelope.task_id} failed in agent "
                    f"{self.agent_id!r}: {e}"
                )
                result = ResultEnvelope.failure(
                    task_id=envelope.task_id,
                    job_id=envelope.job_id,
                    agent_id=self.agent_id,
                    retry_count=envelope.retry_count,
                    max_retries=envelope.max_retries,
                    error=str(e),
                )
            except Exception as e:
                # Genuinely unexpected - parser bug, schema mismatch, etc.
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
        
            # The transport delivers the result and decides retry vs. done.
            try:
                await self._transport.submit(envelope, result)
            except TaskNotHeld:
                logger.warning(
                    f"Task {envelope.task_id} was reassigned before agent "
                    f"{self.agent_id!r} submitted it; result discarded"
                )
            except FATAL_TRANSPORT_ERRORS as e:
                self._stop_fatally(e)
                break
            except Exception as e:
                # The task is still claimed server-side; stale-task recovery
                # will hand it to an agent again, so nothing is lost for good.
                logger.error(
                    f"Agent {self.agent_id!r} could not submit task "
                    f"{envelope.task_id}: {e}. It will be re-run after stale recovery."
                )

            if result.status == "completed":
                self.tasks_completed += 1
            else:
                self.tasks_failed += 1

        logger.debug(f"Agent {self.agent_id!r} run loop exited")

    # ── Task execution ────────────────────────────────────────────────────────

    async def _execute(self, envelope: TaskEnvelope) -> ResultEnvelope:
        """
        Rate-limit → HTTP (with backoff retry) → return the raw response.

        Auth headers are re-applied on every retry attempt.

        A background task renews this task's claim in Redis every 20s for as
        long as execution is in flight (including HTTP backoff sleeps).
        Cancelled in `finally` regardless of how this method exits.
        """
        payload      = envelope.payload
        url          = payload["url"]
        method       = payload.get("method", "GET")
        task_headers = dict(payload.get("headers", {}))
        params       = payload.get("params")
        body         = payload.get("body")

        async def _renew_loop() -> None:
            while True:
                await asyncio.sleep(20)
                try:
                    await self._transport.renew(envelope.task_id)
                except Exception as e:
                    # A missed renewal only risks stale recovery re-running
                    # the task; the fetch itself carries on.
                    logger.warning(f"Renewing task {envelope.task_id} failed: {e}")
        self._current_parser_key = envelope.parser_key
        renew_task = asyncio.create_task(_renew_loop())
        try:
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
                            raise ScrapeFailedError(
                                f"HTTP {e.status} after {len(_HTTP_RETRY_BACKOFF)} retries "
                                f"(parser={envelope.parser_key}): {e.url}"
                            )
                        logger.warning(
                            f"HTTP {e.status} — attempt {attempt + 1}/{len(_HTTP_RETRY_BACKOFF)} "
                            f"(parser={envelope.parser_key}) — retrying in {backoff_secs}s"
                        )
                        await asyncio.sleep(backoff_secs)
                    else:
                        raise ScrapeFailedError(f"HTTP {e.status} from {e.url}: {e.message}")

                except NetworkError as e:
                    raise ScrapeFailedError(str(e))

            return ResultEnvelope.fetched(
                task=envelope.to_dict(),
                agent_id=self.agent_id,
                response={
                    "status":       response.status,
                    "content_type": response.content_type,
                    "headers":      dict(response.headers or {}),
                    "text":         response.text,
                    "url":          url,
                },
            )
        finally:
            self._current_parser_key = None
            renew_task.cancel()
            try:
                await renew_task
            except asyncio.CancelledError:
                pass

    # ── Rate limiting ─────────────────────────────────────────────────────────

    async def _rate_limit(self, url: str, parser_key: str = "") -> None:
        """
        Enforce per-parser-key rate limiting.

        Each parser key in rate_limit_config gets its own Lock and timestamp,
        so distinct parsers run against independent buckets and don't block each other.
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
        """
        Announce presence every heartbeat_interval, with a TTL of three
        intervals, and announce departure on shutdown. Failures are logged
        and retried on the next beat; fatal transport errors stop the agent.
        """
        expiry = int(self._heartbeat_interval * 3)

        while not self._stop_event.is_set():
            try:
                await self._transport.heartbeat(self.agent_id, self._heartbeat_info(), expiry)
                logger.debug(f"Heartbeat sent for agent {self.agent_id!r}")
            except FATAL_TRANSPORT_ERRORS as e:
                self._stop_fatally(e)
                break
            except Exception as e:
                logger.warning(f"Heartbeat for agent {self.agent_id!r} failed: {e}")
            await self._wait(self._heartbeat_interval)

        if self._fatal_error is None:
            try:
                await self._transport.leave(self.agent_id)
                logger.debug(f"Agent {self.agent_id!r} left (presence removed)")
            except Exception as e:
                # Presence expires on its own after the heartbeat TTL.
                logger.warning(f"Agent {self.agent_id!r} could not announce leaving: {e}")

    def _heartbeat_info(self) -> dict[str, Any]:
        """TODO: Docstring"""
        return {
            "host": self._host,
            "pid": os.getpid(),
            "version": _SCRAPECORE_VERSION,
            "started_at": self._started_at,
            "state": "busy" if self._current_parser_key else "idle",
            "current_parser_key": self._current_parser_key,
            "tasks_completed": self.tasks_completed,
            "tasks_failed": self.tasks_failed,
        }
    # ── Introspection ─────────────────────────────────────────────────────────

    def get_statistics(self) -> dict[str, Any]:
        """TODO: Docstring"""
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