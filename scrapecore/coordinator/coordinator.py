"""
scrapecore/coordinator/coordinator.py

Coordinator — server-side orchestration process.

Runs four concurrent loops:
    1. dispatch_loop    — picks up pending jobs, builds and enqueues TaskEnvelopes
    2. result_loop      — drains ResultEnvelopes, updates job state, triggers stages
    3. recovery_loop    — re-enqueues tasks whose agents died mid-execution
    4. heartbeat_loop   — writes coordinator liveness key to Redis

Consumer integration:
    The coordinator operates entirely on:
        - JobRecord dicts   (provided by the job store adapter)
        - TaskEnvelopes     (built by a consumer-supplied task factory)
        - ResultEnvelopes   (built by agents)

    The consumer supplies two required callables and one optional hook:

    task_factory(job: dict) -> list[TaskEnvelope]
        Given a job record, return the first-stage TaskEnvelopes to enqueue.

    stage_handler(result: ResultEnvelope) -> list[TaskEnvelope] | None
        Given a completed result that contains stage_output, return the
        next-stage TaskEnvelopes to enqueue, or None if this is a final result.
        For single-stage sites: always return None.

    on_result(result: ResultEnvelope) -> Awaitable[None]   (optional)
        Awaited once per result for a live, tracked job, before the
        coordinator updates its own job state. Use it for per-task side
        effects such as persisting output or updating progress.
        Delivery is at-least-once: the same task may be reported twice
        (stale-task recovery racing a slow agent), so the hook must be
        idempotent. Exceptions are logged and swallowed.

Job store adapter:
    The coordinator interacts with the job store through a narrow interface
    (JobStoreAdapter defined below) to keep it testable in isolation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Optional

import redis.asyncio as aioredis

from scrapecore.models.task import TaskEnvelope
from scrapecore.models.result import ResultEnvelope
from scrapecore.queue.task_queue import TaskQueue
from scrapecore.queue.result_queue import ResultQueue
from scrapecore.queue.presence import AgentPresence

logger = logging.getLogger(__name__)


# ── Job store adapter interface ───────────────────────────────────────────────

class JobStoreAdapter:
    """
    Narrow interface between the coordinator and the job store.

    Every method is async even on this in-memory default implementation,
    where nothing actually waits on I/O. See RedisJobStoreAdapter in the consumer project for the
    backend that actually needs the async-ness.
    """
    def __init__(self, store: dict[str, dict[str, Any]]) -> None:
        self._store = store

    async def get_pending_jobs(self) -> list[dict[str, Any]]:
        """TODO: Docstring"""
        return [job for job in self._store.values() if job["status"] == "pending"]

    async def mark_running(self, job_id: str) -> None:
        """TODO: Docstring"""
        if job_id in self._store:
            self._store[job_id]["status"] = "running"

    async def mark_completed(self, job_id: str, results: list[dict]) -> None:
        """TODO: Docstring"""
        existing = await self.get(job_id)
        if existing is not None and existing.get("status") in ("completed", "failed", "cancelled"):
            logger.warning(
                f"mark_completed called for job {job_id} which is already "
                f"'{existing['status']}' - ignoring to avoid overwriting final results."
            )
            return
        self._store[job_id]["status"] = "completed"
        self._store[job_id]["results"] = results

    async def mark_failed(self, job_id: str, error: str) -> None:
        """TODO: Docstring"""
        if job_id in self._store:
            self._store[job_id]["status"] = "failed"
            self._store[job_id]["error"] = error

    async def is_cancelled(self, job_id: str) -> bool:
        """TODO: Docstring"""
        job = self._store.get(job_id)
        return job is not None and job["status"] == "cancelled"

    async def get(self, job_id: str) -> Optional[dict[str, Any]]:
        """TODO: Docstring"""
        return self._store.get(job_id)


# ── Job tracker ───────────────────────────────────────────────────────────────

class _JobTracker:
    """
    Tracks in-flight task counts and accumulated results per job.

    This tracker maintains:
        - How many tasks are still outstanding for each job
        - The accumulated result dicts as they arrive

    Not persisted — lives only in coordinator memory. If the coordinator
    restarts mid-job, this state is lost: the job stays "running" in the
    store and every later result for it is dropped as untracked. Stale-task
    recovery does NOT cover this case.
    """

    def __init__(self) -> None:
        self._pending: dict[str, int] = {}        # job_id → outstanding task count
        self._results: dict[str, list[dict]] = {} # job_id → accumulated results

    def register(self, job_id: str, task_count: int) -> None:
        """TODO: Docstring"""
        self._pending[job_id] = self._pending.get(job_id, 0) + task_count
        if job_id not in self._results:
            self._results[job_id] = []

    def is_tracked(self, job_id: str) -> bool:
        """
        True until cleanup() runs for this job_id.
        Callers MUST check this before record_result()/decrement().
        A result arriving for an untracked job_id means the job was already finalised:
        must be dropped, not re-finalised.
        """
        return job_id in self._pending

    def record_result(self, job_id: str, output: dict) -> None:
        """TODO: Docstring"""
        self._results.setdefault(job_id, []).append(output)

    def decrement(self, job_id: str) -> Optional[int]:
        """
        Decrement outstanding count.
        :return: new count, or None if job_id is not currently tracked.
        Callers must check is_tracked() first.
        """
        if job_id not in self._pending:
            return None
        count = max(0, self._pending[job_id] - 1)
        self._pending[job_id] = count
        return count

    def get_results(self, job_id: str) -> list[dict]:
        """TODO: Docstring"""
        return self._results.get(job_id, [])

    def cleanup(self, job_id: str) -> None:
        """TODO: Docstring"""
        self._pending.pop(job_id, None)
        self._results.pop(job_id, None)


# ── Coordinator ───────────────────────────────────────────────────────────────

class Coordinator:
    """
    Server-side orchestration process.

    Args:
        redis:              Connected async Redis client.
        job_store_adapter:  JobStoreAdapter wrapping the job store.
        task_factory:       Callable(job_dict) -> list[TaskEnvelope].
                            Builds first-stage tasks from a job record.
        stage_handler:      Callable(ResultEnvelope) -> list[TaskEnvelope] | None.
                            Builds follow-up tasks from a stage result.
                            Return None for final-stage results.
        namespace:          Must match the namespace used by agents.
        dispatch_interval:  Seconds between job store polls for new pending jobs.
        recovery_interval:  Seconds between stale task recovery sweeps.
        stale_task_timeout: Seconds a task can sit in processing before recovery.
        heartbeat_interval: Seconds between coordinator heartbeat writes.
        on_result:          Optional async callable(ResultEnvelope) -> None.
                            Awaited for every result of a live, tracked job
                            (all statuses), before job state is updated.
                            Must be idempotent. Exceptions are logged and
                            do not affect job bookkeeping.
    """

    def __init__(
        self,
        redis: aioredis.Redis,
        job_store_adapter: JobStoreAdapter,
        task_factory: Callable[[dict], list[TaskEnvelope]],
        stage_handler: Callable[[ResultEnvelope], Optional[list[TaskEnvelope]]],
        namespace: str = "default",
        dispatch_interval: float = 1.0,
        recovery_interval: float = 30.0,
        stale_task_timeout: float = 60.0,
        heartbeat_interval: float = 10.0,
        on_result: Optional[Callable[[ResultEnvelope], Awaitable[None]]] = None,
    ) -> None:
        self._redis = redis
        self._store = job_store_adapter
        self._task_factory = task_factory
        self._stage_handler = stage_handler
        self._namespace = namespace
        self._dispatch_interval = dispatch_interval
        self._recovery_interval = recovery_interval
        self._stale_task_timeout = stale_task_timeout
        self._heartbeat_interval = heartbeat_interval
        self._on_result = on_result

        self._task_queue = TaskQueue(redis, namespace)
        self._result_queue = ResultQueue(redis, namespace)
        self._presence = AgentPresence(redis, namespace)
        self._tracker = _JobTracker()

        self._stop_event = asyncio.Event()

        # Track already dispatched job_ids so the dispatch loop
        # doesn't re-enqueue a running job on the next poll cycle.
        self._dispatched: set[str] = set()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start all coordinator loops. Blocks until stop() is called.
        """
        logger.info("Coordinator starting")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._dispatch_loop())
            tg.create_task(self._result_loop())
            tg.create_task(self._recovery_loop())
            tg.create_task(self._heartbeat_loop())
        logger.info("Coordinator stopped")

    async def stop(self) -> None:
        """#TODO: Docstring"""
        logger.info("Coordinator stop requested")
        self._stop_event.set()

    async def cancel_job(self, job_id: str) -> int:
        """
        Cancel a job's outstanding work.

        Job is published to cancelled_jobs set BEFORE pending tasks are purged.
        Agents consult the set after a failed attempt & acknowledge instead of re-enqueueing.
        
        Tasks already claimed by an agent are not interrupted -- their results are dropped by
        _result_loop once the job store reports the job as cancelled.
        Callers MUST mark the job cancelled in the store before calling this.

        :return: Number of pending tasks removed from the queue.
        """
        cancelled_key = f"scrapecore:{self._namespace}:cancelled_jobs"
        await self._redis.sadd(cancelled_key, job_id)
        await self._redis.expire(cancelled_key, 3600)
        self._tracker.cleanup(job_id)
        removed = await self._task_queue.cancel_job_tasks(job_id)
        return removed

    async def list_agents(self) -> list[dict[str, Any]]:
        """
        Agents with a live heartbeat, sorted by agent_id.

        Presence is key existence (TTL-driven), not last_seen field, which
        carries agent's own clock.
        state is "idle" or "busy"
        """
        return await self._presence.list_online()

    # ── Loop 1: dispatch ──────────────────────────────────────────────────────

    async def _dispatch_loop(self) -> None:
        """
        Poll the job store for pending jobs and enqueue their tasks.

        Runs every dispatch_interval seconds. Skips jobs already dispatched
        this session. Marks jobs as running immediately after dispatch so
        the API reflects state correctly.
        """
        while not self._stop_event.is_set():
            try:
                for job in await self._store.get_pending_jobs():
                    job_id = job["job_id"]
                    if job_id in self._dispatched:
                        continue
                    if await self._store.is_cancelled(job_id):
                        logger.info(f"Skipping dispatch for cancelled job {job_id}")
                        continue

                    try:
                        tasks = self._task_factory(job)
                    except Exception as e:
                        logger.error(f"task_factory failed for job {job_id}: {e}")
                        await self._store.mark_failed(job_id, f"task_factory error: {e}")
                        self._dispatched.add(job_id)
                        continue

                    if not tasks:
                        logger.warning(f"task_factory returned no tasks for job {job_id}")
                        await self._store.mark_failed(job_id, "No tasks produced by task_factory")
                        self._dispatched.add(job_id)
                        continue

                    self._tracker.register(job_id, len(tasks))
                    for envelope in tasks:
                        await self._task_queue.push(
                            envelope.task_id,
                            envelope.to_dict(),
                            priority=envelope.priority,
                        )

                    await self._store.mark_running(job_id)
                    self._dispatched.add(job_id)
                    logger.info(
                        f"Job {job_id} dispatched: {len(tasks)} task(s) enqueued"
                    )

            except Exception as e:
                logger.error(f"Dispatch loop error: {e}", exc_info=True)

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._dispatch_interval,
                )
            except asyncio.TimeoutError:
                pass

    # ── Loop 2: results ───────────────────────────────────────────────────────

    async def _result_loop(self) -> None:
        """
        Drain the result queue and update job state.

        For each ResultEnvelope:
            - job cancelled or no longer tracked → drop (no hook, no state change)
            - otherwise → await on_result, then dispatch on status:
            - "completed" with stage_output → call stage_handler, enqueue more tasks
            - "completed" without stage_output → record output, decrement counter
            - "failed" → task will be retried (agent already re-enqueued it)
            - "exhausted" → record failure, decrement counter

        When a job's outstanding task count reaches zero, finalise it.
        """
        while not self._stop_event.is_set():
            try:
                result = await self._result_queue.pop(timeout=2.0)
                if result is None:
                    continue

                job_id = result.job_id

                if await self._store.is_cancelled(job_id):
                    logger.info(f"Dropping result for cancelled job {job_id}")
                    continue

                if not self._tracker.is_tracked(job_id):
                    logger.warning(
                        f"Dropping {result.status} result for task "
                        f"{result.task_id}: job {job_id} is not tracked "
                        f"(already finalised). This is almost always a "
                        f"duplicate task execution from stale-task recovery "
                        f"racing a slow (not dead) agent. No job state was "
                        f"modified."
                    )
                    continue

                await self._call_on_result(result)

                if result.status == "completed":
                    await self._handle_completed(result)

                elif result.status == "failed":
                    # Task will be retried — the agent already re-enqueued it.
                    logger.info(
                        f"Task {result.task_id} failed "
                        f"(attempt {result.retry_count}/{result.max_retries}) "
                        f"— will retry"
                    )


                elif result.status == "exhausted":
                    logger.error(
                        f"Task {result.task_id} exhausted retries: {result.error}"
                    )
                    # decrement() returns None if the job was cancelled while
                    # on_result was being awaited.
                    remaining = self._tracker.decrement(job_id)
                    if remaining == 0:
                        await self._finalise_job(job_id)

            except Exception as e:
                logger.error(f"Result loop error: {e}", exc_info=True)

    async def _call_on_result(self, result: ResultEnvelope) -> None:
        """
        Await the consumer's on_result hook, if one was supplied.

        Exceptions are logged and swallowed: a failing hook must not kill the
        result loop or leave the job stuck in "running". The result has
        already been popped from Redis, so a failed hook means that result's
        consumer-side effects are lost (not retried).
        CancelledError is not caught, so shutdown still propagates.
        """
        if self._on_result is None:
            return
        try:
            await self._on_result(result)
        except Exception as e:
            logger.error(
                f"on_result hook failed for task {result.task_id} "
                f"(job {result.job_id}, status={result.status}): {e}. "
                f"Job bookkeeping continues; this result's consumer-side "
                f"effects were not applied.",
                exc_info=True,
            )

    async def _handle_completed(self, result: ResultEnvelope) -> None:
        """Process a successfully completed task result."""
        job_id = result.job_id

        # Re-check: the job may have been cancelled (tracker cleaned up)
        # while on_result was being awaited.
        if not self._tracker.is_tracked(job_id):
            logger.info(
                f"Dropping result for task {result.task_id}: job {job_id} "
                f"stopped being tracked while on_result was running."
            )
            return

        # Stage result — spawn follow-up tasks
        if result.stage_output is not None:
            try:
                follow_up = self._stage_handler(result)
            except Exception as e:
                logger.error(
                    f"stage_handler failed for task {result.task_id}: {e}"
                )
                follow_up = None

            if follow_up:
                # Replace this task's slot with N new tasks
                self._tracker.register(job_id, len(follow_up))
                for envelope in follow_up:
                    await self._task_queue.push(
                        envelope.task_id,
                        envelope.to_dict(),
                        priority=envelope.priority,
                    )
                logger.info(
                    f"Stage transition for job {job_id}: "
                    f"{len(follow_up)} follow-up task(s) enqueued"
                )

        # Record output if present (stage tasks may have no final output)
        if result.output:
            self._tracker.record_result(job_id, result.output)

        remaining = self._tracker.decrement(job_id)
        if remaining is None:
            logger.warning(
                f"Job {job_id} finalised mid-handling for task "
                f"{result.task_id} — dropping without re-finalising."
            )
            return

        logger.info(
            f"Task {result.task_id} completed. "
            f"Job {job_id}: {remaining} task(s) remaining"
        )

        if remaining == 0:
            await self._finalise_job(job_id)

    async def _finalise_job(self, job_id: str) -> None:
        """Mark a job completed or failed based on accumulated results."""
        results = self._tracker.get_results(job_id)
        self._tracker.cleanup(job_id)

        if results:
            await self._store.mark_completed(job_id, results)
            logger.info(f"Job {job_id} completed with {len(results)} result(s)")
        else:
            await self._store.mark_failed(job_id, "All tasks failed or produced no output")
            logger.error(f"Job {job_id} failed — no results collected")

    # ── Loop 3: recovery ──────────────────────────────────────────────────────

    async def _recovery_loop(self) -> None:
        """
        Periodically re-enqueue tasks whose agents died mid-execution.

        Calls task_queue.recover_stale() on a timer. Any task sitting in
        the processing list longer than stale_task_timeout is moved back
        to pending and will be claimed by the next available agent.
        """
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._recovery_interval,
                )
            except asyncio.TimeoutError:
                pass

            if self._stop_event.is_set():
                break

            try:
                recovered = await self._task_queue.recover_stale(
                    stale_after_seconds=self._stale_task_timeout
                )
                if recovered:
                    logger.info(f"Recovery: re-enqueued {recovered} stale task(s)")
            except Exception as e:
                logger.error(f"Recovery loop error: {e}", exc_info=True)

    # ── Loop 4: heartbeat ─────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """
        Writes coordinator liveness key to Redis.

        Key: scrapecore:{namespace}:coordinator:heartbeat
        Agents and monitoring tools can check this key to confirm the
        coordinator is alive. TTL is 3x the interval — auto-expires on crash.
        """
        key = f"scrapecore:{self._namespace}:coordinator:heartbeat"
        expiry = int(self._heartbeat_interval * 3)

        while not self._stop_event.is_set():
            await self._redis.set(key, str(time.time()), ex=expiry)
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._heartbeat_interval,
                )
            except asyncio.TimeoutError:
                pass

        await self._redis.delete(key)