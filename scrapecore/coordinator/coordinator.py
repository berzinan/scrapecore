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
        Delivery is at-least-once: the same task may be reported twice,
        so the hook must be idempotent. A failing hook is retried with
        exponential backoff (hook_max_attempts); if it never succeeds the
        result is dead-lettered and job bookkeeping continues.

Result delivery:
    Results are claimed from Redis with claim() and acked only after the
    hook and job bookkeeping succeed. Any other failure requeues it.
    Results left in-flight by a crashed coordinator are recovered at the next start().
    This assumes exactly one coordinator runs per namespace.

Coordinator state:
    Which tasks each running job is still waiting on lives in Redis
    (JobTracker), not in process memory, so a restarted coordinator picks up
    every running job where it left off. Redis must persist to disk (AOF)
    for this to survive a Redis restart too.

    Jobs are claimed atomically from the job store (pending → running), so a
    job is dispatched once. A job left "running" but untracked by a crash
    mid-dispatch is re-dispatched on the next dispatch cycle.

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
from scrapecore.queue.job_tracker import JobTracker

logger = logging.getLogger(__name__)


# ── Job store adapter interface ───────────────────────────────────────────────

class JobStoreAdapter:
    """
    Narrow interface between the coordinator and the job store.

    This default implementation wraps an in-memory dict and is meant for
    tests and prototyping. Consumers subclass it and back it with a real
    database. Every method is async for that reason.

    A job record is a dict with at least "job_id" and "status", where
    status is one of: pending, running, completed, failed, cancelled.
    """
    def __init__(self, store: dict[str, dict[str, Any]]) -> None:
        self._store = store

    async def claim_pending_jobs(self) -> list[dict[str, Any]]:
        """
        Atomically move every pending job to running and return those jobs.

        Must be atomic in a real backend: a job returned here must never be
        returned to another caller. In SQL this is a single
        UPDATE ... SET status = 'running' WHERE status = 'pending' RETURNING *.
        """
        claimed = []
        for job in self._store.values():
            if job["status"] == "pending":
                job["status"] = "running"
                claimed.append(job)
        return claimed

    async def get_running_jobs(self) -> list[dict[str, Any]]:
        """
        Return every job with status running.

        Polled each dispatch cycle to find jobs a crashed coordinator
        claimed but never finished dispatching.
        """
        return [job for job in self._store.values() if job["status"] == "running"]

    async def mark_completed(self, job_id: str, summary: dict[str, int]) -> None:
        """
        Mark a job completed. Ignored if the job already reached a final status.

        :param summary: {"succeeded": n, "exhausted": m} task outcome counts.
                        Task outputs are delivered through on_result, not here.
        """
        existing = await self.get(job_id)
        if existing is not None and existing.get("status") in ("completed", "failed", "cancelled"):
            logger.warning(
                f"mark_completed called for job {job_id} which is already "
                f"'{existing['status']}' - ignoring."
            )
            return
        self._store[job_id]["status"] = "completed"
        self._store[job_id]["summary"] = summary

    async def mark_failed(self, job_id: str, error: str) -> None:
        """Mark a job failed with a human-readable reason."""
        if job_id in self._store:
            self._store[job_id]["status"] = "failed"
            self._store[job_id]["error"] = error

    async def is_cancelled(self, job_id: str) -> bool:
        """True if the job's status is cancelled."""
        job = self._store.get(job_id)
        return job is not None and job["status"] == "cancelled"

    async def get(self, job_id: str) -> Optional[dict[str, Any]]:
        """Return the job record, or None if it does not exist."""
        return self._store.get(job_id)


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
                            Must be idempotent.
        hook_max_attempts:  Times on_result is tried for one result before it
                            is dead-lettered and bookkeeping continues.
        hook_retry_base_delay: Backoff before the 2nd hook attempt, doubled
                            each retry, capped at 30s.
        result_retry_delay: Pause after a result is requeued because handling
                            failed outside the hook (e.g. job store down).
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
        hook_max_attempts: int = 5,
        hook_retry_base_delay: float = 1.0,
        result_retry_delay: float = 1.0,
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
        self._hook_max_attempts = max(1, hook_max_attempts)
        self._hook_retry_base_delay = hook_retry_base_delay
        self._result_retry_delay = result_retry_delay

        self._task_queue = TaskQueue(redis, namespace)
        self._result_queue = ResultQueue(redis, namespace)
        self._presence = AgentPresence(redis, namespace)
        self._tracker = JobTracker(redis, namespace)

        self._stop_event = asyncio.Event()

        # Serialises "push tasks + update tracker" (dispatch, stage
        # transitions) against result handling. Without it, an agent could
        # finish a just-pushed task and the result loop could handle it
        # before the tracker knows the task exists, dropping it as untracked.
        self._state_lock = asyncio.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start all coordinator loops. Blocks until stop() is called.
        """
        logger.info("Coordinator starting")
        # Results a previous (crashed) coordinator claimed but never acked.
        await self._result_queue.recover_inflight()
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
        await self._tracker.cleanup(job_id)
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
        Each cycle: re-dispatch orphaned running jobs, then claim and
        dispatch pending ones.

        Runs every dispatch_interval seconds. A failure dispatching one job
        does not block the others; a job left running but untracked by that
        failure is picked up by the next cycle's reconcile step.
        """
        while not self._stop_event.is_set():
            try:
                await self._reconcile_running_jobs()
                claimed = await self._store.claim_pending_jobs()
            except Exception as e:
                logger.error(f"Dispatch loop error: {e}", exc_info=True)
                claimed = []

            for job in claimed:
                try:
                    await self._dispatch_job(job)
                except Exception as e:
                    logger.error(
                        f"Dispatching job {job.get('job_id')} failed: {e} "
                        f"— will retry next cycle",
                        exc_info=True,
                    )

            await self._sleep(self._dispatch_interval)

    async def _reconcile_running_jobs(self) -> None:
        """
        Re-dispatch jobs the store says are running but the tracker does not know.

        That state only arises when a coordinator claimed a job and died (or
        hit an error) before registering its tasks. Finalisation writes the
        store before clearing the tracker, so finished jobs never match.
        Re-dispatching may duplicate tasks already pushed; their results are
        dropped as not outstanding.
        """
        for job in await self._store.get_running_jobs():
            job_id = job["job_id"]
            if await self._tracker.is_tracked(job_id):
                continue
            logger.warning(
                f"Job {job_id} is running in the store but not tracked "
                f"— re-dispatching"
            )
            await self._dispatch_job(job)

    async def _dispatch_job(self, job: dict[str, Any]) -> None:
        """
        Build a claimed job's first-stage tasks, enqueue them, and track them.

        Tasks are pushed before being registered, under _state_lock. If the
        process dies between the two, the job is running but untracked, and
        _reconcile_running_jobs() re-dispatches it. The opposite order would
        leave registered task IDs that were never enqueued, and the job
        would wait on them forever.
        """
        job_id = job["job_id"]

        try:
            tasks = self._task_factory(job)
        except Exception as e:
            logger.error(f"task_factory failed for job {job_id}: {e}")
            await self._store.mark_failed(job_id, f"task_factory error: {e}")
            return

        if not tasks:
            logger.warning(f"task_factory returned no tasks for job {job_id}")
            await self._store.mark_failed(job_id, "No tasks produced by task_factory")
            return

        task_ids = [t.task_id for t in tasks]
        if len(set(task_ids)) != len(task_ids):
            # Completion is tracked by task ID, so duplicates would let a
            # job finish while a same-ID task is still running.
            await self._store.mark_failed(job_id, "task_factory produced duplicate task_ids")
            return

        async with self._state_lock:
            for envelope in tasks:
                await self._task_queue.push(
                    envelope.task_id,
                    envelope.to_dict(),
                    priority=envelope.priority,
                )
            await self._tracker.register(job_id, task_ids)

        # Cancelled between claim and register: cancel_job() may already
        # have run and found nothing to clean up, so clean up now.
        if await self._store.is_cancelled(job_id):
            await self.cancel_job(job_id)
            return

        logger.info(f"Job {job_id} dispatched: {len(tasks)} task(s) enqueued")

    # ── Loop 2: results ───────────────────────────────────────────────────────

    async def _result_loop(self) -> None:
        """
        Claim results from the queue, handle each one, then ack it.

        claim() moves a result into the processing list; it is only removed
        by ack() after _process_result() returns.
        If _process_result() raises, the result is requeued and handled
        again after result_retry_delay. If the process dies mid-handling, the
        result stays in processing and recover_inflight() returns it to the
        queue at the next start().
        """
        while not self._stop_event.is_set():
            try:
                claimed = await self._result_queue.claim(timeout=2.0)
            except Exception as e:
                logger.error(f"Result claim failed: {e}", exc_info=True)
                await self._sleep(self._result_retry_delay)
                continue
            if claimed is None:
                continue

            raw, result = claimed
            try:
                async with self._state_lock:
                    await self._process_result(raw, result)
            except Exception as e:
                logger.error(
                    f"Handling result for task {result.task_id} "
                    f"(job {result.job_id}) failed: {e} — requeueing",
                    exc_info=True,
                )
                try:
                    await self._result_queue.requeue(raw)
                except Exception as requeue_err:
                    # Still in processing; recovered at next start().
                    logger.error(f"Requeue failed: {requeue_err}", exc_info=True)
                await self._sleep(self._result_retry_delay)
                continue

            await self._result_queue.ack(raw)

    async def _process_result(self, raw: str, result: ResultEnvelope) -> None:
        """
        Handle one claimed result.

            - job cancelled or not tracked      → drop
            - task no longer outstanding        → duplicate: drop, but finalise
                                                  if nothing is outstanding
            - otherwise → on_result (with retries), then by status:
            - "failed"                          → no state change (agent retries)
            - "completed" with stage_output     → replace task with follow-ups
            - "completed" / "exhausted"         → complete task; finalise at zero

        Raising from here causes the result to be requeued, so every step
        must be safe to repeat.
        """
        job_id, task_id = result.job_id, result.task_id

        if await self._store.is_cancelled(job_id):
            logger.info(f"Dropping result for cancelled job {job_id}")
            return

        if not await self._tracker.is_tracked(job_id):
            logger.warning(
                f"Dropping {result.status} result for task {task_id}: "
                f"job {job_id} is not tracked (already finalised)."
            )
            return

        if not await self._tracker.is_outstanding(job_id, task_id):
            # Either a duplicate execution, or this result is being handled
            # again because a later step failed last time, most often
            # _finalise_job(). In the second case the job is waiting on
            # nothing and only finalisation is left to do.
            if await self._tracker.remaining(job_id) == 0:
                await self._finalise_job(job_id)
            else:
                logger.info(
                    f"Dropping duplicate {result.status} result for task "
                    f"{task_id}: already finished for job {job_id}."
                )
            return

        await self._call_on_result(raw, result)

        if result.status == "failed":
            # Task will be retried — the agent already re-enqueued it.
            logger.info(
                f"Task {task_id} failed "
                f"(attempt {result.retry_count}/{result.max_retries}) — will retry"
            )
            return

        if result.status == "completed" and result.stage_output is not None:
            if await self._handle_stage_transition(result):
                return

        if result.status == "exhausted":
            logger.error(f"Task {task_id} exhausted retries: {result.error}")
            outcome = "exhausted"
        else:
            outcome = "succeeded"

        remaining = await self._tracker.complete_task(job_id, task_id, outcome)
        if remaining is None:
            logger.info(f"Job {job_id} was cancelled while task {task_id} was being handled.")
            return

        logger.info(f"Task {task_id} {outcome}. Job {job_id}: {remaining} task(s) remaining")
        if remaining == 0:
            await self._finalise_job(job_id)

    async def _handle_stage_transition(self, result: ResultEnvelope) -> bool:
        """
        Enqueue a stage result's follow-up tasks and swap them in for it.

        Follow-ups are pushed before the tracker swap, for the same reason
        as in _dispatch_job: a crash in between leaves the parent task
        outstanding, so the requeued result runs this again (duplicating
        some tasks) rather than leaving the job waiting on unqueued IDs.

        :return: True if follow-ups replaced the task. False if there were
                 none, in which case the caller completes it as final.
        """
        job_id = result.job_id
        try:
            follow_up = self._stage_handler(result)
        except Exception as e:
            logger.error(f"stage_handler failed for task {result.task_id}: {e}")
            return False

        if not follow_up:
            return False

        new_ids = [envelope.task_id for envelope in follow_up]
        if len(set(new_ids)) != len(new_ids):
            logger.error(
                f"stage_handler returned duplicate task_ids for task "
                f"{result.task_id}; completing it without follow-ups."
            )
            return False

        for envelope in follow_up:
            await self._task_queue.push(
                envelope.task_id,
                envelope.to_dict(),
                priority=envelope.priority,
            )
        await self._tracker.replace_task(job_id, result.task_id, new_ids)
        logger.info(
            f"Stage transition for job {job_id}: "
            f"{len(follow_up)} follow-up task(s) enqueued"
        )
        return True

    async def _call_on_result(self, raw: str, result: ResultEnvelope) -> None:
        """
        Await the consumer's on_result hook, retrying with exponential backoff.

        The result stays in the processing list throughout.
        After hook_max_attempts failures the result is copied
        to the dead-letter list and this returns normally.

        Retry sleeps block the result loop.
        A stop() during backoff raises, so the result is requeued rather
        than dead-lettered. CancelledError is never caught.
        """
        if self._on_result is None:
            return

        for attempt in range(1, self._hook_max_attempts + 1):
            try:
                await self._on_result(result)
                return
            except Exception as e:
                last_error = e
                if attempt == self._hook_max_attempts:
                    break
                delay = min(self._hook_retry_base_delay * 2 ** (attempt - 1), 30.0)
                logger.warning(
                    f"on_result hook failed for task {result.task_id} "
                    f"(attempt {attempt}/{self._hook_max_attempts}): {e} "
                    f"— retrying in {delay:.1f}s"
                )
                if await self._sleep(delay):
                    raise RuntimeError(
                        "Coordinator stopping during on_result retry"
                    ) from e

        logger.error(
            f"on_result hook failed {self._hook_max_attempts} times for task "
            f"{result.task_id} (job {result.job_id}, status={result.status}): "
            f"{last_error}. Result dead-lettered; job bookkeeping continues.",
            exc_info=last_error,
        )
        await self._result_queue.dead_letter(
            raw, f"on_result: {type(last_error).__name__}: {last_error}"
        )

    async def _finalise_job(self, job_id: str) -> None:
        """
        Mark a job completed if any task succeeded, otherwise failed.

        Writes the store BEFORE clearing tracker state. If the store raises,
        the job is still tracked, and the requeued result finalises it on
        the retry instead of being dropped as untracked.
        """
        stats = await self._tracker.stats(job_id)
        if stats["succeeded"]:
            await self._store.mark_completed(job_id, stats)
            logger.info(f"Job {job_id} completed: {stats}")
        else:
            await self._store.mark_failed(
                job_id, f"All {stats['exhausted']} task(s) exhausted their retries"
            )
            logger.error(f"Job {job_id} failed: {stats}")

        await self._tracker.cleanup(job_id)

    async def _sleep(self, seconds: float) -> bool:
        """
        Sleep for `seconds`, waking early if stop() is called.
        :return: True if the coordinator is stopping.
        """
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
        return self._stop_event.is_set()

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