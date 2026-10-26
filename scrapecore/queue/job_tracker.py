"""
scrapecore/queue/job_tracker.py

JobTracker — Redis-backed record of which tasks each running job is still
waiting on. Replaces the coordinator's in-memory counter so job progress
survives a coordinator restart.

Tracking task IDs instead of a count makes completion idempotent: removing
a task ID that is already gone is a no-op, so a duplicate result (stale-task
recovery racing a slow agent) cannot finish a job early.

Keys:
    scrapecore:{namespace}:jobs:active               SET  job_ids being tracked
    scrapecore:{namespace}:job:{job_id}:outstanding  SET  task_ids not yet finished
    scrapecore:{namespace}:job:{job_id}:stats        HASH succeeded / exhausted counts

`jobs:active` is the source of truth for "is this job tracked". The
outstanding set cannot serve that purpose because Redis deletes a set when
its last member is removed, so "tracked with nothing outstanding" and "not
tracked" would look the same.

Requires Redis persistence (AOF) to survive a Redis restart.
"""
from __future__ import annotations

from typing import Literal, Optional

import redis.asyncio as aioredis

TaskOutcome = Literal["succeeded", "exhausted"]


class JobTracker:
    """
    Per-job set of outstanding task IDs, plus outcome counters.

    Every multi-key update runs in a MULTI/EXEC transaction, so a crash
    never leaves a job half-updated.
    """

    def __init__(self, redis: aioredis.Redis, namespace: str = "default") -> None:
        self._redis = redis
        self._prefix = f"scrapecore:{namespace}"
        self._active_key = f"{self._prefix}:jobs:active"

    def _outstanding_key(self, job_id: str) -> str:
        return f"{self._prefix}:job:{job_id}:outstanding"

    def _stats_key(self, job_id: str) -> str:
        return f"{self._prefix}:job:{job_id}:stats"

    async def register(self, job_id: str, task_ids: list[str]) -> None:
        """
        Start tracking a job and add its first-stage task IDs.
        Safe to call again for the same job; IDs are set members.
        """
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.sadd(self._active_key, job_id)
            if task_ids:
                pipe.sadd(self._outstanding_key(job_id), *task_ids)
            await pipe.execute()

    async def is_tracked(self, job_id: str) -> bool:
        """True from register() until cleanup()."""
        return bool(await self._redis.sismember(self._active_key, job_id))

    async def is_outstanding(self, job_id: str, task_id: str) -> bool:
        """True if the job is still waiting on this task."""
        return bool(await self._redis.sismember(self._outstanding_key(job_id), task_id))

    async def remaining(self, job_id: str) -> int:
        """Number of outstanding tasks for a job."""
        return await self._redis.scard(self._outstanding_key(job_id))

    async def complete_task(
        self, job_id: str, task_id: str, outcome: TaskOutcome
    ) -> Optional[int]:
        """
        Mark a task finished and count its outcome.

        Callers must check is_outstanding() first. The coordinator processes
        results one at a time, so the check and this update cannot interleave
        with another result for the same task.

        :return: Outstanding tasks left for the job, or None if the job
                 stopped being tracked (e.g. cancelled from another process).
        """
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.sismember(self._active_key, job_id)
            pipe.srem(self._outstanding_key(job_id), task_id)
            pipe.hincrby(self._stats_key(job_id), outcome, 1)
            pipe.scard(self._outstanding_key(job_id))
            tracked, _, _, remaining = await pipe.execute()
        if not tracked:
            # Undo the stray counter write; the job's keys were cleaned up.
            await self._redis.delete(self._stats_key(job_id))
            return None
        return remaining

    async def replace_task(self, job_id: str, task_id: str, new_task_ids: list[str]) -> bool:
        """
        Swap a finished stage task for its follow-up tasks, atomically.

        New IDs are added before the old one is removed, inside one
        transaction, so the outstanding set is never momentarily empty and
        the job cannot be finalised mid-transition.

        :return: False if the job stopped being tracked (e.g. cancelled from
                 another process); the stray set is deleted.
        """
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.sismember(self._active_key, job_id)
            pipe.sadd(self._outstanding_key(job_id), *new_task_ids)
            pipe.srem(self._outstanding_key(job_id), task_id)
            tracked, _, _ = await pipe.execute()
        if not tracked:
            await self._redis.delete(self._outstanding_key(job_id))
            return False
        return True

    async def stats(self, job_id: str) -> dict[str, int]:
        """Outcome counts so far: {"succeeded": n, "exhausted": m}."""
        raw = await self._redis.hgetall(self._stats_key(job_id))
        return {
            "succeeded": int(raw.get("succeeded", 0)),
            "exhausted": int(raw.get("exhausted", 0)),
        }

    async def cleanup(self, job_id: str) -> None:
        """Stop tracking a job and delete all of its keys."""
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.srem(self._active_key, job_id)
            pipe.delete(self._outstanding_key(job_id), self._stats_key(job_id))
            await pipe.execute()
