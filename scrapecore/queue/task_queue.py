"""
scrapecore/queue/task_queue.py

Reliable task queue backed by two Redis lists.

Concept — the two-list pattern:

    PENDING LIST          PROCESSING LIST
    ┌──────────┐          ┌──────────────┐
    │  task_3  │          │   task_1     │  ← claimed by agent-01
    │  task_4  │          │   task_2     │  ← claimed by agent-02
    │  task_5  │          └──────────────┘
    └──────────┘

When an agent claims a task, it moves atomically from PENDING to PROCESSING.
"Atomically" means: no other agent can see a state between "in pending" and
"in processing" — it is in exactly one list at all times, even if the network
drops mid-operation. This is guaranteed by Redis's BLMOVE command.

If an agent crashes, its task stays in PROCESSING forever — until the
coordinator's recovery loop detects the stale entry and moves it back to
PENDING. This is the heartbeat/recovery mechanism we build in a later step.

Key naming convention:
    scrapecore:{namespace}:pending      — waiting to be claimed
    scrapecore:{namespace}:processing  — currently held by an agent
    scrapecore:{namespace}:task:{id}   — full task payload stored as a hash

The namespace parameter lets multiple independent scrape jobs share one Redis
instance without their queues colliding.
"""

import json
import time
import logging
from typing import Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class TaskQueue:
    """
    Reliable task queue using two Redis lists.

    Producers call push() to enqueue tasks.
    Consumers call claim() to atomically move a task to the processing list.
    On completion, consumers call acknowledge() to remove it from processing.
    On failure, consumers call reject() to move it back to pending.

    The coordinator's recovery loop uses recover_stale() to re-enqueue tasks
    whose processing entries are older than a timeout.
    """

    def __init__(self, redis: aioredis.Redis, namespace: str = "default") -> None:
        """
        :param redis:     Shared async Redis client.
        :param namespace: Logical queue name. Use one namespace per job type
                          if you want independent queues on one Redis instance.
        """
        self._redis = redis
        self._ns = namespace

        # Key names
        self._pending_key    = f"scrapecore:{namespace}:pending"
        self._processing_key = f"scrapecore:{namespace}:processing"
        self._payload_prefix = f"scrapecore:{namespace}:task:"

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _payload_key(self, task_id: str) -> str:
        return self._payload_prefix + task_id

    async def _store_payload(self, task_id: str, payload: dict) -> None:
        """Persist the full task payload as a Redis hash keyed by task_id."""
        await self._redis.hset(
            self._payload_key(task_id),
            mapping={
                "data":       json.dumps(payload),
                "queued_at":  str(time.time()),
            }
        )

    async def _load_payload(self, task_id: str) -> Optional[dict]:
        """Load a task payload from Redis. Returns None if not found."""
        raw = await self._redis.hget(self._payload_key(task_id), "data")
        if raw is None:
            return None
        return json.loads(raw)

    async def _delete_payload(self, task_id: str) -> None:
        await self._redis.delete(self._payload_key(task_id))

    # ── Producer API ──────────────────────────────────────────────────────────

    async def push(self, task_id: str, payload: dict, priority: int = 0) -> None:
        """
        Enqueue a task.

        Priority is encoded by pushing high-priority tasks to the front of the
        list (RPUSH for normal, LPUSH for elevated). This is a simple scheme —
        a full priority queue would use a Redis sorted set instead. We keep it
        simple for now and can upgrade later.

        :param task_id:  Unique identifier for this task.
        :param payload:  Full task data as a dict. Must be JSON-serializable.
        :param priority: Integer priority. Any value > 0 gets front-of-queue placement.
        """
        await self._store_payload(task_id, payload)

        # claim() pops from the RIGHT end of the list, so high-priority
        # items must be pushed to the RIGHT to be claimed first.
        if priority > 0:
            await self._redis.rpush(self._pending_key, task_id)
        else:
            await self._redis.lpush(self._pending_key, task_id)

        logger.debug(f"[{self._ns}] Pushed task {task_id} (priority={priority})")

    # ── Consumer API ──────────────────────────────────────────────────────────

    async def claim(self, timeout: float = 5.0) -> Optional[tuple[str, dict]]:
        """
        Atomically claim the next available task.

        Blocks for up to `timeout` seconds if the queue is empty. Returns None
        on timeout — callers should loop and call claim() again.

        The BLMOVE command moves the task_id from the pending list to the
        processing list in a single atomic operation. No other agent can claim
        the same task_id.

        :param timeout: Seconds to block waiting for a task.
        :return: (task_id, payload) tuple, or None if queue was empty.
        """
        # BLMOVE source destination LEFT RIGHT timeout
        # Pops from the RIGHT end of pending (oldest item), pushes to processing.
        task_id = await self._redis.blmove(
            self._pending_key,
            self._processing_key,
            timeout=timeout,
            src="RIGHT",
            dest="LEFT",
        )

        if task_id is None:
            return None

        payload = await self._load_payload(task_id)
        if payload is None:
            # Payload missing — data corruption or manual deletion.
            # Remove from processing and move on.
            await self._redis.lrem(self._processing_key, 1, task_id)
            logger.warning(f"[{self._ns}] Missing payload for task {task_id} — discarded.")
            return None

        # Record when this task was claimed (used by recovery to detect stale entries)
        await self._redis.hset(
            self._payload_key(task_id),
            mapping={"claimed_at": str(time.time())}
        )

        logger.debug(f"[{self._ns}] Claimed task {task_id}")
        return task_id, payload

    async def renew_claim(self, task_id: str) -> None:
        """
        Refresh claimed_at for a task still being actively worked.

        Called periodically by an agent while a task is in flight (e.g.
        during HTTP backoff sleep) so recover_stale() does not mistake a
        slow-but-alive execution for a dead one.
        """
        await self._redis.hset(
            self._payload_key(task_id),
            mapping={"claimed_at": str(time.time())}
        )

    async def acknowledge(self, task_id: str) -> None:
        """
        Mark a task as successfully completed.

        Removes it from the processing list and deletes the payload.
        After this call the task no longer exists in Redis.

        :param task_id: The task to remove.
        """
        await self._redis.lrem(self._processing_key, 1, task_id)
        await self._delete_payload(task_id)
        logger.debug(f"[{self._ns}] Acknowledged task {task_id}")

    async def reject(self, task_id: str, payload: dict, priority: int = 0) -> None:
        """
        Return a failed task to the pending queue.

        Called by an agent when a task fails but should be retried.
        Removes from processing, updates payload (e.g. incremented retry count),
        and re-enqueues.

        :param task_id: The task to re-enqueue.
        :param payload: Updated payload (should include incremented retry_count).
        :param priority: Queue priority for the re-enqueued task.
        """
        await self._redis.lrem(self._processing_key, 1, task_id)
        await self.push(task_id, payload, priority=priority)
        logger.debug(f"[{self._ns}] Rejected task {task_id} — re-enqueued")

    async def cancel_job_tasks(self, job_id: str) -> int:
        """#TODO: Add docstring"""
        pending_ids = await self._redis.lrange(self._pending_key, 0, -1)
        removed = 0
        for task_id in pending_ids:
            payload = await self._load_payload(task_id)
            if payload and payload.get("job_id") == job_id:
                await self._redis.lrem(self._pending_key, 1, task_id)
                await self._delete_payload(task_id)
                removed += 1
                logger.info(f"[{self._ns}] Cancelled pending task {task_id} for job {job_id}")
        return removed

    # ── Recovery API (coordinator) ────────────────────────────────────────────

    async def recover_stale(self, stale_after_seconds: float = 60.0) -> int:
        """
        Re-enqueue tasks that have been in the processing list too long.

        This is the fault tolerance mechanism. If an agent claims a task and
        then crashes, the task_id stays in the processing list. This method
        is called periodically by the coordinator — it checks the claimed_at
        timestamp for each processing entry and re-enqueues anything older
        than stale_after_seconds.

        :param stale_after_seconds: Tasks held longer than this are considered lost.
        :return: Number of tasks recovered.
        """
        processing_ids = await self._redis.lrange(self._processing_key, 0, -1)
        recovered = 0
        now = time.time()

        for task_id in processing_ids:
            claimed_at_raw = await self._redis.hget(self._payload_key(task_id), "claimed_at")

            if claimed_at_raw is None:
                # No claim timestamp — orphaned entry, recover it
                stale = True
            else:
                stale = (now - float(claimed_at_raw)) > stale_after_seconds

            if stale:
                payload = await self._load_payload(task_id)
                if payload is not None:
                    await self._redis.lrem(self._processing_key, 1, task_id)
                    await self.push(task_id, payload)
                    logger.warning(f"[{self._ns}] Recovered stale task {task_id}")
                    recovered += 1
                else:
                    # Payload gone — discard the processing entry
                    await self._redis.lrem(self._processing_key, 1, task_id)

        return recovered

    # ── Introspection ─────────────────────────────────────────────────────────

    async def pending_count(self) -> int:
        return await self._redis.llen(self._pending_key)

    async def processing_count(self) -> int:
        return await self._redis.llen(self._processing_key)

    async def flush(self) -> None:
        """Delete all queue state. Useful for testing."""
        await self._redis.delete(self._pending_key, self._processing_key)
        logger.warning(f"[{self._ns}] Queue flushed.")