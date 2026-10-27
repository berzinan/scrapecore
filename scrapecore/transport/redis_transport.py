"""
scrapecore/transport/redis_transport.py

RedisTransport — AgentTransport backed directly by the scrapecore Redis queues.

Used when an agent can reach Redis itself: local development, tests, and the
server side of the future HTTPS gateway (which will call these same methods
on behalf of remote agents). Behaviour matches what Agent did inline before
transports existed.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import redis.asyncio as aioredis

from scrapecore.models.result import ResultEnvelope
from scrapecore.models.task import TaskEnvelope
from scrapecore.queue.presence import AgentPresence
from scrapecore.queue.result_queue import ResultQueue
from scrapecore.queue.task_queue import TaskQueue

logger = logging.getLogger(__name__)


class RedisTransport:
    """
    Args:
        redis:     Connected async Redis client.
        namespace: Must match the coordinator's namespace.
    """

    def __init__(self, redis: aioredis.Redis, namespace: str = "default") -> None:
        self._redis = redis
        self._namespace = namespace
        self._task_queue = TaskQueue(redis, namespace)
        self._result_queue = ResultQueue(redis, namespace)
        self._presence = AgentPresence(redis, namespace)
        self._cancelled_key = f"scrapecore:{namespace}:cancelled_jobs"

    async def claim(self, timeout: float) -> Optional[TaskEnvelope]:
        """Atomically move the next task to processing and return it."""
        claimed = await self._task_queue.claim(timeout=timeout)
        if claimed is None:
            return None
        _, payload = claimed
        return TaskEnvelope.from_dict(payload)

    async def renew(self, task_id: str) -> None:
        """Refresh the task's claimed_at so stale recovery leaves it alone."""
        await self._task_queue.renew_claim(task_id)

    async def submit(self, task: TaskEnvelope, result: ResultEnvelope) -> None:
        """
        Push the result, then acknowledge or re-enqueue the task.

        Order matters: the result is pushed before the claim is released. If
        the process dies in between, the task is still in processing, stale
        recovery re-runs it, and the coordinator drops the duplicate result.
        The reverse order could lose the result entirely.

        Retry policy:
            completed                          → acknowledge
            failed, job cancelled              → acknowledge (stop retrying)
            failed, retries exhausted          → acknowledge
            failed, retries remain             → re-enqueue with retry_count + 1
        """
        await self._result_queue.push(result)

        if result.status == "completed":
            await self._task_queue.acknowledge(task.task_id)
            return

        cancelled = await self._redis.sismember(self._cancelled_key, task.job_id)
        if cancelled or not task.should_retry():
            await self._task_queue.acknowledge(task.task_id)
        else:
            retried = task.increment_retry()
            await self._task_queue.reject(task.task_id, retried.to_dict(), retried.priority)

    async def heartbeat(self, agent_id: str, info: dict[str, Any], ttl: int) -> None:
        """Write the agent's presence key with a TTL."""
        await self._presence.beat(agent_id, info, ttl)

    async def leave(self, agent_id: str) -> None:
        """Delete the agent's presence key."""
        await self._presence.remove(agent_id)
