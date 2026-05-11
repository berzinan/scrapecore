"""
scrapecore/queue/result_queue.py

Result queue — agents push ResultEnvelopes here, coordinator drains it.

Simpler than TaskQueue: single list, no two-list reliability pattern.
Results that are lost (coordinator crash mid-drain) cause the originating
task to be recovered by the stale task mechanism and retried. This is
acceptable because tasks are idempotent by design.

Key:
    scrapecore:{namespace}:results
"""
import logging
from typing import Optional

import redis.asyncio as aioredis

from scrapecore.models.result import ResultEnvelope

logger = logging.getLogger(__name__)


class ResultQueue:
    """
    Single-list queue for ResultEnvelopes.

    Agents call push() after completing or failing a task.
    The coordinator calls drain() in a loop to process results as they arrive.
    """

    def __init__(self, redis: aioredis.Redis, namespace: str = "default") -> None:
        self._redis = redis
        self._key = f"scrapecore:{namespace}:results"

    async def push(self, result: ResultEnvelope) -> None:
        """
        Push a result envelope onto the queue.
        Called by the agent after task execution finishes.
        """
        await self._redis.lpush(self._key, result.to_json())
        logger.debug(
            f"[result_queue] Pushed result for task {result.task_id} "
            f"(status={result.status})"
        )

    async def pop(self, timeout: float = 5.0) -> Optional[ResultEnvelope]:
        """
        Blocking pop — waits up to `timeout` seconds for a result.
        Returns None on timeout.
        Called by the coordinator in a polling loop.
        """
        raw = await self._redis.brpop(self._key, timeout=timeout)
        if raw is None:
            return None

        _, json_str = raw  # brpop returns (key, value)
        try:
            return ResultEnvelope.from_json(json_str)
        except Exception as e:
            logger.error(f"[result_queue] Failed to deserialize result: {e} — raw: {json_str}")
            return None

    async def length(self) -> int:
        return await self._redis.llen(self._key)

    async def flush(self) -> None:
        await self._redis.delete(self._key)
        logger.warning("[result_queue] Result queue flushed.")