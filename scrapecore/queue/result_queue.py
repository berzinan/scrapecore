"""
scrapecore/queue/result_queue.py

Result queue — agents push ResultEnvelopes here, coordinator drains it.

Uses the same two-list pattern as TaskQueue. The agent has
already acknowledged its task by the time it pushes a result, so a result
that is lost here is lost permanently.

    claim()  atomically moves a result from pending to processing (BLMOVE).
    ack()    removes it from processing once the coordinator has fully
             handled it (hook + job bookkeeping).
    requeue() moves it back to pending if handling failed.

If the coordinator dies between claim() and ack(), the result stays in
processing. recover_inflight() moves it back to pending on the next start.
This relies on the coordinator being a singleton: at startup, anything in
processing belongs to a dead predecessor.

Results that can never be handled are copied to a dead-letter list with the
reason, so they can be inspected or replayed by hand.

Keys:
    scrapecore:{namespace}:results              — pending
    scrapecore:{namespace}:results:processing   — claimed, not yet acked
    scrapecore:{namespace}:results:dead         — dead-letter records (JSON)
"""
import json
import logging
import time
from typing import Optional

import redis.asyncio as aioredis

from scrapecore.models.result import ResultEnvelope

logger = logging.getLogger(__name__)


class ResultQueue:
    """
    Reliable queue for ResultEnvelopes.

    Agents call push() after completing or failing a task.
    The coordinator calls claim() → handle → ack() (or requeue() on failure).
    pop() is a destructive, unacknowledged read kept for tests and tooling;
    coordinator must not use it.
    """

    def __init__(self, redis: aioredis.Redis, namespace: str = "default") -> None:
        self._redis = redis
        self._key = f"scrapecore:{namespace}:results"
        self._processing_key = f"{self._key}:processing"
        self._dead_key = f"{self._key}:dead"

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

    async def claim(self, timeout: float = 5.0) -> Optional[tuple[str, ResultEnvelope]]:
        """
        Atomically move the oldest result from pending to processing.

        Blocks for up to `timeout` seconds if the queue is empty.
        A corrupt payload is moved straight to the dead-letter list and
        None is returned

        :param timeout: Seconds to block waiting for a result.
        :return: (raw, envelope). Pass `raw` back to ack()/requeue().
                 None on timeout or corrupt payload.
        """
        raw = await self._redis.blmove(
            self._key,
            self._processing_key,
            timeout=timeout,
            src="RIGHT",
            dest="LEFT",
        )
        if raw is None:
            return None

        try:
            return raw, ResultEnvelope.from_json(raw)
        except Exception as e:
            logger.error(f"[result_queue] Corrupt result moved to dead-letter: {e} — raw: {raw}")
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.lrem(self._processing_key, 1, raw)
                pipe.lpush(self._dead_key, self._dead_record(raw, f"deserialize: {e}"))
                await pipe.execute()
            return None

    async def ack(self, raw: str) -> None:
        """
        Remove a fully handled result from processing.
        Call only after the hook and all job bookkeeping have succeeded.
        """
        await self._redis.lrem(self._processing_key, 1, raw)

    async def requeue(self, raw: str) -> None:
        """
        Move a claimed result back to pending so it is handled again.

        Atomic (MULTI/EXEC): the result is never in neither list.
        """
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.lrem(self._processing_key, 1, raw)
            pipe.lpush(self._key, raw)
            await pipe.execute()

    async def dead_letter(self, raw: str, reason: str) -> None:
        """
        Record a copy of a result that could not be handled.

        Does NOT remove it from processing: the caller still ack()s it once
        job bookkeeping is done.
        """
        await self._redis.lpush(self._dead_key, self._dead_record(raw, reason))

    async def recover_inflight(self) -> int:
        """
        Move every result in processing back to pending.

        Call once at coordinator startup, before the result loop runs.
        Relative order of recovered results is not preserved.

        :return: Number of results recovered.
        """
        recovered = 0
        while await self._redis.lmove(self._processing_key, self._key, "RIGHT", "RIGHT"):
            recovered += 1
        if recovered:
            logger.warning(f"[result_queue] Recovered {recovered} in-flight result(s)")
        return recovered

    @staticmethod
    def _dead_record(raw: str, reason: str) -> str:
        return json.dumps({"raw": raw, "reason": reason, "dead_at": time.time()})

    async def pop(self, timeout: float = 5.0) -> Optional[ResultEnvelope]:
        """
        Destructive, unacknowledged pop. For tests and tooling only.

        The result is gone from Redis as soon as this returns; if the caller
        fails to handle it, it is lost. The coordinator uses claim()/ack().
        Returns None on timeout or corrupt payload.
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
        """Number of results waiting to be claimed."""
        return await self._redis.llen(self._key)

    async def processing_count(self) -> int:
        """Number of results claimed but not yet acked."""
        return await self._redis.llen(self._processing_key)

    async def dead_letters(self) -> list[dict]:
        """All dead-letter records, newest first: {"raw", "reason", "dead_at"}."""
        return [json.loads(r) for r in await self._redis.lrange(self._dead_key, 0, -1)]

    async def flush(self) -> None:
        """Delete pending, processing and dead-letter lists."""
        await self._redis.delete(self._key, self._processing_key, self._dead_key)
        logger.warning("[result_queue] Result queue flushed.")