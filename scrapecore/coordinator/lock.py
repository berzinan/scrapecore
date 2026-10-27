"""
scrapecore/coordinator/lock.py

CoordinatorLock — guarantees at most one active coordinator per namespace.

The coordinator's correctness rests on being the only one running: its
startup recovery moves every in-flight result back to the queue, and its
reconcile step re-dispatches any running job it is not tracking. Two
coordinators would steal each other's in-flight results and double-dispatch
jobs. This lock turns "run exactly one coordinator" from an assumption into
a guarantee.

Mechanism (a standard Redis lease):
    acquire  SET key <token> NX PX <ttl>   — only if nobody holds it
    renew    extend the TTL only if the value is still our token
    release  delete the key only if the value is still our token

Renew and release compare-then-act in a Lua script, which Redis runs
atomically, so a coordinator can never extend or delete a lock that expired
and was taken by someone else in between.

Key:
    scrapecore:{namespace}:coordinator:lock   value: unique per instance

Limitation: the lock is a lease, not a fence. If a coordinator is paused for
longer than the TTL (debugger, VM freeze, multi-second GC), another one can
take over while the first is mid-operation, before the first notices on its
next renewal. Keep the TTL comfortably above any plausible pause.
"""
from __future__ import annotations

import os
import socket
import uuid

import redis.asyncio as aioredis

# Extend the key's TTL only if we still own it. Returns 1 if extended, 0 if not ours.
_RENEW_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

# Delete the key only if we still own it. Returns 1 if deleted, 0 if not ours.
_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class CoordinatorLockLost(RuntimeError):
    """Raised by Coordinator.start() when the lock was lost while running."""


class CoordinatorLock:
    """
    Redis lease identifying the single active coordinator for a namespace.

    Args:
        redis:     Connected async Redis client.
        namespace: Must match the coordinator's namespace.
        ttl:       Seconds the lease lasts without renewal. The holder renews
                   every ttl/3, so two consecutive missed renewals still
                   leave a margin before expiry.
    """

    def __init__(self, redis: aioredis.Redis, namespace: str = "default", ttl: float = 30.0) -> None:
        self._redis = redis
        self._key = f"scrapecore:{namespace}:coordinator:lock"
        self._ttl_ms = max(1, int(ttl * 1000))
        # Unique per instance, readable in redis-cli for debugging.
        self.token = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

    @property
    def ttl(self) -> float:
        """Lease duration in seconds."""
        return self._ttl_ms / 1000

    async def acquire(self) -> bool:
        """Take the lock if nobody holds it. :return: True if acquired."""
        return bool(await self._redis.set(self._key, self.token, nx=True, px=self._ttl_ms))

    async def renew(self) -> bool:
        """
        Extend the lease if we still hold it.
        :return: False if the lock expired or now belongs to someone else.
        """
        return bool(await self._redis.eval(_RENEW_SCRIPT, 1, self._key, self.token, self._ttl_ms))

    async def release(self) -> bool:
        """Delete the lock if we still hold it. :return: True if deleted."""
        return bool(await self._redis.eval(_RELEASE_SCRIPT, 1, self._key, self.token))

    async def holder(self) -> str | None:
        """Token of the current holder, or None if the lock is free."""
        return await self._redis.get(self._key)
