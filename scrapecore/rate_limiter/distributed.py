"""
scrapecore/rate_limiter/distributed.py

Distributed rate limiter using a Redis sliding window counter.

Two layers of rate limiting exist in the system:

    Layer 1 — Local (per-agent, in agent.py)
        Enforces minimum delay between requests to the same domain
        within a single agent. Fast — no network round trip.
        This is what prevents a single agent from hammering a site.

    Layer 2 — Global (this module, coordinator-configured)
        Enforces a maximum request rate across ALL agents combined
        for a given domain. Stored in Redis so every agent sees the
        same counter.
        Use this when a site has a global rate cap regardless of IP.

The two layers work together:
    - Layer 1 always runs (built into the agent)
    - Layer 2 is optional — only needed when you want to cap total
      throughput across the fleet for a specific domain

How the sliding window works:

    Redis key: scrapecore:ratelimit:{domain}:{window_id}
    window_id = int(current_unix_time / window_seconds)

    Each key represents one time window (default: 1 second).
    On each request:
        1. Compute current window_id
        2. INCR the counter for that window (atomic)
        3. SET expiry to 2x window_seconds (auto-cleanup)
        4. If counter > limit → sleep until next window, then retry

    Because INCR is atomic in Redis, two agents incrementing
    simultaneously are serialised — one gets count=1, the other
    gets count=2. No race condition possible.

Lua script:
    The check-and-increment is done in a Lua script so that the
    read (GET) and write (INCR + EXPIRE) happen atomically.
    Without this, two agents could both read count=4 (limit=5),
    both decide to proceed, and both increment — overshooting the limit.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# Lua script: atomically increment a windowed counter and return the new value.
# If the key is new, also set its expiry.
# KEYS[1] = the counter key
# ARGV[1] = expiry in seconds
_INCR_SCRIPT = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
"""


class DistributedRateLimiter:
    """
    Global rate limiter shared across all agents via Redis.

    Usage on agent side:
        limiter = DistributedRateLimiter(redis, namespace="default")
        await limiter.acquire("autopiter.ru", limit=2, window_seconds=1.0)
        # proceed with request

    Args:
        redis:      Connected async Redis client.
        namespace:  Matches the namespace used by queues and coordinator.
        max_wait:   Maximum seconds to wait for a slot before raising.
                    Set to None to wait indefinitely.
    """

    def __init__(
        self,
        redis: aioredis.Redis,
        namespace: str = "default",
        max_wait: Optional[float] = None,
    ) -> None:
        self._redis = redis
        self._namespace = namespace
        self._max_wait = max_wait
        self._script_sha: Optional[str] = None

    async def _load_script(self) -> str:
        """
        Register the Lua script with Redis on first use.
        Redis returns a SHA hash — we use EVALSHA on subsequent calls
        instead of sending the full script every time.
        """
        if self._script_sha is None:
            self._script_sha = await self._redis.script_load(_INCR_SCRIPT)
        return self._script_sha

    def _window_key(self, domain: str, window_id: int) -> str:
        return f"scrapecore:{self._namespace}:ratelimit:{domain}:{window_id}"

    async def acquire(
        self,
        domain: str,
        limit: int,
        window_seconds: float = 1.0,
    ) -> None:
        """
        Block until a request slot is available for this domain.

        Args:
            domain:         The domain being rate-limited, e.g. "autopiter.ru".
            limit:          Maximum requests across all agents per window.
            window_seconds: Length of the sliding window in seconds.
        """
        sha = await self._load_script()
        expiry = math.ceil(window_seconds * 2)
        waited = 0.0

        while True:
            now = time.time()
            window_id = int(now / window_seconds)
            key = self._window_key(domain, window_id)

            count = await self._redis.evalsha(sha, 1, key, expiry)

            if count <= limit:
                # Slot acquired
                logger.debug(
                    f"[ratelimit] {domain} window={window_id} "
                    f"count={count}/{limit} — allowed"
                )
                return

            # Window is full — calculate how long until the next window opens
            window_end = (window_id + 1) * window_seconds
            sleep_for = window_end - time.time()

            if sleep_for <= 0:
                # Window rolled over between our INCR and now — try immediately
                continue

            if self._max_wait is not None and waited + sleep_for > self._max_wait:
                raise TimeoutError(
                    f"Rate limit for {domain!r} exceeded and max_wait "
                    f"({self._max_wait}s) would be breached."
                )

            logger.debug(
                f"[ratelimit] {domain} window={window_id} "
                f"count={count}/{limit} — waiting {sleep_for:.3f}s"
            )
            await asyncio.sleep(sleep_for)
            waited += sleep_for

    async def current_count(self, domain: str, window_seconds: float = 1.0) -> int:
        """
        Return the current request count for this domain in the active window.
        Useful for monitoring and debugging.
        """
        window_id = int(time.time() / window_seconds)
        key = self._window_key(domain, window_id)
        raw = await self._redis.get(key)
        return int(raw) if raw else 0


class RateLimitConfig:
    """
    Holds per-domain rate limit configuration.

    The consumer populates this and passes it to the agent.
    Domains not explicitly configured fall back to defaults.

    Example:
        config = RateLimitConfig(default_limit=5)
        config.set("autopiter.ru", limit=2, window_seconds=1.0)
        config.set("autodoc.ru",   limit=1, window_seconds=1.0)
    """

    def __init__(
        self,
        default_limit: int = 10,
        default_window: float = 1.0,
    ) -> None:
        self._default_limit = default_limit
        self._default_window = default_window
        self._rules: dict[str, tuple[int, float]] = {}

    def set(self, domain: str, limit: int, window_seconds: float = 1.0) -> None:
        self._rules[domain] = (limit, window_seconds)

    def get(self, domain: str) -> tuple[int, float]:
        """Return (limit, window_seconds) for the given domain."""
        return self._rules.get(domain, (self._default_limit, self._default_window))