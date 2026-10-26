"""
scrapecore/queue/presence.py

Agent presence: who is online right now.

Each agent publishes a JSON heartbeat under its own key with a TTL of a few
heartbeat intervals.
A key that exists means the agent is alive; a crashed or sleeping agent 
disappears by itself when TTL lapses.

Key:
    scrapecore:{namespace}:heartbeat:{agent_id}
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class AgentPresence:

    def __init__(self, redis: aioredis.Redis, namespace: str = "default") -> None:
        self._redis = redis
        self._prefix = f"scrapecore:{namespace}:heartbeat:"

    def _key(self, agent_id: str) -> str:
        return self._prefix + agent_id

    async def beat(self, agent_id: str, info: dict[str, Any], ttl: int) -> None:
        """Publish/refresh this agent's heartbeat."""
        payload = {**info, "agent_id": agent_id, "last_seen": time.time()}
        await self._redis.set(self._key(agent_id), json.dumps(payload), ex=ttl)

    async def remove(self, agent_id: str) -> None:
        """Clean shutdown: disappear immediately instead of waiting for the TTL."""
        await self._redis.delete(self._key(agent_id))

    async def list_online(self) -> list[dict[str, Any]]:
        """
        One record per agent with a live heartbeat, sorted by agent_id.
        Values that aren't a JSON object are skipped with a warning.
        agent_id comes from the key.
        """
        keys = []

        async for key in self._redis.scan_iter(match=self._prefix + "*", count=100):
            keys.append(key)

        if not keys:
            return []
        
        values = await self._redis.mget(keys)
        agents = []

        for key, raw in zip(keys, values):
            agent_id = key[len(self._prefix):]
            if raw is None:
                # Key expired between SCAN & MGET - agent died mid-scan
                continue
            try:
                info = json.loads(raw)
            
            except json.JSONDecodeError:
                logger.warning(f"Malformed heartbeat for {key!r}: {raw!r}")
                continue
            
            if not isinstance(info, dict):
                # Corrupted key, manual SET, or legacy bare-timestamp format.
                logger.warning(f"Non-dict heartbeat for {key!r}: {info!r}")
                continue
            info["agent_id"] = agent_id
            agents.append(info)

        agents.sort(key=lambda a: a["agent_id"])
        return agents