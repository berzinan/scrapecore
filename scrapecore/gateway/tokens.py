"""
scrapecore/gateway/tokens.py

AgentTokenStore — per-agent bearer tokens for the gateway.

Each remote agent gets its own random token. Only a SHA-256 hash of the
token is stored, so someone who can read Redis cannot recover a usable
token. A fast hash (rather than bcrypt/argon2) is appropriate here because
tokens are 256 bits of randomness: there is nothing to brute-force.

Revoking one agent deletes its hash; no other agent is affected.

Keys:
    scrapecore:{namespace}:gateway:tokens   HASH  sha256(token) → agent_id
    scrapecore:{namespace}:gateway:agents   HASH  agent_id → sha256(token)
"""
from __future__ import annotations

import hashlib
import re
import secrets
from typing import Optional

import redis.asyncio as aioredis

_AGENT_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class AgentTokenStore:
    def __init__(self, redis: aioredis.Redis, namespace: str = "default") -> None:
        self._redis = redis
        self._by_hash = f"scrapecore:{namespace}:gateway:tokens"
        self._by_agent = f"scrapecore:{namespace}:gateway:agents"

    async def add_agent(self, agent_id: str) -> str:
        """
        Issue a token for `agent_id` and return it. Shown once; not stored.

        If the agent already has a token, it is replaced (rotation): the old
        token stops working immediately.
        """
        if not _AGENT_ID_RE.match(agent_id):
            raise ValueError(
                "agent_id must be 1-64 characters of letters, digits, '.', '_' or '-'"
            )
        token = secrets.token_urlsafe(32)
        new_hash = _hash(token)
        old_hash = await self._redis.hget(self._by_agent, agent_id)
        async with self._redis.pipeline(transaction=True) as pipe:
            if old_hash:
                pipe.hdel(self._by_hash, old_hash)
            pipe.hset(self._by_hash, new_hash, agent_id)
            pipe.hset(self._by_agent, agent_id, new_hash)
            await pipe.execute()
        return token

    async def authenticate(self, token: str) -> Optional[str]:
        """Return the agent_id a token belongs to, or None if it is unknown."""
        if not token:
            return None
        return await self._redis.hget(self._by_hash, _hash(token))

    async def revoke(self, agent_id: str) -> bool:
        """Invalidate an agent's token. :return: False if the agent had none."""
        old_hash = await self._redis.hget(self._by_agent, agent_id)
        if not old_hash:
            return False
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hdel(self._by_hash, old_hash)
            pipe.hdel(self._by_agent, agent_id)
            await pipe.execute()
        return True

    async def list_agents(self) -> list[str]:
        """Agent IDs that currently have a valid token, sorted."""
        return sorted(await self._redis.hkeys(self._by_agent))
