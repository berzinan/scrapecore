"""
scrapecore/queue/redis_client.py

Redis connection factory.

A single Redis client instance is shared across the coordinator and agent
processes. This module provides a factory function that reads connection
parameters from environment variables and returns a configured client.

The caller is responsible for calling aclose() on the client when shutting down.
"""

import os
import redis.asyncio as aioredis


def create_redis_client() -> aioredis.Redis:
    """
    Create an async Redis client from environment variables.

    Environment variables:
        REDIS_URL:      Full Redis URL. Takes precedence over individual fields.
                        Example: redis://localhost:6379/0
        REDIS_HOST:     Redis hostname. Default: localhost
        REDIS_PORT:     Redis port.     Default: 6379
        REDIS_DB:       Redis database. Default: 0
        REDIS_PASSWORD: Redis password. Default: None

    Returns:
        A configured aioredis.Redis instance. Connection is not established
        until the first command is issued.
    """
    url = os.getenv("REDIS_URL")

    if url:
        return aioredis.from_url(url, decode_responses=True)

    return aioredis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )