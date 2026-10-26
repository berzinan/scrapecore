"""Layer 2 — AgentPresence: Redis-backed presence enumeration."""

import json
import time

from scrapecore.queue.presence import AgentPresence


async def test_presence_round_trip(fake_redis, namespace):
    presence = AgentPresence(fake_redis, namespace=namespace)

    before = time.time()
    await presence.beat("agent-a", {"host": "h1", "state": "idle"}, ttl=30)
    await presence.beat("agent-b", {"host": "h2", "state": "busy"}, ttl=30)
    after = time.time()

    agents = await presence.list_online()

    assert [a["agent_id"] for a in agents] == ["agent-a", "agent-b"]
    # agent_id and last_seen are injected by beat()
    assert agents[0]["host"] == "h1"
    assert agents[1]["state"] == "busy"
    for a in agents:
        assert before <= a["last_seen"] <= after

    await presence.remove("agent-a")
    agents = await presence.list_online()
    assert [a["agent_id"] for a in agents] == ["agent-b"]


async def test_list_online_skips_non_dict_value(fake_redis, namespace):
    """A bare-float value (legacy timestamp) parses as JSON but isn't a dict."""
    presence = AgentPresence(fake_redis, namespace=namespace)
    await fake_redis.set(
        f"scrapecore:{namespace}:heartbeat:old", "1727400000.1"
    )
    assert await presence.list_online() == []


async def test_list_online_skips_malformed_json(fake_redis, namespace):
    """A non-JSON value is skipped rather than raising."""
    presence = AgentPresence(fake_redis, namespace=namespace)
    await fake_redis.set(
        f"scrapecore:{namespace}:heartbeat:bad", "not json"
    )
    assert await presence.list_online() == []