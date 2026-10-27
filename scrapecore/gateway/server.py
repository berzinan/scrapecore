"""
scrapecore/gateway/server.py

Gateway — the HTTPS service remote agents talk to instead of Redis.

Agents never reach Redis. They call this gateway, which
authenticates them and performs each operation through RedisTransport on
their behalf. Redis, Postgres and the coordinator stay private; only the
gateway's port is exposed.

Endpoints (JSON over POST, `Authorization: Bearer <token>`):
    POST /v1/claim      {"timeout": s}              → 200 {"task": {...}} | 204
    POST /v1/renew      {"task_id": id}             → 204 | 409
    POST /v1/submit     {"task_id": id, "result"}   → 204 | 409
    POST /v1/heartbeat  {"info": {...}, "ttl": s}   → 204
    POST /v1/leave      {}                          → 204
    GET  /v1/health                                 → 200 (no auth)

Remote agents are treated as semi-trusted. The gateway therefore:
    - takes the agent's identity from its token, never from the request body;
    - records which agent claimed each task, and rejects renew/submit from
      any other agent (409);
    - on submit, uses the server's stored copy of the task, never the copy
      the agent sends back, so a task cannot be altered in transit;
    - strips any parsed output from submitted results and requires a raw
      response for completed ones: parsing, and therefore what reaches the
      database, stays on the server;
    - refuses agents older than min_agent_version (426) when one is set.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Awaitable, Callable, Optional

import redis.asyncio as aioredis
from aiohttp import web

from scrapecore.gateway.tokens import AgentTokenStore
from scrapecore.models.result import ResultEnvelope
from scrapecore.models.task import TaskEnvelope
from scrapecore.queue.task_queue import TaskQueue
from scrapecore.transport.redis_transport import RedisTransport

logger = logging.getLogger(__name__)

API_PREFIX = "/v1"
VERSION_HEADER = "X-Scrapecore-Version"

_TRANSPORT = web.AppKey("transport", RedisTransport)
_TASK_QUEUE = web.AppKey("task_queue", TaskQueue)
_TOKENS = web.AppKey("tokens", AgentTokenStore)
_REDIS = web.AppKey("redis", aioredis.Redis)
_CONFIG = web.AppKey("config", dict)
# Typed request key on aiohttp >= 3.12; plain string key on older versions.
_AGENT_ID = web.RequestKey("agent_id", str) if hasattr(web, "RequestKey") else "agent_id"


# ── Versions ──────────────────────────────────────────────────────────────────

def _version_tuple(version: Optional[str]) -> Optional[tuple[int, ...]]:
    match = re.match(r"^(\d+(?:\.\d+)*)", version or "")
    if not match:
        return None
    return tuple(int(part) for part in match.group(1).split("."))


def version_at_least(version: Optional[str], minimum: str) -> bool:
    """
    True if `version` >= `minimum`, comparing the leading numeric parts
    ("0.4.1rc2" → (0, 4, 1)). An unparseable version ("unknown", missing)
    never satisfies a minimum.
    """
    have, need = _version_tuple(version), _version_tuple(minimum)
    if have is None or need is None:
        return False
    width = max(len(have), len(need))
    return have + (0,) * (width - len(have)) >= need + (0,) * (width - len(need))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _error(status: int, code: str, **extra: Any) -> web.Response:
    return web.json_response({"error": code, **extra}, status=status)


async def _json_body(request: web.Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text='{"error": "invalid_json"}', content_type="application/json")
    if not isinstance(body, dict):
        raise web.HTTPBadRequest(text='{"error": "body_must_be_object"}', content_type="application/json")
    return body


def _claim_key(request: web.Request, task_id: str) -> str:
    return f"scrapecore:{request.app[_CONFIG]['namespace']}:gateway:claim:{task_id}"


async def _holds(request: web.Request, task_id: Any) -> bool:
    if not isinstance(task_id, str) or not task_id:
        return False
    holder = await request.app[_REDIS].get(_claim_key(request, task_id))
    return holder == request[_AGENT_ID]


# ── Middleware ────────────────────────────────────────────────────────────────

@web.middleware
async def _auth_middleware(
    request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
) -> web.StreamResponse:
    if request.path == f"{API_PREFIX}/health":
        return await handler(request)

    auth = request.headers.get("Authorization", "")
    token = auth[7:].strip() if auth.startswith("Bearer ") else ""
    agent_id = await request.app[_TOKENS].authenticate(token)
    if agent_id is None:
        return _error(401, "unauthorized")

    minimum = request.app[_CONFIG]["min_agent_version"]
    if minimum and not version_at_least(request.headers.get(VERSION_HEADER), minimum):
        return _error(426, "upgrade_required", min_version=minimum)

    request[_AGENT_ID] = agent_id
    return await handler(request)


# ── Handlers ──────────────────────────────────────────────────────────────────

async def _health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def _claim(request: web.Request) -> web.Response:
    """Long-poll: hold the request until a task arrives or the timeout passes."""
    body = await _json_body(request)
    config = request.app[_CONFIG]
    try:
        timeout = float(body.get("timeout", config["max_claim_timeout"]))
    except (TypeError, ValueError):
        return _error(400, "invalid_timeout")
    timeout = min(max(timeout, 0.0), config["max_claim_timeout"])

    task = await request.app[_TRANSPORT].claim(timeout)
    if task is None:
        return web.Response(status=204)

    await request.app[_REDIS].set(
        _claim_key(request, task.task_id), request[_AGENT_ID], ex=config["claim_ttl"]
    )
    return web.json_response({"task": task.to_dict()})


async def _renew(request: web.Request) -> web.Response:
    body = await _json_body(request)
    task_id = body.get("task_id")
    if not await _holds(request, task_id):
        return _error(409, "task_not_held")
    await request.app[_TRANSPORT].renew(task_id)
    await request.app[_REDIS].expire(_claim_key(request, task_id), request.app[_CONFIG]["claim_ttl"])
    return web.Response(status=204)


async def _submit(request: web.Request) -> web.Response:
    body = await _json_body(request)
    task_id = body.get("task_id")
    if not await _holds(request, task_id):
        return _error(409, "task_not_held")

    payload = await request.app[_TASK_QUEUE].get_payload(task_id)
    if payload is None:
        # Task no longer exists server-side (e.g. purged by cancel_job).
        await request.app[_REDIS].delete(_claim_key(request, task_id))
        return _error(409, "task_not_held")
    task = TaskEnvelope.from_dict(payload)

    try:
        result = ResultEnvelope.from_dict(body["result"])
    except Exception:
        return _error(400, "invalid_result")
    if result.status not in ("completed", "failed", "exhausted"):
        return _error(400, "invalid_result")

    # Identity and task facts come from the server, never from the agent.
    result.task_id = task.task_id
    result.job_id = task.job_id
    result.agent_id = request[_AGENT_ID]
    result.retry_count = task.retry_count
    result.max_retries = task.max_retries
    # Parsing happens on the server: remote agents cannot submit parsed data.
    result.output = None
    result.stage_output = None
    if result.status == "completed":
        if not isinstance(result.response, dict):
            return _error(400, "completed_result_needs_response")
        result.task = task.to_dict()
        result.error = None
    else:
        result.status = "failed" if task.should_retry() else "exhausted"
        result.response = None
        result.task = None

    await request.app[_TRANSPORT].submit(task, result)
    await request.app[_REDIS].delete(_claim_key(request, task_id))
    return web.Response(status=204)


async def _heartbeat(request: web.Request) -> web.Response:
    body = await _json_body(request)
    info = body.get("info", {})
    if not isinstance(info, dict):
        return _error(400, "invalid_info")
    try:
        ttl = int(body.get("ttl", 30))
    except (TypeError, ValueError):
        return _error(400, "invalid_ttl")
    ttl = min(max(ttl, 1), 300)
    await request.app[_TRANSPORT].heartbeat(request[_AGENT_ID], info, ttl)
    return web.Response(status=204)


async def _leave(request: web.Request) -> web.Response:
    await request.app[_TRANSPORT].leave(request[_AGENT_ID])
    return web.Response(status=204)


# ── App factory ───────────────────────────────────────────────────────────────

def create_gateway_app(
    redis: aioredis.Redis,
    namespace: str = "default",
    *,
    min_agent_version: Optional[str] = None,
    max_claim_timeout: float = 20.0,
    max_body_bytes: int = 20 * 1024 * 1024,
    claim_ttl: int = 24 * 3600,
) -> web.Application:
    """
    Build the gateway application.

    Args:
        redis:             Connected async Redis client (same Redis as the coordinator).
        namespace:         Must match the coordinator's namespace.
        min_agent_version: Refuse agents reporting an older scrapecore version (426).
        max_claim_timeout: Upper bound on how long a claim long-poll is held open.
        max_body_bytes:    Largest accepted request body; raw pages travel in submit.
        claim_ttl:         Seconds a task-ownership record lives without renewal.
    """
    app = web.Application(middlewares=[_auth_middleware], client_max_size=max_body_bytes)
    app[_REDIS] = redis
    app[_TRANSPORT] = RedisTransport(redis, namespace)
    app[_TASK_QUEUE] = TaskQueue(redis, namespace)
    app[_TOKENS] = AgentTokenStore(redis, namespace)
    app[_CONFIG] = {
        "namespace": namespace,
        "min_agent_version": min_agent_version,
        "max_claim_timeout": max_claim_timeout,
        "claim_ttl": claim_ttl,
    }
    app.router.add_get(f"{API_PREFIX}/health", _health)
    app.router.add_post(f"{API_PREFIX}/claim", _claim)
    app.router.add_post(f"{API_PREFIX}/renew", _renew)
    app.router.add_post(f"{API_PREFIX}/submit", _submit)
    app.router.add_post(f"{API_PREFIX}/heartbeat", _heartbeat)
    app.router.add_post(f"{API_PREFIX}/leave", _leave)
    return app
