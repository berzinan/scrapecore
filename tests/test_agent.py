"""Layer 3 — Agent execution path with mocked HTTP and fake Redis."""

import asyncio

import pytest
from aioresponses import aioresponses
from yarl import URL

from scrapecore.agent.agent import Agent
from scrapecore.models.task import TaskEnvelope
from scrapecore.queue.task_queue import TaskQueue
from scrapecore.queue.result_queue import ResultQueue

# ── Fixtures ───────────────────────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def fast_http_backoff(monkeypatch):
    """In-place 429/5xxd retries sleep 10s+; collapse to 2 instant attempts."""
    monkeypatch.setattr("scrapecore.agent.agent._HTTP_RETRY_BACKOFF", (0, 0))

# ── Helpers ───────────────────────────────────────────────────────────────────


async def _enqueue(fake_redis, namespace, envelope: TaskEnvelope) -> None:
    await TaskQueue(fake_redis, namespace=namespace).push(
        envelope.task_id, envelope.to_dict(), priority=envelope.priority
    )


async def _run_agent_until(agent: Agent, predicate, timeout: float = 3.0) -> None:
    """Start the agent in the background, wait until `predicate()` is true, then stop."""
    runner = asyncio.create_task(agent.start())
    try:
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if predicate():
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("Agent did not reach the expected state in time")
    finally:
        await agent.stop()
        await asyncio.wait_for(runner, timeout=5.0)


def _make_agent(fake_redis, namespace, registry, **kwargs) -> Agent:
    """Replaces old rps=100.0: effectively no local RL."""
    kwargs.setdefault("rate_limit_config", {"site.parse": 1000.0})
    return Agent(
        agent_id="test-agent",
        redis=fake_redis,
        parser_registry=registry,
        namespace=namespace,
        claim_timeout=0.1,            # fast shutdown
        heartbeat_interval=1.0,       # >= 1s so int(interval * 3) >= 1
        task_timeout=5,
        **kwargs,
    )


# ── Successful task ──────────────────────────────────────────────────────────

async def test_successful_task(fake_redis, namespace):
    def parser(raw, metadata):
        return {"parsed": raw["data"], "meta": metadata}

    agent = _make_agent(fake_redis, namespace, {"site.parse": parser})
    result_queue = ResultQueue(fake_redis, namespace=namespace)
    task_queue = TaskQueue(fake_redis, namespace=namespace)

    envelope = TaskEnvelope(
        job_id="job-1",
        parser_key="site.parse",
        payload={"url": "https://example.com/api", "metadata": {"x": 1}},
    )
    await _enqueue(fake_redis, namespace, envelope)

    with aioresponses() as mock:
        mock.get("https://example.com/api", payload={"data": "value"})
        await _run_agent_until(
            agent,
            lambda: agent.tasks_completed >= 1,
        )

    result = await result_queue.pop(timeout=1.0)
    assert result is not None
    assert result.status == "completed"
    assert result.output == {"parsed": "value", "meta": {"x": 1}}
    assert result.agent_id == "test-agent"

    # Task acknowledged → nothing left in queue
    assert await task_queue.processing_count() == 0
    assert await task_queue.pending_count() == 0

# ── Task cancellation ──────────────────────────────────────────────────────────
async def test_cancelled_job_stops_retries(fake_redis, namespace):
    """When a job is in the cancelled_jobs set, a failed task is acknowledged
    instead of re-enqueued at queue level, so the queue drains."""
    def parser(raw, metadata):
        return {}

    agent = _make_agent(fake_redis, namespace, {"site.parse": parser})
    result_queue = ResultQueue(fake_redis, namespace=namespace)
    task_queue = TaskQueue(fake_redis, namespace=namespace)

    cancelled_key = f"scrapecore:{namespace}:cancelled_jobs"
    await fake_redis.sadd(cancelled_key, "job-cancel")

    url = "https://example.com/cancelled"
    envelope = TaskEnvelope(
        job_id="job-cancel", parser_key="site.parse",
        payload={"url": url},
        max_retries=3,
    )
    await _enqueue(fake_redis, namespace, envelope)

    with aioresponses() as mock:
        mock.get(url, status=500, repeat=True)
        await _run_agent_until(agent, lambda: agent.tasks_failed >= 1, timeout=5.0)

    # Exhausted once, then acknowledged — not retried at the queue level.
    assert await task_queue.pending_count() == 0
    assert await task_queue.processing_count() == 0

    statuses = []
    while True:
        r = await result_queue.pop(timeout=0.1)
        if r is None:
            break
        statuses.append(r.status)

    assert statuses == ["failed"]

# ── HTTP errors ──────────────────────────────────────────────────────────────

async def test_http_error_pushes_failed_result_and_retries(fake_redis, namespace):
    def parser(raw, metadata):
        return {"parsed": raw}

    agent = _make_agent(fake_redis, namespace, {"site.parse": parser})
    result_queue = ResultQueue(fake_redis, namespace=namespace)
    task_queue = TaskQueue(fake_redis, namespace=namespace)

    envelope = TaskEnvelope(
        job_id="j", parser_key="site.parse",
        payload={"url": "https://example.com/fail"},
        max_retries=2,
    )
    await _enqueue(fake_redis, namespace, envelope)

    with aioresponses() as mock:
        mock.get("https://example.com/fail", status=500, repeat=True)
        await _run_agent_until(
            agent,
            lambda: agent.tasks_failed >= envelope.max_retries + 1,
            timeout=5.0,
        )

    statuses = []
    errors = []
    while True:
        r = await result_queue.pop(timeout=0.1)
        if r is None:
            break
        statuses.append(r.status)
        errors.append(r.error or "")

    # First N are "failed", final one is "exhausted".
    assert statuses[:-1] == ["failed"] * (len(statuses) - 1)
    assert statuses[-1] == "exhausted"

    # Every failure must be the real HTTP 500, not an unhandled mock/TypeError
    assert len(errors) == envelope.max_retries + 1
    for err in errors:
        assert "HTTP 500" in err, f"expected HTTP 500, got: {err!r}"
        assert "Unhandled" not in err

    # After exhaustion the task is acknowledged, not re-queued.
    assert await task_queue.pending_count() == 0
    assert await task_queue.processing_count() == 0

async def test_http_failure_recovers_within_single_execution(fake_redis, namespace):
    def parser(raw, metadata):
        return {"v": 1}
    
    agent = _make_agent(fake_redis, namespace, {"site.parse": parser})
    result_queue = ResultQueue(fake_redis, namespace=namespace)
    task_queue = TaskQueue(fake_redis, namespace=namespace)

    url = "https://example.com/fail"
    envelope = TaskEnvelope(
        job_id="j", parser_key="site.parse",
        payload={"url": url},
    )
    await _enqueue(fake_redis, namespace, envelope)

    with aioresponses() as mock:
        mock.get(url, status=500)
        mock.get(url, status=200, payload={"data": "recovered"})
        await _run_agent_until(agent, lambda: agent.tasks_completed >= 1)
        calls = mock.requests[("GET", URL(url))]

    assert len(calls) == 2

    result = await result_queue.pop(timeout=1.0)
    assert result is not None
    assert result.status == "completed"
    assert result.retry_count == 0
    assert result.error is None
    assert await result_queue.pop(timeout=0.1) is None

    assert agent.tasks_failed == 0
    assert await task_queue.pending_count() == 0
    assert await task_queue.processing_count() == 0


# ── Unknown parser_key ───────────────────────────────────────────────────────

async def test_unknown_parser_key_produces_failure(fake_redis, namespace):
    agent = _make_agent(fake_redis, namespace, {})  # empty registry
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    envelope = TaskEnvelope(
        job_id="j", parser_key="missing.parser",
        payload={"url": "https://example.com"},
        max_retries=0,
    )
    await _enqueue(fake_redis, namespace, envelope)

    await _run_agent_until(agent, lambda: agent.tasks_failed >= 1)

    result = await result_queue.pop(timeout=1.0)
    assert result is not None
    assert result.status == "exhausted"
    assert result.error is not None
    assert "missing.parser" in result.error


# ── Parser raises ────────────────────────────────────────────────────────────

async def test_parser_exception_produces_failure(fake_redis, namespace):
    def boom(raw, metadata):
        raise RuntimeError("kaboom")

    agent = _make_agent(fake_redis, namespace, {"site.parse": boom})
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    envelope = TaskEnvelope(
        job_id="j", parser_key="site.parse",
        payload={"url": "https://example.com"},
        max_retries=0,
    )
    await _enqueue(fake_redis, namespace, envelope)

    with aioresponses() as mock:
        mock.get("https://example.com", payload={"data": "x"})
        await _run_agent_until(agent, lambda: agent.tasks_failed >= 1)

    result = await result_queue.pop(timeout=1.0)
    assert result is not None
    assert result.status == "exhausted"
    assert "kaboom" in result.error

# ── Stage output ─────────────────────────────────────────────────────────────

async def test_stage_output_extracted_from_parser_return(fake_redis, namespace):
    def parser(raw, metadata):
        return {
            "kept": "value",
            "__stage_output__": {
                "next_parser_key": "site.next",
                "items": [1, 2, 3],
            },
        }

    agent = _make_agent(fake_redis, namespace, {"site.parse": parser})
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    envelope = TaskEnvelope(
        job_id="j", parser_key="site.parse",
        payload={"url": "https://example.com/s"},
    )
    await _enqueue(fake_redis, namespace, envelope)

    with aioresponses() as mock:
        mock.get("https://example.com/s", payload={})
        await _run_agent_until(agent, lambda: agent.tasks_completed >= 1)

    result = await result_queue.pop(timeout=1.0)
    assert result is not None
    assert result.status == "completed"
    assert result.output == {"kept": "value"}
    assert "__stage_output__" not in result.output
    assert result.stage_output == {
        "next_parser_key": "site.next",
        "items": [1, 2, 3],
    }


# ── Sync parser ──────────────────────────────────────────────────────────────

async def test_sync_parser_runs_to_completion(fake_redis, namespace):
    def sync_parser(raw, metadata):
        # Plain sync function — must work without being async.
        return {"sync": True, "echo": raw.get("hello")}

    agent = _make_agent(fake_redis, namespace, {"site.parse": sync_parser})
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    envelope = TaskEnvelope(
        job_id="j", parser_key="site.parse",
        payload={"url": "https://example.com/sync"},
    )
    await _enqueue(fake_redis, namespace, envelope)

    with aioresponses() as mock:
        mock.get("https://example.com/sync", payload={"hello": "world"})
        await _run_agent_until(agent, lambda: agent.tasks_completed >= 1)

    result = await result_queue.pop(timeout=1.0)
    assert result is not None
    assert result.output == {"sync": True, "echo": "world"}


async def test_async_parser_runs_to_completion(fake_redis, namespace):
    async def async_parser(raw, metadata):
        await asyncio.sleep(0)
        return {"async": True}

    agent = _make_agent(fake_redis, namespace, {"site.parse": async_parser})
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    envelope = TaskEnvelope(
        job_id="j", parser_key="site.parse",
        payload={"url": "https://example.com/async"},
    )
    await _enqueue(fake_redis, namespace, envelope)

    with aioresponses() as mock:
        mock.get("https://example.com/async", payload={})
        await _run_agent_until(agent, lambda: agent.tasks_completed >= 1)

    result = await result_queue.pop(timeout=1.0)
    assert result is not None
    assert result.output == {"async": True}


# ── Heartbeat ────────────────────────────────────────────────────────────────

async def test_heartbeat_key_written_and_cleaned_up(fake_redis, namespace):
    def parser(raw, metadata):
        return {}

    agent = _make_agent(fake_redis, namespace, {"site.parse": parser})
    heartbeat_key = f"scrapecore:{namespace}:heartbeat:test-agent"

    runner = asyncio.create_task(agent.start())
    try:
        # Wait until heartbeat appears.
        for _ in range(50):
            if await fake_redis.get(heartbeat_key) is not None:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("heartbeat key was never written")
    finally:
        await agent.stop()
        await asyncio.wait_for(runner, timeout=5.0)

    # After clean shutdown, the heartbeat key should be deleted.
    assert await fake_redis.get(heartbeat_key) is None
