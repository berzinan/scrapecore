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


# ── Parser contract ──────────────────────────────────────────────────────────

async def _run_single_task(fake_redis, namespace, parser, max_retries=0):
    """Run one task through `parser` and return its ResultEnvelope."""
    agent = _make_agent(fake_redis, namespace, {"site.parse": parser})
    envelope = TaskEnvelope(
        job_id="j", parser_key="site.parse",
        payload={"url": "https://example.com/c"}, max_retries=max_retries,
    )
    await _enqueue(fake_redis, namespace, envelope)
    with aioresponses() as mock:
        mock.get("https://example.com/c", payload={})
        await _run_agent_until(
            agent, lambda: agent.tasks_completed + agent.tasks_failed >= 1
        )
    return await ResultQueue(fake_redis, namespace=namespace).pop(timeout=1.0)


async def test_non_dict_parser_output_fails_task(fake_redis, namespace):
    class Model:  # e.g. a typed object returned without converting it
        def to_dict(self):
            return {"a": 1}

    result = await _run_single_task(fake_redis, namespace, lambda raw, md: Model())

    assert result.status == "exhausted"
    assert "must return a dict" in result.error


async def test_non_json_serializable_output_fails_task_and_agent_survives(
    fake_redis, namespace
):
    import datetime

    calls = []

    def parser(raw, metadata):
        calls.append(1)
        if len(calls) == 1:
            return {"scraped_at": datetime.datetime(2026, 1, 1)}
        return {"ok": True}

    agent = _make_agent(fake_redis, namespace, {"site.parse": parser})
    result_queue = ResultQueue(fake_redis, namespace=namespace)
    for task_id in ("bad", "good"):
        await _enqueue(fake_redis, namespace, TaskEnvelope(
            job_id="j", task_id=task_id, parser_key="site.parse",
            payload={"url": "https://example.com/c"}, max_retries=0,
        ))

    with aioresponses() as mock:
        mock.get("https://example.com/c", payload={}, repeat=True)
        await _run_agent_until(
            agent, lambda: agent.tasks_completed + agent.tasks_failed >= 2
        )

    results = {r.task_id: r for r in [await result_queue.pop(1.0), await result_queue.pop(1.0)]}
    assert results["bad"].status == "exhausted"
    assert "JSON-serializable" in results["bad"].error
    # Before: json.dumps raised inside ResultQueue.push(), outside the run
    # loop's error handling, so the agent died and never ran "good".
    assert results["good"].status == "completed"


async def test_non_dict_stage_output_fails_task(fake_redis, namespace):
    def parser(raw, metadata):
        return {"x": 1, "__stage_output__": ["not", "a", "dict"]}

    result = await _run_single_task(fake_redis, namespace, parser)

    assert result.status == "exhausted"
    assert "must be a dict" in result.error


async def test_parser_return_value_is_not_mutated(fake_redis, namespace):
    returned = {"kept": 1, "__stage_output__": {"next_parser_key": "site.next"}}

    result = await _run_single_task(fake_redis, namespace, lambda raw, md: returned)

    assert result.status == "completed"
    assert result.output == {"kept": 1}
    assert "__stage_output__" in returned  # agent popped from a copy


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

async def test_heartbeat_info_reports_busy_then_idle(fake_redis, namespace):
    """White-box: _heartbeat_info reflects the in-flight parser key while a
    task is executing, and returns to idle once it finishes."""
    release = asyncio.Event()

    async def blocked_parser(raw, metadata):
        await release.wait()
        return {"done": True}

    agent = _make_agent(fake_redis, namespace, {"site.parse": blocked_parser})
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    envelope = TaskEnvelope(
        job_id="j", parser_key="site.parse",
        payload={"url": "https://example.com/block"},
    )
    await _enqueue(fake_redis, namespace, envelope)

    with aioresponses() as mock:
        mock.get("https://example.com/block", payload={})
        runner = asyncio.create_task(agent.start())
        try:
            # Wait until the parser is actually blocked inside _execute.
            for _ in range(100):
                if agent._current_parser_key is not None:
                    break
                await asyncio.sleep(0.02)
            else:
                pytest.fail("agent never entered busy state")

            info = agent._heartbeat_info()
            assert info["state"] == "busy"
            assert info["current_parser_key"] == "site.parse"

            release.set()

            for _ in range(100):
                if agent.tasks_completed >= 1:
                    break
                await asyncio.sleep(0.02)
            else:
                pytest.fail("agent never completed the task")
        finally:
            await agent.stop()
            await asyncio.wait_for(runner, timeout=5.0)

    info = agent._heartbeat_info()
    assert info["state"] == "idle"
    assert info["current_parser_key"] is None
    assert info["tasks_completed"] == 1
    assert info["tasks_failed"] == 0

    result = await result_queue.pop(timeout=1.0)
    assert result is not None and result.status == "completed"


async def test_heartbeat_info_advertises_registered_parsers(fake_redis, namespace):
    registry = {
        "site.b": lambda raw, md: {},
        "site.a": lambda raw, md: {},
        "site.c": lambda raw, md: {},
    }
    agent = _make_agent(fake_redis, namespace, registry)

    info = agent._heartbeat_info()
    assert info["parsers"] == ["site.a", "site.b", "site.c"]
    assert agent._current_parser_key is None
