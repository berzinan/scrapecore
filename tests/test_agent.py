"""Layer 3 — Agent fetch path with mocked HTTP and fake Redis.

Agents only fetch; parsing is tested on the coordinator side
(tests/test_server_parsing.py).
"""

import asyncio

import pytest
from aioresponses import aioresponses
from yarl import URL

from scrapecore.agent.agent import Agent
from scrapecore.http.base import HttpResponse
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


def _make_agent(fake_redis, namespace, **kwargs) -> Agent:
    """Replaces old rps=100.0: effectively no local RL."""
    kwargs.setdefault("rate_limit_config", {"site.parse": 1000.0})
    return Agent(
        agent_id="test-agent",
        redis=fake_redis,
        namespace=namespace,
        claim_timeout=0.1,            # fast shutdown
        heartbeat_interval=1.0,       # >= 1s so int(interval * 3) >= 1
        task_timeout=5,
        **kwargs,
    )


# ── Successful fetch ─────────────────────────────────────────────────────────

async def test_successful_fetch_returns_raw_response(fake_redis, namespace):
    agent = _make_agent(fake_redis, namespace)
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
        await _run_agent_until(agent, lambda: agent.tasks_completed >= 1)

    result = await result_queue.pop(timeout=1.0)
    assert result is not None
    assert result.status == "completed"
    assert result.agent_id == "test-agent"
    assert result.output is None  # agents do not parse
    assert result.response["status"] == 200
    assert "application/json" in result.response["content_type"]
    assert result.response["text"] == '{"data": "value"}'
    assert result.response["url"] == "https://example.com/api"
    # The originating task travels with the result, metadata included.
    assert result.task["task_id"] == envelope.task_id
    assert result.task["payload"]["metadata"] == {"x": 1}

    # Task acknowledged → nothing left in queue
    assert await task_queue.processing_count() == 0
    assert await task_queue.pending_count() == 0


async def test_html_body_is_passed_through_as_text(fake_redis, namespace):
    agent = _make_agent(fake_redis, namespace)
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    await _enqueue(fake_redis, namespace, TaskEnvelope(
        job_id="j", parser_key="site.parse",
        payload={"url": "https://example.com/page"},
    ))

    with aioresponses() as mock:
        mock.get("https://example.com/page", body="<html><b>hi</b></html>",
                 content_type="text/html")
        await _run_agent_until(agent, lambda: agent.tasks_completed >= 1)

    result = await result_queue.pop(timeout=1.0)
    assert result.response["text"] == "<html><b>hi</b></html>"
    assert "text/html" in result.response["content_type"]


async def test_agent_needs_no_parser_for_any_parser_key(fake_redis, namespace):
    agent = _make_agent(fake_redis, namespace)
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    await _enqueue(fake_redis, namespace, TaskEnvelope(
        job_id="j", parser_key="some.site.nobody.registered",
        payload={"url": "https://example.com/any"},
    ))

    with aioresponses() as mock:
        mock.get("https://example.com/any", payload={})
        await _run_agent_until(agent, lambda: agent.tasks_completed >= 1)

    result = await result_queue.pop(timeout=1.0)
    assert result.status == "completed"
    assert result.task["parser_key"] == "some.site.nobody.registered"


# ── Task cancellation ──────────────────────────────────────────────────────────

async def test_cancelled_job_stops_retries(fake_redis, namespace):
    """When a job is in the cancelled_jobs set, a failed task is acknowledged
    instead of re-enqueued at queue level, so the queue drains."""
    agent = _make_agent(fake_redis, namespace)
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

    # Failed once, then acknowledged — not retried at the queue level.
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
    agent = _make_agent(fake_redis, namespace)
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
    agent = _make_agent(fake_redis, namespace)
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
    assert result.response["text"] == '{"data": "recovered"}'
    assert await result_queue.pop(timeout=0.1) is None

    assert agent.tasks_failed == 0
    assert await task_queue.pending_count() == 0
    assert await task_queue.processing_count() == 0


# ── Heartbeat ────────────────────────────────────────────────────────────────

class _BlockingBackend:
    """HttpBackend that holds every request until `release` is set."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.entered = asyncio.Event()

    async def request(self, method, url, *, headers=None, params=None, body=None, proxy=None):
        self.entered.set()
        await self.release.wait()
        return HttpResponse(status=200, content_type="text/html", text="ok", headers={})

    async def close(self) -> None:
        pass


async def test_heartbeat_key_written_and_cleaned_up(fake_redis, namespace):
    agent = _make_agent(fake_redis, namespace)
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
    fetch is executing, and returns to idle once it finishes."""
    backend = _BlockingBackend()
    agent = _make_agent(fake_redis, namespace, http_backend=backend)
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    await _enqueue(fake_redis, namespace, TaskEnvelope(
        job_id="j", parser_key="site.parse",
        payload={"url": "https://example.com/block"},
    ))

    runner = asyncio.create_task(agent.start())
    try:
        await asyncio.wait_for(backend.entered.wait(), timeout=3.0)

        info = agent._heartbeat_info()
        assert info["state"] == "busy"
        assert info["current_parser_key"] == "site.parse"

        backend.release.set()

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
    assert "parsers" not in info  # agents carry no parsers

    result = await result_queue.pop(timeout=1.0)
    assert result is not None and result.status == "completed"
