"""Layer 2 — ResultQueue against fakeredis."""

import pytest

from scrapecore.queue.result_queue import ResultQueue
from scrapecore.models.result import ResultEnvelope


@pytest.fixture
def queue(fake_redis, namespace):
    return ResultQueue(fake_redis, namespace=namespace)


def _success_result() -> ResultEnvelope:
    return ResultEnvelope.success(
        task_id="t-1", job_id="j-1", agent_id="agent-01",
        retry_count=0, max_retries=3,
        output={"k": "v"},
    )


async def test_push_then_pop_returns_envelope(queue):
    await queue.push(_success_result())
    popped = await queue.pop(timeout=1.0)

    assert popped is not None
    assert popped.task_id == "t-1"
    assert popped.status == "completed"
    assert popped.output == {"k": "v"}


async def test_pop_empty_returns_none(queue):
    popped = await queue.pop(timeout=0.1)
    assert popped is None


async def test_pop_in_fifo_order(queue):
    a = ResultEnvelope.success(
        task_id="a", job_id="j", agent_id="agent",
        retry_count=0, max_retries=3, output={"i": 1},
    )
    b = ResultEnvelope.success(
        task_id="b", job_id="j", agent_id="agent",
        retry_count=0, max_retries=3, output={"i": 2},
    )
    await queue.push(a)
    await queue.push(b)

    first = await queue.pop(timeout=1.0)
    second = await queue.pop(timeout=1.0)
    assert first.task_id == "a"
    assert second.task_id == "b"


async def test_corrupt_payload_returns_none(fake_redis, queue, namespace):
    await fake_redis.lpush(f"scrapecore:{namespace}:results", "not-json")
    popped = await queue.pop(timeout=1.0)
    assert popped is None
