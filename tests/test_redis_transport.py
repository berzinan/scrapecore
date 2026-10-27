"""RedisTransport — the AgentTransport backed directly by Redis queues."""

import time

import pytest

from scrapecore.models.result import ResultEnvelope
from scrapecore.models.task import TaskEnvelope
from scrapecore.queue.presence import AgentPresence
from scrapecore.queue.result_queue import ResultQueue
from scrapecore.queue.task_queue import TaskQueue
from scrapecore.transport import AgentTransport, RedisTransport


@pytest.fixture
def transport(fake_redis, namespace):
    return RedisTransport(fake_redis, namespace)


async def _enqueue_and_claim(fake_redis, namespace, transport, **task_kwargs):
    task = TaskEnvelope(job_id="j", parser_key="p", payload={"url": "u"}, **task_kwargs)
    await TaskQueue(fake_redis, namespace).push(task.task_id, task.to_dict())
    claimed = await transport.claim(timeout=1.0)
    assert claimed is not None
    return claimed


def _failure(task, error="boom"):
    return ResultEnvelope.failure(
        task_id=task.task_id, job_id=task.job_id, agent_id="a",
        retry_count=task.retry_count, max_retries=task.max_retries, error=error,
    )


def test_satisfies_agent_transport_protocol(transport):
    assert isinstance(transport, AgentTransport)


async def test_claim_returns_envelope(fake_redis, namespace, transport):
    task = TaskEnvelope(job_id="j", parser_key="p", payload={"url": "u", "metadata": {"k": 1}})
    await TaskQueue(fake_redis, namespace).push(task.task_id, task.to_dict())

    claimed = await transport.claim(timeout=1.0)

    assert isinstance(claimed, TaskEnvelope)
    assert claimed.task_id == task.task_id
    assert claimed.payload["metadata"] == {"k": 1}
    assert await TaskQueue(fake_redis, namespace).processing_count() == 1


async def test_claim_empty_returns_none(transport):
    assert await transport.claim(timeout=0.1) is None


async def test_renew_refreshes_claim(fake_redis, namespace, transport):
    task = await _enqueue_and_claim(fake_redis, namespace, transport)
    key = f"scrapecore:{namespace}:task:{task.task_id}"
    await fake_redis.hset(key, "claimed_at", "0")

    await transport.renew(task.task_id)

    assert float(await fake_redis.hget(key, "claimed_at")) > time.time() - 5


async def test_submit_completed_pushes_result_and_acknowledges(fake_redis, namespace, transport):
    task = await _enqueue_and_claim(fake_redis, namespace, transport)
    result = ResultEnvelope.fetched(task.to_dict(), "a", {"status": 200, "text": "x"})

    await transport.submit(task, result)

    tq = TaskQueue(fake_redis, namespace)
    assert await tq.processing_count() == 0 and await tq.pending_count() == 0
    pushed = await ResultQueue(fake_redis, namespace).pop(timeout=1.0)
    assert pushed.task_id == task.task_id and pushed.status == "completed"


async def test_submit_retriable_failure_reenqueues_with_incremented_retry(
    fake_redis, namespace, transport
):
    task = await _enqueue_and_claim(fake_redis, namespace, transport, max_retries=3)

    await transport.submit(task, _failure(task))

    assert (await ResultQueue(fake_redis, namespace).pop(timeout=1.0)).status == "failed"
    again = await transport.claim(timeout=1.0)
    assert again.task_id == task.task_id
    assert again.retry_count == 1


async def test_submit_exhausted_failure_acknowledges(fake_redis, namespace, transport):
    task = await _enqueue_and_claim(fake_redis, namespace, transport, max_retries=0)

    await transport.submit(task, _failure(task))

    tq = TaskQueue(fake_redis, namespace)
    assert await tq.processing_count() == 0 and await tq.pending_count() == 0
    assert (await ResultQueue(fake_redis, namespace).pop(timeout=1.0)).status == "exhausted"


async def test_submit_failure_of_cancelled_job_stops_retrying(fake_redis, namespace, transport):
    task = await _enqueue_and_claim(fake_redis, namespace, transport, max_retries=3)
    await fake_redis.sadd(f"scrapecore:{namespace}:cancelled_jobs", task.job_id)

    await transport.submit(task, _failure(task))

    tq = TaskQueue(fake_redis, namespace)
    assert await tq.processing_count() == 0 and await tq.pending_count() == 0


async def test_submit_pushes_result_before_releasing_claim(fake_redis, namespace, transport):
    """If the process dies mid-submit, the result must not be lost."""
    task = await _enqueue_and_claim(fake_redis, namespace, transport)
    order = []
    real_push = transport._result_queue.push
    real_ack = transport._task_queue.acknowledge

    async def push(result):
        order.append("push")
        await real_push(result)

    async def ack(task_id):
        order.append("ack")
        await real_ack(task_id)

    transport._result_queue.push = push
    transport._task_queue.acknowledge = ack

    await transport.submit(task, ResultEnvelope.fetched(task.to_dict(), "a", {"text": "x"}))

    assert order == ["push", "ack"]


async def test_heartbeat_and_leave(fake_redis, namespace, transport):
    presence = AgentPresence(fake_redis, namespace)

    await transport.heartbeat("agent-1", {"state": "idle"}, ttl=30)
    online = await presence.list_online()
    assert [a["agent_id"] for a in online] == ["agent-1"]

    await transport.leave("agent-1")
    assert await presence.list_online() == []
