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


# ── reliable claim / ack ─────────────────────────────────────────────────────

async def test_claim_moves_to_processing_until_ack(queue):
    await queue.push(_success_result())
    claimed = await queue.claim(timeout=1.0)

    assert claimed is not None
    raw, envelope = claimed
    assert envelope.task_id == "t-1"
    assert await queue.length() == 0
    assert await queue.processing_count() == 1

    await queue.ack(raw)
    assert await queue.processing_count() == 0


async def test_claim_empty_returns_none(queue):
    assert await queue.claim(timeout=0.1) is None


async def test_claim_is_fifo(queue):
    for task_id in ("a", "b"):
        await queue.push(ResultEnvelope.success(
            task_id=task_id, job_id="j", agent_id="agent",
            retry_count=0, max_retries=3, output={},
        ))
    first = await queue.claim(timeout=1.0)
    second = await queue.claim(timeout=1.0)
    assert [first[1].task_id, second[1].task_id] == ["a", "b"]


async def test_requeue_returns_result_to_pending(queue):
    await queue.push(_success_result())
    raw, _ = await queue.claim(timeout=1.0)

    await queue.requeue(raw)

    assert await queue.processing_count() == 0
    assert await queue.length() == 1
    again = await queue.claim(timeout=1.0)
    assert again[1].task_id == "t-1"


async def test_requeued_result_goes_behind_newer_results(queue):
    await queue.push(_success_result())
    raw, _ = await queue.claim(timeout=1.0)
    await queue.push(ResultEnvelope.success(
        task_id="newer", job_id="j-1", agent_id="agent",
        retry_count=0, max_retries=3, output={},
    ))

    await queue.requeue(raw)

    first = await queue.claim(timeout=1.0)
    assert first[1].task_id == "newer"


async def test_claim_dead_letters_corrupt_payload(fake_redis, queue, namespace):
    await fake_redis.lpush(f"scrapecore:{namespace}:results", "not-json")

    assert await queue.claim(timeout=1.0) is None
    assert await queue.processing_count() == 0
    dead = await queue.dead_letters()
    assert len(dead) == 1
    assert dead[0]["raw"] == "not-json"
    assert dead[0]["reason"].startswith("deserialize")


async def test_dead_letter_copies_without_removing_from_processing(queue):
    await queue.push(_success_result())
    raw, _ = await queue.claim(timeout=1.0)

    await queue.dead_letter(raw, "on_result: boom")

    assert await queue.processing_count() == 1  # caller still acks
    dead = await queue.dead_letters()
    assert dead[0]["raw"] == raw
    assert dead[0]["reason"] == "on_result: boom"


async def test_recover_inflight_moves_processing_back_to_pending(queue):
    for task_id in ("a", "b"):
        await queue.push(ResultEnvelope.success(
            task_id=task_id, job_id="j", agent_id="agent",
            retry_count=0, max_retries=3, output={},
        ))
    await queue.claim(timeout=1.0)
    await queue.claim(timeout=1.0)

    recovered = await queue.recover_inflight()

    assert recovered == 2
    assert await queue.processing_count() == 0
    assert await queue.length() == 2


async def test_flush_clears_all_lists(queue):
    await queue.push(_success_result())
    raw, _ = await queue.claim(timeout=1.0)
    await queue.dead_letter(raw, "x")
    await queue.push(_success_result())

    await queue.flush()

    assert await queue.length() == 0
    assert await queue.processing_count() == 0
    assert await queue.dead_letters() == []
