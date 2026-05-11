"""Layer 2 — TaskQueue against fakeredis."""

import asyncio
import time

import pytest

from scrapecore.queue.task_queue import TaskQueue


@pytest.fixture
def queue(fake_redis, namespace):
    return TaskQueue(fake_redis, namespace=namespace)


async def test_push_and_claim_round_trips_payload(queue):
    await queue.push("task-1", {"url": "https://example.com"})
    claimed = await queue.claim(timeout=1.0)
    assert claimed is not None
    task_id, payload = claimed
    assert task_id == "task-1"
    assert payload == {"url": "https://example.com"}


async def test_claim_returns_none_on_timeout(queue):
    claimed = await queue.claim(timeout=0.1)
    assert claimed is None


async def test_high_priority_claimed_before_low_priority(queue):
    await queue.push("low-1", {"order": 1})
    await queue.push("low-2", {"order": 2})
    await queue.push("urgent", {"order": "first"}, priority=10)

    first = await queue.claim(timeout=1.0)
    assert first is not None
    assert first[0] == "urgent"


async def test_acknowledge_removes_task_from_processing(queue):
    await queue.push("task-1", {})
    await queue.claim(timeout=1.0)
    assert await queue.processing_count() == 1

    await queue.acknowledge("task-1")
    assert await queue.processing_count() == 0
    assert await queue.pending_count() == 0


async def test_reject_moves_task_back_to_pending(queue):
    await queue.push("task-1", {"v": 1})
    await queue.claim(timeout=1.0)
    assert await queue.processing_count() == 1

    await queue.reject("task-1", {"v": 2})
    assert await queue.processing_count() == 0
    assert await queue.pending_count() == 1

    again = await queue.claim(timeout=1.0)
    assert again == ("task-1", {"v": 2})


async def test_recover_stale_reenqueues_old_processing_entries(fake_redis, queue, namespace):
    await queue.push("task-1", {"url": "https://example.com"})
    await queue.claim(timeout=1.0)

    # Backdate claimed_at to far in the past
    await fake_redis.hset(
        f"scrapecore:{namespace}:task:task-1",
        mapping={"claimed_at": str(time.time() - 999)},
    )

    recovered = await queue.recover_stale(stale_after_seconds=60.0)
    assert recovered == 1
    assert await queue.pending_count() == 1
    assert await queue.processing_count() == 0


async def test_recover_stale_leaves_fresh_tasks_alone(queue):
    await queue.push("task-1", {})
    await queue.claim(timeout=1.0)

    recovered = await queue.recover_stale(stale_after_seconds=60.0)
    assert recovered == 0
    assert await queue.pending_count() == 0
    assert await queue.processing_count() == 1


async def test_concurrent_claims_receive_distinct_tasks(queue):
    await queue.push("task-1", {"id": 1})
    await queue.push("task-2", {"id": 2})

    a, b = await asyncio.gather(
        queue.claim(timeout=1.0),
        queue.claim(timeout=1.0),
    )
    assert a is not None and b is not None
    assert {a[0], b[0]} == {"task-1", "task-2"}


async def test_claim_with_missing_payload_skips_silently(fake_redis, queue, namespace):
    # Insert a task_id into pending without storing the payload.
    await fake_redis.lpush(f"scrapecore:{namespace}:pending", "ghost")
    result = await queue.claim(timeout=1.0)
    assert result is None
    assert await queue.processing_count() == 0


async def test_flush_clears_queues(fake_redis, queue, namespace):
    await queue.push("t1", {})
    await queue.push("t2", {})
    await queue.flush()
    assert await queue.pending_count() == 0
