"""Layer 2 — JobTracker against fakeredis."""

import pytest

from scrapecore.queue.job_tracker import JobTracker


@pytest.fixture
def tracker(fake_redis, namespace):
    return JobTracker(fake_redis, namespace=namespace)


async def test_register_tracks_job_and_tasks(tracker):
    await tracker.register("j", ["a", "b"])

    assert await tracker.is_tracked("j")
    assert await tracker.is_outstanding("j", "a")
    assert await tracker.remaining("j") == 2


async def test_untracked_job(tracker):
    assert not await tracker.is_tracked("nope")
    assert not await tracker.is_outstanding("nope", "a")
    assert await tracker.remaining("nope") == 0


async def test_complete_task_counts_down_and_records_outcome(tracker):
    await tracker.register("j", ["a", "b"])

    assert await tracker.complete_task("j", "a", "succeeded") == 1
    assert not await tracker.is_outstanding("j", "a")
    assert await tracker.complete_task("j", "b", "exhausted") == 0
    assert await tracker.stats("j") == {"succeeded": 1, "exhausted": 1}


async def test_job_stays_tracked_when_nothing_outstanding(tracker):
    # Redis deletes an empty set; tracking must not depend on that set existing.
    await tracker.register("j", ["a"])
    await tracker.complete_task("j", "a", "succeeded")

    assert await tracker.is_tracked("j")
    assert await tracker.remaining("j") == 0


async def test_complete_task_on_untracked_job_returns_none_and_leaves_no_keys(
    fake_redis, tracker, namespace
):
    assert await tracker.complete_task("ghost", "a", "succeeded") is None
    assert await fake_redis.keys(f"scrapecore:{namespace}:job:*") == []


async def test_replace_task_swaps_parent_for_followups(tracker):
    await tracker.register("j", ["stage"])

    assert await tracker.replace_task("j", "stage", ["x", "y"]) is True
    assert not await tracker.is_outstanding("j", "stage")
    assert await tracker.remaining("j") == 2


async def test_replace_task_on_untracked_job_leaves_no_keys(fake_redis, tracker, namespace):
    assert await tracker.replace_task("ghost", "stage", ["x"]) is False
    assert await fake_redis.keys(f"scrapecore:{namespace}:job:*") == []


async def test_register_is_idempotent(tracker):
    await tracker.register("j", ["a", "b"])
    await tracker.register("j", ["a", "b"])
    assert await tracker.remaining("j") == 2


async def test_cleanup_removes_everything(fake_redis, tracker, namespace):
    await tracker.register("j", ["a", "b"])
    await tracker.complete_task("j", "a", "succeeded")

    await tracker.cleanup("j")

    assert not await tracker.is_tracked("j")
    assert await fake_redis.keys(f"scrapecore:{namespace}:job:*") == []
    assert await tracker.stats("j") == {"succeeded": 0, "exhausted": 0}
