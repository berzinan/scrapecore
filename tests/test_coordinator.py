"""Layer 4 — Coordinator loops with fake Redis and dict-based job store."""

import asyncio

import pytest

from scrapecore.coordinator.coordinator import (
    Coordinator,
    JobStoreAdapter,
    _JobTracker,
)
from scrapecore.models.task import TaskEnvelope
from scrapecore.models.result import ResultEnvelope
from scrapecore.queue.task_queue import TaskQueue
from scrapecore.queue.result_queue import ResultQueue


def _pending_job(job_id: str = "job-1", site: str = "mysite") -> dict:
    return {
        "job_id": job_id,
        "status": "pending",
        "site": site,
        "results": None,
        "error": None,
    }


def _make_coordinator(
    fake_redis,
    namespace,
    store: dict,
    task_factory,
    stage_handler=lambda r: [],
    **kwargs,
) -> Coordinator:
    return Coordinator(
        redis=fake_redis,
        job_store_adapter=JobStoreAdapter(store),
        task_factory=task_factory,
        stage_handler=stage_handler,
        namespace=namespace,
        dispatch_interval=kwargs.pop("dispatch_interval", 0.05),
        recovery_interval=kwargs.pop("recovery_interval", 0.1),
        stale_task_timeout=kwargs.pop("stale_task_timeout", 60.0),
        heartbeat_interval=kwargs.pop("heartbeat_interval", 1.0),
    )


async def _run_until(coordinator: Coordinator, predicate, timeout: float = 3.0) -> None:
    runner = asyncio.create_task(coordinator.start())
    try:
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if predicate():
                break
            await asyncio.sleep(0.02)
        else:
            pytest.fail("Coordinator did not reach the expected state in time")
    finally:
        await coordinator.stop()
        await asyncio.wait_for(runner, timeout=5.0)


# ── _JobTracker (no async) ───────────────────────────────────────────────────

def test_tracker_register_and_decrement():
    t = _JobTracker()
    t.register("j", 3)
    assert t.decrement("j") == 2
    assert t.decrement("j") == 1
    assert t.decrement("j") == 0


def test_tracker_record_and_get_results():
    t = _JobTracker()
    t.register("j", 2)
    t.record_result("j", {"a": 1})
    t.record_result("j", {"b": 2})
    assert t.get_results("j") == [{"a": 1}, {"b": 2}]


def test_tracker_decrement_floors_at_zero():
    t = _JobTracker()
    t.register("j", 1)
    t.decrement("j")
    assert t.decrement("j") == 0


def test_tracker_cleanup_removes_state():
    t = _JobTracker()
    t.register("j", 1)
    t.record_result("j", {"a": 1})
    t.cleanup("j")
    assert t.get_results("j") == []


# ── dispatch_loop ────────────────────────────────────────────────────────────

async def test_dispatch_loop_enqueues_tasks_and_marks_running(fake_redis, namespace):
    store = {"job-1": _pending_job()}
    tasks_built = []

    def task_factory(job):
        env = TaskEnvelope(job_id=job["job_id"], parser_key="p", payload={})
        tasks_built.append(env)
        return [env]

    coordinator = _make_coordinator(fake_redis, namespace, store, task_factory)
    task_queue = TaskQueue(fake_redis, namespace=namespace)

    await _run_until(
        coordinator,
        lambda: store["job-1"]["status"] == "running",
    )

    assert len(tasks_built) == 1
    assert await task_queue.pending_count() == 1


async def test_dispatch_loop_does_not_redispatch(fake_redis, namespace):
    store = {"job-1": _pending_job()}
    calls = []

    def task_factory(job):
        calls.append(job["job_id"])
        return [TaskEnvelope(job_id=job["job_id"], parser_key="p", payload={})]

    coordinator = _make_coordinator(fake_redis, namespace, store, task_factory)

    await _run_until(
        coordinator,
        lambda: store["job-1"]["status"] == "running",
    )
    # Give the loop time to poll again.
    await asyncio.sleep(0.15)
    assert calls.count("job-1") == 1


async def test_dispatch_loop_skips_cancelled_jobs(fake_redis, namespace):
    store = {"job-1": _pending_job()}
    store["job-1"]["status"] = "cancelled"
    calls = []

    def task_factory(job):
        calls.append(job["job_id"])
        return [TaskEnvelope(job_id=job["job_id"], parser_key="p", payload={})]

    coordinator = _make_coordinator(fake_redis, namespace, store, task_factory)

    runner = asyncio.create_task(coordinator.start())
    await asyncio.sleep(0.2)
    await coordinator.stop()
    await asyncio.wait_for(runner, timeout=5.0)

    assert calls == []


async def test_dispatch_loop_marks_failed_when_factory_returns_empty(fake_redis, namespace):
    store = {"job-1": _pending_job()}

    def task_factory(job):
        return []

    coordinator = _make_coordinator(fake_redis, namespace, store, task_factory)
    await _run_until(coordinator, lambda: store["job-1"]["status"] == "failed")
    assert "No tasks" in store["job-1"]["error"]


async def test_dispatch_loop_marks_failed_when_factory_raises(fake_redis, namespace):
    store = {"job-1": _pending_job()}

    def task_factory(job):
        raise RuntimeError("kaboom")

    coordinator = _make_coordinator(fake_redis, namespace, store, task_factory)
    await _run_until(coordinator, lambda: store["job-1"]["status"] == "failed")
    assert "kaboom" in store["job-1"]["error"]


# ── result_loop ──────────────────────────────────────────────────────────────

async def test_result_loop_completes_single_task_job(fake_redis, namespace):
    store = {"job-1": _pending_job()}

    def task_factory(job):
        return [TaskEnvelope(job_id=job["job_id"], task_id="t-1",
                             parser_key="p", payload={})]

    coordinator = _make_coordinator(fake_redis, namespace, store, task_factory)
    task_queue = TaskQueue(fake_redis, namespace=namespace)
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    async def driver():
        # Wait for dispatch to happen, then simulate an agent's result.
        while await task_queue.pending_count() == 0:
            await asyncio.sleep(0.01)
        await result_queue.push(ResultEnvelope.success(
            task_id="t-1", job_id="job-1", agent_id="agent",
            retry_count=0, max_retries=3,
            output={"value": 42},
        ))

    runner = asyncio.create_task(coordinator.start())
    drive = asyncio.create_task(driver())
    try:
        for _ in range(200):
            if store["job-1"]["status"] == "completed":
                break
            await asyncio.sleep(0.02)
    finally:
        await coordinator.stop()
        await asyncio.wait_for(runner, timeout=5.0)
        await drive

    assert store["job-1"]["status"] == "completed"
    assert store["job-1"]["results"] == [{"value": 42}]


async def test_result_loop_finalises_only_after_all_results_arrive(fake_redis, namespace):
    store = {"job-1": _pending_job()}

    task_ids = ["t-1", "t-2", "t-3"]

    def task_factory(job):
        return [
            TaskEnvelope(job_id=job["job_id"], task_id=tid,
                         parser_key="p", payload={})
            for tid in task_ids
        ]

    coordinator = _make_coordinator(fake_redis, namespace, store, task_factory)
    task_queue = TaskQueue(fake_redis, namespace=namespace)
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    runner = asyncio.create_task(coordinator.start())
    try:
        # Wait for tasks to be dispatched.
        for _ in range(200):
            if await task_queue.pending_count() == len(task_ids):
                break
            await asyncio.sleep(0.02)

        # Push results one at a time.
        for i, tid in enumerate(task_ids):
            await result_queue.push(ResultEnvelope.success(
                task_id=tid, job_id="job-1", agent_id="agent",
                retry_count=0, max_retries=3,
                output={"i": i},
            ))
            if i < len(task_ids) - 1:
                # Job must not be completed yet.
                await asyncio.sleep(0.1)
                assert store["job-1"]["status"] != "completed"

        for _ in range(200):
            if store["job-1"]["status"] == "completed":
                break
            await asyncio.sleep(0.02)
    finally:
        await coordinator.stop()
        await asyncio.wait_for(runner, timeout=5.0)

    assert store["job-1"]["status"] == "completed"
    assert sorted(r["i"] for r in store["job-1"]["results"]) == [0, 1, 2]


async def test_result_loop_marks_failed_when_all_tasks_exhausted(fake_redis, namespace):
    store = {"job-1": _pending_job()}

    def task_factory(job):
        return [TaskEnvelope(job_id=job["job_id"], task_id="t-1",
                             parser_key="p", payload={})]

    coordinator = _make_coordinator(fake_redis, namespace, store, task_factory)
    task_queue = TaskQueue(fake_redis, namespace=namespace)
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    runner = asyncio.create_task(coordinator.start())
    try:
        while await task_queue.pending_count() == 0:
            await asyncio.sleep(0.01)
        await result_queue.push(ResultEnvelope.failure(
            task_id="t-1", job_id="job-1", agent_id="agent",
            retry_count=3, max_retries=3, error="boom",
        ))
        for _ in range(200):
            if store["job-1"]["status"] == "failed":
                break
            await asyncio.sleep(0.02)
    finally:
        await coordinator.stop()
        await asyncio.wait_for(runner, timeout=5.0)

    assert store["job-1"]["status"] == "failed"
    assert store["job-1"]["error"]  # some error message was recorded


async def test_stage_output_triggers_stage_handler_and_enqueues_followups(fake_redis, namespace):
    store = {"job-1": _pending_job()}

    def task_factory(job):
        return [TaskEnvelope(
            job_id=job["job_id"], task_id="t-search",
            parser_key="search", payload={},
        )]

    def stage_handler(result):
        return [TaskEnvelope(
            job_id=result.job_id,
            task_id="t-followup-1",
            parser_key="appraise",
            payload={"catalog": result.stage_output["catalogs"][0]},
        )]

    coordinator = _make_coordinator(
        fake_redis, namespace, store, task_factory, stage_handler=stage_handler
    )
    task_queue = TaskQueue(fake_redis, namespace=namespace)
    result_queue = ResultQueue(fake_redis, namespace=namespace)

    runner = asyncio.create_task(coordinator.start())
    try:
        # Wait for the stage-1 task to be dispatched, then claim it (simulating an agent).
        while await task_queue.pending_count() == 0:
            await asyncio.sleep(0.01)
        await task_queue.claim(timeout=1.0)

        # Push the stage-1 result with stage_output.
        await result_queue.push(ResultEnvelope.success(
            task_id="t-search", job_id="job-1", agent_id="agent",
            retry_count=0, max_retries=3,
            output={},
            stage_output={"catalogs": ["cat-1"]},
        ))

        # Wait for the stage-2 task to appear in pending.
        for _ in range(200):
            if await task_queue.pending_count() == 1:
                break
            await asyncio.sleep(0.02)

        # Now push the stage-2 result with final output.
        await result_queue.push(ResultEnvelope.success(
            task_id="t-followup-1", job_id="job-1", agent_id="agent",
            retry_count=0, max_retries=3,
            output={"final": "data"},
        ))

        for _ in range(200):
            if store["job-1"]["status"] == "completed":
                break
            await asyncio.sleep(0.02)
    finally:
        await coordinator.stop()
        await asyncio.wait_for(runner, timeout=5.0)

    assert store["job-1"]["status"] == "completed"
    assert store["job-1"]["results"] == [{"final": "data"}]


# ── recovery_loop ────────────────────────────────────────────────────────────

async def test_recovery_loop_delegates_to_task_queue(fake_redis, namespace):
    import time as _time

    store = {}  # no jobs — coordinator only needs to recover stale entries
    coordinator = _make_coordinator(
        fake_redis, namespace, store,
        task_factory=lambda j: [],
        recovery_interval=0.1,
        stale_task_timeout=0.5,
    )

    task_queue = TaskQueue(fake_redis, namespace=namespace)
    await task_queue.push("orphan", {"url": "https://example.com"})
    await task_queue.claim(timeout=1.0)
    # Backdate claimed_at so the recovery sweep picks it up.
    await fake_redis.hset(
        f"scrapecore:{namespace}:task:orphan",
        mapping={"claimed_at": str(_time.time() - 999)},
    )

    runner = asyncio.create_task(coordinator.start())
    try:
        for _ in range(200):
            if await task_queue.pending_count() == 1:
                break
            await asyncio.sleep(0.02)
    finally:
        await coordinator.stop()
        await asyncio.wait_for(runner, timeout=5.0)

    assert await task_queue.pending_count() == 1
    assert await task_queue.processing_count() == 0
