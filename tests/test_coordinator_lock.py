"""CoordinatorLock — at most one active coordinator per namespace."""

import asyncio

import pytest

from scrapecore.coordinator.coordinator import Coordinator, JobStoreAdapter
from scrapecore.coordinator.lock import CoordinatorLock, CoordinatorLockLost
from scrapecore.models.result import ResultEnvelope
from scrapecore.models.task import TaskEnvelope
from scrapecore.queue.result_queue import ResultQueue
from scrapecore.queue.task_queue import TaskQueue


def _lock_key(namespace):
    return f"scrapecore:{namespace}:coordinator:lock"


def _job(job_id):
    return {"job_id": job_id, "status": "pending", "error": None}


def _coordinator(fake_redis, namespace, store, factory_calls, lock_ttl=0.3):
    def task_factory(job):
        factory_calls.append(job["job_id"])
        return [TaskEnvelope(job_id=job["job_id"], parser_key="p", payload={"url": "u"})]

    return Coordinator(
        redis=fake_redis,
        job_store_adapter=JobStoreAdapter(store),
        task_factory=task_factory,
        stage_handler=lambda r: [],
        namespace=namespace,
        dispatch_interval=0.05,
        recovery_interval=0.1,
        heartbeat_interval=1.0,
        lock_ttl=lock_ttl,
    )


async def _wait_until(predicate, timeout=3.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while not predicate():
        if asyncio.get_event_loop().time() > deadline:
            pytest.fail("Condition not reached in time")
        await asyncio.sleep(0.02)


async def _stop(coordinator, runner):
    await coordinator.stop()
    await asyncio.wait_for(runner, timeout=5.0)


# ── CoordinatorLock unit tests ───────────────────────────────────────────────

async def test_acquire_is_exclusive(fake_redis, namespace):
    a = CoordinatorLock(fake_redis, namespace)
    b = CoordinatorLock(fake_redis, namespace)

    assert await a.acquire()
    assert not await b.acquire()
    assert await a.holder() == a.token


async def test_only_the_holder_can_renew(fake_redis, namespace):
    a = CoordinatorLock(fake_redis, namespace, ttl=10)
    b = CoordinatorLock(fake_redis, namespace, ttl=10)
    await a.acquire()
    await fake_redis.pexpire(_lock_key(namespace), 1000)  # let it run down

    assert not await b.renew()
    assert await a.renew()
    assert await fake_redis.pttl(_lock_key(namespace)) > 5000  # extended back to ~10s


async def test_only_the_holder_can_release(fake_redis, namespace):
    a = CoordinatorLock(fake_redis, namespace)
    b = CoordinatorLock(fake_redis, namespace)
    await a.acquire()

    assert not await b.release()
    assert await a.holder() == a.token
    assert await a.release()
    assert await a.holder() is None


async def test_expired_lock_can_be_taken_and_old_holder_cannot_renew(fake_redis, namespace):
    a = CoordinatorLock(fake_redis, namespace, ttl=0.2)
    b = CoordinatorLock(fake_redis, namespace, ttl=0.2)
    await a.acquire()
    await asyncio.sleep(0.3)

    assert await b.acquire()
    assert not await a.renew()   # a must notice it lost the lock
    assert not await a.release()  # and must not delete b's lock
    assert await b.holder() == b.token


# ── Coordinator behaviour ────────────────────────────────────────────────────

async def test_second_coordinator_waits_then_takes_over_on_clean_stop(fake_redis, namespace):
    store = {"job-1": _job("job-1")}
    calls: list[str] = []
    a = _coordinator(fake_redis, namespace, store, calls)
    b = _coordinator(fake_redis, namespace, store, calls)

    run_a = asyncio.create_task(a.start())
    await _wait_until(lambda: a.holds_lock)
    run_b = asyncio.create_task(b.start())
    try:
        await asyncio.sleep(0.5)  # several of B's acquire attempts
        assert not b.holds_lock
        assert calls == ["job-1"]  # dispatched once, by A

        await _stop(a, run_a)
        await _wait_until(lambda: b.holds_lock)

        # B is now the active coordinator and dispatches new work.
        store["job-2"] = _job("job-2")
        await _wait_until(lambda: "job-2" in calls)
    finally:
        await _stop(b, run_b)

    assert calls == ["job-1", "job-2"]


async def test_standby_takes_over_after_holder_dies_without_releasing(fake_redis, namespace):
    # A coordinator that crashed: its lock is still there, counting down.
    await fake_redis.set(_lock_key(namespace), "crashed-holder", px=300)
    store = {"job-1": _job("job-1")}
    calls: list[str] = []
    coordinator = _coordinator(fake_redis, namespace, store, calls)

    runner = asyncio.create_task(coordinator.start())
    try:
        await asyncio.sleep(0.1)
        assert not coordinator.holds_lock and calls == []
        await _wait_until(lambda: coordinator.holds_lock, timeout=2.0)
        await _wait_until(lambda: calls == ["job-1"])
    finally:
        await _stop(coordinator, runner)


async def test_standby_does_not_touch_inflight_results(fake_redis, namespace):
    """
    The original bug: a second coordinator's startup recovery would move the
    active coordinator's in-flight results back to the queue. A standby must
    not run recovery, or anything else, until it holds the lock.
    """
    await fake_redis.set(_lock_key(namespace), "active-coordinator", px=60_000)
    rq = ResultQueue(fake_redis, namespace=namespace)
    await rq.push(ResultEnvelope.success(task_id="t", job_id="j", agent_id="a",
                                         retry_count=0, max_retries=3, output={}))
    await rq.claim(timeout=1.0)  # the active coordinator is handling it

    coordinator = _coordinator(fake_redis, namespace, {}, [])
    runner = asyncio.create_task(coordinator.start())
    await asyncio.sleep(0.4)
    await asyncio.wait_for(_stop(coordinator, runner), timeout=2.0)  # stop returns promptly

    assert await rq.processing_count() == 1
    assert await rq.length() == 0
    assert await fake_redis.get(_lock_key(namespace)) == "active-coordinator"


async def test_clean_stop_releases_lock(fake_redis, namespace):
    coordinator = _coordinator(fake_redis, namespace, {}, [])
    runner = asyncio.create_task(coordinator.start())
    await _wait_until(lambda: coordinator.holds_lock)
    await _stop(coordinator, runner)

    assert not coordinator.holds_lock
    assert await fake_redis.get(_lock_key(namespace)) is None


async def test_losing_the_lock_stops_everything_and_raises(fake_redis, namespace):
    store = {"job-1": _job("job-1")}
    calls: list[str] = []
    coordinator = _coordinator(fake_redis, namespace, store, calls)

    runner = asyncio.create_task(coordinator.start())
    await _wait_until(lambda: coordinator.holds_lock)
    await _wait_until(lambda: calls == ["job-1"])

    # Another instance took over (e.g. this one was frozen past the TTL).
    await fake_redis.set(_lock_key(namespace), "intruder", px=60_000)

    # Up to ~2s: the result loop finishes its in-flight blocking claim first.
    with pytest.raises(CoordinatorLockLost):
        await asyncio.wait_for(runner, timeout=4.0)

    assert not coordinator.holds_lock
    # It must not delete the new holder's lock on the way out.
    assert await fake_redis.get(_lock_key(namespace)) == "intruder"

    # And it dispatches nothing after losing the lock.
    store["job-2"] = _job("job-2")
    await asyncio.sleep(0.2)
    assert calls == ["job-1"]


async def test_redis_unreachable_for_full_ttl_stops_coordinator(fake_redis, namespace):
    coordinator = _coordinator(fake_redis, namespace, {}, [])

    runner = asyncio.create_task(coordinator.start())
    await _wait_until(lambda: coordinator.holds_lock)

    async def unreachable():
        raise ConnectionError("redis down")

    coordinator._lock.renew = unreachable

    # The lease has expired by the time renewal has failed for a full TTL,
    # so a standby may own it: stopping is the only safe option.
    with pytest.raises(CoordinatorLockLost):
        await asyncio.wait_for(runner, timeout=3.0)


async def test_brief_renewal_failure_is_tolerated(fake_redis, namespace):
    coordinator = _coordinator(fake_redis, namespace, {}, [], lock_ttl=0.6)

    runner = asyncio.create_task(coordinator.start())
    await _wait_until(lambda: coordinator.holds_lock)

    real_renew = coordinator._lock.renew
    failures = {"left": 1}

    async def flaky():
        if failures["left"]:
            failures["left"] -= 1
            raise ConnectionError("blip")
        return await real_renew()

    coordinator._lock.renew = flaky
    try:
        await asyncio.sleep(1.0)  # several renewal cycles, one of them failing
        assert failures["left"] == 0
        assert coordinator.holds_lock
        assert not runner.done()
    finally:
        await _stop(coordinator, runner)


async def test_result_claimed_while_stopping_is_handed_back(fake_redis, namespace):
    """
    A blocking claim can return after the lock is lost. That result must go
    back to the queue for the new active coordinator, not be processed here.
    """
    handled: list[str] = []

    async def on_result(result):
        handled.append(result.task_id)

    coordinator = _coordinator(fake_redis, namespace, {}, [])
    coordinator._on_result = on_result
    rq = ResultQueue(fake_redis, namespace=namespace)
    real_claim = coordinator._result_queue.claim

    async def claim_returning_after_lock_loss(timeout=2.0):
        claimed = await real_claim(timeout=timeout)
        if claimed is not None:
            coordinator._stop_event.set()  # lock lost while claim was blocked
        return claimed

    coordinator._result_queue.claim = claim_returning_after_lock_loss

    runner = asyncio.create_task(coordinator.start())
    await _wait_until(lambda: coordinator.holds_lock)
    await rq.push(ResultEnvelope.success(task_id="t-1", job_id="j", agent_id="a",
                                         retry_count=0, max_retries=3, output={}))
    await asyncio.wait_for(runner, timeout=4.0)

    assert handled == []
    assert await rq.length() == 1           # back in pending
    assert await rq.processing_count() == 0
