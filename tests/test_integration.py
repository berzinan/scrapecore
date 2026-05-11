"""
End-to-end pipeline test against a live Redis. Excluded from the default run.

Run with:
    docker run --rm -p 6379:6379 redis:7-alpine &
    pytest -m integration
"""

import asyncio
import os

import pytest
from aioresponses import aioresponses

from scrapecore.agent.agent import Agent
from scrapecore.coordinator.coordinator import Coordinator, JobStoreAdapter
from scrapecore.models.task import TaskEnvelope
from scrapecore.queue.redis_client import create_redis_client


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_full_pipeline_single_stage():
    redis = create_redis_client()
    # Bail early if no Redis is reachable.
    try:
        await redis.ping()
    except Exception as e:
        pytest.skip(f"Redis not reachable: {e}")

    namespace = f"itest-{os.getpid()}"
    store = {
        "job-1": {
            "job_id": "job-1", "status": "pending", "site": "mysite",
            "results": None, "error": None,
        }
    }

    def task_factory(job):
        return [TaskEnvelope(
            job_id=job["job_id"], task_id="t-1",
            parser_key="site.parse",
            payload={"url": "https://example.com/api"},
        )]

    def parser(raw, metadata):
        return {"value": raw["v"]}

    coordinator = Coordinator(
        redis=redis,
        job_store_adapter=JobStoreAdapter(store),
        task_factory=task_factory,
        stage_handler=lambda r: [],
        namespace=namespace,
        dispatch_interval=0.1,
        recovery_interval=10.0,
        heartbeat_interval=1.0,
    )
    agent = Agent(
        agent_id="itest-agent",
        redis=redis,
        parser_registry={"site.parse": parser},
        namespace=namespace,
        num_workers=1,
        requests_per_second=100.0,
        claim_timeout=0.5,
        heartbeat_interval=1.0,
    )

    co_task = asyncio.create_task(coordinator.start())
    ag_task = asyncio.create_task(agent.start())
    try:
        with aioresponses() as mock:
            mock.get("https://example.com/api", payload={"v": 99})
            for _ in range(200):
                if store["job-1"]["status"] == "completed":
                    break
                await asyncio.sleep(0.05)
    finally:
        await coordinator.stop()
        await agent.stop()
        await asyncio.wait_for(co_task, timeout=10.0)
        await asyncio.wait_for(ag_task, timeout=10.0)
        # Clean up any leftover keys for this namespace.
        keys = await redis.keys(f"scrapecore:{namespace}:*")
        if keys:
            await redis.delete(*keys)
        await redis.aclose()

    assert store["job-1"]["status"] == "completed"
    assert store["job-1"]["results"] == [{"value": 99}]
