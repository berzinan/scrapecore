import asyncio

import pytest
import pytest_asyncio
import fakeredis.aioredis as fake_aioredis


def _patch_blocking_yields(client: fake_aioredis.FakeRedis) -> None:
    """
    Make BLMOVE actually wait when the source is empty.

    fakeredis returns None immediately from BLMOVE on empty lists, which
    means a tight async loop calling it never yields and starves the
    event loop. The agent's worker_loop is exactly such a loop, so we
    wrap BLMOVE to sleep for the requested timeout when the source list
    is empty. This is the same behaviour real Redis exhibits.

    BRPOP behaves correctly in fakeredis already, so we leave it alone.
    """
    original_blmove = client.blmove

    async def patched_blmove(*args, **kwargs):
        timeout = kwargs.get("timeout", 0)
        result = await original_blmove(*args, **kwargs)
        if result is None and timeout:
            await asyncio.sleep(float(timeout))
        return result

    client.blmove = patched_blmove


@pytest_asyncio.fixture
async def fake_redis():
    client = fake_aioredis.FakeRedis(decode_responses=True)
    _patch_blocking_yields(client)
    try:
        yield client
    finally:
        try:
            await asyncio.wait_for(client.aclose(), timeout=1.0)
        except asyncio.TimeoutError:
            pass


@pytest.fixture
def namespace() -> str:
    return "test"
