import asyncio

import pytest
import pytest_asyncio
import fakeredis.aioredis as fake_aioredis


def _patch_blocking_yields(client: fake_aioredis.FakeRedis) -> None:
    """
    Make BLMOVE actually block when the source is empty.

    fakeredis returns None immediately from BLMOVE on empty lists, which
    means a tight async loop calling it never yields and starves the
    event loop. The agent's run loop and the coordinator's result loop
    are both such loops, so we wrap BLMOVE to poll every 10ms until an
    item arrives or the timeout expires. Like real Redis, it returns as
    soon as something is pushed rather than sleeping the full timeout.

    BRPOP behaves correctly in fakeredis already, so we leave it alone.
    """
    original_blmove = client.blmove

    async def patched_blmove(*args, **kwargs):
        timeout = float(kwargs.get("timeout", 0) or 0)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            result = await original_blmove(*args, **kwargs)
            remaining = deadline - loop.time()
            if result is not None or remaining <= 0:
                return result
            await asyncio.sleep(min(0.01, remaining))

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
