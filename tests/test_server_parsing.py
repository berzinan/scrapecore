"""Server-side parsing — the coordinator parses fetched results from agents."""

import asyncio
import datetime
import json
import time

import pytest

from scrapecore.coordinator.coordinator import Coordinator, JobStoreAdapter
from scrapecore.models.result import ResultEnvelope
from scrapecore.models.task import TaskEnvelope
from scrapecore.plugins import (
    BaseParser,
    ParserRegistry,
    RetryableParseError,
    with_stage_output,
)
from scrapecore.queue.result_queue import ResultQueue
from scrapecore.queue.task_queue import TaskQueue


# ── Helpers ───────────────────────────────────────────────────────────────────

def _job(job_id="job-1", **extra):
    return {"job_id": job_id, "status": "pending", "error": None, **extra}


def _json_response(data, url="https://example.com/x"):
    return {"status": 200, "content_type": "application/json", "headers": {},
            "text": json.dumps(data), "url": url}


def _html_response(html, url="https://example.com/x"):
    return {"status": 200, "content_type": "text/html; charset=utf-8", "headers": {},
            "text": html, "url": url}


def _fetched(envelope: TaskEnvelope, response) -> ResultEnvelope:
    """What a fetch-only agent pushes after executing `envelope`."""
    return ResultEnvelope.fetched(envelope.to_dict(), "agent-1", response)


def _coordinator(fake_redis, namespace, store, task_factory, parsers,
                 stage_handler=lambda r: [], on_result=None):
    return Coordinator(
        redis=fake_redis,
        job_store_adapter=JobStoreAdapter(store),
        task_factory=task_factory,
        stage_handler=stage_handler,
        namespace=namespace,
        parsers=parsers,
        on_result=on_result,
        dispatch_interval=0.05,
        recovery_interval=0.1,
        heartbeat_interval=1.0,
        hook_retry_base_delay=0.01,
        result_retry_delay=0.01,
    )


async def _wait_until(predicate, timeout=3.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        value = predicate()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return
        if asyncio.get_event_loop().time() > deadline:
            pytest.fail("Condition not reached in time")
        await asyncio.sleep(0.02)


class _Running:
    """async with _Running(coordinator): ... starts and cleanly stops it."""

    def __init__(self, coordinator):
        self.coordinator = coordinator

    async def __aenter__(self):
        self.runner = asyncio.create_task(self.coordinator.start())
        return self.coordinator

    async def __aexit__(self, *exc):
        await self.coordinator.stop()
        await asyncio.wait_for(self.runner, timeout=5.0)


# ── Basic parsing ─────────────────────────────────────────────────────────────

async def test_fetched_result_is_parsed_before_hook(fake_redis, namespace):
    store = {"job-1": _job()}
    env = TaskEnvelope(job_id="job-1", task_id="t-1", parser_key="site.item",
                       payload={"url": "https://example.com/x", "metadata": {"code": "A1"}})
    seen: list[ResultEnvelope] = []

    async def on_result(result):
        seen.append(result)

    def parse_item(raw, metadata):
        return {"code": metadata["code"], "price": raw["price"]}

    coordinator = _coordinator(fake_redis, namespace, store, lambda job: [env],
                               {"site.item": parse_item}, on_result=on_result)
    rq = ResultQueue(fake_redis, namespace=namespace)

    async with _Running(coordinator):
        await _wait_until(lambda: TaskQueue(fake_redis, namespace=namespace).pending_count())
        await rq.push(_fetched(env, _json_response({"price": 9.5})))
        await _wait_until(lambda: store["job-1"]["status"] == "completed")

    assert len(seen) == 1
    assert seen[0].status == "completed"
    assert seen[0].output == {"code": "A1", "price": 9.5}
    # The raw page is still available to the hook, e.g. for archiving.
    assert seen[0].response["text"] == '{"price": 9.5}'
    assert store["job-1"]["summary"] == {"succeeded": 1, "exhausted": 0}


async def test_html_reaches_parser_as_string(fake_redis, namespace):
    store = {"job-1": _job()}
    env = TaskEnvelope(job_id="job-1", parser_key="site.page", payload={"url": "u"})
    received = []

    def parse_page(raw, metadata):
        received.append(raw)
        return {"len": len(raw)}

    coordinator = _coordinator(fake_redis, namespace, store, lambda job: [env],
                               {"site.page": parse_page})
    async with _Running(coordinator):
        await _wait_until(lambda: TaskQueue(fake_redis, namespace=namespace).pending_count())
        await ResultQueue(fake_redis, namespace=namespace).push(
            _fetched(env, _html_response("<p>hi</p>")))
        await _wait_until(lambda: store["job-1"]["status"] == "completed")

    assert received == ["<p>hi</p>"]


async def test_async_parser_is_awaited(fake_redis, namespace):
    store = {"job-1": _job()}
    env = TaskEnvelope(job_id="job-1", parser_key="site.async", payload={"url": "u"})
    seen = []

    async def parse_async(raw, metadata):
        await asyncio.sleep(0)
        return {"async": True}

    async def on_result(result):
        seen.append(result.output)

    coordinator = _coordinator(fake_redis, namespace, store, lambda job: [env],
                               {"site.async": parse_async}, on_result=on_result)
    async with _Running(coordinator):
        await _wait_until(lambda: TaskQueue(fake_redis, namespace=namespace).pending_count())
        await ResultQueue(fake_redis, namespace=namespace).push(_fetched(env, _json_response({})))
        await _wait_until(lambda: store["job-1"]["status"] == "completed")

    assert seen == [{"async": True}]


async def test_slow_sync_parser_does_not_block_event_loop(fake_redis, namespace):
    store = {"job-1": _job()}
    env = TaskEnvelope(job_id="job-1", parser_key="site.slow", payload={"url": "u"})

    def parse_slow(raw, metadata):
        time.sleep(0.4)  # CPU-bound / blocking parse
        return {"ok": True}

    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.02)

    coordinator = _coordinator(fake_redis, namespace, store, lambda job: [env],
                               {"site.slow": parse_slow})
    async with _Running(coordinator):
        await _wait_until(lambda: TaskQueue(fake_redis, namespace=namespace).pending_count())
        tick_task = asyncio.create_task(ticker())
        await ResultQueue(fake_redis, namespace=namespace).push(_fetched(env, _json_response({})))
        await _wait_until(lambda: store["job-1"]["status"] == "completed")
        tick_task.cancel()

    # 0.4s of blocking parse; if it ran on the event loop, ticks would stall.
    assert ticks >= 10


# ── Multi-stage, end to end through ParserRegistry ───────────────────────────

async def test_stage_transition_through_registry(fake_redis, namespace):
    class SearchParser(BaseParser):
        key = "site.search"

        def parse(self, raw, metadata):
            return with_stage_output({}, next_parser_key="site.detail",
                                     catalogs=raw["catalogs"])

        def build_tasks(self, job):
            return [TaskEnvelope(job_id=job["job_id"], task_id="t-search",
                                 parser_key=self.key, payload={"url": "https://x/search"})]

    class DetailParser(BaseParser):
        key = "site.detail"

        def parse(self, raw, metadata):
            return {"catalog": metadata["catalog"], "items": raw["items"]}

        def build_stage_tasks(self, result):
            return [
                TaskEnvelope(job_id=result.job_id, task_id=f"t-{c}", parser_key=self.key,
                             payload={"url": f"https://x/{c}", "metadata": {"catalog": c}})
                for c in result.stage_output["catalogs"]
            ]

    registry = ParserRegistry()
    registry.register(SearchParser(), entry_point_for="site")
    registry.register(DetailParser())

    store = {"job-1": _job(site="site")}
    outputs = []

    async def on_result(result):
        if result.output:
            outputs.append(result.output)

    coordinator = _coordinator(
        fake_redis, namespace, store, registry.task_factory,
        registry.as_callable_dict(), stage_handler=registry.stage_handler,
        on_result=on_result,
    )
    tq = TaskQueue(fake_redis, namespace=namespace)
    rq = ResultQueue(fake_redis, namespace=namespace)

    async with _Running(coordinator):
        await _wait_until(lambda: tq.pending_count())
        _, search_task = await tq.claim(timeout=1.0)
        await rq.push(ResultEnvelope.fetched(search_task, "agent-1",
                                             _json_response({"catalogs": ["a", "b"]})))

        # Both detail tasks appear, built by DetailParser.build_stage_tasks.
        await _wait_until(lambda: _pending_is(tq, 2))
        details = [(await tq.claim(timeout=1.0))[1] for _ in range(2)]
        assert sorted(d["parser_key"] for d in details) == ["site.detail", "site.detail"]

        for d in details:
            await rq.push(ResultEnvelope.fetched(d, "agent-1", _json_response({"items": [1]})))
        await _wait_until(lambda: store["job-1"]["status"] == "completed")

    assert sorted(o["catalog"] for o in outputs) == ["a", "b"]
    assert store["job-1"]["summary"] == {"succeeded": 2, "exhausted": 0}


async def _pending_is(tq, n):
    return await tq.pending_count() == n


# ── Parse failures ───────────────────────────────────────────────────────────

async def test_retryable_parse_error_requeues_fetch(fake_redis, namespace):
    store = {"job-1": _job()}
    env = TaskEnvelope(job_id="job-1", task_id="t-1", parser_key="site.p",
                       payload={"url": "u"}, max_retries=3)
    statuses = []

    def parse(raw, metadata):
        if raw.get("captcha"):
            raise RetryableParseError("captcha page")
        return {"ok": True}

    async def on_result(result):
        statuses.append(result.status)

    coordinator = _coordinator(fake_redis, namespace, store, lambda job: [env],
                               {"site.p": parse}, on_result=on_result)
    tq = TaskQueue(fake_redis, namespace=namespace)
    rq = ResultQueue(fake_redis, namespace=namespace)

    async with _Running(coordinator):
        await _wait_until(lambda: tq.pending_count())
        await tq.claim(timeout=1.0)  # agent takes the first attempt (and acks it)
        await tq.acknowledge("t-1")
        await rq.push(_fetched(env, _json_response({"captcha": True})))

        # The task is back in the queue, one retry used.
        await _wait_until(lambda: _pending_is(tq, 1))
        _, retried = await tq.claim(timeout=1.0)
        assert retried["retry_count"] == 1
        assert store["job-1"]["status"] == "running"

        await rq.push(ResultEnvelope.fetched(retried, "agent-2", _json_response({})))
        await _wait_until(lambda: store["job-1"]["status"] == "completed")

    assert statuses == ["failed", "completed"]


async def test_retryable_parse_error_exhausts_after_max_retries(fake_redis, namespace):
    store = {"job-1": _job()}
    env = TaskEnvelope(job_id="job-1", task_id="t-1", parser_key="site.p",
                       payload={"url": "u"}, max_retries=0)

    def parse(raw, metadata):
        raise RetryableParseError("captcha page")

    coordinator = _coordinator(fake_redis, namespace, store, lambda job: [env], {"site.p": parse})
    tq = TaskQueue(fake_redis, namespace=namespace)
    rq = ResultQueue(fake_redis, namespace=namespace)

    async with _Running(coordinator):
        await _wait_until(lambda: tq.pending_count())
        await tq.claim(timeout=1.0)
        await tq.acknowledge("t-1")
        await rq.push(_fetched(env, _json_response({})))
        await _wait_until(lambda: store["job-1"]["status"] == "failed")

    assert await tq.pending_count() == 0  # no further re-fetch
    dead = await rq.dead_letters()
    assert len(dead) == 1 and "RetryableParseError" in dead[0]["reason"]


@pytest.mark.parametrize(
    "parser, expected",
    [
        (lambda raw, md: (_ for _ in ()).throw(ValueError("selector not found")),
         "ValueError: selector not found"),
        (lambda raw, md: ["not", "a", "dict"], "must return a dict"),
        (lambda raw, md: {"when": datetime.datetime(2026, 1, 1)}, "JSON-serializable"),
        (lambda raw, md: {"__stage_output__": "nope"}, "must be a dict"),
    ],
    ids=["parser-raises", "non-dict", "non-json", "bad-stage-output"],
)
async def test_permanent_parse_failure_exhausts_immediately_and_keeps_raw_page(
    fake_redis, namespace, parser, expected
):
    store = {"job-1": _job()}
    env = TaskEnvelope(job_id="job-1", task_id="t-1", parser_key="site.p",
                       payload={"url": "u"}, max_retries=3)
    coordinator = _coordinator(fake_redis, namespace, store, lambda job: [env], {"site.p": parser})
    tq = TaskQueue(fake_redis, namespace=namespace)
    rq = ResultQueue(fake_redis, namespace=namespace)

    async with _Running(coordinator):
        await _wait_until(lambda: tq.pending_count())
        await tq.claim(timeout=1.0)
        await tq.acknowledge("t-1")
        await rq.push(_fetched(env, _html_response("<html>changed layout</html>")))
        await _wait_until(lambda: store["job-1"]["status"] == "failed")

    # Retries remained, but re-fetching identical HTML would not help.
    assert await tq.pending_count() == 0
    dead = await rq.dead_letters()
    assert len(dead) == 1
    assert expected in dead[0]["reason"]
    # The dead letter holds the full fetched result, so it can be re-parsed later.
    kept = ResultEnvelope.from_json(dead[0]["raw"])
    assert kept.response["text"] == "<html>changed layout</html>"


async def test_unknown_parser_key_exhausts_and_dead_letters(fake_redis, namespace):
    store = {"job-1": _job()}
    env = TaskEnvelope(job_id="job-1", task_id="t-1", parser_key="missing.parser",
                       payload={"url": "u"})
    coordinator = _coordinator(fake_redis, namespace, store, lambda job: [env], {})
    rq = ResultQueue(fake_redis, namespace=namespace)

    async with _Running(coordinator):
        await _wait_until(lambda: TaskQueue(fake_redis, namespace=namespace).pending_count())
        await rq.push(_fetched(env, _json_response({})))
        await _wait_until(lambda: store["job-1"]["status"] == "failed")

    dead = await rq.dead_letters()
    assert "missing.parser" in dead[0]["reason"]
