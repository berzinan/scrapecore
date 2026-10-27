"""Gateway + HttpTransport: remote agents reach the queues only through HTTPS."""

import asyncio
import gzip
import json
import shutil
import ssl

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from aioresponses import aioresponses

from scrapecore.agent.agent import Agent
from scrapecore.coordinator.coordinator import Coordinator, JobStoreAdapter
from scrapecore.gateway import AgentTokenStore, create_gateway_app, version_at_least
from scrapecore.gateway.certs import cert_fingerprint, generate_self_signed, server_ssl_context
from scrapecore.models.result import ResultEnvelope
from scrapecore.models.task import TaskEnvelope
from scrapecore.queue.presence import AgentPresence
from scrapecore.queue.result_queue import ResultQueue
from scrapecore.queue.task_queue import TaskQueue
from scrapecore.transport import (
    AgentTransport,
    AuthenticationFailed,
    HttpTransport,
    TaskNotHeld,
    TransportUnavailable,
    UpgradeRequired,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
async def tokens(fake_redis, namespace):
    store = AgentTokenStore(fake_redis, namespace)
    return {
        "store": store,
        "anna": await store.add_agent("anna"),
        "boris": await store.add_agent("boris"),
    }


@pytest.fixture
async def gateway(fake_redis, namespace):
    """A running gateway (plain HTTP) and a test client for raw requests."""
    app = create_gateway_app(fake_redis, namespace, max_claim_timeout=1.0)
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    yield client
    await client.close()


def _auth(token, version=None):
    headers = {"Authorization": f"Bearer {token}"}
    if version:
        headers["X-Scrapecore-Version"] = version
    return headers


async def _enqueue(fake_redis, namespace, **kwargs):
    task = TaskEnvelope(job_id=kwargs.pop("job_id", "job-1"), parser_key="site.p",
                        payload={"url": "https://example.com/x"}, **kwargs)
    await TaskQueue(fake_redis, namespace).push(task.task_id, task.to_dict())
    return task


async def _claim(gateway, token):
    resp = await gateway.post("/v1/claim", json={"timeout": 0.5}, headers=_auth(token))
    assert resp.status == 200
    return (await resp.json())["task"]


def _fetched_body(task_dict, text="<html/>"):
    result = ResultEnvelope.fetched(task_dict, "whoever", {
        "status": 200, "content_type": "text/html", "headers": {}, "text": text, "url": "u"})
    return {"task_id": task_dict["task_id"], "result": result.to_dict()}


# ── Tokens ────────────────────────────────────────────────────────────────────

async def test_tokens_authenticate_and_are_stored_hashed(fake_redis, namespace, tokens):
    store = tokens["store"]
    assert await store.authenticate(tokens["anna"]) == "anna"
    assert await store.authenticate("not-a-token") is None
    assert await store.list_agents() == ["anna", "boris"]

    stored = json.dumps(await fake_redis.hgetall(f"scrapecore:{namespace}:gateway:tokens"))
    assert tokens["anna"] not in stored  # only the hash is kept


async def test_add_agent_again_rotates_token(tokens):
    store = tokens["store"]
    new = await store.add_agent("anna")
    assert await store.authenticate(tokens["anna"]) is None
    assert await store.authenticate(new) == "anna"


async def test_revoke_invalidates_only_that_agent(tokens):
    store = tokens["store"]
    assert await store.revoke("anna")
    assert await store.authenticate(tokens["anna"]) is None
    assert await store.authenticate(tokens["boris"]) == "boris"
    assert not await store.revoke("anna")


async def test_agent_id_is_validated(tokens):
    with pytest.raises(ValueError):
        await tokens["store"].add_agent("bad id with spaces")


# ── Auth & version gate ───────────────────────────────────────────────────────

async def test_health_needs_no_auth(gateway):
    resp = await gateway.get("/v1/health")
    assert resp.status == 200


async def test_requests_without_valid_token_are_rejected(gateway, tokens):
    assert (await gateway.post("/v1/claim", json={})).status == 401
    assert (await gateway.post("/v1/claim", json={}, headers=_auth("wrong"))).status == 401


async def test_version_gate(fake_redis, namespace, tokens):
    app = create_gateway_app(fake_redis, namespace, min_agent_version="0.4.0", max_claim_timeout=0.1)
    async with TestClient(TestServer(app)) as client:
        old = await client.post("/v1/claim", json={"timeout": 0}, headers=_auth(tokens["anna"], "0.3.9"))
        assert old.status == 426
        assert (await old.json())["min_version"] == "0.4.0"
        unknown = await client.post("/v1/claim", json={"timeout": 0}, headers=_auth(tokens["anna"]))
        assert unknown.status == 426
        ok = await client.post("/v1/claim", json={"timeout": 0}, headers=_auth(tokens["anna"], "0.4.0"))
        assert ok.status == 204


def test_version_comparison():
    assert version_at_least("0.4.0", "0.4.0")
    assert version_at_least("0.10.0", "0.9.9")
    assert version_at_least("1.0", "0.9.0")
    assert version_at_least("0.4.1rc2", "0.4.1")
    assert not version_at_least("0.3.9", "0.4")
    assert not version_at_least("unknown", "0.1")
    assert not version_at_least(None, "0.1")


# ── Claim ownership ───────────────────────────────────────────────────────────

async def test_claim_returns_task_or_204(fake_redis, namespace, gateway, tokens):
    empty = await gateway.post("/v1/claim", json={"timeout": 0.1}, headers=_auth(tokens["anna"]))
    assert empty.status == 204

    task = await _enqueue(fake_redis, namespace)
    claimed = await _claim(gateway, tokens["anna"])
    assert claimed["task_id"] == task.task_id


async def test_only_the_claiming_agent_can_renew_or_submit(fake_redis, namespace, gateway, tokens):
    await _enqueue(fake_redis, namespace)
    task = await _claim(gateway, tokens["anna"])

    renew = await gateway.post("/v1/renew", json={"task_id": task["task_id"]}, headers=_auth(tokens["boris"]))
    assert renew.status == 409
    submit = await gateway.post("/v1/submit", json=_fetched_body(task), headers=_auth(tokens["boris"]))
    assert submit.status == 409
    assert await ResultQueue(fake_redis, namespace).length() == 0

    renew = await gateway.post("/v1/renew", json={"task_id": task["task_id"]}, headers=_auth(tokens["anna"]))
    assert renew.status == 204
    submit = await gateway.post("/v1/submit", json=_fetched_body(task), headers=_auth(tokens["anna"]))
    assert submit.status == 204
    # Claim released: a second submit is refused.
    again = await gateway.post("/v1/submit", json=_fetched_body(task), headers=_auth(tokens["anna"]))
    assert again.status == 409


async def test_submit_uses_server_copy_and_strips_agent_claims(fake_redis, namespace, gateway, tokens):
    await _enqueue(fake_redis, namespace, max_retries=3)
    task = await _claim(gateway, tokens["anna"])

    tampered = dict(task, payload={"url": "https://evil.example"}, job_id="other-job", retry_count=99)
    body = _fetched_body(tampered)
    body["result"].update(agent_id="boris", output={"price": 0.01}, stage_output={"x": 1})

    resp = await gateway.post("/v1/submit", json=body, headers=_auth(tokens["anna"]))
    assert resp.status == 204

    pushed = await ResultQueue(fake_redis, namespace).pop(timeout=1.0)
    assert pushed.agent_id == "anna"                    # from the token
    assert pushed.job_id == "job-1"                     # from the server copy
    assert pushed.task["payload"]["url"] == "https://example.com/x"
    assert pushed.retry_count == 0
    assert pushed.output is None and pushed.stage_output is None  # parsing stays server-side
    assert pushed.response["text"] == "<html/>"


async def test_completed_result_without_response_is_rejected(fake_redis, namespace, gateway, tokens):
    await _enqueue(fake_redis, namespace)
    task = await _claim(gateway, tokens["anna"])
    parsed = ResultEnvelope.success(task_id=task["task_id"], job_id="job-1", agent_id="anna",
                                    retry_count=0, max_retries=3, output={"price": 1})
    resp = await gateway.post("/v1/submit", json={"task_id": task["task_id"], "result": parsed.to_dict()},
                              headers=_auth(tokens["anna"]))
    assert resp.status == 400


async def test_failure_requeues_server_copy(fake_redis, namespace, gateway, tokens):
    await _enqueue(fake_redis, namespace, max_retries=3)
    task = await _claim(gateway, tokens["anna"])
    failure = ResultEnvelope.failure(task_id=task["task_id"], job_id="job-1", agent_id="anna",
                                     retry_count=3, max_retries=3, error="HTTP 500")  # lies about retries
    resp = await gateway.post("/v1/submit", json={"task_id": task["task_id"], "result": failure.to_dict()},
                              headers=_auth(tokens["anna"]))
    assert resp.status == 204

    assert (await ResultQueue(fake_redis, namespace).pop(timeout=1.0)).status == "failed"
    again = await _claim(gateway, tokens["boris"])
    assert again["retry_count"] == 1  # re-enqueued from the server's copy


async def test_heartbeat_identity_comes_from_token(fake_redis, namespace, gateway, tokens):
    resp = await gateway.post("/v1/heartbeat", json={"info": {"agent_id": "boris", "state": "idle"}, "ttl": 30},
                              headers=_auth(tokens["anna"]))
    assert resp.status == 204
    online = await AgentPresence(fake_redis, namespace).list_online()
    assert [a["agent_id"] for a in online] == ["anna"]

    assert (await gateway.post("/v1/leave", json={}, headers=_auth(tokens["anna"]))).status == 204
    assert await AgentPresence(fake_redis, namespace).list_online() == []


async def test_large_gzipped_submit_is_accepted(fake_redis, namespace, gateway, tokens):
    await _enqueue(fake_redis, namespace)
    task = await _claim(gateway, tokens["anna"])
    page = "<p>" + "x" * 3_000_000 + "</p>"  # a 3 MB page
    data = gzip.compress(json.dumps(_fetched_body(task, text=page)).encode())

    resp = await gateway.post("/v1/submit", data=data, headers={
        **_auth(tokens["anna"]), "Content-Type": "application/json", "Content-Encoding": "gzip"})
    assert resp.status == 204
    assert (await ResultQueue(fake_redis, namespace).pop(timeout=1.0)).response["text"] == page


# ── HttpTransport ─────────────────────────────────────────────────────────────

def _transport(gateway, token, **kwargs):
    return HttpTransport(str(gateway.make_url("")), token, **kwargs)


async def test_http_transport_round_trip(fake_redis, namespace, gateway, tokens):
    transport = _transport(gateway, tokens["anna"])
    assert isinstance(transport, AgentTransport)
    try:
        assert await transport.claim(timeout=0.1) is None

        task = await _enqueue(fake_redis, namespace)
        claimed = await transport.claim(timeout=0.5)
        assert claimed.task_id == task.task_id

        await transport.renew(claimed.task_id)
        await transport.heartbeat("anna", {"state": "busy"}, ttl=30)
        result = ResultEnvelope.fetched(claimed.to_dict(), "anna", {
            "status": 200, "content_type": "text/html", "headers": {}, "text": "ok", "url": "u"})
        await transport.submit(claimed, result)
        await transport.leave("anna")
    finally:
        await transport.close()

    assert (await ResultQueue(fake_redis, namespace).pop(timeout=1.0)).response["text"] == "ok"


async def test_http_transport_maps_errors(fake_redis, namespace, gateway, tokens):
    bad = _transport(gateway, "wrong-token")
    with pytest.raises(AuthenticationFailed):
        await bad.claim(timeout=0.1)
    await bad.close()

    await _enqueue(fake_redis, namespace)
    anna, boris = _transport(gateway, tokens["anna"]), _transport(gateway, tokens["boris"])
    try:
        task = await anna.claim(timeout=0.5)
        with pytest.raises(TaskNotHeld):
            await boris.renew(task.task_id)
    finally:
        await anna.close()
        await boris.close()


async def test_http_transport_upgrade_required(fake_redis, namespace, tokens):
    app = create_gateway_app(fake_redis, namespace, min_agent_version="999.0")
    async with TestClient(TestServer(app)) as client:
        transport = _transport(client, tokens["anna"])
        with pytest.raises(UpgradeRequired) as info:
            await transport.claim(timeout=0.1)
        assert info.value.min_version == "999.0"
        await transport.close()


async def test_unreachable_gateway_is_transient():
    transport = HttpTransport("http://127.0.0.1:9", "t", request_timeout=1.0)  # discard port
    with pytest.raises(TransportUnavailable):
        await transport.heartbeat("a", {}, 30)
    await transport.close()


async def test_submit_retries_and_treats_late_409_as_success(fake_redis, namespace, tokens):
    """First submit reaches the server but its response is lost (simulated 503)."""
    real_app = create_gateway_app(fake_redis, namespace, max_claim_timeout=0.5)
    calls = {"submit": 0}

    @web.middleware
    async def lose_first_response(request, handler):
        response = await handler(request)
        if request.path == "/v1/submit":
            calls["submit"] += 1
            if calls["submit"] == 1:
                return web.Response(status=503)  # processed, but the agent never hears
        return response

    real_app.middlewares.append(lose_first_response)
    async with TestClient(TestServer(real_app)) as client:
        await _enqueue(fake_redis, namespace)
        transport = _transport(client, tokens["anna"], submit_backoff=(0.01, 0.01))
        task = await transport.claim(timeout=0.5)
        result = ResultEnvelope.fetched(task.to_dict(), "anna", {"status": 200, "text": "x"})
        await transport.submit(task, result)  # must not raise
        await transport.close()

    assert calls["submit"] == 2
    assert await ResultQueue(fake_redis, namespace).length() == 1  # delivered exactly once


# ── TLS with a pinned self-signed certificate ────────────────────────────────

@pytest.fixture
def cert(tmp_path):
    if shutil.which("openssl") is None:
        pytest.skip("openssl not available")
    cert_path, key_path = tmp_path / "cert.pem", tmp_path / "key.pem"
    fingerprint = generate_self_signed(cert_path, key_path)
    return cert_path, key_path, fingerprint


async def test_tls_with_pinned_fingerprint(fake_redis, namespace, tokens, cert):
    cert_path, key_path, fingerprint = cert
    assert fingerprint == cert_fingerprint(cert_path)

    app = create_gateway_app(fake_redis, namespace, max_claim_timeout=0.2)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=server_ssl_context(cert_path, key_path))
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    url = f"https://127.0.0.1:{port}"
    try:
        good = HttpTransport(url, tokens["anna"], fingerprint)
        assert await good.claim(timeout=0.1) is None  # TLS handshake + auth succeed
        await good.close()

        # Colon-separated, upper-case form (as openssl prints it) also works.
        colons = ":".join(fingerprint[i:i + 2] for i in range(0, 64, 2)).upper()
        alt = HttpTransport(url, tokens["anna"], colons)
        assert await alt.claim(timeout=0.1) is None
        await alt.close()

        wrong = HttpTransport(url, tokens["anna"], "00" * 32)
        with pytest.raises(AuthenticationFailed, match="fingerprint mismatch"):
            await wrong.claim(timeout=0.1)
        await wrong.close()
    finally:
        await runner.cleanup()


def test_https_requires_fingerprint():
    with pytest.raises(ValueError):
        HttpTransport("https://example.com", "t")
    with pytest.raises(ValueError):
        HttpTransport("https://example.com", "t", "abcd")  # not 32 bytes
    with pytest.raises(ValueError):
        HttpTransport("ftp://example.com", "t")


# ── End to end: agent → gateway → coordinator ────────────────────────────────

async def test_job_completes_with_agent_behind_gateway(fake_redis, namespace, gateway, tokens):
    store = {"job-1": {"job_id": "job-1", "status": "pending", "error": None}}
    outputs = []

    def task_factory(job):
        return [TaskEnvelope(job_id=job["job_id"], parser_key="site.price",
                             payload={"url": "https://shop.example/item", "metadata": {"code": "A1"}})]

    def parse_price(raw, metadata):
        return {"code": metadata["code"], "price": raw["price"]}

    async def on_result(result):
        outputs.append((result.agent_id, result.output))

    coordinator = Coordinator(
        redis=fake_redis, job_store_adapter=JobStoreAdapter(store),
        task_factory=task_factory, stage_handler=lambda r: [],
        namespace=namespace, parsers={"site.price": parse_price}, on_result=on_result,
        dispatch_interval=0.05, recovery_interval=0.5, heartbeat_interval=1.0,
    )
    agent = Agent(
        agent_id="laptop-anna",
        transport=_transport(gateway, tokens["anna"]),
        rate_limit_config={"site.price": 1000.0},
        claim_timeout=0.3, heartbeat_interval=1.0,
    )

    co_run = asyncio.create_task(coordinator.start())
    try:
        with aioresponses(passthrough=[str(gateway.make_url(""))]) as mock:
            mock.get("https://shop.example/item", payload={"price": 12.5})
            ag_run = asyncio.create_task(agent.start())
            for _ in range(150):
                if store["job-1"]["status"] == "completed":
                    break
                await asyncio.sleep(0.05)
            await agent.stop()
            await asyncio.wait_for(ag_run, timeout=5.0)
    finally:
        await coordinator.stop()
        await asyncio.wait_for(co_run, timeout=5.0)

    assert store["job-1"]["status"] == "completed"
    # Fetched on the agent, carried over HTTP, parsed on the coordinator;
    # identity is the token's ("anna"), not the agent's self-chosen name.
    assert outputs == [("anna", {"code": "A1", "price": 12.5})]


# ── Agent resilience ─────────────────────────────────────────────────────────

class _FlakyTransport:
    """claim() fails `failures` times, then serves one task."""

    def __init__(self, task, failures=2, fatal=None, submit_error=None):
        self.task, self.failures, self.fatal, self.submit_error = task, failures, fatal, submit_error
        self.claims = 0
        self.submitted = []

    async def claim(self, timeout):
        self.claims += 1
        if self.fatal:
            raise self.fatal
        if self.failures:
            self.failures -= 1
            raise TransportUnavailable("offline")
        if self.task:
            task, self.task = self.task, None
            return task
        await asyncio.sleep(0.05)
        return None

    async def renew(self, task_id):
        pass

    async def submit(self, task, result):
        if self.submit_error:
            raise self.submit_error
        self.submitted.append(result)

    async def heartbeat(self, agent_id, info, ttl):
        pass

    async def leave(self, agent_id):
        pass


def _agent(transport):
    return Agent(agent_id="a", transport=transport, rate_limit_config={"site.p": 1000.0},
                 claim_timeout=0.1, heartbeat_interval=1.0)


async def test_agent_survives_unreachable_server(monkeypatch):
    monkeypatch.setattr("scrapecore.agent.agent._TRANSPORT_BACKOFF_START", 0.01)
    task = TaskEnvelope(job_id="j", parser_key="site.p", payload={"url": "https://e.example/t"})
    transport = _FlakyTransport(task, failures=3)
    agent = _agent(transport)
    with aioresponses() as mock:
        mock.get("https://e.example/t", payload={})
        run = asyncio.create_task(agent.start())
        for _ in range(100):
            if transport.submitted:
                break
            await asyncio.sleep(0.02)
        await agent.stop()
        await asyncio.wait_for(run, timeout=5.0)
    assert transport.claims >= 4
    assert len(transport.submitted) == 1


async def test_agent_stops_and_raises_on_fatal_error():
    transport = _FlakyTransport(None, fatal=AuthenticationFailed("revoked"))
    with pytest.raises(AuthenticationFailed):
        await asyncio.wait_for(_agent(transport).start(), timeout=3.0)


async def test_agent_continues_after_task_not_held():
    task = TaskEnvelope(job_id="j", parser_key="site.p", payload={"url": "https://e.example/t"})
    transport = _FlakyTransport(task, failures=0, submit_error=TaskNotHeld("reassigned"))
    agent = _agent(transport)
    with aioresponses() as mock:
        mock.get("https://e.example/t", payload={})
        run = asyncio.create_task(agent.start())
        await asyncio.sleep(0.5)
        assert not run.done()  # still running after the rejected submit
        await agent.stop()
        await asyncio.wait_for(run, timeout=5.0)
