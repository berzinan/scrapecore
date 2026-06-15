"""
tests/test_auth.py

Verifies the BaseAuth <-> Agent wiring contract:

    - Agent._execute calls auth_provider.prepare_request(headers, url)
      BEFORE the HTTP request, and uses the returned headers.
    - Agent._execute calls auth_provider.handle_response(status, headers, url)
      AFTER a successful response.
    - Agent._execute calls auth_provider.handle_response(status, headers, url)
      AFTER an HttpBackendError (e.g. 401), before re-raising as RuntimeError.
    - Agent without an auth_provider (default None) executes normally.

No Redis, no network. A fake HttpBackend stands in for AiohttpBackend, and
Agent._execute is called directly — it never touches self._redis, so the
agent can be constructed with redis=None.

This tests scrapecore's side of the contract only. It does NOT require any
site-specific auth implementation (e.g. autopiter session-cookie refresh) —
that belongs in gng_pricing's own test suite once it exists.

Run with: python -m tests.test_auth
"""

from __future__ import annotations

import asyncio
from typing import Optional

from scrapecore.agent.agent import Agent
from scrapecore.models.task import TaskEnvelope
from scrapecore.http.base import HttpResponse, HttpBackendError
from scrapecore.plugins.auth import BaseAuth


# ── Test doubles ──────────────────────────────────────────────────────────

class FakeBackend:
    """
    Stands in for AiohttpBackend/CurlCffiBackend.

    Records the headers it was called with and returns (or raises)
    whatever was configured at construction time.
    """

    def __init__(
        self,
        response: Optional[HttpResponse] = None,
        error: Optional[HttpBackendError] = None,
    ) -> None:
        self._response = response
        self._error = error
        self.last_headers: dict | None = None
        self.call_count = 0

    async def request(self, method, url, *, headers=None, params=None,
                       body=None, proxy=None) -> HttpResponse:
        self.call_count += 1
        self.last_headers = dict(headers or {})
        if self._error is not None:
            raise self._error
        return self._response

    async def close(self) -> None:
        pass


class RecordingAuth(BaseAuth):
    """
    Minimal BaseAuth implementation for testing.

    prepare_request injects a bearer token. handle_response records every
    call so the test can assert on arguments and call order.
    """

    def __init__(self, token: str = "test-token") -> None:
        self.token = token
        self.prepare_calls: list[tuple[dict, str]] = []
        self.response_calls: list[tuple[int, dict, str]] = []

    async def prepare_request(self, headers: dict, url: str) -> dict:
        self.prepare_calls.append((dict(headers), url))
        headers = dict(headers)
        headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def handle_response(self, status: int, headers: dict, url: str) -> None:
        self.response_calls.append((status, dict(headers), url))


def _make_agent(backend: FakeBackend, auth: BaseAuth | None) -> Agent:
    return Agent(
        agent_id="test-agent",
        redis=None,                    # _execute never touches self._redis
        parser_registry={"dummy.parse": lambda raw, meta: {"ok": True}},
        namespace="test",
        num_workers=1,
        requests_per_second=1000.0,    # negligible rate-limit delay
        http_backend=backend,
        auth_provider=auth,
    )


def _envelope(url: str = "http://example.com/resource") -> TaskEnvelope:
    return TaskEnvelope(
        job_id="job-1",
        parser_key="dummy.parse",
        payload={
            "url": url,
            "method": "GET",
            "headers": {"Accept": "application/json"},
            "metadata": {},
        },
    )


# ── Tests ─────────────────────────────────────────────────────────────────

async def test_prepare_request_headers_reach_backend():
    """auth_provider.prepare_request runs first; the backend receives ITS
    returned headers, not the envelope's original headers untouched."""
    backend = FakeBackend(response=HttpResponse(
        status=200, content_type="application/json", text='{"ok": true}', headers={},
    ))
    auth = RecordingAuth(token="abc123")
    agent = _make_agent(backend, auth)

    result = await agent._execute(_envelope())

    assert result.status == "completed"
    assert backend.last_headers["Authorization"] == "Bearer abc123"
    assert backend.last_headers["Accept"] == "application/json"

    assert len(auth.prepare_calls) == 1
    sent_headers, sent_url = auth.prepare_calls[0]
    assert sent_headers == {"Accept": "application/json"}
    assert sent_url == "http://example.com/resource"
    print("OK  prepare_request headers reach the backend")


async def test_handle_response_called_on_success():
    backend = FakeBackend(response=HttpResponse(
        status=200, content_type="application/json", text='{"ok": true}',
        headers={"X-RateLimit-Remaining": "10"},
    ))
    auth = RecordingAuth()
    agent = _make_agent(backend, auth)

    await agent._execute(_envelope())

    assert len(auth.response_calls) == 1
    status, headers, url = auth.response_calls[0]
    assert status == 200
    assert headers == {"X-RateLimit-Remaining": "10"}
    assert url == "http://example.com/resource"
    print("OK  handle_response called on success with status/headers/url")


async def test_handle_response_called_on_http_error():
    """On a 401, handle_response runs (e.g. to invalidate a cached token)
    BEFORE _execute converts the error into a RuntimeError."""
    error = HttpBackendError(
        status=401, message="unauthorized",
        url="http://example.com/resource",
        headers={"WWW-Authenticate": "Bearer"},
    )
    backend = FakeBackend(error=error)
    auth = RecordingAuth()
    agent = _make_agent(backend, auth)

    try:
        await agent._execute(_envelope())
    except RuntimeError as e:
        assert "401" in str(e)
    else:
        raise AssertionError("Expected RuntimeError for HTTP 401")

    assert len(auth.response_calls) == 1
    status, headers, url = auth.response_calls[0]
    assert status == 401
    assert headers == {"WWW-Authenticate": "Bearer"}
    print("OK  handle_response called on HttpBackendError before re-raise")


async def test_no_auth_provider_is_a_noop():
    """Agent works unchanged with the default auth_provider=None."""
    backend = FakeBackend(response=HttpResponse(
        status=200, content_type="application/json", text='{"ok": true}', headers={},
    ))
    agent = _make_agent(backend, auth=None)

    result = await agent._execute(_envelope())

    assert result.status == "completed"
    assert "Authorization" not in backend.last_headers
    print("OK  agent runs normally with no auth_provider configured")


async def main() -> None:
    print("Testing BaseAuth <-> Agent wiring...\n")
    await test_prepare_request_headers_reach_backend()
    await test_handle_response_called_on_success()
    await test_handle_response_called_on_http_error()
    await test_no_auth_provider_is_a_noop()
    print("\nAll auth wiring tests passed.")


if __name__ == "__main__":
    asyncio.run(main())