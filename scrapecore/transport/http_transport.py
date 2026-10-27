"""
scrapecore/transport/http_transport.py

HttpTransport — AgentTransport that talks to the scrapecore gateway over HTTPS.

This is what agents on remote machines use. Every operation is one HTTPS request to the gateway,
authenticated with the agent's own bearer token.

TLS without a domain or certificate authority: the gateway uses a
self-signed certificate, and the agent is configured with that
certificate's SHA-256 fingerprint. aiohttp then accepts exactly that
certificate and nothing else, so the connection cannot be intercepted even
though no CA vouches for it.

Failure handling:
    401 → AuthenticationFailed  (fatal: wrong or revoked token)
    426 → UpgradeRequired       (fatal: agent too old for the server)
    409 → TaskNotHeld           (the task was reassigned; drop it)
    network error / 5xx → TransportUnavailable (transient: back off, retry)

submit() retries transient failures itself, because a lost submit means a
lost result. If a retry gets 409, an earlier attempt must have reached the
server (which then released the claim), so it is treated as success.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from typing import Any, Optional

import aiohttp

from scrapecore.models.result import ResultEnvelope
from scrapecore.models.task import TaskEnvelope
from scrapecore.transport.base import (
    AuthenticationFailed,
    TaskNotHeld,
    TransportUnavailable,
    UpgradeRequired,
)

logger = logging.getLogger(__name__)

try:
    _SCRAPECORE_VERSION = _pkg_version("scrapecore")
except PackageNotFoundError:  # running from a source tree
    _SCRAPECORE_VERSION = "unknown"

VERSION_HEADER = "X-Scrapecore-Version"
_SUBMIT_BACKOFF: tuple[float, ...] = (1, 2, 5, 10, 20)


def _parse_fingerprint(fingerprint: str) -> bytes:
    """Accept 'ab12...' or 'AB:12:...' (as openssl prints it) → 32 raw bytes."""
    cleaned = fingerprint.replace(":", "").replace(" ", "").strip()
    raw = bytes.fromhex(cleaned)
    if len(raw) != 32:
        raise ValueError("cert_fingerprint must be a SHA-256 fingerprint (64 hex digits)")
    return raw


class HttpTransport:
    """
    Args:
        base_url:          Gateway URL, e.g. "https://176.114.86.95:8443".
        token:             This agent's bearer token (from `add-agent`).
        cert_fingerprint:  SHA-256 fingerprint of the gateway's certificate
                           (from `fingerprint`). Required for https URLs.
        request_timeout:   Seconds allowed for non-claim requests.
        submit_backoff:    Delays between submit retries on transient errors.
        compress_min_bytes: Request bodies at least this large are gzipped.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        cert_fingerprint: Optional[str] = None,
        *,
        request_timeout: float = 30.0,
        submit_backoff: tuple[float, ...] = _SUBMIT_BACKOFF,
        compress_min_bytes: int = 16 * 1024,
    ) -> None:
        self._base = base_url.rstrip("/") + "/v1"
        if base_url.startswith("https://"):
            if not cert_fingerprint:
                raise ValueError("https gateway requires cert_fingerprint")
            self._ssl: Any = aiohttp.Fingerprint(_parse_fingerprint(cert_fingerprint))
        elif base_url.startswith("http://"):
            # Plain HTTP: local development and tests only.
            self._ssl = None
        else:
            raise ValueError("base_url must start with https:// or http://")
        self._headers = {
            "Authorization": f"Bearer {token}",
            VERSION_HEADER: _SCRAPECORE_VERSION,
            "Content-Type": "application/json",
        }
        self._request_timeout = request_timeout
        self._submit_backoff = submit_backoff
        self._compress_min_bytes = compress_min_bytes
        self._session: Optional[aiohttp.ClientSession] = None

    # ── HTTP plumbing ─────────────────────────────────────────────────────────

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self._headers)
        return self._session

    async def _post(self, path: str, body: dict[str, Any], timeout: float) -> tuple[int, Any]:
        """
        POST JSON and return (status, parsed body or None).
        Raises the transport error matching the response.
        """
        data = json.dumps(body).encode()
        headers = {}
        if len(data) >= self._compress_min_bytes:
            data = gzip.compress(data)
            headers["Content-Encoding"] = "gzip"
        try:
            async with self._get_session().post(
                f"{self._base}/{path}",
                data=data,
                headers=headers,
                ssl=self._ssl,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                status = resp.status
                text = await resp.text()
        except aiohttp.ServerFingerprintMismatch as e:
            # Someone other than our gateway answered. Never retry past this silently.
            raise AuthenticationFailed(f"gateway certificate fingerprint mismatch: {e}") from e
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            raise TransportUnavailable(f"{path}: {type(e).__name__}: {e}") from e

        try:
            payload = json.loads(text) if text else None
        except ValueError:
            payload = None  # e.g. an HTML error page from a proxy in between

        if status in (200, 204):
            return status, payload
        if status == 401:
            raise AuthenticationFailed("gateway rejected the agent token")
        if status == 426:
            minimum = (payload or {}).get("min_version")
            raise UpgradeRequired(f"gateway requires agent version >= {minimum}", minimum)
        if status == 409:
            raise TaskNotHeld(f"{path}: task not held by this agent")
        if status >= 500:
            raise TransportUnavailable(f"{path}: gateway returned {status}")
        raise TransportUnavailable(f"{path}: unexpected status {status}: {payload}")

    # ── AgentTransport ────────────────────────────────────────────────────────

    async def claim(self, timeout: float) -> Optional[TaskEnvelope]:
        """Long-poll the gateway for up to `timeout` seconds."""
        status, payload = await self._post(
            "claim", {"timeout": timeout}, timeout=timeout + self._request_timeout
        )
        if status == 204 or not payload:
            return None
        return TaskEnvelope.from_dict(payload["task"])

    async def renew(self, task_id: str) -> None:
        await self._post("renew", {"task_id": task_id}, timeout=self._request_timeout)

    async def submit(self, task: TaskEnvelope, result: ResultEnvelope) -> None:
        """
        Deliver the result, retrying transient failures with backoff.
        Only the task_id is sent: the gateway uses its own copy of the task.
        """
        body = {"task_id": task.task_id, "result": result.to_dict()}
        attempt = 0
        while True:
            try:
                await self._post("submit", body, timeout=self._request_timeout)
                return
            except TaskNotHeld:
                if attempt > 0:
                    # An earlier attempt reached the server and released the
                    # claim; only its response was lost.
                    return
                raise
            except TransportUnavailable as e:
                if attempt >= len(self._submit_backoff):
                    raise
                delay = self._submit_backoff[attempt]
                logger.warning(f"submit for task {task.task_id} failed ({e}); retrying in {delay}s")
                await asyncio.sleep(delay)
                attempt += 1

    async def heartbeat(self, agent_id: str, info: dict[str, Any], ttl: int) -> None:
        """agent_id is ignored by the gateway: identity comes from the token."""
        await self._post("heartbeat", {"info": info, "ttl": ttl}, timeout=self._request_timeout)

    async def leave(self, agent_id: str) -> None:
        await self._post("leave", {}, timeout=self._request_timeout)

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
