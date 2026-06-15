# scrapecore/http/base.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass
class HttpResponse:
    """
    Normalised HTTP response returned by every backend.

    Attributes:
        status:       HTTP status code.
        content_type: Value of the Content-Type response header, or "".
        text:         Response body as a decoded string.
                      The agent JSON-parses this if content_type indicates JSON.
        headers:      #TODO: Fill this in.
    """
    status:       int
    content_type: str
    text:         str
    headers:      dict[str, str]


class HttpBackendError(Exception):
    """
    Raised by a backend when the server returns a non-2xx status.

    Replaces aiohttp.ClientResponseError so agent.py stays backend-agnostic.
    """
    def __init__(
        self,
        status: int,
        message: str,
        url: str,
        headers: dict | None = None,
    ) -> None:
        self.status  = status
        self.message = message
        self.url      = url
        self.headers  = headers or {}
        super().__init__(f"HTTP {status} from {url}: {message}")


class NetworkError(Exception):
    """
    Raised by a backend for connection-level failures (timeouts, DNS, etc.).

    Replaces aiohttp.ClientError.
    """


@runtime_checkable
class HttpBackend(Protocol):
    """
    Structural protocol for HTTP backends.

    Any class that implements `request` and `close` with these signatures
    satisfies the protocol — no inheritance from HttpBackend required.

    Lifecycle:
        The agent calls close() once after all workers have stopped.
        Backends that hold a session or connection pool should release it there.
    """

    async def request(
        self,
        method:  str,
        url:     str,
        *,
        headers: dict | None = None,
        params:  dict | None = None,
        body:    dict | None = None,
        proxy:   str  | None = None,
    ) -> HttpResponse:
        """
        Execute one HTTP request and return a normalised response.

        Raises:
            HttpBackendError: Server returned a non-2xx status.
            NetworkError:     Connection-level failure.
        """
        ...

    async def close(self) -> None:
        """Release any held sessions or connection pools."""
        ...