# scrapecore/http/aiohttp_backend.py

from __future__ import annotations

import logging
from typing import Optional

import aiohttp

from scrapecore.http.base import HttpBackendError, HttpResponse, NetworkError

logger = logging.getLogger(__name__)


class AiohttpBackend:
    """
    HTTP backend backed by aiohttp.

    This is the default backend. It works for the majority of sites
    that do not perform TLS fingerprint checks. For Cloudflare-protected
    sites, use CurlCffiBackend instead.

    Args:
        timeout:    Total request timeout in seconds.
        user_agent: Default User-Agent header sent on every request.
                    Individual tasks can override this via their headers dict.
    """

    def __init__(
        self,
        timeout:    int = 30,
        user_agent: str = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    ) -> None:
        self._timeout    = aiohttp.ClientTimeout(total=timeout)
        self._user_agent = user_agent
        self._session: Optional[aiohttp.ClientSession] = None

    # ── Session lifecycle ─────────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        """
        Return the shared session, creating it on first use.

        Lazy initialisation means the session is created inside the running
        event loop, which is a requirement for aiohttp.
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(force_close=True),
                timeout=self._timeout,
                headers={"User-Agent": self._user_agent},
            )
        return self._session

    async def close(self) -> None:
        """Close the underlying aiohttp session and release connections."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("AiohttpBackend: session closed")

    # ── Request ───────────────────────────────────────────────────────────────

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
        Execute one HTTP request.

        Raises:
            HttpBackendError: Server returned a non-2xx status.
                              Includes a 60-second back-off on 429.
            NetworkError:     Connection-level failure (timeout, DNS, etc.).
        """
        session = await self._get_session()

        try:
            async with session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=body,
                proxy=proxy,
            ) as response:
                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "")
                text = await response.text()
                return HttpResponse(
                    status=response.status,
                    content_type=content_type,
                    text=text,
                )

        except aiohttp.ClientResponseError as e:
            raise HttpBackendError(
                status=e.status,
                message=e.message,
                url=url,
            )

        except aiohttp.ClientError as e:
            raise NetworkError(f"Network error fetching {url}: {e}")