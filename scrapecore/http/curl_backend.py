# scrapecore/http/curl_backend.py

from __future__ import annotations

import logging
from typing import Optional

from curl_cffi.requests import AsyncSession

from scrapecore.http.base import HttpBackendError, HttpResponse, NetworkError

logger = logging.getLogger(__name__)

# Supported impersonation targets. This list covers the most common choices;
# curl_cffi supports more — see its documentation for the full set.
_SUPPORTED_TARGETS = {
    "chrome99", "chrome100", "chrome101", "chrome104", "chrome107",
    "chrome110", "chrome116", "chrome119", "chrome120", "chrome123",
    "chrome124", "chrome131",
    "firefox91", "firefox95", "firefox98", "firefox100", "firefox102",
    "safari15_3", "safari15_5", "safari17_0", "safari17_2",
}


class CurlCffiBackend:
    """
    HTTP backend backed by curl_cffi.

    Produces a TLS ClientHello that matches a real browser, bypassing
    JA3/TLS fingerprint checks used by Cloudflare and similar WAFs.
    Use this in place of AiohttpBackend for bot-protected sites.

    Args:
        impersonate: Browser profile to impersonate. Must be one of the
                     profiles supported by curl_cffi (e.g. "chrome120").
                     Defaults to "chrome120".
        timeout:     Total request timeout in seconds.

    Raises:
        ValueError:  If an unrecognised impersonation target is supplied.
        ImportError: If curl_cffi is not installed.
    """

    def __init__(
        self,
        impersonate: str = "chrome120",
        timeout:     int = 30,
    ) -> None:
        if impersonate not in _SUPPORTED_TARGETS:
            raise ValueError(
                f"Unsupported impersonation target: {impersonate!r}. "
                f"Supported: {sorted(_SUPPORTED_TARGETS)}"
            )

        self._impersonate = impersonate
        self._timeout     = timeout
        self._session: Optional[AsyncSession] = None

    # ── Session lifecycle ─────────────────────────────────────────────────────

    async def _get_session(self) -> AsyncSession:
        """Return the shared session, creating it on first use."""
        if self._session is None:
            self._session = AsyncSession(impersonate=self._impersonate)
        return self._session

    async def close(self) -> None:
        """Close the underlying curl_cffi session."""
        if self._session is not None:
            await self._session.close()
            self._session = None
            logger.debug(f"CurlCffiBackend: session closed ({self._impersonate})")

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
        Execute one HTTP request with browser TLS impersonation.

        Raises:
            HttpBackendError: Server returned a non-2xx status.
            NetworkError:     Connection-level failure (timeout, DNS, etc.).
        """
        import curl_cffi.requests.errors as curl_errors

        session = await self._get_session()

        try:
            response = await session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=body,
                proxy=proxy,
                timeout=self._timeout,
            )
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "")
            return HttpResponse(
                status=response.status_code,
                content_type=content_type,
                text=response.text,
            )

        except curl_errors.RequestsError as e:
            # curl_cffi raises RequestsError for both HTTP errors and
            # network-level failures. We distinguish them by status code.
            status = getattr(e, "response", None)
            if status is not None:
                code = e.response.status_code
                raise HttpBackendError(
                    status=code,
                    message=str(e),
                    url=url,
                )
            raise NetworkError(f"Network error fetching {url}: {e}")