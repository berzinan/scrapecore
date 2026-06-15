"""
scrapecore/plugins/auth.py

Authentication plugin contracts.

Consumer applications implement these classes
for sites that require authentication.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseAuth(ABC):
    """
    Base authentication provider.

    Authentication providers are responsible for:
    - maintaining authentication state
    - injecting request credentials
    - refreshing expired sessions
    """

    @abstractmethod
    async def prepare_request(
        self,
        headers: dict[str, str],
        url: str,
    ) -> dict[str, str]:
        """
        Modify request headers immediately before HTTP execution.

        Called by Agent after loading the TaskEnvelope,
        before passing the request to HttpBackend.
        """
        ...


    async def handle_response(
        self,
        status: int,
        headers: dict[str, Any],
        url: str,
    ) -> None:
        """
        Called after HTTP response.

        Override if authentication state depends
        on server responses.

        Examples:
        - 401 token refresh
        - 403 session renewal
        """
        pass