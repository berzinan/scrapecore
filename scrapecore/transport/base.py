"""
scrapecore/transport/base.py

AgentTransport — everything an agent needs from the server, as one interface.

The agent never touches Redis directly. It claims tasks, renews claims,
submits outcomes and reports presence through a transport, so the same agent
code runs against Redis in-process (RedisTransport) or against a remote
gateway over HTTPS (HttpTransport, in a later change).
"""
from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable

from scrapecore.models.result import ResultEnvelope
from scrapecore.models.task import TaskEnvelope


# ── Errors ────────────────────────────────────────────────────────────────────

class TransportError(Exception):
    """Base class for transport failures."""


class TransportUnavailable(TransportError):
    """
    The server could not be reached or answered with a server error.
    Transient: the agent backs off and tries again.
    """


class AuthenticationFailed(TransportError):
    """The server rejected the agent's credentials. Fatal: the agent stops."""


class UpgradeRequired(TransportError):
    """
    The server requires a newer agent version. Fatal: the agent stops.

    Attributes:
        min_version: The minimum version the server accepts.
    """

    def __init__(self, message: str, min_version: str | None = None) -> None:
        super().__init__(message)
        self.min_version = min_version


class TaskNotHeld(TransportError):
    """
    The agent renewed or submitted a task it no longer holds, typically
    because stale-task recovery gave it to another agent after this one
    went quiet. The agent drops the task and carries on.
    """


#: Errors after which an agent must stop rather than retry.
FATAL_TRANSPORT_ERRORS = (AuthenticationFailed, UpgradeRequired)


# ── Protocol ──────────────────────────────────────────────────────────────────

@runtime_checkable
class AgentTransport(Protocol):
    """Structural protocol: any class with these methods is a transport."""

    async def claim(self, timeout: float) -> Optional[TaskEnvelope]:
        """
        Claim the next task, waiting up to `timeout` seconds.
        :return: The claimed task, or None if none arrived in time.
        """
        ...

    async def renew(self, task_id: str) -> None:
        """
        Tell the server this agent is still working on `task_id`, so stale-task
        recovery does not hand it to another agent. Called periodically while
        a task is in flight.
        """
        ...

    async def submit(self, task: TaskEnvelope, result: ResultEnvelope) -> None:
        """
        Report the outcome of a claimed task and release the claim.

        The transport delivers `result` to the coordinator, then either
        finishes the task or, for a retriable failure of a job that is not
        cancelled, re-enqueues it with retry_count + 1.
        """
        ...

    async def heartbeat(self, agent_id: str, info: dict[str, Any], ttl: int) -> None:
        """Announce the agent as online for `ttl` seconds, with `info`."""
        ...

    async def leave(self, agent_id: str) -> None:
        """Announce that the agent is going offline (clean shutdown)."""
        ...
