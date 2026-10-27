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
