"""
scrapecore/models/result.py

ResultEnvelope — what an agent sends back to the coordinator after
executing a task, whether it succeeded or failed.

The result travels through a separate Redis list:
    scrapecore:{namespace}:results

The coordinator polls this list and updates the job store accordingly.

Two outcomes:

    Success:
        status = "completed"
        output = consumer-defined dict (serialized PriceList, etc.)
        error  = None

    Failure (retriable):
        status = "failed"
        output = None
        error  = exception message string

    Failure (permanent — retry_count >= max_retries):
        status = "exhausted"
        output = None
        error  = exception message string

The coordinator inspects status to decide whether to re-enqueue the
original TaskEnvelope (with incremented retry_count) or mark the task
as permanently failed in the job store.

Stage metadata:
    Certain parsers produce intermediate data that the coordinator needs
    to build the next stage of tasks (e.g. autopiter Stage 1 producing
    catalog IDs for Stage 2). This data is returned in the `stage_output`
    field — a separate dict from the final `output`. The coordinator reads
    stage_output to spawn follow-up tasks; it is not stored in job results.
"""

from __future__ import annotations

import json
import dataclasses
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Literal, Optional
from uuid import uuid4


ResultStatus = Literal["completed", "failed", "exhausted"]


@dataclass
class ResultEnvelope:
    """
    Carries the outcome of a single task execution back to the coordinator.

    Attributes:
        task_id:      Matches the TaskEnvelope.task_id this result is for.
        job_id:       Matches the TaskEnvelope.job_id.
        agent_id:     Identifier of the agent that executed the task.
        status:       One of "completed", "failed", "exhausted".
        output:       Consumer-defined result dict. None on failure.
        stage_output: Intermediate data for multi-stage pipelines. The
                      coordinator uses this to spawn follow-up tasks.
                      Never stored as a final job result.
        error:        Exception message. None on success.
        completed_at: ISO 8601 timestamp of when the agent finished.
        retry_count:  The retry_count of the TaskEnvelope that produced this
                      result. Carried forward so the coordinator can decide
                      whether to retry without re-reading the task.
        max_retries:  Carried forward from the TaskEnvelope.
    """
    task_id:      str
    job_id:       str
    agent_id:     str
    status:       ResultStatus
    retry_count:  int
    max_retries:  int
    output:       Optional[dict[str, Any]]  = None
    stage_output: Optional[dict[str, Any]]  = None
    error:        Optional[str]             = None
    completed_at: str                       = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # ── Convenience constructors ──────────────────────────────────────────────

    @classmethod
    def success(
        cls,
        task_id: str,
        job_id: str,
        agent_id: str,
        retry_count: int,
        max_retries: int,
        output: dict[str, Any],
        stage_output: Optional[dict[str, Any]] = None,
    ) -> ResultEnvelope:
        """Build a successful result envelope."""
        return cls(
            task_id=task_id,
            job_id=job_id,
            agent_id=agent_id,
            status="completed",
            retry_count=retry_count,
            max_retries=max_retries,
            output=output,
            stage_output=stage_output,
        )

    @classmethod
    def failure(
        cls,
        task_id: str,
        job_id: str,
        agent_id: str,
        retry_count: int,
        max_retries: int,
        error: str,
    ) -> ResultEnvelope:
        """
        Build a failure envelope. Status is set automatically:
            - "failed"    if retries remain
            - "exhausted" if this was the final attempt
        """
        status: ResultStatus = (
            "failed" if retry_count < max_retries else "exhausted"
        )
        return cls(
            task_id=task_id,
            job_id=job_id,
            agent_id=agent_id,
            status=status,
            retry_count=retry_count,
            max_retries=max_retries,
            error=error,
        )

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResultEnvelope:
        known_fields = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)

    @classmethod
    def from_json(cls, raw: str) -> ResultEnvelope:
        return cls.from_dict(json.loads(raw))

    # ── Representation ────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"ResultEnvelope(task_id={self.task_id!r}, "
            f"status={self.status!r}, "
            f"agent={self.agent_id!r}, "
            f"error={self.error!r})"
        )