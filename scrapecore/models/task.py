"""
scrapecore/models/task.py

TaskEnvelope — the unit of work that travels through the queue.

Design principle: the library owns the envelope, the consumer owns the payload.

The envelope carries everything the library needs to route, retry, and track
a task. The payload is an opaque dict defined entirely by the consumer — the
library stores and forwards it without inspecting it.

Example of what an envelope looks like as JSON in Redis:

    {
        "task_id":       "3f7a1b2c-...",
        "job_id":        "9e2d4f1a-...",
        "parser_key":    "autopiter.parse_appraise",
        "priority":      0,
        "retry_count":   0,
        "max_retries":   3,
        "created_at":    "2026-05-10T14:30:00.123456",
        "payload": {
            "url":         "https://autopiter.ru/api/...",
            "method":      "GET",
            "headers":     {...},
            "metadata":    {"part_code": "38H5003", "stage": "appraise"}
        }
    }

The agent reads parser_key to know which function to call.
The agent passes payload to that function.
The agent never needs to understand what is inside payload.
"""

from __future__ import annotations

import json
import dataclasses
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


@dataclass
class TaskEnvelope:
    """
    Wrapper around a unit of scrape work.

    Attributes:
        task_id:     Unique ID for this specific task. Auto-generated.
        job_id:      ID of the parent job this task belongs to. Set by coordinator.
        parser_key:  Dotted string identifying the parser function the agent
                     should call. Example: "autopiter.parse_appraise".
                     The agent resolves this against its local parser registry.
        priority:    Integer. Higher = claimed sooner. Passed through to the queue.
        retry_count: How many times this task has been attempted. Starts at 0.
        max_retries: Maximum attempts before the task is permanently failed.
        created_at:  When the envelope was first created. ISO 8601 string.
        payload:     Consumer-defined dict. Anything needed to execute the task:
                     URL, HTTP method, headers, metadata, etc.
    """
    job_id:      str
    parser_key:  str
    payload:     dict[str, Any]
    task_id:     str            = field(default_factory=lambda: str(uuid4()))
    priority:    int            = 0
    retry_count: int            = 0
    max_retries: int            = 3
    created_at:  str            = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to a plain dict suitable for JSON serialization.
        All values are JSON-safe primitives — no datetime objects, no Enums.
        """
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskEnvelope:
        """
        Reconstruct a TaskEnvelope from a plain dict (e.g. after JSON parse).
        Unknown keys are ignored — forward compatibility when new fields are added.
        """
        known_fields = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)

    @classmethod
    def from_json(cls, raw: str) -> TaskEnvelope:
        return cls.from_dict(json.loads(raw))

    # ── Retry logic ───────────────────────────────────────────────────────────

    def should_retry(self) -> bool:
        return self.retry_count < self.max_retries

    def increment_retry(self) -> TaskEnvelope:
        """
        Return a new envelope with retry_count incremented by one.
        Envelopes are treated as immutable — we never mutate in place.
        """
        data = self.to_dict()
        data["retry_count"] += 1
        return TaskEnvelope.from_dict(data)

    # ── Representation ────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"TaskEnvelope(task_id={self.task_id!r}, "
            f"job_id={self.job_id!r}, "
            f"parser_key={self.parser_key!r}, "
            f"retry={self.retry_count}/{self.max_retries})"
        )