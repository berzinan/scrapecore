"""
scrapecore/plugins/registry.py

Parser discovery and routing.

The registry maps parser keys to parser implementations
and provides task/stage factory callbacks for the coordinator.
"""

from __future__ import annotations

from typing import Optional, Any

from scrapecore.models.task import TaskEnvelope
from scrapecore.models.result import ResultEnvelope
from scrapecore.plugins.base import BaseParser

class ParserRegistry:
    """
    Maps parser_key strings to BaseParser instances.

    The consumer creates one registry, registers their parsers,
    and passes it to both the Agent (for execution) and the Coordinator
    (for task/stage factory delegation).

    Example:
        registry = ParserRegistry()
        registry.register(SearchParser())
        registry.register(AppraiseParser())

        # For the agent — callable dict
        agent = Agent(..., parser_registry=registry.as_callable_dict())

        # For the coordinator — factory functions
        coordinator = Coordinator(
            ...,
            task_factory=registry.task_factory,
            stage_handler=registry.stage_handler,
        )
    """

    def __init__(self) -> None:
        self._parsers: dict[str, BaseParser] = {}
        # Maps job "site" field to the entry-point parser key for that site
        self._entry_points: dict[str, str] = {}

    def register(self, parser: BaseParser, entry_point_for: Optional[str] = None) -> None:
        """
        Register a parser instance.

        Args:
            parser:           A BaseParser subclass instance.
            entry_point_for:  If this parser is the first stage for a site,
                              pass the site key here (e.g. "autopiter").
                              The registry will route jobs with site="autopiter"
                              to this parser's build_tasks().
        """
        if not parser.key:
            raise ValueError("Parser must have a non-empty key attribute.")

        self._parsers[parser.key] = parser

        if entry_point_for:
            self._entry_points[entry_point_for] = parser.key

    def as_callable_dict(self) -> dict[str, Any]:
        """
        Return a dict mapping parser keys to their parse() callables.
        Pass this to Agent(..., parser_registry=...).
        """
        return {key: parser.parse for key, parser in self._parsers.items()}

    def task_factory(self, job: dict[str, Any]) -> list[TaskEnvelope]:
        """
        Coordinator task_factory implementation.
        Routes to the entry-point parser for the job's site.
        """
        site = job.get("site", "")
        parser_key = self._entry_points.get(site)

        if not parser_key:
            raise ValueError(
                f"No entry-point parser registered for site {site!r}. "
                f"Registered sites: {list(self._entry_points.keys())}"
            )

        parser = self._parsers[parser_key]
        return parser.build_tasks(job)

    def stage_handler(self, result: ResultEnvelope) -> list[TaskEnvelope]:
        """
        Coordinator stage_handler implementation.
        Routes to the parser whose key matches the result's task parser_key.
        """
        # The task's parser_key is stored in the result envelope
        # via the task payload — we need to look it up from stage_output
        parser_key = (result.stage_output or {}).get("next_parser_key")

        if not parser_key:
            return []

        parser = self._parsers.get(parser_key)
        if not parser:
            return []

        return parser.build_stage_tasks(result)