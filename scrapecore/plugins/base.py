"""
scrapecore/plugins/base.py

Abstract base classes that consumer parsers and output models must implement.

These classes define the contract between the library and the consumer.
The library never knows what a PriceList or a _Part is — it only knows
that parsers return something with a to_dict() method, and that task
builders return something with a to_envelope() method.

Consumer usage example (in gng_pricing):

    from scrapecore.plugins.base import BaseParser, BaseOutput
    from scrapecore.models.task import TaskEnvelope

    @dataclass
    class PriceList(BaseOutput):
        query_code: str
        items: list

        def to_dict(self) -> dict:
            return {"query_code": self.query_code, "items": [...]}

    class AppraiseParser(BaseParser):
        key = "autopiter.parse_appraise"

        def parse(self, raw: Any, metadata: dict) -> PriceList:
            # ... parse raw JSON into PriceList ...
            return PriceList(...)

        def build_task(self, job: dict) -> list[TaskEnvelope]:
            # ... construct TaskEnvelopes from job record ...
            return [TaskEnvelope(...)]
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

from scrapecore.models.task import TaskEnvelope
from scrapecore.models.result import ResultEnvelope


# ── Output base ───────────────────────────────────────────────────────────────

class BaseOutput(ABC):
    """
    Base class for all data objects returned by parsers.

    The agent calls to_dict() on the parser's return value before
    putting it into a ResultEnvelope. Subclasses must implement this.

    Optionally, subclasses can implement to_stage_output() if they
    carry intermediate data for a multi-stage pipeline. Returning a
    non-None value from to_stage_output() causes the agent to populate
    ResultEnvelope.stage_output, which triggers a stage transition in
    the coordinator.
    """

    @abstractmethod
    def to_dict(self) -> dict[str, Any]:
        """
        Serialize this output to a JSON-safe dict.
        All values must be primitives: str, int, float, bool, None,
        or nested dicts/lists of the same.
        """
        ...

    def to_stage_output(self) -> Optional[dict[str, Any]]:
        """
        Return intermediate data for the coordinator to use when building
        the next stage of tasks. Return None for final-stage outputs.

        Override this in Stage 1 parsers that need to pass data to Stage 2.
        The returned dict will appear in ResultEnvelope.stage_output.

        Example (autopiter search parser):
            def to_stage_output(self) -> dict:
                return {"catalogs": self.catalogs}
        """
        return None


# ── Parser base ───────────────────────────────────────────────────────────────

class BaseParser(ABC):
    """
    Base class for all site-specific parsers.

    Subclasses must define:
        key:   A unique string identifying this parser. Must match the
               parser_key field in TaskEnvelope exactly.
               Convention: "{site}.{stage}"  e.g. "autopiter.parse_appraise"

        parse: The parsing function. Receives the raw HTTP response and
               the metadata dict from the task payload. Returns a BaseOutput.

    Subclasses may define:
        build_tasks: Given a job record dict, return the first-stage
                     TaskEnvelopes to enqueue. Only needed for parsers
                     that handle the entry point of a job.

        build_stage_tasks: Given a ResultEnvelope containing stage_output,
                           return the next-stage TaskEnvelopes. Only needed
                           for multi-stage pipelines.
    """

    #: Must be overridden. Used as the registry key.
    key: str = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        Enforce that every subclass declares a non-empty key.
        Called automatically by Python when a class inherits from BaseParser.
        """
        super().__init_subclass__(**kwargs)
        if not cls.key:
            raise TypeError(
                f"{cls.__name__} must define a non-empty class attribute 'key'. "
                f"Example: key = 'autopiter.parse_appraise'"
            )

    @abstractmethod
    def parse(self, raw: Any, metadata: dict[str, Any]) -> BaseOutput:
        """
        Parse a raw HTTP response into a structured output object.

        Args:
            raw:      The HTTP response body. Will be a parsed dict for
                      JSON responses, a string for HTML responses.
            metadata: The metadata dict from the task payload. Contains
                      anything the task builder put there — part codes,
                      article IDs, catalog names, etc.

        Returns:
            A BaseOutput subclass instance. The agent will call to_dict()
            on it to produce the result payload, and to_stage_output() to
            check for stage transition data.
        """
        ...

    def build_tasks(self, job: dict[str, Any]) -> list[TaskEnvelope]:
        """
        Build the first-stage TaskEnvelopes for a job.

        Override this in the entry-point parser for a site.
        The coordinator calls task_factory(job), which should delegate
        to the appropriate parser's build_tasks().

        Default: raises NotImplementedError — not all parsers are entry points.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.build_tasks() is not implemented. "
            f"Only entry-point parsers need to implement this."
        )

    def build_stage_tasks(self, result: ResultEnvelope) -> list[TaskEnvelope]:
        """
        Build follow-up TaskEnvelopes from a stage result.

        Override this in parsers whose output triggers a next stage.
        The coordinator calls stage_handler(result), which should delegate
        to the appropriate parser's build_stage_tasks().

        Default: returns empty list — single-stage parsers never need this.
        """
        return []


# ── Registry ──────────────────────────────────────────────────────────────────

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