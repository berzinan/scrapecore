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
