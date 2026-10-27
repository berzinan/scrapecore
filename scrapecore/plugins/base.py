"""
scrapecore/plugins/base.py

Contract between the library and consumer parsers.

A parser returns a plain, JSON-serializable dict. That dict becomes
ResultEnvelope.output. To trigger a next stage, wrap the output with
with_stage_output(), which adds the data the coordinator needs to build
follow-up tasks:

    class SearchParser(BaseParser):
        key = "mysite.search"

        def parse(self, raw, metadata):
            return with_stage_output(
                {"query": metadata["query"]},          # final output
                next_parser_key="mysite.detail",       # parser that builds stage 2
                catalogs=raw["catalogs"],              # data stage 2 needs
            )

If you want typed models internally (dataclasses, pydantic), convert them
to a dict before returning; the library only ever sees the dict.

Parsers run on the coordinator, not on agents: agents only fetch. Raise
RetryableParseError for pages that should be fetched again (block pages).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from scrapecore.models.task import TaskEnvelope
from scrapecore.models.result import STAGE_OUTPUT_KEY, ResultEnvelope


# ── Stage output helper ───────────────────────────────────────────────────────

def with_stage_output(
    output: dict[str, Any], next_parser_key: str, **stage_data: Any
) -> dict[str, Any]:
    """
    Attach stage data to a parser's output, triggering a stage transition.

    The agent strips the stage data out of the output and sends it as
    ResultEnvelope.stage_output. ParserRegistry.stage_handler then calls
    build_stage_tasks() on the parser registered under next_parser_key.

    Args:
        output:          The final output for this task (may be empty).
        next_parser_key: Key of the parser whose build_stage_tasks() builds
                         the follow-up tasks.
        **stage_data:    JSON-serializable values build_stage_tasks() needs.

    Returns:
        A new dict; `output` is not modified.
    """
    return {
        **output,
        STAGE_OUTPUT_KEY: {"next_parser_key": next_parser_key, **stage_data},
    }


# ── Parse errors ──────────────────────────────────────────────────────────────

class RetryableParseError(Exception):
    """
    Raise from parse() when the page should be fetched again.

    Use it for responses that are transient rather than wrong: a captcha or
    block page served with HTTP 200, a half-rendered page, a rate-limit
    notice. The coordinator re-enqueues the task (counting against its
    max_retries), so another attempt, often on a different agent, fetches it.

    Any other exception from parse() is treated as permanent: the same HTML
    would fail the same way, so the task is exhausted immediately and the raw
    response is dead-lettered for re-parsing once the parser is fixed.
    """


# ── Parser base ───────────────────────────────────────────────────────────────

class BaseParser(ABC):
    """
    Base class for all site-specific parsers.

    Subclasses must define:
        key:   A unique string identifying this parser. Must match the
               parser_key field in TaskEnvelope exactly.
               Convention: "{site}.{stage}"  e.g. "mysite.endpoint1"

        parse: The parsing function. Receives the raw HTTP response and
               the metadata dict from the task payload. Returns a
               JSON-serializable dict.

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
                f"Example: key = 'mysite.endpoint1'"
            )

    @abstractmethod
    def parse(self, raw: Any, metadata: dict[str, Any]) -> dict[str, Any]:
        """
        Parse a raw HTTP response into a structured output object.

        Args:
            raw:      The HTTP response body. Will be a parsed dict for
                      JSON responses, a string for HTML responses.
            metadata: The metadata dict from the task payload. Contains
                      anything the task builder put there. 

        Returns:
            A JSON-serializable dict: str, int, float, bool, None, and
            nested dicts/lists of those. It becomes ResultEnvelope.output.
            Wrap it with with_stage_output() to trigger a next stage.
            Anything else (an object, a datetime value, a set) fails the
            task with a TypeError.
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
