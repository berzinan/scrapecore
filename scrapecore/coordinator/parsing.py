"""
scrapecore/coordinator/parsing.py

Server-side parsing helpers. Agents only fetch; the coordinator turns each
fetched response into a parsed output with these functions.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Optional

from scrapecore.models.result import STAGE_OUTPUT_KEY

ParserFn = Callable[[Any, dict], Any]


def decode_body(response: dict[str, Any]) -> Any:
    """
    Return the value a parser receives as `raw`.

    JSON responses (Content-Type contains application/json) are decoded to
    Python objects; everything else is passed through as the body string.
    Matches what agents passed to parsers before parsing moved server-side.
    """
    text = response.get("text", "")
    if "application/json" in (response.get("content_type") or ""):
        return json.loads(text)
    return text


async def run_parser(parser_fn: ParserFn, raw: Any, metadata: dict[str, Any]) -> Any:
    """
    Run a parser without blocking the event loop.

    Async parsers are awaited. Sync parsers run in the default thread pool,
    so a slow parse cannot stall the coordinator's heartbeat or dispatch.
    """
    if asyncio.iscoroutinefunction(parser_fn):
        return await parser_fn(raw, metadata)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, parser_fn, raw, metadata)


def validate_parser_output(
    parser_key: str, parsed: Any
) -> tuple[dict[str, Any], Optional[dict[str, Any]]]:
    """
    Enforce the parser contract and split output from stage data.

    :return: (output, stage_output). The parser's dict is not mutated.
    :raises TypeError: if the value is not a dict, stage data is not a dict,
                       or anything in it is not JSON-serializable.
    """
    if not isinstance(parsed, dict):
        raise TypeError(
            f"Parser {parser_key!r} must return a dict, "
            f"got {type(parsed).__name__}"
        )

    output = dict(parsed)
    stage_output = output.pop(STAGE_OUTPUT_KEY, None)

    if stage_output is not None and not isinstance(stage_output, dict):
        raise TypeError(
            f"Parser {parser_key!r} returned {STAGE_OUTPUT_KEY!r} as "
            f"{type(stage_output).__name__}; it must be a dict. "
            f"Use plugins.base.with_stage_output()."
        )

    try:
        json.dumps({"output": output, "stage_output": stage_output})
    except (TypeError, ValueError) as e:
        raise TypeError(
            f"Parser {parser_key!r} returned a value that is not "
            f"JSON-serializable: {e}"
        ) from e

    return output, stage_output
