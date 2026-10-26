"""Parser contract: with_stage_output() → agent → ParserRegistry.stage_handler."""

from scrapecore.agent.agent import Agent
from scrapecore.models.result import STAGE_OUTPUT_KEY, ResultEnvelope
from scrapecore.models.task import TaskEnvelope
from scrapecore.plugins import BaseParser, ParserRegistry, with_stage_output


def test_with_stage_output_shape():
    out = with_stage_output({"query": "q"}, next_parser_key="site.detail", catalogs=["a"])

    assert out == {
        "query": "q",
        STAGE_OUTPUT_KEY: {"next_parser_key": "site.detail", "catalogs": ["a"]},
    }


def test_with_stage_output_does_not_mutate_input():
    original = {"query": "q"}
    with_stage_output(original, next_parser_key="site.detail")
    assert original == {"query": "q"}


def test_with_stage_output_allows_empty_output():
    out = with_stage_output({}, next_parser_key="site.detail")
    assert out == {STAGE_OUTPUT_KEY: {"next_parser_key": "site.detail"}}


def test_agent_validation_splits_helper_output():
    output, stage = Agent._validate_parser_output(
        "site.search",
        with_stage_output({"query": "q"}, next_parser_key="site.detail", catalogs=["a"]),
    )
    assert output == {"query": "q"}
    assert stage == {"next_parser_key": "site.detail", "catalogs": ["a"]}


def test_helper_output_routes_through_registry_stage_handler():
    class SearchParser(BaseParser):
        key = "site.search"

        def parse(self, raw, metadata):
            return with_stage_output(
                {"query": metadata["query"]},
                next_parser_key="site.detail",
                catalogs=raw["catalogs"],
            )

    class DetailParser(BaseParser):
        key = "site.detail"

        def parse(self, raw, metadata):
            return {"catalog": metadata["catalog"]}

        def build_stage_tasks(self, result):
            return [
                TaskEnvelope(
                    job_id=result.job_id,
                    parser_key=self.key,
                    payload={"url": f"https://x/{c}", "metadata": {"catalog": c}},
                )
                for c in result.stage_output["catalogs"]
            ]

    registry = ParserRegistry()
    registry.register(SearchParser())
    registry.register(DetailParser())

    parsed = registry.as_callable_dict()["site.search"]({"catalogs": ["a", "b"]}, {"query": "q"})
    output, stage = Agent._validate_parser_output("site.search", parsed)
    result = ResultEnvelope.success(
        task_id="t", job_id="j", agent_id="a", retry_count=0, max_retries=3,
        output=output, stage_output=stage,
    )

    follow_up = registry.stage_handler(result)

    assert output == {"query": "q"}
    assert [t.payload["metadata"]["catalog"] for t in follow_up] == ["a", "b"]
    assert all(t.parser_key == "site.detail" for t in follow_up)
