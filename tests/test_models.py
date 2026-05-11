"""Layer 1 — pure-dataclass tests for TaskEnvelope and ResultEnvelope."""

import json

from scrapecore.models.task import TaskEnvelope
from scrapecore.models.result import ResultEnvelope


# ── TaskEnvelope ──────────────────────────────────────────────────────────────

def test_task_envelope_to_dict_is_json_safe():
    env = TaskEnvelope(
        job_id="job-1",
        parser_key="site.parse",
        payload={"url": "https://example.com"},
    )
    data = env.to_dict()
    # All required fields present
    for key in (
        "task_id", "job_id", "parser_key", "payload",
        "priority", "retry_count", "max_retries", "created_at",
    ):
        assert key in data
    # And the dict must round-trip through JSON without losing anything.
    assert json.loads(json.dumps(data)) == data


def test_task_envelope_round_trip():
    original = TaskEnvelope(
        job_id="job-1",
        parser_key="site.parse",
        payload={"url": "https://example.com", "metadata": {"k": "v"}},
        priority=5,
        max_retries=7,
    )
    restored = TaskEnvelope.from_dict(original.to_dict())
    assert restored.task_id == original.task_id
    assert restored.job_id == original.job_id
    assert restored.parser_key == original.parser_key
    assert restored.payload == original.payload
    assert restored.priority == original.priority
    assert restored.max_retries == original.max_retries
    assert restored.retry_count == original.retry_count
    assert restored.created_at == original.created_at


def test_task_envelope_from_dict_ignores_unknown_keys():
    data = {
        "task_id": "t-1",
        "job_id": "j-1",
        "parser_key": "p",
        "payload": {},
        "priority": 0,
        "retry_count": 0,
        "max_retries": 3,
        "created_at": "2026-01-01T00:00:00",
        "unknown_field": "should be ignored",
        "another_extra": 42,
    }
    env = TaskEnvelope.from_dict(data)
    assert env.task_id == "t-1"
    assert not hasattr(env, "unknown_field")


def test_increment_retry_returns_new_envelope_with_incremented_count():
    env = TaskEnvelope(job_id="j", parser_key="p", payload={})
    retried = env.increment_retry()
    assert retried.retry_count == env.retry_count + 1
    assert retried.task_id == env.task_id
    assert retried.job_id == env.job_id


def test_increment_retry_does_not_mutate_original():
    env = TaskEnvelope(job_id="j", parser_key="p", payload={})
    _ = env.increment_retry()
    assert env.retry_count == 0


def test_should_retry_true_below_max():
    env = TaskEnvelope(
        job_id="j", parser_key="p", payload={},
        retry_count=1, max_retries=3,
    )
    assert env.should_retry() is True


def test_should_retry_false_at_max():
    env = TaskEnvelope(
        job_id="j", parser_key="p", payload={},
        retry_count=3, max_retries=3,
    )
    assert env.should_retry() is False


def test_task_envelope_json_round_trip():
    env = TaskEnvelope(
        job_id="j-1", parser_key="p",
        payload={"a": 1, "b": [1, 2, 3]},
    )
    restored = TaskEnvelope.from_json(env.to_json())
    assert restored.to_dict() == env.to_dict()


# ── ResultEnvelope ────────────────────────────────────────────────────────────

def test_result_envelope_success():
    res = ResultEnvelope.success(
        task_id="t", job_id="j", agent_id="a",
        retry_count=0, max_retries=3,
        output={"k": "v"},
    )
    assert res.status == "completed"
    assert res.output == {"k": "v"}
    assert res.error is None
    assert res.stage_output is None


def test_result_envelope_failure_retries_remaining():
    res = ResultEnvelope.failure(
        task_id="t", job_id="j", agent_id="a",
        retry_count=1, max_retries=3,
        error="boom",
    )
    assert res.status == "failed"
    assert res.error == "boom"
    assert res.output is None


def test_result_envelope_failure_retries_exhausted():
    res = ResultEnvelope.failure(
        task_id="t", job_id="j", agent_id="a",
        retry_count=3, max_retries=3,
        error="boom",
    )
    assert res.status == "exhausted"


def test_result_envelope_round_trip():
    original = ResultEnvelope.success(
        task_id="t", job_id="j", agent_id="a",
        retry_count=2, max_retries=4,
        output={"x": 1},
        stage_output={"catalogs": ["a", "b"]},
    )
    restored = ResultEnvelope.from_dict(original.to_dict())
    assert restored.to_dict() == original.to_dict()


def test_result_envelope_from_dict_ignores_unknown_keys():
    base = ResultEnvelope.success(
        task_id="t", job_id="j", agent_id="a",
        retry_count=0, max_retries=3, output={},
    ).to_dict()
    base["surprise"] = 123
    res = ResultEnvelope.from_dict(base)
    assert not hasattr(res, "surprise")
