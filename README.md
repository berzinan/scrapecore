# scrapecore

A generic distributed web-scraping library. `scrapecore` owns the orchestration
— queuing, agent coordination, retries, recovery from agent crashes, and rate
limiting — and leaves all site-specific work (HTTP payload shape, parsing, data
models) to the consumer.

Run one coordinator next to Redis, run any number of agents on any number of
machines, and feed jobs in through the coordinator's job store. The agents
will pick tasks up, execute them, and push results back.

```
   CRM / API ─► JobStore ─► Coordinator ─► Redis ─► Agents ─► HTTP
                                  ▲                   │
                                  └── ResultEnvelope ─┘
```

---

## Installation

`scrapecore` is not yet on PyPI. Install from source:

```bash
git clone https://github.com/berzinan/scrapecore.git
cd scrapecore
pip install -e .
```

Runtime dependencies (install separately if not using `pip install -e .`):

```bash
pip install aiohttp "redis>=5" 
```

Requires Python 3.11+ (uses `asyncio.TaskGroup`).

You also need a Redis instance reachable from both the coordinator and every
agent. For local development:

```bash
docker run --rm -p 6379:6379 redis:7-alpine
```

---

## Mental model

There are three things you write as a consumer:

1. **Parsers** — one per HTTP endpoint you scrape. A parser is a function
   that takes a raw HTTP response and a metadata dict and returns a plain
   `dict`. Subclass `BaseParser`.

2. **Output models** (optional) — typed wrappers around the dicts your
   parsers return. Subclass `BaseOutput` and implement `to_dict()`. You can
   skip this and return dicts directly; the library only requires that the
   final value passed back is a dict.

3. **An entry point** — for each site, exactly one parser implements
   `build_tasks(job)` to convert a job record into the first batch of
   `TaskEnvelope`s. For multi-stage pipelines, the same or another parser
   implements `build_stage_tasks(result)` to spawn follow-up tasks.

You then register everything in a `ParserRegistry` and hand it to both the
coordinator and the agents.

---

## Quickstart

A complete one-site, single-stage scraper.

### 1. Define a parser

```python
# myscraper/parsers.py
from scrapecore.plugins.base import BaseParser, BaseOutput
from scrapecore.models.task import TaskEnvelope


class ItemParser(BaseParser):
    key = "mysite.parse_item"

    def parse(self, raw, metadata):
        # raw is a parsed dict for JSON responses, a string for HTML.
        # metadata is whatever your task payload included.
        return {
            "code":  metadata["code"],
            "price": raw["product"]["price"],
            "title": raw["product"]["title"],
        }

    def build_tasks(self, job):
        return [
            TaskEnvelope(
                job_id=job["job_id"],
                parser_key=self.key,
                payload={
                    "url": f"https://mysite.example/api/item/{code}",
                    "metadata": {"code": code},
                },
            )
            for code in job["item_codes"]
        ]
```

The parser MUST return a plain `dict`. If you want structured outputs,
subclass `BaseOutput` and call `.to_dict()` yourself before returning.

### 2. Start an agent

Run one of these on every worker machine.

```python
# agent_main.py
import asyncio
from scrapecore.queue.redis_client import create_redis_client
from scrapecore.agent.agent import Agent
from scrapecore.plugins.base import ParserRegistry
from myscraper.parsers import ItemParser


async def main():
    redis = create_redis_client()

    registry = ParserRegistry()
    registry.register(ItemParser(), entry_point_for="mysite")

    agent = Agent(
        agent_id="machine-01",
        redis=redis,
        parser_registry=registry.as_callable_dict(),
        namespace="default",
        num_workers=4,
        requests_per_second=2.0,
    )
    await agent.start()

asyncio.run(main())
```

### 3. Start the coordinator

Run this on your server, alongside Redis and whatever API submits jobs.

```python
# coordinator_main.py
import asyncio
from scrapecore.queue.redis_client import create_redis_client
from scrapecore.coordinator.coordinator import Coordinator, JobStoreAdapter
from scrapecore.plugins.base import ParserRegistry
from myscraper.parsers import ItemParser

# Replace with your real store (Redis hash, Postgres table, etc.)
JOB_STORE: dict = {}


async def main():
    redis = create_redis_client()

    registry = ParserRegistry()
    registry.register(ItemParser(), entry_point_for="mysite")

    coordinator = Coordinator(
        redis=redis,
        job_store_adapter=JobStoreAdapter(JOB_STORE),
        task_factory=registry.task_factory,
        stage_handler=registry.stage_handler,
        namespace="default",
    )
    await coordinator.start()

asyncio.run(main())
```

### 4. Submit a job

A job is just a dict in your job store. Minimum required shape:

```python
JOB_STORE["job-42"] = {
    "job_id":  "job-42",
    "status":  "pending",          # coordinator picks this up
    "site":    "mysite",           # routes to the registered entry-point parser
    "results": None,
    "error":   None,
    # any other fields your task_factory / parser needs:
    "item_codes": ["A1", "B2", "C3"],
}
```

The coordinator polls for `status == "pending"`, calls
`registry.task_factory(job)` to build `TaskEnvelope`s, pushes them onto
Redis, and flips the status to `"running"`. Agents claim them, execute them,
push `ResultEnvelope`s back, and when all tasks for the job have settled the
coordinator writes results to the store and flips status to `"completed"`.

---

## The task payload contract

Every `TaskEnvelope.payload` you build is handed to the agent verbatim. The
agent reads a few well-known keys to make the HTTP request, then passes the
whole thing through to your parser:

| Key        | Type   | Default | Used by                              |
|------------|--------|---------|--------------------------------------|
| `url`      | str    | —       | required; target URL                 |
| `method`   | str    | `"GET"` | HTTP method                          |
| `headers`  | dict   | `{}`    | extra headers                        |
| `params`   | dict   | `None`  | query string parameters              |
| `body`     | dict   | `None`  | JSON body for POST/PUT               |
| `metadata` | dict   | `{}`    | passed to `parser.parse(raw, meta)`  |

JSON responses are decoded automatically (when `Content-Type:
application/json`); other content types are passed to the parser as a string.

---

## Multi-stage pipelines

Some sites need a search request whose response drives a second batch of
detail requests. Express that by having Stage 1 return a `__stage_output__`
key, then implement `build_stage_tasks` on the parser whose key is named in
`next_parser_key`.

```python
class SearchParser(BaseParser):
    key = "mysite.parse_search"

    def parse(self, raw, metadata):
        catalogs = raw["data"]["catalogs"]
        return {
            # final output kept in the job record (can be empty):
            "search_query": metadata["query"],
            # data the coordinator uses to build the next stage:
            "__stage_output__": {
                "next_parser_key": "mysite.parse_detail",
                "catalogs":        catalogs,
                "query":           metadata["query"],
            },
        }

    def build_tasks(self, job):
        return [TaskEnvelope(
            job_id=job["job_id"],
            parser_key=self.key,
            payload={
                "url": f"https://mysite.example/search?q={job['query']}",
                "metadata": {"query": job["query"]},
            },
        )]


class DetailParser(BaseParser):
    key = "mysite.parse_detail"

    def parse(self, raw, metadata):
        return {"catalog": metadata["catalog"], "items": raw["items"]}

    def build_stage_tasks(self, result):
        stage = result.stage_output
        return [
            TaskEnvelope(
                job_id=result.job_id,
                parser_key=self.key,
                payload={
                    "url": f"https://mysite.example/catalog/{cat}",
                    "metadata": {"catalog": cat, "query": stage["query"]},
                },
            )
            for cat in stage["catalogs"]
        ]
```

The `next_parser_key` field is how the registry's default `stage_handler`
finds the right `build_stage_tasks`. If you write your own `stage_handler`
you can route however you like.

Job completion: the coordinator counts outstanding tasks per job. A stage
transition replaces one task with N follow-ups (counter goes up by N-1).
When the counter reaches zero, the job is finalised with whatever non-empty
`output` dicts were collected.

---

## Retries and failures

Every `TaskEnvelope` carries `retry_count` and `max_retries` (default 3). On
failure, the agent re-enqueues the task with `retry_count + 1` and pushes a
`ResultEnvelope(status="failed")`. When `retry_count == max_retries`, the
next failure is final: the agent acknowledges the task (removes it from
Redis) and pushes `ResultEnvelope(status="exhausted")`. The coordinator
decrements the job's outstanding-task counter only on `completed` or
`exhausted`, never on `failed`.

A job whose tasks all exhaust without producing any output is marked
`failed` rather than `completed`.

If an agent crashes mid-task, the task stays in the Redis processing list.
The coordinator's recovery loop (every `recovery_interval` seconds, default
30s) re-enqueues anything older than `stale_task_timeout` (default 60s) for
another agent to pick up.

---

## Rate limiting

Two layers, used together:

**Local (per-agent, always on).** Set `requests_per_second` on the `Agent`
constructor. The agent enforces a minimum delay between requests to the
same domain across all its workers.

**Global (across all agents, optional).** Use `DistributedRateLimiter` plus
a `RateLimitConfig` for sites with a fleet-wide cap regardless of source
IP. The global limiter uses a Redis sliding-window counter and is
coordinated via an atomic Lua script.

```python
from scrapecore.rate_limiter.distributed import RateLimitConfig

config = RateLimitConfig(default_limit=10)
config.set("mysite.example", limit=2, window_seconds=1.0)
```

(Wiring the global limiter into the agent's request path is the consumer's
job — call `await limiter.acquire(domain, limit, window_seconds)` from a
custom subclass or middleware.)

---

## Environment variables

The Redis client factory reads these:

| Variable         | Default       | Description                              |
|------------------|---------------|------------------------------------------|
| `REDIS_URL`      | —             | Full URL (`redis://host:port/db`). Wins. |
| `REDIS_HOST`     | `localhost`   |                                          |
| `REDIS_PORT`     | `6379`        |                                          |
| `REDIS_DB`       | `0`           |                                          |
| `REDIS_PASSWORD` | —             |                                          |

---

## Job store: bring your own

The default `JobStoreAdapter` wraps an in-memory dict. For anything beyond
prototyping, subclass it and back it with the database of your choice
(Postgres, Redis hashes, DynamoDB, etc.) — keep the same method shape:

```python
class JobStoreAdapter:
    def get_pending_jobs(self) -> list[dict]: ...
    def mark_running(self, job_id: str) -> None: ...
    def mark_completed(self, job_id: str, results: list[dict]) -> None: ...
    def mark_failed(self, job_id: str, error: str) -> None: ...
    def is_cancelled(self, job_id: str) -> bool: ...
    def get(self, job_id: str) -> dict | None: ...
```

A `JobRecord` must always have at least: `job_id`, `status`, `results`,
`error`, plus any fields your `task_factory` reads (`site`, `query`, etc.).

---

## Namespaces

Every Redis key is prefixed `scrapecore:{namespace}:...`. Use one namespace
per logical scraping system if you share Redis with other consumers. The
namespace passed to the coordinator MUST match the one passed to its
agents.

---

## Running the tests

```bash
pip install -r requirements-test.txt

# unit suite (no live services required)
python -m pytest

# end-to-end integration test (needs a live Redis)
docker run --rm -p 6379:6379 redis:7-alpine &
python -m pytest -m integration
```

The unit suite uses `fakeredis` and `aioresponses` — no Redis, no network.

---

## Architecture reference

```
                        ┌─────────────────────────────┐
                        │         SERVER              │
  CRM / API  ──POST──▶  │  JobStore                   │
                        │      │                      │
                        │  Coordinator                │
                        │  ├── dispatch_loop          │
                        │  ├── result_loop            │
                        │  ├── recovery_loop          │
                        │  └── heartbeat_loop         │
                        │      │            ▲         │
                        │   TaskQueue   ResultQueue   │
                        │      │(Redis)      │(Redis) │
                        └──────┼─────────────┼────────┘
                               │             │
              ┌────────────────┼─────────────┼────────────────┐
              │  AGENT (machine-01)          │                │
              │                │             │                │
              │  Worker 0 ◀────┘             │                │
              │  Worker 1 ◀────claim()    push(result)        │
              │  Worker N                    │                │
              │       │                      │                │
              │  parser_registry             │                │
              │  rate_limiter ───────────────┘                │
              │  heartbeat_loop                               │
              └───────────────────────────────────────────────┘
```

| Component               | Role                                                      |
|-------------------------|-----------------------------------------------------------|
| `Coordinator`           | Polls job store, dispatches tasks, drains results.        |
| `Agent`                 | Runs N worker coroutines per machine.                     |
| `TaskQueue`             | Two-list Redis queue with atomic claim and stale recovery.|
| `ResultQueue`           | Single Redis list of `ResultEnvelope`s.                   |
| `TaskEnvelope`          | Unit of work. Carries `parser_key` and consumer payload.  |
| `ResultEnvelope`        | Outcome of a single task execution.                       |
| `BaseParser`            | Consumer-supplied parse + task-building logic.            |
| `BaseOutput`            | Optional typed wrapper over parser return dicts.          |
| `ParserRegistry`        | Maps `parser_key` → parser, routes jobs by `site`.        |
| `JobStoreAdapter`       | Narrow interface to whatever holds job records.           |
| `DistributedRateLimiter`| Optional Redis-backed global rate cap per domain.         |

---

## Status

Library is in active development. Public surface (envelope schemas,
`BaseParser` contract, registry, adapter shape) is stable enough to build
on; internal recovery/heartbeat behaviour may change without notice. See
the test suite in `tests/` for executable usage examples of every layer.
