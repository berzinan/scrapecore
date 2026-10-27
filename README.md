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
   that takes a raw HTTP response and a metadata dict and returns a plain,
   JSON-serializable `dict`. Subclass `BaseParser`.

2. **An entry point** — for each site, exactly one parser implements
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
from scrapecore.plugins.base import BaseParser
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

The parser MUST return a plain, JSON-serializable `dict` (str, int, float,
bool, None, and nested dicts/lists of those). 
Anything else — an object, a `datetime`, a `set` — fails the task with a
`TypeError`.

Parsers run on the **coordinator**, not on agents. When a page should be
fetched again — a captcha or block page served with HTTP 200 — raise
`RetryableParseError` and the task is re-queued (counting against
`max_retries`). Any other exception is treated as permanent: the task is
exhausted immediately and the fetched result, raw page included, goes to
the result dead-letter list so it can be re-parsed once the parser is fixed.

```python
from scrapecore.plugins import RetryableParseError

def parse(self, raw, metadata):
    if "captcha" in raw:
        raise RetryableParseError("captcha page")
    ...
```

### 2. Start an agent

Run one of these on every worker machine. Agents only **fetch**: they hold
no parsers, and send each raw response back for the coordinator to parse.

An agent talks to the server through a **transport**, never to Redis
directly. `RedisTransport` connects straight to Redis, which suits local
development and agents inside your own network.

```python
# agent_main.py
import asyncio
from scrapecore.queue.redis_client import create_redis_client
from scrapecore.agent.agent import Agent
from scrapecore.transport import RedisTransport


async def main():
    redis = create_redis_client()

    agent = Agent(
        agent_id="machine-01",
        transport=RedisTransport(redis, namespace="default"),
        rate_limit_config={"mysite.parse_item": 2.0},
    )
    await agent.start()

asyncio.run(main())
```

### 3. Start the coordinator

Run this on your server, alongside Redis and whatever API submits jobs.
The coordinator runs the parsers on every fetched response.

```python
# coordinator_main.py
import asyncio
from scrapecore.queue.redis_client import create_redis_client
from scrapecore.coordinator.coordinator import Coordinator, JobStoreAdapter
from scrapecore.plugins import ParserRegistry
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
        parsers=registry.as_callable_dict(),
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
detail requests. Express that by wrapping Stage 1's output with
`with_stage_output()`, then implement `build_stage_tasks` on the parser
named by `next_parser_key`.

```python
from scrapecore.plugins.base import BaseParser, with_stage_output


class SearchParser(BaseParser):
    key = "mysite.parse_search"

    def parse(self, raw, metadata):
        return with_stage_output(
            # final output, delivered to on_result like any other (can be empty):
            {"search_query": metadata["query"]},
            # parser whose build_stage_tasks() builds the next stage:
            next_parser_key="mysite.parse_detail",
            # data build_stage_tasks() needs, available as result.stage_output:
            catalogs=raw["data"]["catalogs"],
            query=metadata["query"],
        )

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

Retries happen at 2 levels. Within a single execution, agent retries HTTP 429 and 5xx responses in-place: up-to 5 attempts, sleeping 10s, 20s, 40s, and 80s between them. These in-place retries do not consume `retry_count`, and the agent renews its claim on thetask every 20s while it waits, so stale-task recovery does not mistake it for a dead agent. Other 4xx statuses, network errors and parser exceptions skip this and fail execution immediately. Only then does the queue-level retry below apply.

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

**Local (per-agent, always on).** Pass `rate_limit_config` to the `Agent` constructor: a dict mapping `parser_key` to requests per second. Each key gets its own independent bucket, so pipeline stages with different server-side limits can be tuned separately. Parser keys not listed fall back to 1 rps per domain. An agent runs 1 task at a time, so the limit applies to that single loop.

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
## Agent presence

Each agent publishes a JSON heartbeat to Redis every
`heartbeat_interval` seconds (default 10s) under
`scrapecore:{namespace}:heartbeat:{agent_id}`, with a TTL of 3× the
interval. Presence is **key existence**, not a field: when an agent
crashes, sleeps, or loses its Redis connection, the key expires on its own and the agent disappears from the listing. No cleanup code runs.

The payload:

| Field                | Meaning                                                       |
|----------------------|---------------------------------------------------------------|
| `agent_id`           | Set from the key on write, not from the payload.              |
| `last_seen`          | The agent's own clock. **Informational only.**                 |
| `host`               | `socket.gethostname()` of the agent process.                  |
| `pid`                | Agent OS pid.                                                 |
| `version`            | `scrapecore` package version, or `"unknown"` when running from source. |
| `started_at`         | Agent's own clock at startup.                                 |
| `state`              | `"idle"` or `"busy"`.                                         |
| `current_parser_key` | Parser key of the in-flight task, or `null` when idle.        |
| `tasks_completed`    | Lifetime counter.                                             |
| `tasks_failed`       | Lifetime counter.                                             |


Enumerate live agents:

```python
from scrapecore.coordinator.coordinator import Coordinator
# or, from the API process (no Coordinator needed):
from scrapecore.queue.presence import AgentPresence
from scrapecore.queue.redis_client import create_redis_client

presence = AgentPresence(create_redis_client(), namespace="default")
agents = await presence.list_online()   # sorted by agent_id
```
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
(Postgres, Redis hashes, DynamoDB, etc.) — keep the same method shape (all methods are async):

```python
class JobStoreAdapter:
    async def claim_pending_jobs(self) -> list[dict]: ...   # atomic pending → running
    async def get_running_jobs(self) -> list[dict]: ...
    async def mark_completed(self, job_id: str, summary: dict[str, int]) -> None: ...
    async def mark_failed(self, job_id: str, error: str) -> None: ...
    async def is_cancelled(self, job_id: str) -> bool: ...
    async def get(self, job_id: str) -> dict | None: ...
```

`claim_pending_jobs` **must be atomic**: a job it returns must never be
returned to another caller. In SQL that is one statement:

```sql
UPDATE jobs SET status = 'running' WHERE status = 'pending' RETURNING *;
```

`mark_completed` receives outcome counts (`{"succeeded": n, "exhausted": m}`),
not task outputs. Persist outputs per task in `on_result`.

A `JobRecord` must always have at least: `job_id`, `status` (`pending`,
`running`, `completed`, `failed` or `cancelled`), `error`, plus any fields
your `task_factory` reads (`site`, `query`, etc.).

### Coordinator state and Redis persistence

Which tasks each running job is still waiting on is stored in Redis
(`scrapecore:{namespace}:jobs:active` and `scrapecore:{namespace}:job:{id}:*`),
so a restarted coordinator resumes running jobs where it left off. For this
to survive a **Redis** restart as well, run Redis with append-only
persistence and a persistent volume:

```yaml
redis:
  image: redis:7-alpine
  command: ["redis-server", "--appendonly", "yes"]
  volumes:
    - redisdata:/data
```

### One active coordinator per namespace

Only 1 coordinator per namespace may run at a time.
This is enforced with a Redis lock
(`scrapecore:{namespace}:coordinator:lock`):

- `start()` takes the lock before doing anything. If another coordinator
  holds it, the new one waits as a **standby** and takes over within
  `lock_ttl` (default 30s) of the active one dying.
- The active coordinator renews the lock every `lock_ttl / 3`. If it ever
  finds the lock gone (e.g. it was frozen longer than `lock_ttl` and a
  standby took over), it stops all work and `start()` raises
  `CoordinatorLockLost`. Restart the process and it rejoins as a standby.
- A clean `stop()` releases the lock immediately.

---

## Remote agents: the gateway

Agents on machines outside your server (employee laptops) must not reach
Redis. They connect to the **gateway**, a small HTTPS service that runs next
to the coordinator and performs each queue operation on their behalf. Only
the gateway's port is exposed; Redis, the database and the coordinator stay
private.

### On the server

```bash
# 1. One-time: create a self-signed certificate (needs openssl; on Windows, run in WSL)
scrapecore-gateway gen-cert --cert gateway-cert.pem --key gateway-key.pem
#    → prints the certificate fingerprint; agents need it

# 2. Issue a token per agent (shown once; only a hash is stored)
scrapecore-gateway add-agent laptop-anna

# 3. Run the gateway (same REDIS_* environment variables as the coordinator)
scrapecore-gateway serve --cert gateway-cert.pem --key gateway-key.pem --port 8443
```

Manage access later with `revoke-agent NAME`, `list-agents`, and
`add-agent NAME` again to rotate a token. Add `--min-agent-version X.Y.Z`
to `serve` to refuse outdated agents (they get HTTP 426 and stop).

### On each agent machine

```python
from scrapecore.agent.agent import Agent
from scrapecore.transport import HttpTransport

agent = Agent(
    agent_id="laptop-anna",
    transport=HttpTransport(
        "https://<server-ip>:8443",
        token="<token from add-agent>",
        cert_fingerprint="<fingerprint from gen-cert>",
    ),
)
await agent.start()
```

The agent accepts **only** the certificate with that fingerprint, so no
domain name or certificate authority is needed and the connection cannot
be intercepted.

### What the gateway enforces

- **Identity comes from the token.** Results and presence are recorded
  under the token's agent name, whatever the agent claims to be.
- **Only the claiming agent can renew or submit a task** (others get 409).
- **The server's copy of each task is authoritative.** An agent cannot
  change a task's URL, job or retry count by sending back a modified one.
- **Parsing stays on the server.** Submitted results may carry only the raw
  response; any parsed output from an agent is discarded.

### Network resilience

Agents ride out connectivity loss: an unreachable gateway is retried with
backoff (up to 30s between attempts), a failed submit is retried before
giving up, and a task whose result never arrives is re-run after stale-task
recovery. A revoked token or an outdated version stops the agent, with
`AuthenticationFailed` or `UpgradeRequired` raised from `agent.start()`.

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
                        │  ├── result_loop (parses)   │
                        │  ├── recovery_loop          │
                        │  ├── heartbeat_loop         │
                        │  └── lock_loop              │
                        │      │            ▲         │
                        │   TaskQueue   ResultQueue   │
                        │      │(Redis)      │(Redis) │
                        └──────┼─────────────┼────────┘
                               │             │
                         ┌─────┴─────────────┴─────┐
                         │  AgentTransport         │
                         │  Redis | HTTPS→gateway  │
                         └─────┬─────────────▲─────┘
              ┌────────────────┼─────────────┼────────────────┐
              │  AGENT (machine-01)          │                │
              │                │             │                │
              │  run loop ◀────┘             │                │
              │  claim → rate-limit →   submit(raw response)  │
              │  fetch → submit              |                │
              │       │                      │                │
              │  auth_provider               │                │
              │  rate_limiter ───────────────┘                │
              │  heartbeat_loop                               │
              └───────────────────────────────────────────────┘
```

| Component               | Role                                                      |
|-------------------------|-----------------------------------------------------------|
| `Coordinator`           | Polls job store, dispatches tasks, drains results.        |
| `Agent`                 | Sequential claim/fetch/submit loop; returns raw responses. Scale = more Agents. |
| `AgentTransport`        | Interface between an agent and the server: claim, renew, submit, heartbeat, leave. |
| `RedisTransport`        | `AgentTransport` that talks to the Redis queues directly.  |
| `HttpTransport`         | `AgentTransport` for remote agents: HTTPS to the gateway, pinned certificate. |
| `create_gateway_app`    | The gateway: authenticated HTTPS front for the queues.     |
| `AgentTokenStore`       | Per-agent tokens for the gateway, stored hashed in Redis.  |
| `AgentPresence`         | Reads/writes Redis heartbeat keys; `list_online()` enumarates live agents. |
| `TaskQueue`             | Two-list Redis queue with atomic claim and stale recovery.|
| `ResultQueue`           | Two-list Redis queue of `ResultEnvelope`s: claim/ack, requeue, dead-letter. |
| `JobTracker`            | Redis record of each running job's outstanding tasks.     |
| `TaskEnvelope`          | Unit of work. Carries `parser_key` and consumer payload.  |
| `ResultEnvelope`        | Outcome of a single task execution.                       |
| `BaseParser`            | Consumer-supplied parse + task-building logic.            |
| `with_stage_output()`   | Wraps a parser's output to trigger the next stage.        |
| `ParserRegistry`        | Maps `parser_key` → parser, routes jobs by `site`.        |
| `CoordinatorLock`       | Redis lease: one active coordinator per namespace, others wait as standby. |
| `JobStoreAdapter`       | Narrow interface to whatever holds job records.           |
| `DistributedRateLimiter`| Optional Redis-backed global rate cap per domain.         |

---

## Status

Library is in active development. Public surface (envelope schemas,
`BaseParser` contract, registry, adapter shape) is stable enough to build
on; internal recovery/heartbeat behaviour may change without notice. See
the test suite in `tests/` for executable usage examples of every layer.
