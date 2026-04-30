# Roost Runtime

Roost is a lightweight runtime for durable agent step-machines.

Agent demos are easy. Long-running agent workers are not.

The moment an agent leaves a notebook or chat session, the hard problems change:

- What work exists?
- Which worker owns it right now?
- What was the last durable step?
- Can another worker resume after a crash?
- Which resources are locked?
- What did the agent produce?
- Can operators inspect, retry, replay, or dead-letter the work?

Roost exists for that layer.

Bring your own engine. Roost handles the operational substrate: work items,
snapshots, leases, retries, resource claims, delayed continuation, artifacts,
status indexes, events, and dead-letter recovery.

```python
class Engine:
    engine_id: str

    async def init_snapshot(self, item: WorkItem) -> Snapshot: ...
    async def step(self, snapshot: Snapshot, item: WorkItem) -> Snapshot: ...
```

Engines own domain-specific transitions. The runtime owns durability.

## Why

Roost is for systems where work may take minutes or days, workers may disappear,
multiple agents may compete for the same resources, and every step needs to be
inspectable and recoverable.

It is intentionally small. You do not need to adopt a prompt framework, graph DSL,
model router, hosted control plane, or heavyweight workflow platform. Implement
two methods, run a worker, and let the runtime persist progress between steps.

## How It Differs

Roost is not a replacement for LangChain, LlamaIndex, CrewAI, AutoGen, Temporal,
Celery, or your own agent loop. It sits at a different layer.

| Tool category | Great for | Roost's difference |
| --- | --- | --- |
| LangChain-style frameworks | prompts, tools, retrieval, chains, agent composition | Roost does not prescribe cognition. It runs any engine as a durable step-machine. |
| Temporal-style workflow engines | general distributed workflows with strong orchestration semantics | Roost is much smaller and agent-shaped: snapshots, resources, artifacts, and resumable engine steps without a workflow DSL. |
| Celery-style queues | fire-and-forget background jobs | Roost persists progress after every step, renews leases, recovers orphaned work, and exposes agent state. |
| Cron/scripts | simple repeated automation | Roost gives long-running work identity, retry state, locks, events, DLQ, and inspection. |

The short version:

```text
LangChain helps decide what an agent should do.
Temporal helps coordinate workflows.
Celery runs jobs.
Roost keeps long-running agents alive, inspectable, and resumable.
```

## Install

```bash
uv sync --extra redis --extra dev
```

## Quickstart

Start Redis locally:

```bash
docker run --rm -p 6379:6379 redis:7
```

In one terminal, run a worker:

```bash
uv run roost worker --engines demo
```

In another terminal, enqueue demo work:

```bash
WORK_ID=$(uv run roost enqueue \
  --engine demo \
  --payload '{"message":"hello durable world","count_to":3}')

uv run roost status "$WORK_ID"
uv run roost events
```

The demo engine increments one counter per runtime step. Stop the worker between
steps and start it again; progress resumes from the latest saved snapshot.

## Runtime Shape

```text
WorkItem
  -> Engine.init_snapshot()
  -> Snapshot persisted
  -> Engine.step(snapshot)
  -> Snapshot persisted
  -> re-enqueue until done
```

The runtime owns the queue, leases, retries, resource claims, status metadata,
events, and recovery loop. The engine owns the state transition.

## Core Primitives

- `WorkItem`: durable unit of work.
- `Snapshot`: replayable engine state after each step.
- `Lease`: time-bound worker ownership.
- `Artifact`: content-addressed output produced by an engine.
- `Engine`: small async contract for pluggable execution.
- `RedisSwarm`: Redis + SAQ backed scheduler, lease manager, retry loop, and recovery path.

## Build An Engine

```python
from roost.runtime.models import Snapshot, WorkItem


class MyEngine:
    engine_id = "my-engine"

    async def init_snapshot(self, item: WorkItem) -> Snapshot:
        return Snapshot(
            work_id=item.work_id,
            engine=self.engine_id,
            step="start",
            data={"payload": item.payload},
        )

    async def step(self, snapshot: Snapshot, item: WorkItem) -> Snapshot:
        new_snapshot = snapshot.model_copy()
        new_snapshot.step = "done"
        new_snapshot.is_finished = True
        return new_snapshot


def build_engine(**kwargs):
    return MyEngine()
```

Expose it as an entry point:

```toml
[project.entry-points."roost.engines"]
my-engine = "my_package.engine:build_engine"
```

Then run:

```bash
uv run roost worker --engines my-engine
uv run roost enqueue --engine my-engine --payload '{"task":"ship it"}'
```

## Runtime Guarantees

Roost uses at-least-once execution. Engines should make each `step()` safe to retry
from the same snapshot. The runtime provides:

- optimistic snapshot persistence
- per-work leases with renewal
- optional resource locks
- delayed continuation via `next_step_delay_seconds`
- orphan recovery when a worker dies mid-step
- status metadata and event stream
- bounded retries and dead-letter queue

## CLI

```bash
uv run roost engines
uv run roost enqueue --engine demo --payload '{"count_to":5}'
uv run roost worker --engines demo --concurrency 4
uv run roost status <work_id>
uv run roost list
uv run roost events
uv run roost workspace-path <work_id>
```

Useful environment variables:

- `ROOST_REDIS_URL`
- `ROOST_QUEUE`
- `ROOST_REDIS_PREFIX`
- `ROOST_NAMESPACE`
- `ROOST_ARTIFACT_ROOT`

## What This Is Not

Roost is not a prompt framework, model router, or agent personality system. It is
the runtime underneath those systems.

The goal is not to replace your agent framework. The goal is to make your agent
framework survive contact with production.
