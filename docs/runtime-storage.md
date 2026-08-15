# Runtime Storage Architecture

Roost should stay simple locally and become trustworthy in production.

The long-horizon storage split is:

```text
Redis = movement
Postgres = memory
Object storage = evidence
```

This is not a migration away from Redis. Redis is excellent for fast runtime
coordination. The production question is different: can a team understand,
query, audit, retain, and explain what happened across hundreds of agents over
months?

## Core Principle

Roost is not just a queue. Roost is operational memory for long-running agent
work.

Every storage decision should support one of these runtime promises:

- Work has identity.
- Progress is snapshotted.
- One worker owns a step at a time.
- Resources can be claimed.
- Failures are inspectable.
- Work can resume.
- Operators can intervene.
- Teams can understand what happened later.

If a storage feature does not make those promises simpler to trust, it should
not be added.

## Simple Mode

Simple mode is Redis-only.

It should remain the default for:

- local development
- demos
- lightweight self-hosting
- small deployments
- quick evaluation

Simple mode optimizes for:

- one dependency
- fast startup
- low ceremony
- easy mental model
- excellent demo ergonomics

Redis-only is not a mistake. It is the reason Roost feels small.

## Production Mode

Production mode adds Postgres as the durable system of record. Object storage is
the intended next evidence layer for larger artifacts; the current artifact
store remains local filesystem metadata plus content-addressed files.

It should be used when teams run many agents across projects, verticals, or
customers and need durable operational history.

Production mode optimizes for:

- queryable work history
- event history
- retention policy
- incident debugging
- worker fleet visibility
- customer/project/environment separation
- reliable backups and restore
- future Roost Cloud control plane

## Redis Responsibilities

Redis owns fast movement and coordination:

- queues
- delayed jobs (`next_step_delay_seconds` is movement, not a new snapshot version)
- leases
- in-flight markers
- resource locks
- short-lived worker coordination
- local console state in simple mode
- recent events in simple mode

Redis is the right tool for data that is:

- latency-sensitive
- ephemeral
- naturally TTL-bound
- coordination-oriented
- safe to rebuild from durable state in production mode

Examples:

```text
work_step queue
lease:<work_id>
inflight:<work_id>
resource:<resource_id>
```

## Postgres Responsibilities

Postgres owns durable operational memory:

- work item records
- latest work metadata
- snapshots
- snapshot history (later; optional)
- event history
- dead-letter records
- worker heartbeats
- projects
- verticals
- environments
- worker groups
- operator actions (later)
- retention policy state
- audit log (later)

Postgres is the right tool for data that is:

- queryable
- durable
- relational
- audited
- retained by policy
- needed for incident review
- needed after Redis loss

Examples of production questions Postgres should answer:

```text
Which geopolitical monitoring jobs failed in the last 24 hours?
Which sources changed after 03:00 UTC?
Which worker version produced this report?
Which customer/project has the most retrying work?
Which work item was cancelled, by whom, and why?
What was the latest durable snapshot before escalation?
Which resource locks are hot across worker groups?
```

Those questions should not require hand-built Redis indexes.

## Object Storage Responsibilities

Object storage owns large or long-lived evidence:

- reports
- screenshots
- scraped documents
- raw captures
- generated files
- large logs
- evidence bundles
- trace payloads, if they become large

Postgres stores artifact metadata and pointers. Object storage stores the bytes.

Artifact metadata should include enough context for operators to search and
trust the evidence:

- artifact id
- work id
- project/environment
- kind
- content hash
- URI
- created time
- engine
- metadata

## Data Classification

| Data | Simple mode | Production mode | Notes |
| --- | --- | --- | --- |
| Queue jobs | Redis | Redis | Runtime movement. |
| Leases | Redis | Postgres | Durable ownership record in production. |
| In-flight markers | Redis | Redis | Short-lived recovery hints. |
| Resource locks | Redis | Postgres | Durable resource claims in production. |
| Work items | Redis | Postgres | Durable identity in production. |
| Latest snapshot | Redis | Postgres | Resumable progress. |
| Snapshot history | Not required | Postgres, optional | Useful for audit/debugging. |
| Work metadata | Redis | Postgres | Status, step, timestamps. |
| Events | Redis stream | Postgres | Queryable production history. |
| DLQ | Redis list | Postgres | Failed work should survive Redis loss. |
| Worker heartbeats | Not required | Postgres | Current liveness plus history. |
| Operator actions | Redis event | Postgres events now, audit log later | Required for team trust. |
| Artifact bytes | Local filesystem | Object storage | Keep large data out of DB. |
| Artifact metadata | Snapshot/local meta | Postgres | Searchable evidence index. |

## Runtime Guarantees

Roost should keep these guarantees consistent across storage modes:

- at-least-once execution
- durable snapshots after accepted steps
- one active lease per work item
- best-effort resource isolation
- bounded retries
- dead-letter visibility
- operator retry/cancel/re-run controls
- inspectable work state

Postgres does not change the engine contract. Engines should still implement:

```python
async def init_snapshot(item) -> Snapshot: ...
async def step(snapshot, item) -> Snapshot: ...
```

## Implementation Status

The first production foundation is implemented:

1. Internal store interfaces exist around core runtime concepts.
2. Redis stores sit behind those interfaces for simple mode.
3. Postgres migrations and schema exist for durable production state.
4. `roost migrate` applies packaged migrations.
5. Production mode config is available through `roost.toml`, CLI flags, and
   environment variables.
6. Redis remains the queue and in-flight movement layer.
7. The production watchlist e2e proves crash/resume through Postgres-backed
   durable state.

The next storage work should focus on:

- snapshot history beyond the latest accepted snapshot
- retention controls for events, snapshots, artifacts, and completed work
- backup and restore guidance for Postgres deployments
- object storage adapters for large artifact bytes
- audit-friendly operator action records
- project/environment separation in durable tables

Avoid:

- rewriting the runtime around SQL too early
- making Postgres the local default
- adding an ORM before the schema stabilizes
- adding generic storage abstraction that hides important semantics
- turning Roost into a general workflow engine

## Configuration Shape

The current config shape reads plainly:

```toml
[runtime]
mode = "simple" # simple | production

[redis]
url = "redis://localhost:6379/0"
queue = "default"
prefix = "roost"

[postgres]
url = "postgresql://localhost/roost"

[artifacts]
root = ".roost/artifacts"
```

Production mode should fail loudly if Postgres is missing or migrations are not
applied. `roost doctor` checks this, and
`scripts/e2e_postgres_watchlist.sh` covers the production-mode crash/resume path.

## Decision

Roost should keep Redis as the default local runtime and add Postgres as the
production system of record.

The guiding language:

```text
Redis helps Roost keep moving.
Postgres helps humans understand what happened.
Object storage keeps the evidence.
```
