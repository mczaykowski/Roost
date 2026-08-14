# Roost Roadmap

Roost is meant to become the smallest trustworthy runtime for long-running
agent work: durable work, resumable state, leases, resource locks, artifacts,
retries, observability, and operator visibility.

The direction is intentionally narrow:

- OSS adoption first.
- Python-first runtime and engine SDK.
- Postgres as the production system of record.
- Redis remains useful for local development, queueing, and lightweight runtime
  setups.
- Bring-your-own workers before managed execution.
- No prompt framework, model router, chain DSL, or workflow-language creep.

The storage architecture is captured in
[docs/runtime-storage.md](docs/runtime-storage.md): Redis is movement, Postgres
is memory, and object storage is evidence.

## North Star

Roost should make production agent work feel boring in the best way:

```text
create work
  -> lease it to one worker
  -> run one durable step
  -> save a snapshot
  -> continue, wait, retry, fail, or finish
  -> keep enough history for an operator to understand what happened
```

The engine owns domain-specific behavior. Roost owns the operational substrate.

## Current Status

Roost now has the first production-shaped OSS foundation in place:

- Simple mode remains Redis-only for local development and demos.
- Production mode uses Redis for queueing/in-flight movement and Postgres for
  durable work items, snapshots, leases, resource claims, events, DLQ entries,
  artifact metadata, and worker heartbeats.
- `roost migrate` and production-mode `roost doctor` are available.
- Operator commands and the local console can read production-mode state.
- `examples/production/` provides a Redis + Postgres Docker Compose sandbox.
- CI exercises Redis e2e, Postgres migrations, and production-mode crash/resume.

The remaining roadmap is about making that foundation more complete for teams:
snapshot history beyond the latest accepted snapshot, retention, backup/restore
guidance, stronger audit trails, object storage, environment separation, and
eventually cloud control-plane workflows.

## Phase 0: Make The Project Legible

Goal: a serious engineer lands on GitHub and understands Roost in under five
minutes.

Ship:

- A README centered on one promise: durable, resumable agent work.
- A quickstart that starts Redis, runs a worker, enqueues work, restarts the
  worker, and prints an artifact.
- The watchlist demo as the canonical capability demo.
- Screenshots of the local console and demo flow.
- Local e2e instructions.
- Troubleshooting notes for Redis, artifacts, workers, and namespaces.
- Contribution notes, issue templates, CI, and a release checklist.

Gate:

- A fresh machine can clone the repo, run the demo, open the console, and
  understand what Roost does without reading source code.

## Phase 1: Production Runtime Core

Goal: Roost becomes safe enough to run real background agent workloads.

Ship:

- Postgres-backed durable storage as the canonical production backend. Shipped
  for current work items, snapshots, leases, resource claims, events, DLQ,
  artifact metadata, and worker heartbeats.
- Internal store boundaries for work items, snapshots, leases, events, resource
  claims, artifacts metadata, retry state, dead-letter entries, and worker
  heartbeats. Initial boundaries are in place.
- A boring migration path with `roost migrate`. Shipped.
- Worker identity, stale lease recovery, and crash-resume behavior as
  first-class concepts. Initial production-mode behavior is covered by e2e.
- Clear retry and dead-letter behavior. Initial CLI and console controls are
  shipped.
- Structured logs, health checks, and metrics hooks.
- Honest runtime guarantees in the docs: at-least-once execution, resumable
  snapshots, lease ownership, resource locking, and engine idempotency
  expectations.

Gate:

- Killing a worker mid-step, restarting infrastructure, and re-running failed
  work from its latest snapshot behaves predictably and is covered by tests.

## Phase 2: Plug-And-Play Operator Experience

Goal: production setup should feel simple, not ceremonial.

Ship:

- `roost init` to create a minimal `roost.toml`. Shipped.
- `roost doctor` to validate backend connectivity, migrations, queues, artifact
  storage, and worker health. Shipped for core local and production setup.
- `roost dev` for local demo/runtime startup.
- CLI commands for inspect, retry, cancel, resume, list dead-lettered work,
  re-enqueue dead-lettered work from its latest snapshot, worker heartbeats, and view artifacts. Shipped for
  the current operator surface except a separate `resume` command.
- A local console with work list, detail view, snapshot timeline, artifacts,
  events, retry controls, failed work, and worker status. Shipped as a local
  console, with more timeline polish still useful.
- Human UI wording: Work, Running, Waiting, Done, Failed, Retry, Resume, Last
  step.
- Docker Compose examples for Postgres and Redis. Shipped in
  `examples/production/`.
- Deployment recipes for small-team paths.

Gate:

- A user can start from nothing, initialize Roost, run a worker, inspect work,
  recover a failed job, and understand what happened.

## Phase 3: Team-Grade OSS

Goal: Roost becomes credible for teams before cloud exists.

Ship:

- Namespaces, projects, and environments for separating workloads.
- Artifact storage adapters: local filesystem first, then S3-compatible storage.
- Audit-friendly event history for operator actions.
- A basic auth story for exposed UI deployments.
- Config validation and compatibility policy.
- Retention controls for events, snapshots, artifacts, and completed work.
- Backup and restore guidance for Postgres-backed deployments.
- A stable runtime contract and versioned migration policy.
- Example production workloads: coding-agent tasks, document processing,
  research and monitoring jobs, and integration workflows.

Gate:

- A small team can run Roost themselves and feel the operational burden is lower
  than building their own durable runtime.

## Phase 4: Roost Cloud With BYO Workers

Goal: paid cloud starts as hosted visibility and coordination, not managed
execution.

Ship:

- Hosted dashboard for orgs, projects, environments, workers, work, events,
  retries, and artifacts.
- Worker registration with project tokens.
- Secure event and heartbeat ingestion from customer-run workers.
- Alerts via email, Slack, and webhooks for failed work, stuck work,
  dead-letter entries, and missing workers.
- Hosted artifact metadata and optional object storage integration.
- Team features: members, roles, API keys, audit log, and environment
  separation.
- Usage-based billing around projects, retained history, events, artifacts, and
  alerting.

Gate:

- A team can run workers in its own infrastructure and pay Roost Cloud because
  hosted observability, alerts, history, and recovery controls save real
  operational time.

## Phase 5: Managed Execution Later

Goal: only add managed workers after the control plane is trusted.

Ship:

- Managed worker pools for users who want zero infrastructure.
- Secure secret injection and per-project execution isolation.
- Job templates and scheduled runs.
- Hosted queues and storage.
- Enterprise controls: SSO, SCIM, audit exports, retention policies, and
  private networking.

Gate:

- Managed execution is introduced only after Roost Cloud already has teams
  relying on bring-your-own workers.

## Public Shape

The core engine API should stay small:

```python
async def init_snapshot(item) -> Snapshot: ...
async def step(snapshot, item) -> Snapshot: ...
```

Roost should add production interfaces around that contract, not replace it:

- `roost.toml` for backend, queue, artifacts, namespace, retention, and worker
  configuration.
- CLI commands: `init`, `dev`, `doctor`, `migrate`, `worker`, `list`,
  `inspect`, `retry`, `workers`, `resume`, `cancel`, and `dlq`.
- Internal backend boundaries: work store, snapshot store, lease store, event
  store, artifact store, and queue adapter.
- Cloud worker protocol later: register, heartbeat, upload events, upload
  artifact metadata, and receive control actions.

## Test Strategy

The project should earn production trust with tests that exercise operational
failure, not only happy paths:

- Unit tests for runtime state transitions, leases, retries, resources,
  artifacts, and event recording.
- Integration tests against Postgres and Redis.
- Crash recovery tests that kill a worker during execution and confirm resumed
  progress.
- Duplicate delivery tests for at-least-once execution.
- Resource lock tests with competing workers.
- Migration tests from empty databases and prior schema versions.
- CLI smoke tests for `init`, `doctor`, `migrate`, `retry`, and `dlq`.
- UI smoke tests for list, detail, failed work, retry, and empty states.
- The watchlist e2e as the canonical confidence check in CI.
