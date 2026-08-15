# Changelog

Roost follows simple release notes: what changed, why it matters operationally,
and anything users should know before upgrading.

## 0.2.0 - Unreleased

Production-mode foundation for running Roost with Redis for movement and
Postgres for durable operational memory.

### Added

- Postgres-backed production mode for work items, snapshots, leases, resource
  claims, events, DLQ entries, artifact metadata, and worker heartbeats.
- `roost migrate` for applying packaged Postgres migrations.
- Production-mode `roost doctor` checks for Postgres connectivity and applied
  migrations.
- Production-aware operator commands for status, inspect, list, events, retry,
  cancel, DLQ, and worker heartbeats.
- `roost workers` for inspecting active and stale worker heartbeats.
- Production-aware local console with work, events, failed work, artifacts, and
  workers backed by Postgres state.
- `examples/production/` Docker Compose sandbox for Redis + Postgres.
- Postgres e2e scripts for migration idempotency and production-mode
  crash/resume.
- CI coverage for Redis e2e, Postgres migrations, and production-mode e2e.
- Postgres connection pool so lease renew and snapshot save can run concurrently.
- Per-step Postgres transaction so snapshot, meta, events, and child links commit together.
- Postgres-native orphan recovery (Redis inflight is a hint, not the discovery set).
- Atomic resource claims (`INSERT … ON CONFLICT … WHERE expired or same owner`).
- `roost_work_meta.children` (migration 0002) so `link_child` matches simple mode.
- Operator action records from CLI/console cancel, retry, and DLQ commands; `roost actions`.
- Optional Prometheus counters scraped at `GET /metrics` on the console (`[metrics]` extra).
- Worker JSON step logs on stderr (`roost worker --log-level`); `--stale-after`,
  `--recovery-interval`, and `--heartbeat-interval` on the worker (also
  `[worker]` in `roost.toml`).
- `work_recovered` events when orphan recovery re-enqueues work.
- Wait-only steps re-enqueue without burning a snapshot version.
- `roost workers` JSON `age_seconds`; console Workers view shows age.
- Mid-step crash + Redis-blip drill (`scripts/ops_drill_crash_and_redis_blip.sh`).

### Changed

- Docs and operator labels now say `dlq replay` re-enqueues from the latest
  snapshot; step-by-step snapshot history remains on the roadmap.
- Trigger conditions are a frozen one-level key lookup (`snapshot.data.<key>` /
  `item.payload.<key>` / bare key). Nested paths are rejected.

### Operational Notes

- Simple mode remains Redis-only.
- Production mode still uses Redis for queueing and in-flight markers.
- Postgres is the durable system of record in production mode.
- Object storage adapters, retention controls, backup/restore guidance, and
  project/environment separation remain roadmap items.

## 0.1.0

Initial public release of Roost as a tiny runtime for durable, resumable agent
workers.

### Added

- Redis + SAQ backed runtime for long-running agent step-machines.
- Durable `WorkItem` and `Snapshot` models.
- Per-work leases with renewal.
- Optional resource claims for best-effort worker isolation.
- At-least-once step execution with retry and delayed continuation.
- Orphan recovery for work left in-flight after worker failure.
- Runtime events, status metadata, failed-work tracking, and a dead-letter queue.
- Content-addressed local artifact storage.
- Engine registry through Python entry points.
- Watchlist demo that persists observations and resumes after worker restart.
- Local console for work, snapshots, events, artifacts, failures, and recovery actions.
- `roost init` for creating `roost.toml`.
- `roost doctor` for validating local runtime setup.
- CLI recovery commands: `inspect`, `retry`, `cancel`, `dlq list`, `dlq replay`, and `dlq ack`.
- Phase-gated roadmap, contribution notes, troubleshooting docs, release checklist, and CI.

### Operational Notes

- The 0.1.0 production path was Redis-backed.
- Execution is at-least-once; engine `step()` implementations should be safe to retry from the same snapshot.
- The console is local-only by default.
- Postgres as the production system of record was planned, but not included in
  this release.
