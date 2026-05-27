# Changelog

Roost follows simple release notes: what changed, why it matters operationally,
and anything users should know before upgrading.

## 0.1.0 - Unreleased

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

- The current production path is Redis-backed.
- Execution is at-least-once; engine `step()` implementations should be safe to retry from the same snapshot.
- The console is local-only by default.
- Postgres as the production system of record is planned, but not included in this release.

