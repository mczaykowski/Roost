# Tasks

Actionable, deployable fixes keyed to `.seedrop/view/knowledge/audit.md`.
Each task is self-contained: problem (with file:line), "done when" checklist,
notes, and risk. Read the audit first.

## Index — read this first
- **`TASK-DEPLOY-GATE.md`** — the index and sequencing plan. "Deployable in a
  few steps" = the P0 trio (1–3) with tests, plus the P1 doc reconciliation.

## P0 — correctness bugs in the production hot path
- `TASK-P0-1-postgres-connection-pool.md` — shared single session → concurrency crash
- `TASK-P0-2-step-transaction-boundary.md` — no transaction around a step; durable state can be inconsistent
- `TASK-P0-3-postgres-native-recovery.md` — recovery discovery is Redis-only; violates policy agreement #5

## P1 — incorrect/oversold claims (cheap to fix, high trust payoff)
- `TASK-P1-1-replay-claim-reconciliation.md` — "replay" overstates latest-only snapshots
- `TASK-P1-2-postgres-link-children.md` — `link_child` is a functional regression in production mode
- `TASK-P1-3-operator-actions-table.md` — `roost_operator_actions` is dead schema

## P2 — design smells that will hurt at scale
- `TASK-P2-1-resource-lock-atomicity.md` — `pg_advisory_xact_lock` releases at commit; serializes nothing
- `TASK-P2-2-trigger-dsl-decision.md` — baby DSL; delete or freeze
- `TASK-P2-3-observability-floor.md` — no structured logs/metrics floor for a "production runtime"
- `TASK-P2-4-eager-store-resolution.md` — lazy store swap adds hot-path indirection

## Sequencing
P0-1 → P0-2 (cleanest after pooling, but parallelizable via `commit=False`).
P0-3 independent. P1 batch rides in the same release as the P0 trio so the
README never ships overclaiming against the new code. P2 are improvements,
not gates — see `TASK-DEPLOY-GATE.md`.
