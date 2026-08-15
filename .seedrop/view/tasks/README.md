# Tasks

Actionable fixes keyed to audits and drills. Each task is self-contained:
problem (with file:line), "done when" checklist, notes, and risk.

## Current sprint — read this first
- **`TASK-SPRINT-OPS.md`** — make crash + Redis-blip boring (operability).
  Evidence: `.seedrop/view/knowledge/ops-drill.md`.
  - `TASK-OPS-1-worker-logs.md`
  - `TASK-OPS-2-recovery-knobs.md`
  - `TASK-OPS-3-recovery-event.md`
  - `TASK-OPS-4-skip-wait-saves.md`
  - `TASK-OPS-5-worker-liveness.md`
  - `TASK-OPS-6-midstep-crash-drill.md`

## Previous sprint (landed in PR #13)
- **`TASK-DEPLOY-GATE.md`** — production-mode durability. P0/P1/P2 task files
  below are the original finding set; treat them as done unless a file still
  says open and contradicts the PR.

## Original finding set (audit.md)
### P0 — correctness bugs in the production hot path
- `TASK-P0-1-postgres-connection-pool.md`
- `TASK-P0-2-step-transaction-boundary.md`
- `TASK-P0-3-postgres-native-recovery.md`

### P1 — incorrect/oversold claims
- `TASK-P1-1-replay-claim-reconciliation.md`
- `TASK-P1-2-postgres-link-children.md`
- `TASK-P1-3-operator-actions-table.md`

### P2 — design smells
- `TASK-P2-1-resource-lock-atomicity.md`
- `TASK-P2-2-trigger-dsl-decision.md`
- `TASK-P2-3-observability-floor.md` — metrics shipped; logs still dark (OPS-1)
- `TASK-P2-4-eager-store-resolution.md`
