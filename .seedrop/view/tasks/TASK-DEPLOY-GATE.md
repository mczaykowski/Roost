# Deploy Gate — production-mode readiness

**Severity:** tracking (this is the index, not a code task)
**Status:** landed (PR #13) — durability claims are true; operability is sprint OPS
**Blocks:** — (superseded by `TASK-SPRINT-OPS.md` for operator-readiness)
**Audit ref:** `.seedrop/view/knowledge/audit.md` §"Suggested deployment gate"

## Goal
Land the smallest set of changes that move Roost from "foundation" to a production runtime whose durability claims are true, in dependency order, so it can be deployed in a few steps.

## The few steps (in order)

1. **P0-1 — Postgres connection pool per store.** Fixes the latent `already-executing` concurrency crash. Unblocks trustworthy production-mode execution.
2. **P0-2 — One transaction per step.** Makes "durable system of record" a true statement: meta and snapshot agree after any failure.
3. **P0-3 — Postgres-native recovery scan.** Closes the Redis-flush → silently-stuck-work seam; satisfies policy working agreement #5.
4. **P1-1, P1-2, P1-3 — doc/code reconciliation.** Cheap, removes the largest first-read trust shock (replay verb, link_child asymmetry, dead operator_actions table).

After 1–3 land **with their tests**, Roost can be deployed in production mode with a defensible correctness story. P2 items are improvements, not gates.

## Acceptance (the deploy gate)
- [x] P0-1, P0-2, P0-3 merged with passing tests. (PR #13)
- [x] New `tests/test_postgres_concurrency.py` exists and passes (renew ‖ save).
- [x] New crash-injection test passes (meta/snapshot agree after mid-step failure).
      (`tests/test_postgres_transaction.py`)
- [x] New Redis-flush test passes (recovery still finds work).
      (`tests/test_postgres_recovery.py` + `scripts/ops_drill_crash_and_redis_blip.sh`)
- [x] `uv run pytest` and `uv run ruff check .` green.
- [x] README re-read: no capability is stated that the code does not guarantee.

Next: `TASK-SPRINT-OPS.md` (logs, recovery knobs/events, wait-only saves, worker
liveness, mid-step crash drill).

## Sequencing rationale
P0-2 is cleanest after P0-1 (pooling makes the per-step transaction natural), but P0-2 can proceed in parallel using the existing `commit=False` flag. P0-3 is independent. The P1 batch is doc/migration work and should ride in the same release so the README never ships overclaiming against the new code.
