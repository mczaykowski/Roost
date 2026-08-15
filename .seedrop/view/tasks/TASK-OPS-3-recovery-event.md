# OPS-3 · Emit `work_recovered` when recovery re-enqueues

**Severity:** P1 (operator cannot distinguish recover from continue)
**Status:** done
**Blocked by:** —
**Blocks:** —
**Evidence:** `.seedrop/view/knowledge/ops-drill.md` §3

## Problem
`_recover_one` (`swarm.py:736-783`) enqueues the work, `set_state`s to bump
`updated_at`, and marks inflight. It does not `push_event`. Pre-blip
`roost events` in the drill was only `work_enqueued` and `work_state_changed`.
A Redis-flush recovery is invisible in the audit surface we already have.

## Done when
- [x] Successful `_recover_one` writes one event:
      `kind=work_recovered`, `work_id`, `engine`, `step`, `snapshot_version`,
      `reason` (e.g. `stale_without_lease`).
- [x] Event is durable in production (Postgres events table), so it survives
      another Redis flush.
- [x] One event per successful recover, not per recovery-loop tick (the
      existing `set_state` bump already prevents storms; keep that).
- [x] Test: production recovery after dropping inflight keys produces a
      `work_recovered` row (`tests/test_postgres_recovery.py`).
- [x] Console / `roost events` shows it without new UI work (events list
      already dumps `kind`).

## Notes
Do not add a new table. Events are the append-only surface; operator_actions
are for humans clicking cancel/retry. Recovery is the runtime, so it's an
event.

## Risk if skipped
The next Redis blip looks like a mysterious continue. Operators will not
trust `inspect`.
