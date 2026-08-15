# OPS-4 · Do not CAS-save wait-only steps

**Severity:** P1 (at-least-once is fine; version 18 for 4 checks is not)
**Status:** done
**Blocked by:** —
**Blocks:** —
**Evidence:** `.seedrop/view/knowledge/ops-drill.md` §4

## Problem
Watchlist `step()` when `now < next_check_after` returns a snapshot whose only
meaningful field is `next_step_delay_seconds` (`watchlist/engine.py:65-70`).
The runtime always CAS-saves (`swarm.py` durable write window). Combined with
recovery re-enqueue, the drill reached **snapshot version 18 for 4 checks**.

That burns versions, inflates events, and makes "what step are we on?" lie.

## Done when
- [x] If a step result is not finished and did not change `step`, `data`
      (except delay bookkeeping), `is_finished`, or artifacts, the runtime
      **re-enqueues with the delay and does not save a new version**.
- [x] Explicit contract: delay waits are movement, not memory. Document on
      `Snapshot.next_step_delay_seconds`.
- [x] Watchlist can keep using `next_check_after` internally *or* rely solely
      on `next_step_delay_seconds` — pick one; don't double-book.
- [x] Test: a wait-only step does not increment `snapshots.version`; a real
      observation still does.
- [x] Re-run a shortened drill / watchlist path: checks_required=3 should land
      at version ≈ 1 (init) + 3 (observations), not 10+.

## Notes
Do **not** add an engine lifecycle hook. Prefer a runtime equality check
(snapshot payload minus delay fields) so every engine gets this for free.
Do not skip save if `data` gained an observation.

Watchlist still stores `next_check_after` on a real observation (crash-safety
for the wait schedule). The runtime skips persisting wait-only polls.

## Risk if skipped
Recovery and delays interact into a version storm. Inspect becomes noisy.
CAS conflicts become more likely under retry.
