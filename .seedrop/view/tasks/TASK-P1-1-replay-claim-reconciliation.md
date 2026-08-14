# P1-1 · Reconcile the "replay" claim with the code

**Severity:** P1 (trust shock on first read; cheap to fix)
**Status:** open
**Blocked by:** —
**Blocks:** —
**Audit ref:** `.seedrop/view/knowledge/audit.md` §P1-1

## Problem
Code: `roost_snapshots` is `PRIMARY KEY (work_id)` (latest-only, `0001_initial.sql:36`); `history` is step *names* not snapshots (`:42`); `roost dlq replay` re-enqueues from the current snapshot (`cli.py:598-633`). You can re-run from the last step. You cannot replay a step-by-step history.

Docs: README:264 *"Snapshot: replayable engine state after each step"*; README:20/152 list replay as an operator capability. The verb overstates the feature. CHANGELOG and ROADMAP are honest; the README is not.

## Done when (pick one track)
**Track A — rename (doc-only, recommended for now):**
- [ ] README:264 reword to "Snapshot: durable engine state after each step (re-runnable from the last step)."
- [ ] README:20/152 replace "replay" with "re-run from last step" (or "retry").
- [ ] `roost dlq --help` and `cli.py` subcommand wording aligned.
- [ ] Add a one-line note in README that full step-by-step replay is on the ROADMAP.

**Track B — ship history (defer; larger):**
- [ ] Migration `0002`: `roost_snapshots_history ((work_id, version) PK, ...)`.
- [ ] `PostgresSnapshotStore.save` appends a history row alongside the latest update.
- [ ] New `replay` verb that reconstructs from history.

## Notes
- Track A is a 15-minute doc pass and removes the largest first-read mismatch. Track B is real feature work already anticipated by the ROADMAP. Do not leave the README implying Track B has shipped.
