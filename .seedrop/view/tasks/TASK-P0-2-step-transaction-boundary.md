# P0-2 · One transaction per step

**Severity:** P0 (durability claim is false without it)
**Status:** landed (PR #13)
**Blocked by:** P0-1 (cleanest after pooling lands, but can proceed with `commit=False` mode in parallel)
**Blocks:** —
**Audit ref:** `.seedrop/view/knowledge/audit.md` §P0-2

## Problem
Every Postgres store commits independently (`commit=True` default, `postgres/stores.py:38,126,272,349,427,797`). The step body (`swarm.py:306-369`) calls `snapshots.save` → `control.set_state` → `push_event`/`enqueue` as separate commits. A failure between two leaves durable state internally inconsistent (snapshot says `finished`, meta still says `running`). For a system pitched as a "durable system of record," there is no transaction boundary around the unit of durability.

## Done when
- [ ] Production stores support a `commit=False` mode that defers commit (the flag already exists — flip the default for the production step path).
- [ ] `_execute_one_step_impl` opens one transaction at the start of the durable write sequence and commits exactly once at the end (after `control.set_state`).
- [ ] On exception, the transaction rolls back; the existing error-recording path (`_best_effort_record_error`) runs on a fresh connection.
- [ ] New test: inject a failure between `snapshots.save` and `control.set_state`; assert meta and snapshot agree after recovery (both reflect the pre-step state, not a half-applied step).
- [ ] `uv run pytest` and `uv run ruff check .` pass.

## Notes
- Redis (simple mode) is not transactional by nature and should not be forced to be — keep the boundary Postgres-only. The working agreement says simple mode = Redis for both; don't blur it.
- Decide explicitly whether artifact writes (`artifacts.put`) are inside or outside the transaction. Recommended: inside, since an artifact referenced by a snapshot must exist.
