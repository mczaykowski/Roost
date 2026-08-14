# P1-3 · Wire or drop `roost_operator_actions`

**Severity:** P1 (doc implies a shipped feature; table is empty)
**Status:** landed (PR #13)
**Blocked by:** —
**Blocks:** —
**Audit ref:** `.seedrop/view/knowledge/audit.md` §P1-3

## Problem
`roost_operator_actions` is created and indexed (`0001_initial.sql:135-145`) but has zero writes anywhere (`grep -rn operator_actions src/` → migration only). README and `runtime-storage.md` surface "audit log" / "operator action records" as a capability. `runtime-storage.md:195` is internally honest (*"audit log later"*); the README is not.

## Done when (pick one)
**Wire it:**
- [ ] Add `PostgresOperatorActionStore.record(action, work_id, actor, payload)`.
- [ ] Call it from `cli.py` cancel / replay / ack / dlq commands and from console retry/cancel handlers (`ui/server.py`).
- [ ] New test: a cancel records a row; listing operator actions returns it.

**Drop it (if not now):**
- [ ] Remove the table + index from `0001_initial.sql` (or a `0002` drop migration).
- [ ] Remove "audit log" from README capability lists; keep it only in ROADMAP.

## Notes
- Wiring is the better long-term choice — operator audit is a stated reason teams would trust the runtime. But an empty indexed table is worse than honest absence. Pick one this cycle; don't carry the dead schema forward.
