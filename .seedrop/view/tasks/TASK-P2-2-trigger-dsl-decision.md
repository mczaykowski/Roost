# P2-2 · Decide the fate of the trigger condition DSL

**Severity:** P2 (smell; risk of accreting into load-bearing surface)
**Status:** open
**Blocked by:** —
**Blocks:** —
**Audit ref:** `.seedrop/view/knowledge/audit.md` §P2-2

## Problem
`_eval_triggers` (`swarm.py:540-585`) does flat dot-path resolution against `snapshot.data.*` / `item.payload.*`. It can't express comparisons, nesting, lists, or boolean logic. It's a baby DSL added before its use cases hardened — the shape of feature that grows ad-hoc and becomes load-bearing before it's well-defined.

## Done when (pick one)
**Delete (recommended until a real engine demands it):**
- [ ] Remove `_eval_triggers` and the trigger config fields.
- [ ] Move triggers to ROADMAP with a note: "revisit when a concrete engine needs parent→child fan-out with conditions."

**Constrain (keep but freeze):**
- [ ] Document exactly one contract: "trigger fires when `snapshot.data[<key>]` is truthy; `payload_map` copies matching keys." No path arithmetic beyond one level.
- [ ] Add a deprecation warning if a condition string contains a `.` beyond one segment.
- [ ] Test the documented contract and reject anything else with a clear error.

## Notes
- The cost of leaving it ambiguous is that the first user to rely on `snapshot.data.foo.bar` will force a backwards-compat constraint later. Decide now.
