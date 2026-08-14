# P2-4 · Resolve store mode once at construction

**Severity:** P2 (hot-path indirection that hurts correctness reasoning)
**Status:** open
**Blocked by:** P0-1 (natural moment to revisit store wiring)
**Blocks:** —
**Audit ref:** `.seedrop/view/knowledge/audit.md` §P2-4

## Problem
`_ensure_runtime_stores` (`swarm.py:109-140`) rebinds `self.stores` and re-runs `_activate_stores` at runtime, mutating `self.leases`, `self.snapshots`, etc. on first use. Mode never changes after startup — the lazy swap exists only to defer the Postgres import. But the indirection means every method must reason about "which stores am I talking to *right now*?", which makes correctness review harder.

## Done when
- [ ] Mode (simple vs production) is resolved exactly once at construction.
- [ ] The Postgres import is still deferred (lazy import inside the factory is fine), but runtime bindings (`self.leases`, `self.snapshots`, …) do not mutate after `__init__` completes.
- [ ] `_ensure_runtime_stores` removed or reduced to a no-op guard.
- [ ] Existing tests pass; add an assertion that `type(self.stores.leases)` does not change across the first step.

## Notes
- Lower priority than the P0 trio, but worth doing in the same refactor window as P0-1 since both touch store wiring.
