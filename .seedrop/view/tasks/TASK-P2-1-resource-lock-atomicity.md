# P2-1 · Make resource locks actually atomic

**Severity:** P2 (documented "best-effort," but positioned for multi-customer use)
**Status:** open
**Blocked by:** —
**Blocks:** —
**Audit ref:** `.seedrop/view/knowledge/audit.md` §P2-1

## Problem
`PostgresResourceStore._lock` uses `pg_advisory_xact_lock` (`postgres/stores.py:421-423`), but `acquire` commits immediately (`commit=True`, `:389`). Transaction-scoped advisory locks release at commit — so against two workers on separate connections the lock is gone before the second arrives. It serializes nothing. Two workers can race a "locked" resource.

## Done when
- [ ] Replace the advisory-lock + separate conflict-check + insert with a single atomic upsert using the same pattern that makes leases correct: `INSERT ... ON CONFLICT (resource_key) DO UPDATE ... WHERE expires_at <= now() RETURNING`.
- [ ] Drop `_lock` / `pg_advisory_xact_lock`.
- [ ] New test: two coroutines acquire the same resource concurrently; exactly one wins, the other gets `False` while the claim is live.
- [ ] `uv run pytest` and `uv run ruff check .` pass.

## Notes
- If full atomicity is rejected as out of scope this cycle, then at minimum update the docs to say resource locks are **not** a correctness boundary in production mode, so no one builds a multi-tenant guarantee on them.
