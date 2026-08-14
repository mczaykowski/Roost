# P0-3 · Postgres-native recovery scan

**Severity:** P0 (violates a stated working agreement; silent data loss on Redis flush)
**Status:** landed (PR #13)
**Blocked by:** —
**Blocks:** —
**Audit ref:** `.seedrop/view/knowledge/audit.md` §P0-3

## Problem
`recover_orphans_once` discovers what to recover by scanning Redis inflight keys (`swarm.py:492`). The lease-liveness check that follows *is* durable (`_lease_is_active` → `PostgresLeaseStore.is_active`), but the **discovery set** is Redis-only. A Redis flush drops inflight keys → recovery finds nothing → work that Postgres fully remembers stays stuck forever, silently. This violates policy working agreement #5: *"Recovery paths must not depend on Redis in production mode."*

## Done when
- [ ] In production mode, primary discovery is `SELECT work_id FROM roost_work_meta WHERE state IN ('running','queued')` cross-checked against `roost_leases` (absent or expired lease) and a staleness window.
- [ ] Redis inflight remains usable as a fast-path hint but is no longer the source of truth for discovery.
- [ ] New test: in production mode, flush Redis (drop inflight keys), enqueue work, wait past `stale_after_seconds`, run `recover_orphans_once`; assert the work is re-enqueued.
- [ ] `uv run pytest` and `uv run ruff check .` pass.

## Notes
- Be careful defining "orphan" from meta alone: a genuinely running worker has an *active* lease. The Postgres scan must AND against lease state to avoid re-enqueueing live work. The existing `_lease_is_active` check already does this — reuse it as the filter on the Postgres-discovered set.
- Consider a guard against re-enqueueing work whose snapshot `is_finished` (already handled at `swarm.py:515`) — preserve that check.
