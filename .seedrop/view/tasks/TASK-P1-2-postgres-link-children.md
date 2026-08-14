# P1-2 · Persist link_child in Postgres (close the mode asymmetry)

**Severity:** P1 (silent functional regression between modes)
**Status:** open
**Blocked by:** —
**Blocks:** —
**Audit ref:** `.seedrop/view/knowledge/audit.md` §P1-2

## Problem
Simple mode: `RedisControlPlane.link_child` writes `child_work_ids` into the parent meta (`redis.py:448-484`) — directly queryable.
Production mode: `PostgresControlPlaneStore.link_child` only pushes an event (`postgres/stores.py:528-545`); `roost_work_meta` has no `children` column (`0001_initial.sql:19-27`). The only way to find children in production is a scan of `roost_events WHERE kind = 'work_child_linked'`. Undocumented behavior difference. Trigger pipelines that inspect children work in simple mode and silently degrade in production.

## Done when
- [ ] Migration `0002`: add `children JSONB NOT NULL DEFAULT '[]'` to `roost_work_meta`.
- [ ] `PostgresControlPlaneStore.link_child` writes the child entry to `children` (most-recent-first, capped like the Redis `max_children`) in addition to the event.
- [ ] `get_meta` returns `children` in production parity with the Redis meta shape.
- [ ] New test: link a child in production mode; `get_meta` returns the child without scanning events.
- [ ] `uv run pytest` and `uv run ruff check .` pass.

## Notes
- Keep the event write too — events are the append-only audit surface; the column is the queryable index. Don't choose.
