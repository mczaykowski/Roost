# Roost Maturity Audit

**Audited:** 2026-06-15 · Branch `codex/operator-polish` · commit `0ffb34c`
**Method:** every claim below is verified against current source with file:line anchors. No claim is carried from prior critiques without re-verification.
**Purpose:** separate what Roost *guarantees today* from what its docs *describe as future*, then identify the smallest set of changes that move the project from "foundation" to a deployable, trustworthy production runtime.

---

## TL;DR

The thesis and the primitives are strong. The engine contract (two async methods), the movement/memory/evidence split, CAS-on-version snapshots, and Postgres lease CAS are each individually correct. What is missing is the **composition**: those correct primitives are not wrapped in a transaction boundary, are shared across concurrent coroutines on a single Postgres session, and depend on Redis for the one thing Postgres was supposed to make independent (recovery). On top of that, the README sells "replay" and "durable" as capabilities the code does not yet guarantee.

The path to "deployable in a few steps" is not feature work. It is, in order:

1. **Connection pool per store** — fixes a latent concurrency crash in the production hot path.
2. **One transaction per step** — makes "durable system of record" a true statement.
3. **Postgres-native recovery scan** — closes the Redis-flush → silently-stuck-work seam.
4. **Reconcile the docs with the code** — stop calling latest-only "replay."

Everything else (snapshot history, audit log, retention) is already honestly on the ROADMAP. The risk is only where the docs imply it already shipped.

---

## What is genuinely production-grade today

These hold up under verification and deserve to be defended as invariants during future work.

- **Engine contract is correctly minimal.** `init_snapshot` + `step` (`runtime/engine.py`). No lifecycle hooks, no DSL. This is the right boundary and should not grow without strong justification.
- **Lease CAS is sound on both backends.** Postgres: `INSERT ... ON CONFLICT DO UPDATE ... WHERE expires_at <= now() RETURNING` serializes contending workers via the unique constraint on `work_id`; the loser gets 0 rows → `None` (`postgres/stores.py:276-299`). Redis equivalent uses Lua CAS. This is the one durability primitive that is correct *as composed*.
- **Snapshot optimistic concurrency is correct per-call.** `UPDATE ... WHERE work_id = %s AND version = %s`, `rowcount == 1` decides success (`postgres/stores.py:158-182`). Version-conflict detection at the runtime layer then re-enqueues or declares a race-winner (`swarm.py:322-343`). This is the right pattern.
- **Orphan recovery concept is correct.** Scan inflight → check lease liveness → check snapshot not finished → re-enqueue (`swarm.py:487-529`). The *concept* is right; only the *discovery source* is wrong (see P0-3).
- **At-least-once is stated honestly.** Retry path has backoff + jitter + version-conflict handling (`swarm.py:163-166`, `322-343`). The tax on engine authors is real but it is disclosed.

---

## Findings, by severity

Severity reflects distance between *claimed/desired* state and *current* code, weighted by how hard the gap is to discover.

### P0 — correctness bugs in the production hot path

#### P0-1 · All Postgres stores share one async session → concurrency crash
**Where:** `swarm.py:128-129` (`self._postgres_conn = await psycopg.AsyncConnection.connect(...)`), threaded through every store via `build_postgres_durable_stores(conn)` (`postgres/stores.py:797-806`).

psycopg3 async connections are **single-session**: two coroutines issuing commands on the same connection concurrently raise `ProgrammingError: cannot execute commands: already executing`.

`_execute_one_step_impl` runs `renew_loop()` as a **concurrent task** alongside the main step body (`swarm.py:218-232`). In production mode that loop calls `self.leases.renew` and `self.resources.renew` — on the same connection the main body is using for `snapshots.save`, `control.set_state`, `inflight.mark`. In simple mode this is harmless (Redis is connection-pooled); in production mode two coroutines interleave on one Postgres session.

**Why the suite misses it:** `uv run pytest` runs single-step, low-concurrency happy paths. There is no test that issues a renew concurrently with a snapshot save. 507 lines of tests, zero concurrency/parallel tests.

**Fix shape:** `psycopg.AsyncConnectionPool` shared across stores, or one connection borrowed from a pool per coroutine. The `commit=True`-per-call pattern (below) already implies one logical transaction per call, so pooling is the natural refactor.

---

#### P0-2 · No transaction boundary around a step → durable state can be internally inconsistent
**Where:** every Postgres store defaults to `commit=True` and commits independently (`postgres/stores.py:38, 126, 272, 349, 427`, factory at `:797`). The step body in `swarm.py:306-369` calls `snapshots.save`, then `control.set_state`, then `push_event`/`enqueue` as **separate commits**.

Failure between any two calls leaves durable state contradictory:
- `snapshots.save` commits version N+1 (snapshot says `finished`), then `control.set_state` fails → `roost_work_meta.state` still says `running`. A system whose pitch is "durable system of record" has **no transaction boundary around its unit of durability.**

This is the single most important correctness gap. Each primitive is correct; the composition is not.

**Fix shape:** introduce a per-step transaction. Cleanest is a `commit=False` mode that yields a connection/transaction context to the runtime, with a single `commit()` at the end of `_execute_one_step_impl`. The `commit` flag already exists on every store — it was clearly anticipated — it just defaults the wrong way for the production path.

---

#### P0-3 · Recovery discovery is Redis-only → Redis flush silently orphans Postgres work
**Where:** `recover_orphans_once` scans `self.redis.scan_iter("...:inflight:*")` to decide *what to recover* (`swarm.py:492`). The lease-liveness check that follows *is* durable (`_lease_is_active` → `PostgresLeaseStore.is_active`, `:338`), but the **discovery set** is Redis-only.

Policy.json line N: *"Recovery paths must not depend on Redis in production mode."* The code violates the project's own working agreement. A Redis flush loses the inflight keys → `recover_orphans_once` finds nothing → work that Postgres fully remembers stays stuck forever, silently.

**Fix shape:** in production mode, primary discovery should be `SELECT work_id FROM roost_work_meta WHERE state = 'running'` cross-checked against `roost_leases WHERE expires_at > now()` (absent or expired lease + running meta + stale inflight = orphan). Redis inflight can stay as a fast-path hint, not the source of truth.

---

### P1 — incorrect/oversold claims that erode trust on first read

#### P1-1 · "Replay" is oversold
**Code:** `roost_snapshots` is `PRIMARY KEY (work_id)` — one row, latest only (`0001_initial.sql:36`). The `history` column is `JSONB` of step *names* (strings), not snapshots (`:42`). `roost dlq replay` re-enqueues from the **current** snapshot (`cli.py:598-633`).
**Claim:** README:264 calls `Snapshot` *"replayable engine state after each step"*; README:20 lists `replay` as an operator capability; README:152 says the console can "replay."

You can **re-run from the last step**. You cannot **replay a step-by-step history**. The verb overstates the feature. CHANGELOG and ROADMAP are honest about this ("snapshot history beyond the latest" = future); the README is not.

**Fix shape:** either (a) rename the user-facing verb to "re-run / retry-from-step" and stop calling it replay, or (b) ship append-only snapshot history (new table `roost_snapshots_history` keyed by `(work_id, version)`). (a) is a doc change; (b) is on the ROADMAP already. Pick one before more users read the README.

---

#### P1-2 · `link_child` is a functional regression in production mode
**Simple mode:** `RedisControlPlane.link_child` writes `child_work_ids` into the parent meta JSON (`redis.py:448-484`). Queryable directly.
**Production mode:** `PostgresControlPlaneStore.link_child` only pushes an `event` (`postgres/stores.py:528-545`). `roost_work_meta` has **no children column** (`0001_initial.sql:19-27`). The only way to find children in production is `SELECT ... FROM roost_events WHERE kind = 'work_child_linked'` — a scan of an append-only table.

This is an undocumented behavior difference between the two modes. A trigger pipeline that inspects children works in simple mode and silently degrades in production.

**Fix shape:** add a `children JSONB` column to `roost_work_meta` (migration `0002`) and have `link_child` write it alongside the event. Cheap, closes the asymmetry.

---

#### P1-3 · `roost_operator_actions` is dead schema
**Where:** table + index created (`0001_initial.sql:135-145`), **zero writes anywhere** (`grep -rn operator_actions src/` → only the migration). README and `runtime-storage.md` list "audit log" / "operator action records" as capabilities. `runtime-storage.md:195` is internally honest (*"audit log later"*); the README surfaces it as shipped.

**Fix shape:** wire it (record cancel/replay/ack/dlq operations from `cli.py` and the console) **or** drop it and move it to the ROADMAP. Don't ship empty indexed tables that imply a feature.

---

### P2 — design smells that will hurt at scale

#### P2-1 · `pg_advisory_xact_lock` is a no-op as used
**Where:** `PostgresResourceStore._lock` calls `pg_advisory_xact_lock(hashtextextended(%s, 0))` (`postgres/stores.py:421-423`) before the claim sequence. But `acquire` then commits immediately (`commit=True`, `:389`). **Transaction-scoped advisory locks release at commit/rollback.** Against two workers on separate connections, the lock is gone before the second arrives — it serializes nothing.

Resource locks are also documented "best-effort," which is fine for demos but is the same surface being positioned for "geopolitical monitoring across customers." Two workers can race a "locked" resource.

**Fix shape:** either (a) move advisory locking to `session`-level and hold the connection for the claim's lifetime, or (b) make the claim itself atomic with `INSERT ... ON CONFLICT ... WHERE expires_at <= now()` (same pattern as leases, which is correct). (b) is more consistent with the rest of the codebase.

---

#### P2-2 · The trigger condition DSL is neither expressive nor simple
**Where:** `_eval_triggers` (`swarm.py:540-585`) does flat dot-path resolution against `snapshot.data.*` / `item.payload.*`. It can't express comparisons, nesting, lists, or boolean logic. It's a baby DSL added before its use cases hardened — exactly the kind of surface that grows ad-hoc and becomes load-bearing before it's well-defined.

**Fix shape:** either delete it (until a real engine demands it) or replace with a single explicit contract ("trigger fires when `snapshot.data[<key>]` is truthy" — documented, no path arithmetic). Don't let it accrete.

---

#### P2-3 · Observability is thin for a "production runtime"
No structured logging story. Metrics are an optional extra (`[metrics]`). No tracing. The heartbeat table (`roost_worker_heartbeats`) is the only fleet visibility. Temporal's visibility API is a core reason teams trust it; Roost has no equivalent yet. The console (`ui/server.py`) reads durable state but emits nothing machine-consumable for alerting.

**Fix shape:** minimum viable = structured JSON logs on the step boundary (attempt, work_id, engine, step, status, duration, version) + Prometheus counters for retries/DLQ/lease-contention. This is small and pays off in every incident.

---

#### P2-4 · Runtime lazily swaps store implementations
**Where:** `_ensure_runtime_stores` (`swarm.py:109-140`) rebinds `self.stores` and re-runs `_activate_stores` at runtime, mutating `self.leases`, `self.snapshots`, etc. Works today, but it's indirection in the hot path that makes correctness reasoning harder — every method must consider "which stores am I talking to *right now*?"

**Fix shape:** resolve mode once at construction. Mode does not change after startup; the lazy swap exists only to defer the Postgres import. Defer the import without mutating runtime bindings (e.g., resolve a factory up front).

---

## What is already honestly on the roadmap (no action beyond tracking)

These are gaps, but they are **disclosed** gaps — the risk is only if the README starts implying they shipped:

- Snapshot history beyond latest (CHANGELOG, ROADMAP).
- Retention policy for events/snapshots/artifacts/completed work (`runtime-storage.md:236`).
- Project/vertical/environment separation (`runtime-storage.md:120-126, 240`).
- Audit log beyond events (`runtime-storage.md:195`: *"audit log later"*).

---

## Suggested deployment gate

Before pointing Roost at real work in production mode, the **P0 trio** should land and gain tests:

| Gate | Signal it gives |
|------|-----------------|
| P0-1 fixed + concurrency test (renew ‖ save) | No `already-executing` crashes under load |
| P0-2 fixed + crash-injection test (kill between save & set_state) | Meta and snapshot agree after any failure |
| P0-3 fixed + Redis-flush test (drop inflight, assert recovery still finds work) | No silent orphans |

P1 items are doc/reconciliation work and can land in the same PR; they cost little and remove the largest "trust shock" for a first-time reader.

---

## Verification log

| Claim | Verified at |
|-------|-------------|
| Shared single Postgres connection | `swarm.py:128-129`, factory `postgres/stores.py:797-806` |
| `renew_loop` concurrent with step body | `swarm.py:218-232` |
| Commit-per-call, no step transaction | `postgres/stores.py:38,126,272,349,427`; step body `swarm.py:306-369` |
| Lease CAS correct | `postgres/stores.py:276-299` |
| Snapshot CAS correct per-call | `postgres/stores.py:158-182` |
| Recovery scans Redis only | `swarm.py:492` |
| Latest-only snapshots, `history` = step names | `0001_initial.sql:35-51` |
| DLQ "replay" = re-enqueue from current snapshot | `cli.py:598-633` |
| `link_child` asymmetry (Redis writes meta, PG writes event) | `redis.py:448-484` vs `postgres/stores.py:528-545` |
| `roost_operator_actions` zero writes | `grep -rn operator_actions src/` → migration only |
| Advisory lock releases at commit | `postgres/stores.py:421-423` + `commit=True` at `:389` |
| Trigger DSL = flat dot-path | `swarm.py:540-585` |
| Zero concurrency/transaction tests | `tests/` (507 lines, all single-step) |

**Audit convention:** this file lives in `.seedrop/view/knowledge/` per the knowledge-folder convention ("checked-along-with-code planning artifacts an agent should read before changing code"). It is hand-authored, not generated — `seed view audit` produces a separate machine manifest-drift report (`audit.json`).
