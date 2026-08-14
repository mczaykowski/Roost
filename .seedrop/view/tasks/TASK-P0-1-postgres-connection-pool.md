# P0-1 · Postgres connection pool per store

**Severity:** P0 (production hot-path crash)
**Status:** open
**Blocked by:** —
**Blocks:** P0-2 (cleaner once pooling lands)
**Audit ref:** `.seedrop/view/knowledge/audit.md` §P0-1

## Problem
All Postgres stores share one `psycopg.AsyncConnection` (`swarm.py:128-129`). psycopg3 async connections are single-session: two coroutines issuing commands concurrently raise `ProgrammingError: cannot execute commands: already executing`. `_execute_one_step_impl` runs `renew_loop()` concurrently with the main step body (`swarm.py:218-232`); in production mode both touch the same connection.

## Done when
- [ ] `build_postgres_durable_stores` accepts an `AsyncConnectionPool` (or constructs one) instead of a bare connection.
- [ ] Each store method borrows a connection from the pool for the duration of the call (or holds a per-coroutine connection).
- [ ] `_RedisSwarmRuntime.close()` closes the pool.
- [ ] New test `tests/test_postgres_concurrency.py`: run `leases.renew` and `snapshots.save` concurrently (e.g. `asyncio.gather`) against production stores; assert no `ProgrammingError`, both succeed.
- [ ] `uv run pytest` and `uv run ruff check .` pass.

## Notes
- The `commit=True`-per-call pattern already implies one logical transaction per call, so pooling is the natural shape.
- Do not blur the simple/production boundary (policy working agreement #3): Redis stores stay as-is.

## Risk if skipped
Latent crash under real concurrency. Passes the e2e (low concurrency, fast steps), bites under load.
