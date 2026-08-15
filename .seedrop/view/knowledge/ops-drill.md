# Ops drill — crash + Redis blip (2026-08-14)

**What we ran:** `scripts/ops_drill_crash_and_redis_blip.sh`
Isolated Redis + Postgres, watchlist engine, two-worker fleet, SIGKILL one worker,
`FLUSHALL` Redis, start a replacement worker. Work finished `reachable` with 4
checks and an artifact. Postgres was the source of truth after the flush.

**Durability held.** Operability did not feel boring. These findings are the
next sprint, not more features.

## What worked
- Snapshot + meta survived a total Redis wipe; the replacement worker recovered
  and finished (P0-3 claim, verified).
- After SIGKILL of worker A, checks continued to 2.
- `status` / `inspect` / `list` / `events` could be read from Postgres without
  knowing internals.

## Findings (sprint backlog)

### 1. Worker JSON logs never reach stdout
`_log_step_boundary` in `swarm.py` uses `logging.getLogger("roost.runtime")` at
INFO. `cli.py:main` never calls `logging.basicConfig`. Worker log files in the
drill were empty. Metrics extra can be "enabled" while the operator tails
nothing.

### 2. Recovery knobs are hardcoded
`SwarmConfig.stale_after_seconds = 30` (`swarm.py:84`) is not on the CLI or
`roost.toml`. After FLUSHALL the drill sat ~40s in the dark. Lease TTL *is*
`--lease-ttl`; stale window is not.

### 3. Recovery is silent in the event stream
`_recover_one` (`swarm.py:736`) enqueues + `set_state` + inflight mark. No
`kind=work_recovered` event. Pre-blip events were only `work_enqueued` and
`work_state_changed`. An operator cannot tell a Redis-flush recovery from a
normal continue.

### 4. Wait-only steps burn snapshot versions
Watchlist `step()` when `now < next_check_after` still returns a snapshot the
runtime CAS-saves (`watchlist/engine.py:65-70`, `swarm.py` durable window).
Drill: 4 checks → snapshot version **18**. At-least-once plus delay-polls plus
recovery re-enqueue.

### 5. Heartbeats lag death
Default heartbeat interval is 10s (`swarm.py:82`). SIGKILL cannot write a
goodbye row. Immediately after killing A, `roost workers --stale-after 15`
still showed both workers `stale=False`. No `age_seconds` on the row.

### 6. The drill did not kill mid-`step()`
Watchlist releases the lease in `finally` after every step, then waits via a
delayed SAQ job. Sampling after a saved check always saw `lease.active=false`.
Killing A was an idle-worker death, not a mid-fetch crash. ROADMAP Phase 1
gate still unmet: "Killing a worker mid-step … behaves predictably."

## Not in this sprint
Snapshot history, OTel tracing, retention, backup/restore, multi-tenant
separation — still ROADMAP. Don't let those crowd out making this drill boring.
