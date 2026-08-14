# Sprint OPS — make crash + Redis-blip boring

**Severity:** tracking (this is the index, not a code task)
**Status:** done
**Blocks:** calling production mode "operator-ready"
**Evidence:** `.seedrop/view/knowledge/ops-drill.md`
**Script:** `scripts/ops_drill_crash_and_redis_blip.sh` (passed durability; failed boredom)

## Goal
The last sprint made durability claims true. This sprint makes the same failure
story *watchable*: logs, recovery knobs, recovery events, no version-storm on
waits, honest worker liveness, and a drill that actually kills mid-`step()`.

If an operator who did not write Roost can SIGKILL a worker, flush Redis, and
understand what happened from stdout + `inspect`/`events`/`workers` without
reading source, the sprint is done.

## The few steps (in order)

1. **OPS-1 — Worker logs actually print.** Unblocks every other incident.
2. **OPS-2 — Recovery knobs on CLI/config.** Unblocks a fast, honest drill.
3. **OPS-3 — `work_recovered` event.** Same files as recovery; do with OPS-2.
4. **OPS-4 — Skip durable save on wait-only steps.** Stops version 18 for 4 checks.
5. **OPS-5 — Heartbeat age on `roost workers`.** Don't show corpses as live.
6. **OPS-6 — Mid-step crash drill.** The Phase 1 ROADMAP gate. Needs OPS-2.

## Acceptance
- [x] OPS-1…OPS-5 merged with tests.
- [x] `roost worker` stderr shows a JSON step line per step (no extra setup).
- [x] After Redis FLUSHALL, `roost events` includes `work_recovered` for the work.
- [x] Watchlist delay waits do not bump snapshot version.
- [x] `roost workers` shows `age_seconds`; a SIGKILL'd worker becomes stale within
      `2 * heartbeat_interval` (documented).
- [x] OPS-6 drill script kills a worker **while the lease is held**, then a
      replacement finishes from the latest snapshot. Exit 0.
- [x] `uv run pytest` and `uv run ruff check .` green.
- [x] README / troubleshooting mention `--log-level` and `--stale-after` on worker.

## Sequencing rationale
Logs first (independent, cheapest trust fix). Knobs + recovery event next
(same `swarm.py` / CLI surface). Wait-only save skip can proceed in parallel
with heartbeats. Mid-step drill last so it can pass `--stale-after` instead of
sleeping 32s.

Do **not** pull snapshot history, OTel, or retention into this sprint.
