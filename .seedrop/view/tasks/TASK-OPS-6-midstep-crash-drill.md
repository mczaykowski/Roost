# OPS-6 · Mid-step crash drill (Phase 1 gate)

**Severity:** P1 (the ROADMAP gate we have not actually run)
**Status:** done
**Blocked by:** OPS-2 (so the script can pass `--stale-after` instead of sleeping 32s)
**Blocks:** claiming "kill a worker mid-step and it resumes"
**Evidence:** `.seedrop/view/knowledge/ops-drill.md` §6
**ROADMAP:** Phase 1 gate — "Killing a worker mid-step … behaves predictably and is covered by tests."

## Problem
Watchlist releases the lease in `finally` after every step, then delays via
SAQ. The crash+flush drill killed workers **between** checks (`lease.active`
was always false when sampled). That proves idle failover + Redis-flush
recovery, not mid-`engine.step()` crash.

A mid-step kill while `leases.is_active` is true is the actual ownership
story: lost lease, snapshot at N or N+1, replacement must not fork the work.

## Done when
- [x] Drill (extend `scripts/ops_drill_crash_and_redis_blip.sh` or a sibling)
      waits until Postgres shows an **active lease** for the work_id, then
      SIGKILLs that holder.
- [x] To make the lease window wide enough: either a watchlist URL that
      blocks (local HTTP server sleeps), or `delay_seconds=0` plus a
      slow/blocking fetch. Do not add engine lifecycle hooks.
- [x] After kill: lease expires (or `--lease-ttl` small), replacement worker
      resumes from the latest **saved** snapshot. Checks must be
      monotonic-enough: at-least-once may repeat the in-flight observation
      once, must not skip, must finish.
- [x] Then FLUSHALL Redis and recover (reuse OPS-2 knobs). Same process must
      still finish.
- [x] Script exits 0; mentioned in `docs/release-checklist.md`.
- [x] Optional CI: not required on every PR if it needs Docker + ~60s; required
      on the release checklist.

## Notes
At-least-once means the killed fetch may run twice. That's correct. Fail the
drill only if work stays `running` forever, snapshot goes backwards in
`checks_completed` by more than 1, or two workers finish two artifacts as
split-brain.

## Risk if skipped
We will keep quoting the Phase 1 gate from a test that never held a lease.
