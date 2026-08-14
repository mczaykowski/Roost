# OPS-5 · Honest worker liveness after SIGKILL

**Severity:** P1 (fleet view lied during the drill)
**Status:** done
**Blocked by:** —
**Blocks:** —
**Evidence:** `.seedrop/view/knowledge/ops-drill.md` §5

## Problem
Heartbeat interval defaults to 10s (`swarm.py:82`). SIGKILL cannot write a
final heartbeat. Immediately after killing worker A, `roost workers
--stale-after 15` still listed both workers `stale=False`. The row has
`last_seen_at` but the CLI/console don't show **age**. Operators guess.

## Done when
- [x] `roost workers` JSON includes `age_seconds` and `stale` (stale already
      exists when `stale_after_seconds` is passed).
- [x] Default `--stale-after` is documented as "treat as dead after this many
      seconds without a heartbeat"; recommend `>= 2 * heartbeat interval`.
- [x] Heartbeat interval is configurable (`[worker] heartbeat_interval_seconds`
      + optional CLI flag). Keep default 10s unless you have a reason to drop
      it — the bug is display/defaults, not the write path.
- [x] Console Workers view shows age (seconds or "seen 3s ago"), not just a
      boolean that lags.
- [x] Docs: SIGKILL will look live until `stale_after` elapses. There is no
      goodbye packet. That is expected.

## Notes
Don't try to "detect SIGKILL" from Postgres. You can't. Show age and pick
defaults that match the heartbeat period. A 15s stale window with a 10s
heartbeat is one missed beat — too tight for network jitter, too loose to
show up in a 5s operator glance if you just killed the process.

## Risk if skipped
Operators will SIGKILL a wedged worker, see it "live", and kill the wrong
thing next.
