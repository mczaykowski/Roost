# OPS-2 · Recovery knobs on CLI and roost.toml

**Severity:** P1 (recovery works; you cannot tune or test it at human speed)
**Status:** done
**Blocked by:** —
**Blocks:** OPS-6 (mid-step drill should not sleep 32s)
**Evidence:** `.seedrop/view/knowledge/ops-drill.md` §2

## Problem
`SwarmConfig.stale_after_seconds` defaults to 30 and
`recovery_interval_seconds` to 2 (`swarm.py:82-84`). CLI `worker` passes
`lease_ttl_seconds` (`cli.py:325-335`) but not the stale window. After
FLUSHALL the drill waited ~40s in the dark. That delay is a reasonable
*default* (don't re-enqueue live work). It is not reasonable as an unexposed
constant.

## Done when
- [x] `roost.toml` `[worker]` accepts `stale_after_seconds` and
      `recovery_interval_seconds`.
- [x] `roost worker --stale-after` and `--recovery-interval` override them.
- [x] Defaults stay 30 / 2 unless overridden. Document *why* 30 exists
      (avoid re-enqueueing work whose worker is slow, not dead).
- [x] `SwarmConfig` is populated from those flags in `_cmd_worker`.
- [x] Test: constructing the worker config / a unit on `SwarmConfig` from CLI
      parse; drill script can pass `--stale-after 2 --lease-ttl 4`.
- [x] Troubleshooting: "Redis flushed, work stuck for 30s" → expected unless
      you lower `--stale-after`.

## Notes
Do not default stale_after to 2 in production. The knob is the fix, not a
more aggressive default. Recovery still must AND with lease liveness.

## Risk if skipped
Every Redis-blip test and every real incident waits half a minute for a
constant nobody can change without a code edit.
