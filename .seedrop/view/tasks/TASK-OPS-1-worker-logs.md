# OPS-1 · Worker JSON logs actually print

**Severity:** P0 for operability (the log line exists; nobody can see it)
**Status:** done
**Blocked by:** —
**Blocks:** every incident, OPS-6 drill readability
**Evidence:** `.seedrop/view/knowledge/ops-drill.md` §1
**Related:** `TASK-P2-3-observability-floor.md` (metrics shipped; this is the log half that didn't)

## Problem
`_log_step_boundary` (`swarm.py:274`) emits JSON via
`logging.getLogger("roost.runtime")` at INFO. `cli.py:main` (`cli.py:1283`) never
configures logging. Drill worker log files were empty. `roost doctor` can report
metrics enabled while `tail -f worker.log` is silent.

## Done when
- [x] `roost worker` configures logging (stderr, default INFO) before `run_worker`.
- [x] `--log-level` on `worker` (and documented in `--help`): `debug|info|warning|error`.
- [x] One JSON object per step on stderr with `work_id`, `engine`, `step`,
      `attempt`, `status`, `version`, `duration_ms`.
- [x] Test: run `_log_step_boundary` / a tiny worker helper and capture that
      INFO records are emitted once logging is configured; or capsys on a CLI
      invocation that doesn't need Redis if that's too heavy — at minimum a
      unit test that `logging.getLogger("roost.runtime")` has a handler after
      worker startup path, plus a test that the JSON keys are present.
- [x] Troubleshooting doc: how to tail worker logs.

## Notes
Do not invent a log framework. `logging.basicConfig` on the worker command is
enough. Don't change the JSON schema. Leave OTel out.

## Risk if skipped
The next crash looks like "the worker did nothing."
