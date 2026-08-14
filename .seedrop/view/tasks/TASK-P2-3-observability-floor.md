# P2-3 · Observability floor (structured logs + metrics)

**Severity:** P2 (thin for a "production runtime")
**Status:** partial (Prometheus + `/metrics` shipped in PR #13; worker stdout still silent — see OPS-1)
**Blocked by:** —
**Blocks:** —
**Audit ref:** `.seedrop/view/knowledge/audit.md` §P2-3

## Problem
No structured logging story. Metrics are an optional extra (`[metrics]`). No tracing. `roost_worker_heartbeats` is the only fleet visibility. Temporal's visibility API is a core reason teams trust it; Roost has no equivalent. The console reads durable state but emits nothing machine-consumable for alerting.

## Done when (minimum viable)
- [ ] Structured JSON log line at each step boundary: `work_id`, `engine`, `step`, `attempt`, `status` (success/retry/busy/error), `version`, `duration_ms`.
- [ ] Prometheus counters (under `[metrics]`): `roost_steps_total{engine,status}`, `roost_retries_total`, `roost_dlq_total`, `roost_lease_contention_total`.
- [ ] A `/metrics` endpoint (or documented scrape target).
- [ ] `roost doctor` reports whether metrics are enabled.

## Notes
- This is small and pays off in every incident. Don't gate it on the broader visibility-API question; it's the floor, not the ceiling. Leave tracing (OTel) for a later task.
