# Roost 0.1.0

Roost 0.1.0 is the first public release of a small runtime for durable,
resumable agent workers.

Roost treats an agent as a step-machine. Your engine owns the domain behavior;
Roost owns work identity, snapshots, leases, retries, resource claims, delayed
continuation, events, artifacts, and recovery.

## Highlights

- Run long-lived agent work with durable snapshots after each step.
- Resume work after a worker crash or restart.
- Inspect live work, saved snapshots, events, failed work, and artifacts in the local console.
- Retry, cancel, re-run from the last step, and acknowledge failed work from the CLI or console.
- Use the watchlist demo to see the runtime behavior without an LLM key.

## Quickstart

```bash
uv sync --extra redis --extra dev
uv run roost init
docker run --rm -p 6379:6379 redis:7
uv run roost doctor
uv run roost worker --engines watchlist
```

In another terminal:

```bash
uv run roost enqueue \
  --engine watchlist \
  --resource domain:example.com \
  --payload '{"url":"https://example.com","claim":"Example Domain is reachable","checks_required":3,"delay_seconds":5}'
```

Open the console:

```bash
uv run roost ui
```

## Notes

- Current backend: Redis + SAQ.
- Current package: Python 3.11+.
- Execution guarantee: at-least-once.
- Engines should make `step(snapshot, item)` retry-safe.
- Postgres durable storage is planned for the production runtime roadmap.

