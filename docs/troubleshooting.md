# Troubleshooting

This page covers common local-development and production-mode sandbox issues.

## Redis Is Not Reachable

Start Redis:

```bash
docker run --rm -p 6379:6379 redis:7
```

Then run:

```bash
uv run roost doctor
```

If another Redis instance is already using the port, pick another one:

```bash
docker run --rm -p 6381:6379 redis:7
ROOST_REDIS_URL=redis://localhost:6381/0 uv run roost list
```

## Work Does Not Appear In The Console

The CLI, worker, and console must use the same Redis URL, queue, prefix, and
namespace. In production mode, they must also use the same Postgres URL and
runtime mode.

Check these values:

- `ROOST_REDIS_URL`
- `ROOST_QUEUE`
- `ROOST_REDIS_PREFIX`
- `ROOST_NAMESPACE`
- `ROOST_POSTGRES_URL`

For local testing, start the console with the same values used by the worker:

```bash
uv run roost ui --redis-url redis://localhost:6379/0 --redis-prefix roost
```

For production mode, prefer a shared config file:

```bash
uv run roost worker --config examples/production/roost.toml
uv run roost ui --config examples/production/roost.toml
```

If you are using `roost.toml`, check what Roost sees:

```bash
uv run roost doctor --config roost.toml
```

## Production Doctor Fails

Production mode needs both Redis and Postgres. Start the sandbox services:

```bash
docker compose -f examples/production/docker-compose.yml up -d
```

Then run migrations and doctor:

```bash
uv run roost migrate --config examples/production/roost.toml
uv run roost doctor --config examples/production/roost.toml
```

Common failure meanings:

- `postgres url`: production mode needs `[postgres].url`, `--postgres-url`, or
  `ROOST_POSTGRES_URL`.
- `postgres connection`: Postgres is not reachable or the URL credentials are
  wrong.
- `postgres migrations`: the database is reachable but `roost migrate` has not
  been applied, or the schema checksum does not match this package.
- `redis connection`: Redis is not reachable or the Redis URL points at the
  wrong port.

## Work Is Waiting Forever

Common causes:

- No worker is running for the selected engine.
- The worker is connected to a different Redis URL, queue, prefix, or namespace.
- The work item is delayed by `next_step_delay_seconds`.
- A resource claim is still held by another in-flight item.

Useful commands:

```bash
uv run roost list
uv run roost events
uv run roost status <work_id>
uv run roost inspect <work_id>
```

In production mode, also check worker heartbeats:

```bash
uv run roost workers --config examples/production/roost.toml
```

If the work is safe to run again, re-enqueue it:

```bash
uv run roost retry <work_id>
```

If the work should stop being considered active, cancel it:

```bash
uv run roost cancel <work_id> --reason operator_request
```

## Dead-Lettered Work

List failed work that reached the dead-letter queue:

```bash
uv run roost dlq list
```

Re-enqueue the first dead-letter entry from its latest snapshot, then remove it
from the queue:

```bash
uv run roost dlq replay 0 --ack
```

The local console exposes the same recovery actions in the work detail drawer
and failed-work view:

```bash
uv run roost ui
```

## Workers Are Missing Or Stale

Worker heartbeats are recorded in production mode. If `roost workers` is empty:

```bash
uv run roost worker --config examples/production/roost.toml
uv run roost workers --config examples/production/roost.toml
```

If a worker is stale, it has not heartbeated within the stale threshold. Common
causes:

- The worker process stopped.
- The worker is using a different `roost.toml`.
- The worker is running in simple mode instead of production mode.
- The worker can reach Redis but cannot write to Postgres.

`roost workers` JSON includes `age_seconds` as well as `stale`. A SIGKILL'd
worker cannot write a goodbye heartbeat; it looks live until `--stale-after`
elapses. That is expected. Recommend `--stale-after` of at least twice the
heartbeat interval (default interval is 10s, so 20s or more). The console
Workers view shows age, not only the boolean.

Run `doctor` with the same config the worker uses.

## Worker Logs Are Empty

`roost worker` writes JSON step lines to stderr at INFO. There is no separate
log file. Tail the process:

```bash
uv run roost worker --engines watchlist --log-level info 2> worker.log
tail -f worker.log
```

Each completed step is one JSON object with `work_id`, `engine`, `step`,
`attempt`, `status`, `version`, and `duration_ms`. Use `--log-level debug` for
noisier library logs; `warning` or `error` will hide the step lines.

If you redirect only stdout, the JSON lines will still appear on the terminal
(stderr). The worker does not use OpenTelemetry.

## Redis Flushed And Work Sits For ~30s

After Redis `FLUSHALL`, queue and in-flight keys are gone. Postgres still has
the snapshot. Orphan recovery will not re-enqueue work whose snapshot or meta
is newer than `stale_after_seconds` (default **30**) while it also requires no
live lease.

That 30s window exists so a slow worker is not treated as dead. It is expected
unless you lower it:

```bash
uv run roost worker --stale-after 2 --recovery-interval 1 --lease-ttl 4
```

Do not default production `stale_after_seconds` to 2. Use the knob for drills
and incidents. `roost events` should include `kind=work_recovered` once the
replacement worker re-enqueues the work.

## Artifacts Are Missing

Artifacts are stored on the local filesystem by default. The worker and the CLI
must use the same artifact root.

If a worker used a custom artifact root, pass it when reading:

```bash
uv run roost artifact-show <artifact_id> --ext json --artifact-root <path>
```

## The E2E Script Leaves A Container Behind

The e2e scripts normally remove their Redis and Postgres containers on exit. If
a local run is interrupted hard, remove old containers manually:

```bash
docker ps -a --filter "name=roost-e2e"
docker rm -f <container_id>
```
