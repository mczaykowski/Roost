# Production-Mode Local Sandbox

This example runs Roost in its production-shaped mode:

- Redis moves work through the queue.
- Postgres stores durable runtime state, snapshots, leases, resources, events, DLQ, artifacts metadata, and worker heartbeats.
- Workers still run locally, so nothing is hosted or hidden.

## Start Services

```bash
docker compose -f examples/production/docker-compose.yml up -d
```

## Install Extras

```bash
uv sync --extra redis --extra postgres --extra dev
```

## Prepare Roost

```bash
uv run roost migrate --config examples/production/roost.toml
uv run roost doctor --config examples/production/roost.toml
```

`doctor` should fail before migrations and pass after migrations are applied.
Use it whenever setup feels suspicious; it checks Redis, Postgres, migrations,
engines, artifacts, workspace paths, and queue settings.

## Run A Worker

```bash
uv run roost worker --config examples/production/roost.toml
```

## Open The Console

```bash
uv run roost ui --config examples/production/roost.toml
```

The console runs at `http://127.0.0.1:8766` by default.

## Enqueue Work

In another terminal:

```bash
uv run roost enqueue \
  --config examples/production/roost.toml \
  --engine watchlist \
  --resource domain:example.com \
  --payload '{"url":"https://example.com","claim":"Example is reachable","checks_required":3,"delay_seconds":5}'
```

## Inspect Runtime State

```bash
uv run roost list --config examples/production/roost.toml
uv run roost events --config examples/production/roost.toml
uv run roost workers --config examples/production/roost.toml
uv run roost dlq list --config examples/production/roost.toml
```

What to look for:

- `list` shows durable work metadata from Postgres.
- `events` shows the runtime history.
- `workers` shows active or stale worker heartbeats.
- `dlq list` shows failed work that needs replay or acknowledgement.

## Troubleshooting

- If `doctor` says migrations are missing, run `uv run roost migrate --config examples/production/roost.toml`.
- If `workers` is empty, start `uv run roost worker --config examples/production/roost.toml` and wait a few seconds.
- If the console cannot load work, confirm both Redis and Postgres are running with `docker compose -f examples/production/docker-compose.yml ps`.
- If you changed ports, update `examples/production/roost.toml` to match.

## Stop Services

```bash
docker compose -f examples/production/docker-compose.yml down
```

To remove the local Postgres volume too:

```bash
docker compose -f examples/production/docker-compose.yml down -v
```
