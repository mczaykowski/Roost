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
```

## Stop Services

```bash
docker compose -f examples/production/docker-compose.yml down
```

To remove the local Postgres volume too:

```bash
docker compose -f examples/production/docker-compose.yml down -v
```
