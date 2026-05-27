# Troubleshooting

This page covers the common local-development issues.

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
namespace.

Check these values:

- `ROOST_REDIS_URL`
- `ROOST_QUEUE`
- `ROOST_REDIS_PREFIX`
- `ROOST_NAMESPACE`

For local testing, start the console with the same values used by the worker:

```bash
uv run roost ui --redis-url redis://localhost:6379/0 --redis-prefix roost
```

If you are using `roost.toml`, check what Roost sees:

```bash
uv run roost doctor --config roost.toml
```

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
```

## Artifacts Are Missing

Artifacts are stored on the local filesystem by default. The worker and the CLI
must use the same artifact root.

If a worker used a custom artifact root, pass it when reading:

```bash
uv run roost artifact-show <artifact_id> --ext json --artifact-root <path>
```

## The E2E Script Leaves A Container Behind

The e2e script normally removes its Redis container on exit. If a local run is
interrupted hard, remove old containers manually:

```bash
docker ps -a --filter "name=roost-e2e-redis"
docker rm -f <container_id>
```
