# Release Checklist

Roost releases should be boring and repeatable.

## Before Tagging

- Confirm the version in `pyproject.toml`.
- Update `CHANGELOG.md`.
- Update the README if the public workflow changed.
- Update `ROADMAP.md` if a phase gate moved.
- Build package artifacts:

```bash
uv build
```

- If migrations changed, inspect the packaged migration plan:

```bash
uv run roost migrate --plan
```

- If migration behavior changed, run the local Postgres e2e:

```bash
scripts/e2e_postgres_migrate.sh
```

- If production runtime behavior changed, run the Postgres-backed watchlist e2e:

```bash
scripts/e2e_postgres_watchlist.sh
```

- Run unit tests:

```bash
uv run --extra dev --extra redis pytest -q
```

- Run lint:

```bash
uv run --extra dev ruff check .
```

- Run the local e2e:

```bash
scripts/e2e_watchlist.sh
```

## Manual Smoke Test

- Start Redis.
- Run a watchlist worker.
- Enqueue a watchlist job.
- Kill and restart the worker.
- Confirm the job finishes from the latest persisted snapshot.
- Open the console and verify work, events, and artifacts are visible.

## Release Notes

Use this shape:

```markdown
## Roost <version>

### Changed
- ...

### Fixed
- ...

### Operational Notes
- ...
```
