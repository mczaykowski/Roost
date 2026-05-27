# Contributing

Roost is intentionally small. The best contributions keep the runtime easier to
understand, safer to operate, or simpler to adopt.

## Local Setup

Install dependencies:

```bash
uv sync --extra redis --extra dev
```

Run tests and lint:

```bash
uv run --extra dev --extra redis pytest -q
uv run --extra dev ruff check .
```

Run the local end-to-end watchlist demo:

```bash
scripts/e2e_watchlist.sh
```

## Design Principles

- Keep the engine contract small.
- Prefer boring infrastructure over clever abstractions.
- Add operational guarantees only when tests can prove them.
- Preserve at-least-once execution semantics.
- Make retry and recovery behavior visible to operators.
- Do not add prompt-framework, model-router, or workflow-DSL concepts.

## Pull Requests

For runtime changes, include tests for the failure mode the change affects. For
UI changes, include a short note about the operator workflow being improved.

Before opening a PR, run:

```bash
uv run --extra dev --extra redis pytest -q
uv run --extra dev ruff check .
```

If your change affects worker recovery, retries, resource claims, artifacts, or
the watchlist demo, also run:

```bash
scripts/e2e_watchlist.sh
```

