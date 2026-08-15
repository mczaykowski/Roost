"""Optional Prometheus counters for the Roost worker.

Install with ``uv sync --extra metrics`` (``roost-runtime[metrics]``). Without
the extra, every ``record_*`` call is a no-op so the worker never crashes.

Scrape target: ``GET /metrics`` on the console (``roost ui``) when the extra is
installed. ``roost doctor`` reports whether metrics are enabled.
"""

from __future__ import annotations

from typing import Any

_loaded = False
_client: Any = None
_registry: Any = None
_steps: Any = None
_retries: Any = None
_dlq: Any = None
_lease_contention: Any = None


def _load() -> bool:
    global _loaded, _client, _registry, _steps, _retries, _dlq, _lease_contention
    if _loaded:
        return _client is not None
    _loaded = True
    try:
        import prometheus_client
    except ImportError:
        return False

    _client = prometheus_client
    _registry = prometheus_client.CollectorRegistry()
    _steps = prometheus_client.Counter(
        "roost_steps_total",
        "Roost step outcomes by engine and status",
        ["engine", "status"],
        registry=_registry,
    )
    _retries = prometheus_client.Counter(
        "roost_retries_total",
        "Roost step retries (version conflict / retry status)",
        registry=_registry,
    )
    _dlq = prometheus_client.Counter(
        "roost_dlq_total",
        "Work items pushed to the dead-letter queue",
        registry=_registry,
    )
    _lease_contention = prometheus_client.Counter(
        "roost_lease_contention_total",
        "Step attempts that could not acquire a work lease",
        registry=_registry,
    )
    return True


def enabled() -> bool:
    return _load()


def record_step(engine: str, status: str) -> None:
    if not _load():
        return
    _steps.labels(engine=engine or "unknown", status=status or "unknown").inc()


def record_retry() -> None:
    if not _load():
        return
    _retries.inc()


def record_dlq() -> None:
    if not _load():
        return
    _dlq.inc()


def record_lease_contention() -> None:
    if not _load():
        return
    _lease_contention.inc()


def content_type() -> str:
    if not _load():
        return "text/plain; charset=utf-8"
    return str(_client.CONTENT_TYPE_LATEST)


def generate_latest() -> bytes:
    if not _load():
        return b""
    return bytes(_client.generate_latest(_registry))
