from __future__ import annotations

from roost.runtime import metrics


def test_record_helpers_never_raise() -> None:
    metrics.record_step("demo", "success")
    metrics.record_retry()
    metrics.record_dlq()
    metrics.record_lease_contention()


def test_generate_latest_exports_counters_or_disables_cleanly() -> None:
    try:
        import prometheus_client  # noqa: F401
    except ImportError:
        assert metrics.enabled() is False
        metrics.record_step("demo", "error")
        metrics.record_retry()
        metrics.record_dlq()
        metrics.record_lease_contention()
        assert metrics.generate_latest() == b""
        return

    assert metrics.enabled() is True
    metrics.record_step("demo", "success")
    metrics.record_step("watchlist", "busy")
    metrics.record_retry()
    metrics.record_dlq()
    metrics.record_lease_contention()

    body = metrics.generate_latest().decode("utf-8")
    assert "roost_steps_total" in body
    assert "roost_retries_total" in body
    assert "roost_dlq_total" in body
    assert "roost_lease_contention_total" in body
    assert 'engine="demo"' in body
    assert 'status="success"' in body
    assert "text/plain" in metrics.content_type()
