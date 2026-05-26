from __future__ import annotations

import json

import pytest

from roost.engines.watchlist.engine import WatchlistEngine
from roost.runtime.models import WorkItem


@pytest.mark.asyncio
async def test_watchlist_engine_records_observations_and_writes_report(tmp_path, monkeypatch):
    engine = WatchlistEngine(artifact_root=str(tmp_path))
    item = WorkItem(
        work_id="watch-1",
        engine="watchlist",
        payload={
            "url": "https://example.test",
            "claim": "Example is reachable",
            "checks_required": 2,
            "delay_seconds": 0,
        },
    )

    observed = []

    def fake_observe(url: str):
        observed.append(url)
        return {
            "ok": True,
            "status": 200,
            "url": url,
            "title": "Example",
            "bytes_sampled": 42,
            "body_sha256": "abc",
            "elapsed_ms": 1,
            "observed_at": 123.0 + len(observed),
        }

    monkeypatch.setattr(engine, "_observe_url", fake_observe)

    snapshot = await engine.init_snapshot(item)
    assert snapshot.step == "check"
    assert snapshot.data["checks_completed"] == 0

    snapshot = await engine.step(snapshot, item)
    assert snapshot.is_finished is False
    assert snapshot.next_step_delay_seconds == 0
    assert snapshot.data["checks_completed"] == 1
    assert snapshot.data["observations"][0]["title"] == "Example"

    snapshot = await engine.step(snapshot, item)
    assert snapshot.is_finished is True
    assert snapshot.step == "done"
    assert snapshot.data["verdict"] == "reachable"
    assert len(snapshot.artifacts) == 1

    artifact = snapshot.artifacts[0]
    report_bytes = engine.artifacts.read_bytes(artifact.artifact_id, ext="json")
    assert report_bytes is not None
    report = json.loads(report_bytes)
    assert report["url"] == "https://example.test"
    assert report["checks_completed"] == 2
    assert report["verdict"] == "reachable"


@pytest.mark.asyncio
async def test_watchlist_engine_does_not_observe_before_next_check(tmp_path, monkeypatch):
    engine = WatchlistEngine(artifact_root=str(tmp_path))
    item = WorkItem(
        work_id="watch-2",
        engine="watchlist",
        payload={
            "url": "https://example.test",
            "checks_required": 2,
            "delay_seconds": 60,
        },
    )

    calls = 0

    def fake_observe(url: str):
        nonlocal calls
        calls += 1
        return {
            "ok": True,
            "status": 200,
            "url": url,
            "observed_at": 100.0,
        }

    monkeypatch.setattr(engine, "_observe_url", fake_observe)
    snapshot = await engine.init_snapshot(item)
    snapshot = await engine.step(snapshot, item)

    early = await engine.step(snapshot, item)

    assert calls == 1
    assert early.is_finished is False
    assert early.data["checks_completed"] == 1
    assert early.next_step_delay_seconds > 0
