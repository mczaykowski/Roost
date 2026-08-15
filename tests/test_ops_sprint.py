"""Sprint OPS unit tests that do not need Redis or Postgres."""

from __future__ import annotations

import json
import logging
import sys
import time

from roost.cli import _apply_runtime_config, build_parser, configure_worker_logging
from roost.runtime.backends.postgres.stores import annotate_worker_liveness
from roost.runtime.models import Snapshot
from roost.runtime.swarm import _RedisSwarmRuntime, is_wait_only_step


def test_configure_worker_logging_installs_stderr_handler():
    root = logging.getLogger()
    runtime = logging.getLogger("roost.runtime")
    old_handlers = root.handlers[:]
    old_level = root.level
    old_runtime_level = runtime.level
    try:
        root.handlers.clear()
        configure_worker_logging("warning")
        assert root.level == logging.WARNING
        assert runtime.level == logging.WARNING
        assert any(getattr(handler, "stream", None) is sys.stderr for handler in root.handlers)
    finally:
        root.handlers[:] = old_handlers
        root.setLevel(old_level)
        runtime.setLevel(old_runtime_level)


def test_log_step_boundary_emits_json_schema(caplog):
    runtime = _RedisSwarmRuntime.__new__(_RedisSwarmRuntime)
    started = time.perf_counter()
    with caplog.at_level(logging.INFO, logger="roost.runtime"):
        runtime._log_step_boundary(
            work_id="work-1",
            engine="watchlist",
            step="check",
            attempt=2,
            status="success",
            version=4,
            started_at=started,
        )
    assert caplog.records
    payload = json.loads(caplog.records[0].message)
    assert payload["work_id"] == "work-1"
    assert payload["engine"] == "watchlist"
    assert payload["step"] == "check"
    assert payload["attempt"] == 2
    assert payload["status"] == "success"
    assert payload["version"] == 4
    assert "duration_ms" in payload
    assert isinstance(payload["duration_ms"], int)


def test_worker_cli_exposes_ops_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "worker",
            "--log-level",
            "debug",
            "--stale-after",
            "2",
            "--recovery-interval",
            "1",
            "--heartbeat-interval",
            "5",
            "--lease-ttl",
            "4",
        ]
    )
    assert args.log_level == "debug"
    assert args.stale_after == 2.0
    assert args.recovery_interval == 1.0
    assert args.heartbeat_interval == 5.0
    assert args.lease_ttl == 4


def test_worker_config_file_supplies_recovery_knobs(tmp_path, monkeypatch):
    monkeypatch.delenv("ROOST_REDIS_URL", raising=False)
    path = tmp_path / "roost.toml"
    path.write_text(
        """
[worker]
stale_after_seconds = 7
recovery_interval_seconds = 1.5
heartbeat_interval_seconds = 4
lease_ttl_seconds = 9
""".strip(),
        encoding="utf-8",
    )
    parser = build_parser()
    args = parser.parse_args(["worker", "--config", str(path), "--repo-path", str(tmp_path)])
    _apply_runtime_config(args)
    assert args.stale_after == 7
    assert args.recovery_interval == 1.5
    assert args.heartbeat_interval == 4
    assert args.lease_ttl == 9


def test_worker_cli_overrides_recovery_knobs(tmp_path):
    path = tmp_path / "roost.toml"
    path.write_text(
        """
[worker]
stale_after_seconds = 30
recovery_interval_seconds = 2
heartbeat_interval_seconds = 10
""".strip(),
        encoding="utf-8",
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "worker",
            "--config",
            str(path),
            "--stale-after",
            "2",
            "--recovery-interval",
            "0.5",
            "--heartbeat-interval",
            "3",
        ]
    )
    _apply_runtime_config(args)
    assert args.stale_after == 2.0
    assert args.recovery_interval == 0.5
    assert args.heartbeat_interval == 3.0


def test_workers_cli_keeps_display_stale_after_default():
    parser = build_parser()
    args = parser.parse_args(["workers"])
    assert args.stale_after == 30


def test_wait_only_step_ignores_delay_fields():
    previous = Snapshot(
        work_id="w1",
        engine="watchlist",
        version=3,
        step="check",
        data={"checks_completed": 1, "next_check_after": 100.0},
        next_step_delay_seconds=0.0,
    )
    waiting = previous.model_copy()
    waiting.next_step_delay_seconds = 4.2
    waiting.data = {**previous.data, "next_check_after": 99.0}
    waiting.updated_at = previous.updated_at + 10
    assert is_wait_only_step(previous, waiting) is True

    observed = previous.model_copy()
    observed.data = {
        **previous.data,
        "checks_completed": 2,
        "observations": [{"ok": True}],
        "next_check_after": 200.0,
    }
    observed.next_step_delay_seconds = 5.0
    assert is_wait_only_step(previous, observed) is False

    finished = previous.model_copy()
    finished.is_finished = True
    finished.step = "done"
    assert is_wait_only_step(previous, finished) is False


def test_annotate_worker_liveness_includes_age_and_stale():
    live = annotate_worker_liveness(
        {"worker_id": "a", "last_seen_at": 100.0},
        now=108.0,
        stale_after_seconds=15,
    )
    assert live["age_seconds"] == 8.0
    assert live["stale"] is False

    dead = annotate_worker_liveness(
        {"worker_id": "b", "last_seen_at": 100.0},
        now=125.0,
        stale_after_seconds=15,
    )
    assert dead["age_seconds"] == 25.0
    assert dead["stale"] is True

    no_window = annotate_worker_liveness(
        {"worker_id": "c", "last_seen_at": 100.0},
        now=110.0,
    )
    assert no_window["age_seconds"] == 10.0
    assert "stale" not in no_window
