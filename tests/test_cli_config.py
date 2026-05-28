from __future__ import annotations

from roost.cli import _apply_runtime_config, build_parser


def test_init_writes_minimal_roost_toml(tmp_path):
    path = tmp_path / "roost.toml"
    parser = build_parser()
    args = parser.parse_args(["init", "--path", str(path)])

    args.fn(args)

    content = path.read_text(encoding="utf-8")
    assert "[redis]" in content
    assert "[runtime]" in content
    assert 'url = "redis://localhost:6379/0"' in content
    assert "[postgres]" in content
    assert "[worker]" in content
    assert 'engines = "watchlist"' in content
    assert "[artifacts]" in content


def test_config_file_supplies_runtime_defaults(tmp_path, monkeypatch):
    monkeypatch.delenv("ROOST_REDIS_URL", raising=False)
    monkeypatch.delenv("ROOST_QUEUE", raising=False)
    monkeypatch.delenv("ROOST_REDIS_PREFIX", raising=False)
    monkeypatch.delenv("ROOST_NAMESPACE", raising=False)

    path = tmp_path / "roost.toml"
    path.write_text(
        """
[redis]
url = "redis://localhost:6381/0"
queue = "critical"
prefix = "roost-test"
namespace = "acme/dev"

[runtime]
mode = "production"

[postgres]
url = "postgresql://localhost/roost_test"

[worker]
engines = "demo,watchlist"
concurrency = 2
timeout_seconds = 30
retries = 3
lease_ttl_seconds = 15
workspace_root = ".roost/workspaces"
workspace_mode = "clone"

[artifacts]
root = ".roost/artifacts"
""".strip(),
        encoding="utf-8",
    )

    parser = build_parser()
    args = parser.parse_args(["worker", "--config", str(path), "--repo-path", str(tmp_path)])
    _apply_runtime_config(args)

    assert args.redis_url == "redis://localhost:6381/0"
    assert args.queue == "critical"
    assert args.redis_prefix == "roost-test"
    assert args.namespace == "acme/dev"
    assert args.engines == "demo,watchlist"
    assert args.concurrency == 2
    assert args.timeout == 30
    assert args.retries == 3
    assert args.lease_ttl == 15
    assert args.workspace_mode == "clone"
    assert args.workspace_root == str(tmp_path / ".roost" / "workspaces")
    assert args.artifact_root == str(tmp_path / ".roost" / "artifacts")
    assert args.runtime_mode == "production"
    assert args.postgres_url == "postgresql://localhost/roost_test"

    status_args = parser.parse_args(["status", "--config", str(path), "work-1"])
    _apply_runtime_config(status_args)
    assert status_args.runtime_mode == "production"
    assert status_args.postgres_url == "postgresql://localhost/roost_test"

    ui_args = parser.parse_args(["ui", "--config", str(path), "--repo-path", str(tmp_path)])
    _apply_runtime_config(ui_args)
    assert ui_args.runtime_mode == "production"
    assert ui_args.postgres_url == "postgresql://localhost/roost_test"

    migrate_args = parser.parse_args(["migrate", "--config", str(path), "--repo-path", str(tmp_path)])
    _apply_runtime_config(migrate_args)
    assert migrate_args.runtime_mode == "production"
    assert migrate_args.postgres_url == "postgresql://localhost/roost_test"


def test_cli_flags_override_config_file(tmp_path):
    path = tmp_path / "roost.toml"
    path.write_text(
        """
[redis]
url = "redis://localhost:6381/0"
queue = "from-config"

[worker]
engines = "demo"
concurrency = 2
""".strip(),
        encoding="utf-8",
    )

    parser = build_parser()
    args = parser.parse_args(
        [
            "worker",
            "--config",
            str(path),
            "--redis-url",
            "redis://localhost:6399/0",
            "--queue",
            "from-cli",
            "--engines",
            "watchlist",
            "--concurrency",
            "8",
        ]
    )
    _apply_runtime_config(args)

    assert args.redis_url == "redis://localhost:6399/0"
    assert args.queue == "from-cli"
    assert args.engines == "watchlist"
    assert args.concurrency == 8


def test_doctor_command_is_registered():
    parser = build_parser()
    args = parser.parse_args(
        [
            "doctor",
            "--engines",
            "watchlist",
            "--runtime-mode",
            "production",
            "--postgres-url",
            "postgresql://localhost/roost",
        ]
    )

    assert args.cmd == "doctor"
    assert args.engines == "watchlist"
    assert args.runtime_mode == "production"
    assert args.postgres_url == "postgresql://localhost/roost"


def test_migrate_command_is_registered():
    parser = build_parser()
    args = parser.parse_args(["migrate", "--plan"])

    assert args.cmd == "migrate"
    assert args.plan is True


def test_operator_recovery_commands_are_registered():
    parser = build_parser()

    inspect_args = parser.parse_args(["inspect", "work-1"])
    retry_args = parser.parse_args(["retry", "work-1", "--delay-seconds", "5"])
    cancel_args = parser.parse_args(["cancel", "work-1", "--reason", "operator_request"])
    dlq_list_args = parser.parse_args(["dlq", "list", "--limit", "10"])
    dlq_replay_args = parser.parse_args(["dlq", "replay", "0", "--ack"])
    dlq_ack_args = parser.parse_args(["dlq", "ack", "0"])
    list_args = parser.parse_args(["list", "--runtime-mode", "production", "--postgres-url", "postgresql://localhost/roost"])
    events_args = parser.parse_args(
        ["events", "--runtime-mode", "production", "--postgres-url", "postgresql://localhost/roost"]
    )
    workers_args = parser.parse_args(
        ["workers", "--runtime-mode", "production", "--postgres-url", "postgresql://localhost/roost"]
    )

    assert inspect_args.cmd == "inspect"
    assert retry_args.cmd == "retry"
    assert retry_args.delay_seconds == 5
    assert cancel_args.cmd == "cancel"
    assert cancel_args.reason == "operator_request"
    assert dlq_list_args.cmd == "dlq"
    assert dlq_list_args.dlq_cmd == "list"
    assert dlq_replay_args.dlq_cmd == "replay"
    assert dlq_replay_args.ack is True
    assert dlq_ack_args.dlq_cmd == "ack"
    assert list_args.runtime_mode == "production"
    assert events_args.postgres_url == "postgresql://localhost/roost"
    assert workers_args.cmd == "workers"
    assert workers_args.stale_after == 30


def test_migrate_plan_prints_packaged_migrations(capsys):
    parser = build_parser()
    args = parser.parse_args(["migrate", "--plan"])

    args.fn(args)

    out = capsys.readouterr().out
    assert "0001" in out
    assert "0001_initial.sql" in out
