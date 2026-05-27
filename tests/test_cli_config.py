from __future__ import annotations

from roost.cli import _apply_runtime_config, build_parser


def test_init_writes_minimal_roost_toml(tmp_path):
    path = tmp_path / "roost.toml"
    parser = build_parser()
    args = parser.parse_args(["init", "--path", str(path)])

    args.fn(args)

    content = path.read_text(encoding="utf-8")
    assert "[redis]" in content
    assert 'url = "redis://localhost:6379/0"' in content
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
    args = parser.parse_args(["doctor", "--engines", "watchlist"])

    assert args.cmd == "doctor"
    assert args.engines == "watchlist"
