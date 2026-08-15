from __future__ import annotations

from importlib import resources

from roost.cli import build_parser


def test_ui_command_is_registered():
    parser = build_parser()
    args = parser.parse_args(["ui", "--port", "9001"])

    assert args.cmd == "ui"
    assert args.port == 9001


def test_console_static_tokens_are_packaged():
    tokens = resources.files("roost.ui.static") / "tokens.css"
    index = resources.files("roost.ui.static") / "index.html"

    assert "--color-brand" in tokens.read_text(encoding="utf-8")
    assert "Roost Console" in index.read_text(encoding="utf-8")


def test_console_static_includes_recovery_actions():
    app = resources.files("roost.ui.static").joinpath("app.js").read_text(encoding="utf-8")
    index = resources.files("roost.ui.static").joinpath("index.html").read_text(encoding="utf-8")

    assert "/retry" in app
    assert "/cancel" in app
    assert "/replay" in app
    assert "/ack" in app
    assert "detailActions" in index
    assert "formatAge" in app
    assert ">Age<" in index
    assert "age_seconds" in app
    assert "work_recovered" in app
