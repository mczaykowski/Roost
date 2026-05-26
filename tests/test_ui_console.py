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
