import os
from unittest.mock import patch

from dispatch.cli import build_parser, main


def test_cli_accepts_db_after_subcommands():
    parser = build_parser()

    # After 'import'
    args_import = parser.parse_args(["import", "--campaign", "launch-1", "--file", "waitlist.json", "--db", "custom.db"])
    assert args_import.command == "import"
    assert args_import.campaign == "launch-1"
    assert args_import.file == "waitlist.json"
    assert args_import.db == "custom.db"

    # After 'send'
    args_send = parser.parse_args(["send", "--campaign", "launch-1", "--db", "custom.db"])
    assert args_send.command == "send"
    assert args_send.campaign == "launch-1"
    assert args_send.db == "custom.db"

    # After 'status'
    args_status = parser.parse_args(["status", "--campaign", "launch-1", "--db", "custom.db"])
    assert args_status.command == "status"
    assert args_status.campaign == "launch-1"
    assert args_status.db == "custom.db"


def test_cli_accepts_db_before_subcommands():
    parser = build_parser()
    args = parser.parse_args(["--db", "custom.db", "import", "--campaign", "c1", "--file", "waitlist.json"])
    assert args.command == "import"
    assert args.db == "custom.db"


def test_cli_respects_dispatch_db_path_env():
    with patch.dict(os.environ, {"DISPATCH_DB_PATH": "env_configured.db"}):
        parser = build_parser()
        args = parser.parse_args(["status", "--campaign", "c1"])
        assert args.db == "env_configured.db"


def test_cli_send_template_vars():
    parser = build_parser()
    args = parser.parse_args([
        "send",
        "--campaign", "c1",
        "--template-vars", '{"pre_order_url": "https://example.com/custom"}'
    ])
    assert args.template_vars == '{"pre_order_url": "https://example.com/custom"}'

