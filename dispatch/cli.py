from __future__ import annotations

import argparse
import logging
import os
import sys

from .config import ConfigError, Settings
from .db import StateStore
from .engine import SendEngine, enqueue_campaign, import_recipients
from .providers.resend_provider import ResendProvider
from .sources.json_source import JSONFileSource
from .templates.launch import LaunchTemplate
from .validation import SyntaxAndMXValidator, SyntaxOnlyValidator, DNS_AVAILABLE

# Registries: this is the one place that knows concrete source/template
# names. Add a new Source or Template class, register it here, and every
# CLI command (`--source foo`, `--template bar`) picks it up.
SOURCES = {
    "waitlist_json": lambda args: JSONFileSource(args.file),
}
TEMPLATES = {
    "launch": lambda template_vars, settings: LaunchTemplate(
        **(template_vars or {})
    ),
}


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )


def _build_validator(args: argparse.Namespace, settings: Settings):
    if args.skip_validation or not settings.validate_deliverability:
        return None
    if not DNS_AVAILABLE:
        logging.getLogger(__name__).warning(
            "dnspython not installed — falling back to syntax-only validation. "
            "`pip install dnspython` to also catch dead domains via MX lookup."
        )
        return SyntaxOnlyValidator()
    return SyntaxAndMXValidator(timeout_seconds=settings.mx_lookup_timeout_seconds)


def cmd_import(args: argparse.Namespace) -> int:
    store = StateStore(args.db)
    source = SOURCES[args.source](args)
    stats = import_recipients(source, store, args.campaign)
    print(f"Read {stats.read} records — {stats.new} new, {stats.already_known} already known.")
    print(f"Total recipients on file for campaign '{args.campaign}': {store.count_recipients(args.campaign)}")
    return 0


def cmd_send(args: argparse.Namespace) -> int:
    try:
        settings = Settings.from_env()
    except ConfigError as e:
        if args.dry_run:
            os.environ.setdefault("RESEND_API_KEY", "re_dryrun_placeholder")
            os.environ.setdefault("FROM_EMAIL", "dryrun@example.com")
            try:
                settings = Settings.from_env()
            except ConfigError as e2:
                print(f"Config error: {e2}", file=sys.stderr)
                return 1
        else:
            print(f"Config error: {e}", file=sys.stderr)
            return 1

    store = StateStore(args.db)
    validator = _build_validator(args, settings)
    enqueue_stats = enqueue_campaign(store, args.campaign, validator=validator)
    print(
        f"Queued {enqueue_stats.queued} new job(s) for campaign '{args.campaign}' "
        f"({enqueue_stats.skipped_invalid} skipped for failing deliverability checks, "
        f"{enqueue_stats.already_queued} already queued from a previous run)."
    )

    template_vars = {}
    if args.template_vars:
        import json
        if args.template_vars.startswith("{"):
            template_vars = json.loads(args.template_vars)
        else:
            with open(args.template_vars) as f:
                template_vars = json.load(f)

    template = TEMPLATES[args.template](template_vars, settings)
    provider = ResendProvider(
        settings.resend_api_key,
        rate_limit_per_second=settings.rate_limit_per_second,
        max_batch_size=settings.max_batch_size,
    )
    engine = SendEngine(store, provider, template, settings, dry_run=args.dry_run)

    if args.watch:
        stats = engine.run_until_drained(args.campaign, max_wait_seconds=args.max_wait)
    else:
        stats = engine.run_once(args.campaign)

    print(
        f"claimed={stats.claimed} sent={stats.sent} "
        f"failed_retryable={stats.failed_retryable} failed_exhausted={stats.failed_exhausted}"
    )
    print(f"Campaign totals: {store.campaign_stats(args.campaign)}")
    if stats.failed_retryable and not args.watch:
        print("Some jobs are backed off and waiting to retry — run `dispatch send` again later, "
              "or pass --watch, or schedule this command on a cron/systemd timer.")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    store = StateStore(args.db)
    print(f"Campaign: {args.campaign}")
    print(store.campaign_stats(args.campaign))

    failed = store.failed_jobs(args.campaign, limit=args.limit)
    if failed:
        print(f"\nMost recent failures (showing up to {args.limit}):")
        for row in failed:
            print(f"  [{row['status']:<7}] {row['email']:<40} attempts={row['attempts']:<3} {row['last_error']}")

    skipped = store.skipped_jobs(args.campaign, limit=args.limit)
    if skipped:
        print(f"\nSkipped at enqueue time — never attempted (showing up to {args.limit}):")
        for row in skipped:
            print(f"  {row['email']:<40} {row['last_error']}")
    return 0


class DispatchParser(argparse.ArgumentParser):
    """Custom parser that ensures `--db` default is safely applied even when
    `--db` was specified before a subcommand."""

    def parse_args(self, args=None, namespace=None):
        res = super().parse_args(args=args, namespace=namespace)
        if not hasattr(res, "db") or res.db is None:
            res.db = os.environ.get("DISPATCH_DB_PATH", "dispatch.db")
        return res


def build_parser() -> argparse.ArgumentParser:
    default_db = os.environ.get("DISPATCH_DB_PATH", "dispatch.db")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=argparse.SUPPRESS, help=f"Path to the SQLite state file (default: {default_db})")
    common.add_argument("-v", "--verbose", action="store_true", help="Enable verbose/debug logging")

    parser = DispatchParser(prog="dispatch", description="MiyuLabs waitlist / campaign mailer", parents=[common])
    sub = parser.add_subparsers(dest="command", required=True)

    p_import = sub.add_parser("import", help="Load recipients from a source into local state", parents=[common])
    p_import.add_argument("--campaign", required=True, help="Campaign to link imported recipients to")
    p_import.add_argument("--source", choices=SOURCES.keys(), default="waitlist_json")
    p_import.add_argument("--file", required=True, help="Path to the source file, e.g. waitlist.json")
    p_import.set_defaults(func=cmd_import)

    p_send = sub.add_parser("send", help="Enqueue and send a campaign to all known recipients", parents=[common])
    p_send.add_argument("--campaign", required=True, help="Campaign name, e.g. launch-2026-09")
    p_send.add_argument("--template", choices=TEMPLATES.keys(), default="launch")
    p_send.add_argument("--template-vars", default=None, help="JSON string or path to JSON file with variables for the template")
    p_send.add_argument("--dry-run", action="store_true", help="Render and log, but never call the provider")
    p_send.add_argument("--watch", action="store_true", help="Keep retrying backed-off jobs until the queue drains")
    p_send.add_argument("--max-wait", type=float, default=3600, help="Max seconds to stay in --watch mode")
    p_send.add_argument("--skip-validation", action="store_true",
                         help="Don't run deliverability checks (syntax + MX) before enqueueing")
    p_send.set_defaults(func=cmd_send)

    p_status = sub.add_parser("status", help="Show campaign progress, failures, and skipped recipients", parents=[common])
    p_status.add_argument("--campaign", required=True)
    p_status.add_argument("--limit", type=int, default=20)
    p_status.set_defaults(func=cmd_status)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
