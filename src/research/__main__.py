"""Command-line entry point for bounded research, audit, and export tasks."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from .credentials import save_bduss_interactively

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(2, f"{self.prog}: error: invalid command line\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(prog="python -m src.research")
    commands = parser.add_subparsers(dest="command", required=True)

    login = commands.add_parser("login", help="save BDUSS locally after manual Baidu login")
    login.add_argument("--browser", action="store_true", help="open a temporary browser for manual QR login")
    login.add_argument("--account-file", type=Path, help="local account JSON path")

    export = commands.add_parser("export", help="export all local research data as CSV")
    export.add_argument("--output", type=Path, default=_repo_root() / "exports")
    export.add_argument(
        "--run-id",
        help=("filter the crawl-quality report and label this export batch; entity CSVs always include all local runs"),
    )
    export.add_argument(
        "--entity-scope",
        choices=("all", "completed-threads"),
        default="all",
        help="all exports every entity by default; completed-threads selects complete current detail scopes",
    )

    from .web_fallback.tasks import WEB_FIELD_TYPES

    field_choices = tuple(sorted(WEB_FIELD_TYPES))
    field_backfill = commands.add_parser(
        "field-backfill",
        help="backfill selected fields for existing API-scoped targets only",
        description=(
            "Process a bounded set of existing rows from one completed crawl run. API-incomplete scopes remain "
            "pending and are never sent to the web provider."
        ),
    )
    _add_run_target_arguments(field_backfill, field_choices, audit=False)
    field_backfill.add_argument("--limit", type=_positive_int_arg, default=5, help="maximum total targets (default: 5)")
    field_backfill.add_argument(
        "--max-seconds", type=_positive_int_arg, default=120, help="overall time limit (default: 120)"
    )
    field_backfill.add_argument(
        "--dry-run", action="store_true", help="plan eligible targets without tasks, provider calls, or observations"
    )
    field_backfill.add_argument(
        "--browser-profile", type=Path, help="explicit project-isolated persistent browser profile path"
    )
    field_backfill.add_argument("--headed", action="store_true", help="show the isolated browser window")
    field_backfill.add_argument(
        "--wait-for-login",
        action="store_true",
        help=(
            "pause without a lease for manual login/verification; waiting counts toward --max-seconds, "
            "then resume this target once"
        ),
    )

    web_audit = commands.add_parser(
        "web-audit",
        help="write a private read-only comparison report for a small sample",
        description=(
            "Collect only exact API-scope targets and write a local report without task or canonical-row writes."
        ),
    )
    _add_run_target_arguments(web_audit, field_choices, audit=True)
    web_audit.add_argument("--limit", type=_positive_int_arg, default=1, help="sample size, 1 to 5 (default: 1)")
    web_audit.add_argument("--max-seconds", type=_positive_int_arg, default=120)
    web_audit.add_argument(
        "--browser-profile", type=Path, required=True, help="explicit project-isolated persistent browser profile path"
    )
    web_audit.add_argument("--headed", action="store_true", help="show the isolated browser window")
    web_audit.add_argument(
        "--wait-for-login",
        action="store_true",
        help=(
            "pause without a lease for manual login/verification; waiting counts toward --max-seconds, "
            "then retry this target once"
        ),
    )
    web_audit.add_argument(
        "--report-dir",
        type=Path,
        default=_repo_root() / "test-data" / "v2-web-audit",
        help="private local report directory",
    )

    user_backfill = commands.add_parser(
        "user-backfill",
        help="fetch current user/profile/forum snapshots for existing authors",
        description=(
            "Updates current ResearchUser/UserForumSnapshot rows only; never rewrites historical content authors."
        ),
    )
    user_backfill.add_argument("--run-id", required=True, help="existing crawl run ID")
    user_backfill.add_argument("--tid", type=_positive_int_arg, help="restrict target authors to this thread")
    user_backfill.add_argument("--fid", type=_positive_int_arg, help="restrict target authors to this forum")
    user_backfill.add_argument(
        "--limit", type=_positive_int_arg, default=5, help="maximum total user/forum targets (default: 5)"
    )
    user_backfill.add_argument("--max-seconds", type=_positive_int_arg, default=120)
    user_backfill.add_argument(
        "--profile", action="store_true", help="fetch current global user profile once per unique user ID"
    )
    user_backfill.add_argument(
        "--forum", action="store_true", help="fetch current forum snapshot once per user/forum pair"
    )
    user_backfill.add_argument("--bawu", action="store_true", help="fetch current forum role from the cached roster")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "field-backfill" and args.wait_for_login and not args.headed:
        raise SystemExit("--wait-for-login requires --headed")
    if args.command == "web-audit" and args.wait_for_login and not args.headed:
        raise SystemExit("--wait-for-login requires --headed")
    if args.command == "login":
        if args.browser:
            path = asyncio.run(_login_with_browser(args.account_file))
        else:
            path = save_bduss_interactively(args.account_file)
        print(f"Saved local Baidu account file: {path}")
        return 0

    if args.command == "field-backfill":
        runner = _run_field_backfill_cli
    elif args.command == "web-audit":
        runner = _run_web_audit_cli
    elif args.command == "user-backfill":
        runner = _run_user_backfill_cli
    else:
        runner = None
    if runner is not None:
        try:
            return asyncio.run(runner(args))
        except (ValueError, OSError) as error:
            print(f"{args.command} stopped safely ({type(error).__name__}).", file=sys.stderr)
            return 2
        except Exception as error:
            from .web_fallback.browser import BrowserProviderConfigurationError

            if isinstance(error, BrowserProviderConfigurationError):
                print(
                    "Browser support setup failed. Install optional browser support with "
                    "`uv sync --group login` and `uv run playwright install chromium`; "
                    "use an explicit project-isolated profile path.",
                    file=sys.stderr,
                )
                return 2
            print(f"{args.command} failed safely ({type(error).__name__}).", file=sys.stderr)
            return 2

    batch_directory = asyncio.run(_run_export(args.output, args.run_id, args.entity_scope))
    if args.entity_scope == "all":
        print(f"Exported all local research rows (all runs) to: {batch_directory}")
    else:
        print(f"Exported rows for completed threads (all runs) to: {batch_directory}")
    return 0


def _add_run_target_arguments(parser: argparse.ArgumentParser, field_choices: tuple[str, ...], *, audit: bool) -> None:
    parser.add_argument("--run-id", required=True, help="existing crawl run ID")
    parser.add_argument("--target-type", choices=("thread", "post", "comment"), required=True)
    parser.add_argument("--tid", type=_positive_int_arg, help="restrict to a thread; ANDed with other filters")
    parser.add_argument("--pid", type=_positive_int_arg, help="post target PID, or comment parent-post PID")
    parser.add_argument("--cid", type=_positive_int_arg, help="comment target CID")
    parser.add_argument("--fid", type=_positive_int_arg, help="restrict to this forum; ANDed with other filters")
    parser.add_argument(
        "--field",
        choices=field_choices,
        action="append",
        required=True,
        help="field to collect; repeat for multiple fields",
    )


def _positive_int_arg(value: str) -> int:
    try:
        number = int(value, 10)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a positive integer") from None
    if not 1 <= number <= (1 << 63) - 1:
        raise argparse.ArgumentTypeError("must be a positive signed-64 integer")
    return number


def _target_filters(args: argparse.Namespace):
    from .web_fallback.service import normalize_target_filters

    return normalize_target_filters(
        args.target_type,
        tid=args.tid,
        pid=args.pid,
        cid=args.cid,
        fid=args.fid,
    )


async def _create_sessionmaker():
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from src.core.config import Config

    engine = create_async_engine(Config().database_url, echo=False)
    sessions = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    return engine, sessions


async def _run_field_backfill_cli(args: argparse.Namespace) -> int:
    from src.core.config import Config

    from .web_fallback.service import make_request_budget, run_field_backfill

    engine, sessions = await _create_sessionmaker()
    try:
        config = Config().pydantic_config
        summary = await run_field_backfill(
            sessions,
            args.run_id,
            tuple(args.field),
            _target_filters(args),
            limit=args.limit,
            max_seconds=args.max_seconds,
            dry_run=args.dry_run,
            browser_profile=args.browser_profile,
            headed=args.headed,
            wait_for_login=args.wait_for_login,
            request_budget=make_request_budget(float(config.rate_limit.global_rps)),
            browser_config=config.web_fallback,
        )
        print(json.dumps(summary.to_dict(), ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        await engine.dispose()


async def _run_web_audit_cli(args: argparse.Namespace) -> int:
    from src.core.config import Config

    from .web_fallback.service import make_request_budget, run_web_audit

    engine, sessions = await _create_sessionmaker()
    try:
        config = Config().pydantic_config
        report_path = await run_web_audit(
            sessions,
            args.run_id,
            tuple(args.field),
            _target_filters(args),
            output_dir=args.report_dir,
            limit=args.limit,
            max_seconds=args.max_seconds,
            browser_profile=args.browser_profile,
            headed=args.headed,
            wait_for_login=args.wait_for_login,
            request_budget=make_request_budget(float(config.rate_limit.global_rps)),
            browser_config=config.web_fallback,
        )
        print(f"Private read-only audit report written to: {report_path}")
        return 0
    finally:
        await engine.dispose()


async def _run_user_backfill_cli(args: argparse.Namespace) -> int:
    from src.core.config import Config

    from .client import ResearchClient
    from .credentials import load_bduss
    from .current_user_backfill import run_current_user_backfill
    from .web_fallback.service import make_request_budget

    engine, sessions = await _create_sessionmaker()
    client = None
    try:
        config = Config().pydantic_config
        budget = make_request_budget(float(config.rate_limit.global_rps))
        kwargs = {"semaphore": budget, "retry_attempts": 1, "try_ws": False}
        if args.profile or args.forum:
            try:
                kwargs["BDUSS"] = load_bduss(config.research.account_file)
            except (OSError, ValueError):
                raise ValueError("A valid local account file is required for profile/forum snapshots") from None
        client = ResearchClient(**kwargs)
        summary = await run_current_user_backfill(
            sessions,
            client,
            args.run_id,
            tid=args.tid,
            fid=args.fid,
            limit=args.limit,
            max_seconds=args.max_seconds,
            profile=args.profile,
            forum=args.forum,
            bawu=args.bawu,
        )
        print(
            json.dumps(
                {
                    "run_id": summary.run_id,
                    "selected_targets": summary.selected_targets,
                    "unique_users": summary.unique_users,
                    "profile_observations": summary.profile_observations,
                    "forum_snapshots": summary.forum_snapshots,
                    "bawu_snapshots": summary.bawu_snapshots,
                    "blocked": summary.blocked,
                    "stopped_reason": summary.stopped_reason,
                    "statuses": summary.statuses,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    finally:
        if client is not None:
            await client.close()
        await engine.dispose()


async def _login_with_browser(account_file: Path | None) -> Path:
    from .browser_login import browser_login

    return await browser_login(account_file)


async def _run_export(output_dir: Path, run_id: str | None, entity_scope: str = "all") -> Path:
    """Connect to the configured local PostgreSQL database and export it."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from src.core.config import Config

    from .exporter import export_research_csvs
    from .migration import create_research_tables

    engine = create_async_engine(Config().database_url, echo=False)
    sessions = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    try:
        await create_research_tables(engine)
        quality_summaries = _quality_summaries(sessions, run_id)
        paths = await export_research_csvs(
            sessions,
            output_dir,
            quality_summaries,
            source_run_id=run_id,
            entity_scope=entity_scope,
        )
        if not paths:
            raise RuntimeError("CSV exporter returned no batch paths.")
        return next(iter(paths.values())).parent
    finally:
        await engine.dispose()


def _quality_summaries(
    sessions: object,
    run_id: str | None,
) -> AsyncIterator[dict[str, object]]:
    """Load crawl-quality summaries without initializing clients or Redis."""
    from src.core.datastore import iter_crawl_quality_summaries

    return iter_crawl_quality_summaries(sessions, run_id=run_id)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


if __name__ == "__main__":
    raise SystemExit(main())
