"""Command-line entry point for local research account setup and export."""

from __future__ import annotations

import argparse
import asyncio
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
        help=(
            "filter the crawl-quality report and label this export batch; entity CSVs always include all local runs"
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "login":
        if args.browser:
            path = asyncio.run(_login_with_browser(args.account_file))
        else:
            path = save_bduss_interactively(args.account_file)
        print(f"Saved local Baidu account file: {path}")
        return 0

    batch_directory = asyncio.run(_run_export(args.output, args.run_id))
    print(f"Exported all local research rows (all runs) to: {batch_directory}")
    return 0


async def _login_with_browser(account_file: Path | None) -> Path:
    from .browser_login import browser_login

    return await browser_login(account_file)


async def _run_export(output_dir: Path, run_id: str | None) -> Path:
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
