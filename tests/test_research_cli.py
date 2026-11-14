"""CLI wiring tests for local research export batches."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest
import sqlalchemy.ext.asyncio as async_ext

from src.core import config as core_config
from src.core import datastore as datastore_module
from src.research import __main__ as research_cli
from src.research import exporter as exporter_module
from src.research import migration as migration_module

if TYPE_CHECKING:
    from pathlib import Path


def test_export_cli_reports_the_created_batch_directory(tmp_path: Path, monkeypatch, capsys):
    batch_directory = tmp_path / "exports" / "all-runs_source-synthetic_20261114_120000_abc"
    calls = []

    async def fake_export(output_dir, run_id, entity_scope):
        calls.append((output_dir, run_id, entity_scope))
        return batch_directory

    monkeypatch.setattr(research_cli, "_run_export", fake_export)
    output_root = tmp_path / "exports"

    assert research_cli.main(["export", "--output", str(output_root), "--run-id", "synthetic-run"]) == 0
    captured = capsys.readouterr()

    assert calls == [(output_root, "synthetic-run", "all")]
    assert str(batch_directory) in captured.out
    assert "all runs" in captured.out


def test_export_cli_help_states_run_id_is_context_not_entity_filter(capsys):
    with pytest.raises(SystemExit):
        research_cli._build_parser().parse_args(["export", "--help"])
    help_text = capsys.readouterr().out

    assert "entity CSVs always include all local runs" in help_text
    assert "completed-threads" in help_text


def test_export_cli_defaults_to_all_entity_scope():
    args = research_cli._build_parser().parse_args(["export"])
    assert args.entity_scope == "all"


@pytest.mark.asyncio
async def test_run_export_passes_run_context_and_returns_real_batch_path(tmp_path: Path, monkeypatch):
    output_root = tmp_path / "exports"
    batch_directory = output_root / "all-runs_source-synthetic-run_20261114_120000_abc"
    engine = SimpleNamespace(dispose=AsyncMock())
    sessions = object()
    calls = []

    class FakeConfig:
        database_url = "postgresql+asyncpg://synthetic"

    async def fake_create_tables(received_engine):
        calls.append(("migration", received_engine))

    def fake_quality(received_sessions, *, run_id=None):
        calls.append(("quality", received_sessions, run_id))
        return ()

    async def fake_export(
        received_sessions,
        received_output,
        quality_rows,
        *,
        source_run_id=None,
        entity_scope="all",
    ):
        calls.append(("export", received_sessions, received_output, quality_rows, source_run_id, entity_scope))
        return {"threads": batch_directory / "threads.csv", "manifest": batch_directory / "manifest.json"}

    monkeypatch.setattr(core_config, "Config", FakeConfig)
    monkeypatch.setattr(async_ext, "create_async_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(async_ext, "async_sessionmaker", lambda **_kwargs: sessions)
    monkeypatch.setattr(migration_module, "create_research_tables", fake_create_tables)
    monkeypatch.setattr(datastore_module, "iter_crawl_quality_summaries", fake_quality)
    monkeypatch.setattr(exporter_module, "export_research_csvs", fake_export)

    result = await research_cli._run_export(output_root, "synthetic-run", "completed-threads")

    assert result == batch_directory
    assert calls[0] == ("migration", engine)
    assert calls[1] == ("quality", sessions, "synthetic-run")
    assert calls[2] == ("export", sessions, output_root, (), "synthetic-run", "completed-threads")
    assert engine.dispose.await_count == 1
