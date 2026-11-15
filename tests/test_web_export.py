from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from stat import S_IMODE
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from tiebameow.models.orm import Base
from tiebameow.models.orm import Comment as BaseComment
from tiebameow.models.orm import Post as BasePost
from tiebameow.models.orm import Thread as BaseThread
from tiebameow.models.orm import User as BaseUser

from src.core.models import ResearchTaskScope
from src.research.exporter import (
    FIELD_QUALITY_FIELDS,
    export_research_csvs,
)
from src.research.models import (
    RESEARCH_TABLES,
    ResearchFieldObservation,
    ResearchThread,
    ResearchWebEnrichmentAttempt,
    ResearchWebEnrichmentFailure,
    ResearchWebEnrichmentTask,
)

if TYPE_CHECKING:
    from pathlib import Path


class _AsyncResult:
    def __init__(self, result):
        self.result = result
        self.rows = iter(result.yield_per(1000))

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.rows)
        except StopIteration:
            raise StopAsyncIteration from None

    async def close(self):
        self.result.close()


class _AsyncSession:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None

    def get_bind(self):
        return SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))

    async def scalar(self, statement):
        return self.session.scalar(statement)

    async def execute(self, statement):
        return self.session.execute(statement)

    async def stream(self, statement):
        return _AsyncResult(self.session.execute(statement))


class _AsyncSessionMaker:
    def __init__(self, engine):
        self.engine = engine

    def __call__(self):
        return _AsyncSession(Session(self.engine))


def _read_csv(path: Path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


@pytest.mark.asyncio
async def test_export_includes_web_audit_csvs_and_distinguishes_zero_and_unknown(tmp_path):
    engine = create_engine("sqlite:///:memory:")
    base_tables = [BaseUser.__table__, BaseThread.__table__, BasePost.__table__, BaseComment.__table__]
    Base.metadata.create_all(engine, tables=base_tables + list(RESEARCH_TABLES) + [ResearchTaskScope.__table__])
    now = datetime(2026, 1, 1, tzinfo=UTC)
    task_key = "a" * 64
    tid = 9007199254740993
    with Session(engine) as session, session.begin():
        session.add(
            BaseThread(
                tid=tid - 1,
                create_time=now,
                title="synthetic legacy thread",
                text="",
                contents=None,
                last_time=now,
                reply_num=0,
                author_level=0,
                fid=7,
                author_id=8,
            )
        )
        session.add(
            ResearchThread(
                tid=tid,
                fid=7,
                author_user_id=8,
                author_nickname="synthetic current nickname",
                forum_level=5,
                global_growth_level=0,
                is_bawu=False,
                agree_count=0,
                source_api="aiotieba.get_posts",
                observed_at=now,
                source_payload_json={},
                missing_reasons_json={"agree_count": "zero_presence_unverified_in_parsed_object"},
                field_sources_json={
                    "author_nickname": {
                        "verified_web": {
                            "observation_time": now.isoformat(),
                            "source_kind": "web_dom",
                            "parser_version": "synthetic-v1",
                            "value": "synthetic current nickname",
                        }
                    },
                    "forum_level": {
                        "verified_content_api": {
                            "source_api": "aiotieba.get_posts",
                            "observed_at": now.isoformat(),
                            "value": 5,
                        }
                    },
                    "global_growth_level": {
                        "verified_content_api": {
                            "source_api": "aiotieba.get_user_info",
                            "observed_at": now.isoformat(),
                            "value": 0,
                        }
                    },
                },
            )
        )
        session.add(
            ResearchWebEnrichmentTask(
                task_key=task_key,
                run_id="synthetic-run",
                target_type="thread",
                target_id=tid,
                tid=tid,
                author_user_id=8,
                fid=7,
                requested_fields_json=["author_nickname"],
                field_statuses_json={"author_nickname": {"status": "applied", "reason": ""}},
                target_observed_at=now,
                scope_generation=1,
                priority=0,
                status="completed",
                attempts=1,
                max_attempts=3,
                identity_verified=True,
                created_at=now,
                updated_at=now,
                completed_at=now,
            )
        )
        session.add(
            ResearchWebEnrichmentAttempt(
                task_key=task_key,
                lease_generation=1,
                started_at=now,
                finished_at=now,
                status="completed",
                diagnostics_json={"requests": 4, "budgeted_actions": 2, "observed_subresource_requests": 2},
            )
        )
        session.add(
            ResearchFieldObservation(
                observation_id="synthetic-observation",
                observation_key="b" * 64,
                task_key=task_key,
                run_id="synthetic-run",
                target_type="thread",
                target_id=tid,
                tid=tid,
                author_user_id=8,
                fid=7,
                field_name="author_nickname",
                raw_value_json="synthetic current nickname",
                normalized_value_json="synthetic current nickname",
                source_url=f"https://tieba.baidu.com/p/{tid}",
                source_path="thread.header.nickname",
                source_kind="web_dom",
                observed_at=now,
                temporal_scope="page_observation",
                parser_version="synthetic-v1",
                identity_evidence_json={"identity_verified": True},
                validation_status="verified",
                applied_status="applied",
                created_at=now,
                applied_at=now,
            )
        )
        session.add(
            ResearchFieldObservation(
                observation_id="synthetic-api-observation",
                observation_key="c" * 64,
                task_key=None,
                run_id=None,
                target_type="thread",
                target_id=tid,
                tid=tid,
                author_user_id=8,
                fid=7,
                field_name="forum_level",
                raw_value_json=5,
                normalized_value_json=5,
                source_url=None,
                source_path="api.comments_preview.author.forum_level",
                source_kind="api_raw",
                observed_at=now,
                temporal_scope="content_observation",
                parser_version="synthetic-v1",
                identity_evidence_json={"identity_verified": True},
                validation_status="verified",
                applied_status="observed_only",
                created_at=now,
            )
        )
        session.add(
            ResearchWebEnrichmentFailure(
                failure_id="synthetic-failure",
                task_key=task_key,
                lease_generation=1,
                status="blocked",
                reason_code="synthetic_blocked",
                occurred_at=now,
            )
        )

    paths = await export_research_csvs(
        _AsyncSessionMaker(engine), tmp_path / "exports", (), source_run_id="synthetic-run"
    )
    expected = {
        "web_enrichment_tasks",
        "web_enrichment_attempts",
        "field_observations",
        "web_enrichment_failures",
        "field_quality_report",
    }
    assert expected.issubset(paths)
    for name in expected:
        assert S_IMODE(paths[name].stat().st_mode) == 0o600

    task_rows = _read_csv(paths["web_enrichment_tasks"])
    attempt_rows = _read_csv(paths["web_enrichment_attempts"])
    observation_rows = _read_csv(paths["field_observations"])
    failure_rows = _read_csv(paths["web_enrichment_failures"])
    assert len(task_rows) == len(attempt_rows) == len(observation_rows) == len(failure_rows) == 1
    assert task_rows[0]["target_id"] == str(tid)
    assert (
        attempt_rows[0]["diagnostics_json"] == '{"requests":4,"budgeted_actions":2,"observed_subresource_requests":2}'
    )
    assert failure_rows[0]["run_id"] == "synthetic-run"

    thread_rows = _read_csv(paths["threads"])
    assert any(row["field_sources_json"] for row in thread_rows)
    quality = _read_csv(paths["field_quality_report"])
    assert list(quality[0]) == list(FIELD_QUALITY_FIELDS)
    by_field = {row["field_name"]: row for row in quality if row["entity_type"] == "thread"}
    assert by_field["author_nickname"]["web_applied_entity_count"] == "1"
    assert by_field["author_nickname"]["web_observation_count"] == "1"
    assert by_field["agree_count"]["unknown_zero_count"] == "1"
    assert by_field["agree_count"]["known_zero_count"] == "0"
    assert by_field["is_bawu"]["unknown_zero_count"] == "1"
    assert by_field["forum_level"]["unknown_zero_count"] == "1"
    assert by_field["forum_level"]["known_zero_count"] == "0"
    assert by_field["reply_count"]["unknown_zero_count"] == "1"
    assert by_field["reply_count"]["known_zero_count"] == "0"
    assert by_field["forum_level"]["api_nonempty_count"] == "1"
    assert by_field["forum_level"]["verified_content_api_entity_count"] == "1"
    assert by_field["forum_level"]["api_observation_count"] == "0"
    assert by_field["forum_level"]["web_applied_entity_count"] == "0"
    assert by_field["global_growth_level"]["known_zero_count"] == "1"
    assert by_field["global_growth_level"]["unknown_zero_count"] == "0"
    assert by_field["global_growth_level"]["verified_content_api_entity_count"] == "1"
    assert json.loads(by_field["agree_count"]["missing_reason_counts_json"]) == {
        "zero_presence_unverified_in_parsed_object": 1
    }

    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    assert manifest["entity_scope_table_semantics"]["threads"] == "all"
    assert (
        manifest["entity_scope_table_semantics"]["field_observations"]
        == "selected_run_id_excludes_unscoped_null_run_id"
    )
    assert "not an analysis sample" in manifest["audit_csv_semantics"]["field_observations"]
    assert manifest["audit_csv_semantics"]["field_quality_report"]["source_run_id"].startswith("filters task")

    all_paths = await export_research_csvs(_AsyncSessionMaker(engine), tmp_path / "all-runs", ())
    all_observations = _read_csv(all_paths["field_observations"])
    assert len(all_observations) == 2
    api_observation = next(row for row in all_observations if row["source_kind"] == "api_raw")
    assert api_observation["task_key"] == api_observation["run_id"] == api_observation["source_url"] == ""
    all_quality = _read_csv(all_paths["field_quality_report"])
    all_by_field = {row["field_name"]: row for row in all_quality if row["entity_type"] == "thread"}
    assert all_by_field["forum_level"]["api_observation_count"] == "1"
    assert all_by_field["forum_level"]["web_observation_count"] == "0"
    assert all_by_field["forum_level"]["unscoped_observation_count"] == "1"
    all_manifest = json.loads(all_paths["manifest"].read_text(encoding="utf-8"))
    assert (
        all_manifest["entity_scope_table_semantics"]["field_observations"] == "all_runs_including_unscoped_null_run_id"
    )
    engine.dispose()
