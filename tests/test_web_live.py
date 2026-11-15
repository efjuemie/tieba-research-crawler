from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.core.models import ResearchRun
from src.research.models import ResearchWebEnrichmentTask
from src.research.web_fallback import live


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _Session:
    def __init__(self, task_run_ids, runs):
        self._results = [_Rows(task_run_ids), _Rows(runs)]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def scalars(self, _statement):
        return self._results.pop(0)


class _SessionMaker:
    def __init__(self, session):
        self._session = session

    def __call__(self):
        return self._session


@pytest.mark.asyncio
async def test_pending_web_runs_include_compatible_unfinished_run_without_task_row():
    expected_forums = ((7, "synthetic"),)
    runs = [
        SimpleNamespace(run_id="current-run", status="completed", forums_json=[{"fid": 7, "fname": "synthetic"}]),
        SimpleNamespace(run_id="prior-run", status="interrupted", forums_json=[{"fid": 7, "fname": "synthetic"}]),
        SimpleNamespace(run_id="durable-web-run", status="completed", forums_json=[{"fid": 7, "fname": "synthetic"}]),
        SimpleNamespace(run_id="cancelled-run", status="cancelled", forums_json=[{"fid": 7, "fname": "synthetic"}]),
        SimpleNamespace(run_id="unrelated-run", status="incomplete", forums_json=[{"fid": 8, "fname": "other"}]),
        SimpleNamespace(run_id="completed-run", status="completed", forums_json=[{"fid": 7, "fname": "synthetic"}]),
    ]
    # prior-run has no WEB task row: startup must rescan it to recover a commit→enqueue crash.
    sessions = _SessionMaker(_Session(["durable-web-run", "cancelled-run", "unrelated-run"], runs))

    run_ids = await live.pending_web_run_ids(
        sessions,
        "current-run",
        responsible_forums=expected_forums,
    )

    assert run_ids == ("current-run", "durable-web-run", "prior-run")


@pytest.mark.asyncio
async def test_pending_web_run_sql_recovers_all_compatible_unfinished_runs():
    engine = create_engine("sqlite:///:memory:")
    ResearchRun.__table__.create(engine)
    ResearchWebEnrichmentTask.__table__.create(engine)
    now = datetime.now(UTC)
    expected_forums = [{"fid": 7, "fname": "synthetic"}]
    other_forums = [{"fid": 8, "fname": "other"}]
    with Session(engine) as session:
        session.add_all(
            [
                ResearchRun(
                    run_id="current-run",
                    mode="periodic",
                    scope_key="current",
                    forums_json=expected_forums,
                    status="running",
                    started_at=now,
                    last_checked=now,
                ),
                ResearchRun(
                    run_id="prior-run-without-web-task",
                    mode="backfill",
                    scope_key="prior",
                    forums_json=expected_forums,
                    status="interrupted",
                    started_at=now,
                    last_checked=now,
                ),
                ResearchRun(
                    run_id="durable-web-run",
                    mode="backfill",
                    scope_key="durable",
                    forums_json=expected_forums,
                    status="completed",
                    started_at=now,
                    last_checked=now,
                ),
                ResearchRun(
                    run_id="cancelled-run",
                    mode="backfill",
                    scope_key="cancelled",
                    forums_json=expected_forums,
                    status="cancelled",
                    started_at=now,
                    last_checked=now,
                ),
                ResearchRun(
                    run_id="unrelated-run",
                    mode="backfill",
                    scope_key="unrelated",
                    forums_json=other_forums,
                    status="incomplete",
                    started_at=now,
                    last_checked=now,
                ),
            ]
        )
        session.add(
            ResearchWebEnrichmentTask(
                task_key="durable-web-task",
                run_id="durable-web-run",
                target_type="thread",
                target_id=1,
                tid=1,
                requested_fields_json=["ip_location_text"],
                field_statuses_json={},
                target_observed_at=now,
                scope_generation=0,
                priority=0,
                status="pending_web",
                attempts=0,
                max_attempts=3,
                lease_generation=0,
                created_at=now,
                updated_at=now,
            )
        )
        session.commit()

    class SQLiteAsyncSession:
        async def __aenter__(self):
            self.session = Session(engine)
            return self

        async def __aexit__(self, *_args):
            self.session.close()
            return False

        async def scalars(self, statement):
            return self.session.scalars(statement)

    run_ids = await live.pending_web_run_ids(
        SQLiteAsyncSession,
        "current-run",
        responsible_forums=((7, "synthetic"),),
    )

    assert run_ids == ("current-run", "durable-web-run", "prior-run-without-web-task")
    engine.dispose()


@pytest.mark.asyncio
async def test_discovery_rescans_only_selected_runs_and_paired_missing_fields(monkeypatch):
    calls = []

    async def enqueue(_sessions, run_id, fields, **kwargs):
        calls.append((run_id, fields, kwargs))
        return SimpleNamespace(created_count=1)

    monkeypatch.setattr(live, "enqueue_web_enrichment_tasks", enqueue)
    created = await live.discover_missing_web_targets(object(), ("run-a", "run-b", "run-a"))

    assert created == 6
    assert [(run_id, kwargs["target_type"]) for run_id, _fields, kwargs in calls] == [
        (run_id, target_type)
        for run_id in ("run-a", "run-b")
        for target_type in ("thread", "post", "comment")
    ]
    assert all(fields == live.MISSING_CONTENT_FIELDS for _run_id, fields, _kwargs in calls)
    assert all(kwargs["only_missing_fields"] and kwargs["pair_growth_fields"] for _run_id, _fields, kwargs in calls)
