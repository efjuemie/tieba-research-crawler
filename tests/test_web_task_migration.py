import os
import shutil
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import delete, inspect, update
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from tiebameow.models.orm import Base

from scripts.migrate_resolved_web_tasks import (
    ROOT,
    RUN_ID,
    SOURCE_ERROR,
    SOURCE_STATUS,
    TARGET_STATUS,
    MigrationRejectedError,
    _apply,
    _evaluate_task,
    _run_dry_run,
    _table_counts,
)
from src.core.models import ResearchCheckpoint, ResearchRun, ResearchTaskScope
from src.research.models import (
    ResearchComment,
    ResearchContentObservation,
    ResearchFieldObservation,
    ResearchPost,
    ResearchThread,
    ResearchWebEnrichmentAttempt,
    ResearchWebEnrichmentTask,
)

BACKUP_ROOT = ROOT / ".local" / "backups"


def _pure_case(*, outcome_status="absent_on_page", reason="ip_location_not_present", checked_at=None, value=None):
    now = datetime(2026, 11, 24, 8, 0, tzinfo=UTC)
    started = now - timedelta(seconds=5)
    finished = now
    task_key = "a" * 64
    task = SimpleNamespace(
        task_key=task_key,
        run_id=RUN_ID,
        target_type="thread",
        target_id=77,
        tid=77,
        pid=None,
        cid=None,
        parent_post_pid=None,
        author_user_id=15,
        author_portrait="portrait",
        fid=9,
        requested_fields_json=["ip_location_text"],
        field_statuses_json={"ip_location_text": {"status": outcome_status, "reason": reason}},
        target_observed_at=now,
        scope_generation=2,
        parent_scope_generation=None,
        status=SOURCE_STATUS,
        attempts=2,
        last_error_type=SOURCE_ERROR,
        last_error=SOURCE_ERROR,
        next_retry_at=now + timedelta(minutes=1),
        lease_token=None,
        lease_owner=None,
        lease_until=None,
        leased_at=None,
        lease_generation=3,
        identity_verified=True,
        identity_reason=None,
        updated_at=now,
        completed_at=None,
    )
    row = SimpleNamespace(
        tid=77,
        pid=None,
        cid=None,
        parent_post_pid=None,
        fid=9,
        author_user_id=15,
        author_portrait="portrait",
        observed_at=now,
        field_sources_json={
            "ip_location_text": {
                "web_check": {
                    "status": outcome_status,
                    "reason": reason,
                    "task_key": task_key,
                    "checked_at": checked_at or finished,
                }
            }
        },
        missing_reasons_json={},
        ip_location_text=value,
    )
    scopes = {
        (RUN_ID, "thread_replies:77"): SimpleNamespace(
            run_id=RUN_ID,
            logical_task_key="thread_replies:77",
            scope_type="thread_replies",
            fid=9,
            tid=77,
            generation=2,
            parent_generation=None,
            parent_pid=None,
            parent_logical_task_key=None,
        )
    }
    attempt = SimpleNamespace(
        task_key=task_key,
        lease_generation=3,
        started_at=started,
        finished_at=finished,
        status=TARGET_STATUS,
    )
    return task, row, scopes, attempt


def test_valid_absence_matches_identity_generation_attempt_and_web_check():
    task, row, scopes, attempt = _pure_case()

    result = _evaluate_task(task, row, scopes, [attempt])

    assert result.payload is not None
    assert result.category == "absent_only"
    assert result.payload["field_evidence"]["ip_location_text"]["checked_at_in_attempt"] is True
    assert result.payload["field_statuses_sha256"]


def test_web_check_must_match_task_and_latest_attempt_window():
    task, row, scopes, attempt = _pure_case(checked_at=datetime(2026, 11, 24, 7, 59, tzinfo=UTC))
    result = _evaluate_task(task, row, scopes, [attempt])
    assert result.reason == "web_check_outside_latest_attempt"

    task, row, scopes, attempt = _pure_case()
    row.field_sources_json["ip_location_text"]["web_check"]["task_key"] = "b" * 64
    result = _evaluate_task(task, row, scopes, [attempt])
    assert result.reason == "web_check_mismatch"


def test_unknown_applied_value_and_ambiguous_growth_evidence_are_rejected():
    task, row, scopes, attempt = _pure_case(outcome_status="confirmed", reason="")
    result = _evaluate_task(task, row, scopes, [attempt])
    assert result.reason == "applied_canonical_value_unknown"

    task, row, scopes, attempt = _pure_case()
    task.requested_fields_json = ["global_growth_icon_url", "global_growth_level"]
    task.field_statuses_json = {
        "global_growth_icon_url": {"status": "absent_on_page", "reason": "global_growth_icon_not_present"},
        "global_growth_level": {"status": "needs_evidence", "reason": "ambiguous_growth_tooltip"},
    }
    row.field_sources_json = {
        name: {
            "web_check": {
                "status": value["status"],
                "reason": value["reason"],
                "task_key": task.task_key,
                "checked_at": attempt.finished_at,
            }
        }
        for name, value in task.field_statuses_json.items()
    }
    result = _evaluate_task(task, row, scopes, [attempt])
    assert result.reason == "field_evidence_not_terminal"


def test_stale_scope_and_nonpositive_identity_are_rejected():
    task, row, scopes, attempt = _pure_case()
    scopes[(RUN_ID, "thread_replies:77")].generation = 3
    assert _evaluate_task(task, row, scopes, [attempt]).group == "identity_scope_snapshot_mismatch"

    task, row, scopes, attempt = _pure_case()
    task.fid = 0
    assert _evaluate_task(task, row, scopes, [attempt]).group == "identity_scope_snapshot_mismatch"


def _migration_test_url():
    raw = os.environ.get("TIEBA_MIGRATION_MARKER_TEST_DATABASE_URL")
    if not raw:
        pytest.skip("Set TIEBA_MIGRATION_MARKER_TEST_DATABASE_URL to the isolated migration marker database.")
    url = make_url(raw)
    if (url.host, url.port, url.database) != ("127.0.0.1", 55432, "tieba_marker_migration_p2"):
        pytest.fail("Migration transaction test requires the isolated local marker database.", pytrace=False)
    return raw


@pytest.mark.asyncio
async def test_marker_postgres_apply_is_status_only_and_rejects_lease_or_stale_digest():
    database_url = _migration_test_url()
    engine = create_async_engine(database_url, echo=False)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    task_key = "a" * 64
    active_task_key = "b" * 64
    tid = 9_000_000_001
    pid = 9_000_000_002
    cid = 9_000_000_003
    now = datetime.now(UTC)
    observed_at = now - timedelta(days=30)
    attempt_started = now - timedelta(minutes=1)
    attempt_finished = now - timedelta(seconds=15)
    tables = [
        ResearchRun.__table__,
        ResearchTaskScope.__table__,
        ResearchCheckpoint.__table__,
        ResearchThread.__table__,
        ResearchPost.__table__,
        ResearchComment.__table__,
        ResearchWebEnrichmentTask.__table__,
        ResearchWebEnrichmentAttempt.__table__,
        ResearchContentObservation.__table__,
        ResearchFieldObservation.__table__,
    ]
    preview_dirs = []
    try:
        async with engine.begin() as connection:
            existing = await connection.run_sync(lambda sync_connection: inspect(sync_connection).get_table_names())
            known_tables = {table.name for table in tables}
            if set(existing) - known_tables:
                pytest.fail("Migration marker database contains unexpected tables.", pytrace=False)
            await connection.run_sync(lambda sync_connection: Base.metadata.create_all(sync_connection, tables=tables))

        async with sessions() as session, session.begin():
            await session.execute(delete(ResearchFieldObservation).where(ResearchFieldObservation.run_id == RUN_ID))
            await session.execute(delete(ResearchContentObservation).where(ResearchContentObservation.owner_id == tid))
            await session.execute(
                delete(ResearchWebEnrichmentAttempt).where(
                    ResearchWebEnrichmentAttempt.task_key.in_([task_key, active_task_key])
                )
            )
            await session.execute(delete(ResearchWebEnrichmentTask).where(ResearchWebEnrichmentTask.run_id == RUN_ID))
            await session.execute(delete(ResearchCheckpoint).where(ResearchCheckpoint.run_id == RUN_ID))
            await session.execute(delete(ResearchTaskScope).where(ResearchTaskScope.run_id == RUN_ID))
            await session.execute(delete(ResearchRun).where(ResearchRun.run_id == RUN_ID))
            await session.execute(delete(ResearchComment).where(ResearchComment.cid == cid))
            await session.execute(delete(ResearchPost).where(ResearchPost.pid == pid))
            await session.execute(delete(ResearchThread).where(ResearchThread.tid == tid))
            session.add(
                ResearchRun(
                    run_id=RUN_ID,
                    mode="backfill",
                    scope_key="migration-marker",
                    forums_json=[],
                    status="incomplete",
                    reason="marker_run_must_not_change",
                    started_at=now,
                    last_checked=now,
                )
            )
            session.add(
                ResearchTaskScope(
                    run_id=RUN_ID,
                    logical_task_key=f"thread_replies:{tid}",
                    generation=0,
                    task_type="full_scan_posts",
                    scope_type="thread_replies",
                    fid=9,
                    tid=tid,
                    status="completed",
                    last_checked=now,
                )
            )
            session.add(
                ResearchThread(
                    tid=tid,
                    fid=9,
                    author_user_id=15,
                    author_portrait="synthetic-portrait",
                    ip_location_text=None,
                    observed_at=observed_at,
                    source_api="test.marker",
                )
            )
            session.add(
                ResearchPost(
                    pid=pid,
                    tid=tid,
                    fid=9,
                    author_user_id=16,
                    observed_at=observed_at,
                    source_api="test.marker",
                )
            )
            session.add(
                ResearchComment(
                    cid=cid,
                    tid=tid,
                    parent_post_pid=pid,
                    fid=9,
                    author_user_id=17,
                    observed_at=observed_at,
                    source_api="test.marker",
                )
            )
            field_statuses = {"ip_location_text": {"status": "absent_on_page", "reason": "ip_location_not_present"}}
            session.add(
                ResearchWebEnrichmentTask(
                    task_key=task_key,
                    run_id=RUN_ID,
                    target_type="thread",
                    target_id=tid,
                    tid=tid,
                    author_user_id=15,
                    author_portrait="synthetic-portrait",
                    fid=9,
                    requested_fields_json=["ip_location_text"],
                    field_statuses_json=field_statuses,
                    target_observed_at=observed_at,
                    scope_generation=0,
                    parent_scope_generation=None,
                    status=SOURCE_STATUS,
                    attempts=2,
                    max_attempts=3,
                    last_error_type=SOURCE_ERROR,
                    last_error=SOURCE_ERROR,
                    next_retry_at=now + timedelta(minutes=5),
                    lease_generation=1,
                    identity_verified=True,
                    identity_reason=None,
                    created_at=now - timedelta(days=1),
                    updated_at=now - timedelta(minutes=1),
                )
            )
            session.add(
                ResearchWebEnrichmentAttempt(
                    task_key=task_key,
                    lease_generation=1,
                    started_at=attempt_started,
                    finished_at=attempt_finished,
                    status=TARGET_STATUS,
                )
            )
            session.add(
                ResearchCheckpoint(
                    run_id=RUN_ID,
                    task_key=f"thread_replies:{tid}",
                    page_number=1,
                    task_type="full_scan_posts",
                    scope_type="thread_replies",
                    fid=9,
                    tid=tid,
                    status="completed",
                    scope_status="completed",
                    has_more=False,
                    last_checked=now,
                )
            )
            session.add(
                ResearchContentObservation(
                    owner_type="thread",
                    owner_id=tid,
                    tid=tid,
                    source_api="test.marker",
                    observed_at=observed_at,
                )
            )
            session.add(
                ResearchFieldObservation(
                    observation_id="00000000-0000-0000-0000-000000000001",
                    observation_key="c" * 64,
                    task_key=task_key,
                    run_id=RUN_ID,
                    target_type="thread",
                    target_id=tid,
                    tid=tid,
                    author_user_id=15,
                    fid=9,
                    field_name="ip_location_text",
                    raw_value_json={"synthetic": True},
                    normalized_value_json=None,
                    source_path="marker.test",
                    source_kind="web_dom",
                    observed_at=attempt_finished,
                    temporal_scope="web_observation_at_observed_time",
                    parser_version="marker",
                    identity_evidence_json={"identity_verified": True},
                    validation_status="verified",
                    applied_status="audit_only",
                    missing_reason="ip_location_not_present",
                    created_at=attempt_finished,
                )
            )
            await session.flush()
            target = await session.get(ResearchThread, tid)
            target.field_sources_json = {
                "ip_location_text": {
                    "web_check": {
                        "status": "absent_on_page",
                        "reason": "ip_location_not_present",
                        "task_key": task_key,
                        "checked_at": attempt_finished.isoformat(),
                    }
                }
            }

        snapshot, report, backup_path, _report_path = await _run_dry_run(sessions)
        preview_dirs.append(backup_path.parent)
        assert report["eligible_count"] == 1
        assert snapshot["active_lease_count"] == 0

        async with sessions() as session, session.begin():
            before_counts = await _table_counts(session)
            active = ResearchWebEnrichmentTask(
                task_key=active_task_key,
                run_id=RUN_ID,
                target_type="thread",
                target_id=tid,
                tid=tid,
                author_user_id=15,
                fid=9,
                requested_fields_json=["ip_location_text"],
                field_statuses_json={},
                target_observed_at=observed_at,
                scope_generation=0,
                status="running",
                attempts=1,
                lease_token="marker-active-lease",
                lease_owner="marker-worker",
                lease_until=now + timedelta(minutes=5),
                leased_at=now,
                lease_generation=1,
                identity_verified=True,
                created_at=now,
                updated_at=now,
            )
            session.add(active)

        with pytest.raises(MigrationRejectedError, match="active web-enrichment lease"):
            await _apply(sessions, expected_digest=snapshot["candidate_digest"], backup_path=backup_path)

        async with sessions() as session, session.begin():
            await session.execute(
                delete(ResearchWebEnrichmentTask).where(ResearchWebEnrichmentTask.task_key == active_task_key)
            )
            await session.execute(
                update(ResearchTaskScope)
                .where(ResearchTaskScope.run_id == RUN_ID)
                .values(generation=1)
            )

        with pytest.raises(MigrationRejectedError, match="audited digest"):
            await _apply(sessions, expected_digest=snapshot["candidate_digest"], backup_path=backup_path)

        async with sessions() as session, session.begin():
            await session.execute(
                update(ResearchTaskScope)
                .where(ResearchTaskScope.run_id == RUN_ID)
                .values(generation=0)
            )

        applied = await _apply(sessions, expected_digest=snapshot["candidate_digest"], backup_path=backup_path)
        assert applied["updated_count"] == 1
        assert applied["post_apply_eligible_count"] == 0
        assert applied["table_counts_unchanged"] is True

        async with sessions() as session, session.begin():
            task = await session.get(ResearchWebEnrichmentTask, task_key)
            attempt = await session.get(ResearchWebEnrichmentAttempt, (task_key, 1))
            run = await session.get(ResearchRun, RUN_ID)
            after_counts = await _table_counts(session)
            assert task.status == TARGET_STATUS
            assert task.last_error == SOURCE_ERROR
            assert task.last_error_type == SOURCE_ERROR
            assert task.field_statuses_json == field_statuses
            assert task.attempts == 2
            assert task.lease_generation == 1
            assert task.next_retry_at is None
            assert task.completed_at is not None
            assert attempt.status == TARGET_STATUS
            assert attempt.finished_at == attempt_finished
            assert run.status == "incomplete"
            assert run.reason == "marker_run_must_not_change"
        assert after_counts == before_counts

        post_snapshot, post_report, post_backup, _ = await _run_dry_run(sessions)
        preview_dirs.append(post_backup.parent)
        assert post_report["eligible_count"] == 0
        assert not post_snapshot["candidates"]
    finally:
        await engine.dispose()
        for preview_dir in preview_dirs:
            preview_dir.relative_to(BACKUP_ROOT)
            if preview_dir.name.startswith("resolved_web_tasks_"):
                shutil.rmtree(preview_dir)
