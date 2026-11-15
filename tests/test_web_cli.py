from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from stat import S_IMODE
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from tiebameow.models.orm import Base

from src.research import __main__ as cli
from src.research.models import ResearchComment
from src.research.web_fallback import service
from src.research.web_fallback.tasks import EnqueueSummary, WebEnrichmentTargetPlan


def _plan(task_key: str = "task-a", *, status: str = "pending_web") -> WebEnrichmentTargetPlan:
    return WebEnrichmentTargetPlan(
        task_key=task_key,
        target_type="comment",
        target_id=9007199254740993,
        tid=123,
        pid=None,
        cid=9007199254740993,
        parent_post_pid=456,
        author_user_id=789,
        author_portrait="synthetic-portrait",
        fid=321,
        requested_fields=("ip_location_text",),
        target_observed_at=datetime(2026, 1, 1, tzinfo=UTC),
        scope_generation=1,
        parent_scope_generation=1,
        eligibility_status=status,
        field_statuses={"ip_location_text": {"status": "pending"}},
    )


def _summary(*plans: WebEnrichmentTargetPlan, created: int = 1) -> EnqueueSummary:
    return EnqueueSummary(
        run_id="synthetic-run",
        planned_count=len(plans),
        created_count=created,
        deduplicated_count=0,
        skipped={},
        task_keys=tuple(plan.task_key for plan in plans),
        planned_targets=tuple(plans),
    )


class _SQLiteSessionAdapter:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.session.close()

    async def scalar(self, statement):
        return self.session.scalar(statement)


class _SQLiteSessionMaker:
    def __init__(self, engine):
        self.engine = engine

    def __call__(self):
        return _SQLiteSessionAdapter(Session(self.engine))


def test_cli_parses_bounded_web_and_user_commands_without_browser_runtime(capsys):
    args = cli._build_parser().parse_args([
        "field-backfill",
        "--run-id",
        "synthetic-run",
        "--target-type",
        "comment",
        "--tid",
        "123",
        "--pid",
        "456",
        "--cid",
        "9007199254740993",
        "--fid",
        "321",
        "--field",
        "ip_location_text",
        "--dry-run",
    ])
    assert args.limit == 5
    assert args.max_seconds == 120
    assert args.dry_run is True
    filters = cli._target_filters(args)
    assert filters.target_ids == (9007199254740993,)
    assert filters.parent_pid == 456
    assert filters.tid == 123
    assert filters.fid == 321

    profile = cli._build_parser().parse_args([
        "user-backfill",
        "--run-id",
        "synthetic-run",
        "--profile",
        "--tid",
        "123",
    ])
    assert profile.profile is True
    assert profile.limit == 5

    with pytest.raises(SystemExit):
        cli._build_parser().parse_args(["field-backfill", "--help"])
    help_text = capsys.readouterr().out
    assert "pending" in help_text.lower()
    assert "--wait-for-login" in help_text
    assert "counts toward --max-seconds" in help_text


def test_parser_help_does_not_import_playwright():
    code = (
        "from src.research.__main__ import _build_parser; "
        "_build_parser(); "
        "import sys; "
        "assert not any(name.startswith('playwright') for name in sys.modules)"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_audit_login_wait_requires_headed_mode():
    with pytest.raises(SystemExit, match="--wait-for-login requires --headed"):
        cli.main([
            "web-audit",
            "--run-id",
            "synthetic-run",
            "--target-type",
            "comment",
            "--field",
            "ip_location_text",
            "--browser-profile",
            "/private/synthetic-profile",
            "--wait-for-login",
        ])


@pytest.mark.asyncio
async def test_read_canonical_snapshot_uses_real_sqlite_session():
    plan = _plan()
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[ResearchComment.__table__])
    with Session(engine) as session, session.begin():
        session.add(
            ResearchComment(
                cid=plan.target_id,
                tid=plan.tid,
                parent_post_pid=plan.parent_post_pid,
                fid=plan.fid,
                author_user_id=plan.author_user_id,
                author_portrait=plan.author_portrait,
                source_api="synthetic.api",
                observed_at=plan.target_observed_at,
                source_payload_json={},
                missing_reasons_json={"ip_location_text": "not_returned_by_source"},
                field_sources_json={
                    "ip_location_text": {
                        "verified_web": {"source_kind": "web_network", "value": "合成地区"}
                    }
                },
            )
        )

    try:
        snapshot = await service._read_canonical_snapshot(_SQLiteSessionMaker(engine), plan)
    finally:
        engine.dispose()

    assert snapshot["status"] == "found"
    assert snapshot["source_api"] == "synthetic.api"
    assert not {"post_url", "parent_post_url", "comment_url"}.intersection(snapshot["urls"])
    assert snapshot["fields"]["ip_location_text"] == {
        "canonical_value": None,
        "csv_value": "",
        "api_missing_reason": "not_returned_by_source",
        "field_source_metadata": {
            "verified_web": {"source_kind": "web_network", "value": "合成地区"}
        },
    }


def test_cli_field_choices_track_web_field_types_and_new_identity_fields():
    args = cli._build_parser().parse_args([
        "field-backfill",
        "--run-id",
        "synthetic-run",
        "--target-type",
        "comment",
        "--field",
        "global_growth_icon_url",
        "--field",
        "reply_to_display_name",
        "--dry-run",
    ])
    assert args.field == ["global_growth_icon_url", "reply_to_display_name"]

    with pytest.raises(SystemExit):
        cli._build_parser().parse_args([
            "field-backfill",
            "--run-id",
            "synthetic-run",
            "--target-type",
            "thread",
            "--field",
            "role_name",
        ])


def test_cli_target_filters_keep_post_and_comment_ids_distinct():
    post = service.normalize_target_filters("post", tid=100, pid=200, fid=7)
    assert post.target_ids == (200,)
    assert post.parent_pid is None

    comment = service.normalize_target_filters("comment", tid=100, pid=200, cid=300, fid=7)
    assert comment.target_ids == (300,)
    assert comment.parent_pid == 200
    assert comment.tid == 100
    assert comment.fid == 7

    parent_scope = service.normalize_target_filters("comment", pid=200)
    assert parent_scope.target_ids is None
    assert parent_scope.parent_pid == 200

    with pytest.raises(ValueError, match="thread targets accept"):
        service.normalize_target_filters("thread", pid=200)
    with pytest.raises(ValueError, match="positive integer"):
        service.normalize_target_filters("comment", cid=(1 << 63))


class _FakeBudget:
    pass


class _FakeProvider:
    def __init__(self, results, *, has_manual_login_page=False):
        self.results = list(results)
        self.targets = []
        self.has_manual_login_page = has_manual_login_page
        self.closed = 0
        self.login_page_closed = 0

    async def collect(self, target):
        self.targets.append(target)
        return self.results.pop(0)

    async def close_manual_login_page(self):
        self.login_page_closed += 1

    async def close(self):
        self.closed += 1


def _browser_result(
    status="ok",
    reason=None,
    *,
    candidates=(),
    field_outcomes=None,
    diagnostics=None,
    identity_verified=None,
):
    return SimpleNamespace(
        status=status,
        reason=reason,
        candidates=tuple(candidates),
        field_outcomes=field_outcomes or {},
        diagnostics=diagnostics or {"requests": 1, "budgeted_actions": 1, "observed_subresource_requests": 3},
        identity_verified=identity_verified,
    )


@pytest.mark.asyncio
async def test_field_backfill_dry_run_has_no_provider_or_task_writes(monkeypatch, tmp_path):
    plan = _plan()
    calls = []

    async def enqueue(*_args, **kwargs):
        calls.append(kwargs)
        return _summary(plan, created=0)

    def fail_provider(**_kwargs):
        raise AssertionError("dry-run must not construct provider")

    monkeypatch.setattr(service, "enqueue_web_enrichment_tasks", enqueue)
    result = await service.run_field_backfill(
        object(),
        "synthetic-run",
        ("ip_location_text",),
        service.TargetFilters(
            "comment", (plan.target_id,), tid=plan.tid, fid=plan.fid, parent_pid=plan.parent_post_pid
        ),
        dry_run=True,
        provider_factory=fail_provider,
    )

    assert result.planned_count == 1
    assert result.eligible_count == 1
    assert result.created_count == 0
    assert result.collected_count == 0
    assert calls == [
        {
            "target_type": "comment",
            "target_ids": (plan.target_id,),
            "tid": plan.tid,
            "fid": plan.fid,
            "parent_pid": plan.parent_post_pid,
            "limit": 5,
            "dry_run": True,
            "max_attempts": 3,
                "only_missing_fields": True,
                "pair_growth_fields": True,
        }
    ]


@pytest.mark.asyncio
async def test_pending_api_detail_never_constructs_browser_provider(monkeypatch):
    plan = _plan(status="pending_api_detail")

    async def enqueue(*_args, **_kwargs):
        return _summary(plan)

    def fail_provider(**_kwargs):
        raise AssertionError("API-incomplete scope must not reach provider")

    monkeypatch.setattr(service, "enqueue_web_enrichment_tasks", enqueue)
    monkeypatch.setattr(
        service,
        "lease_web_enrichment_tasks",
        lambda *_args, **_kwargs: pytest.fail("API-incomplete scope must not be leased"),
    )
    result = await service.run_field_backfill(
        object(),
        "synthetic-run",
        ("ip_location_text",),
        service.TargetFilters(
            "comment", (plan.target_id,), tid=plan.tid, fid=plan.fid, parent_pid=plan.parent_post_pid
        ),
        browser_profile="/private/synthetic-profile",
        request_budget=_FakeBudget(),
        browser_config=SimpleNamespace(max_pages=1, max_expansions=1, timeout_seconds=30),
        provider_factory=fail_provider,
    )
    assert result.pending_api_detail_count == 1
    assert result.collected_count == 0


@pytest.mark.asyncio
async def test_field_backfill_leases_only_planned_keys_and_preserves_comment_scope(monkeypatch):
    plan = _plan()
    calls = []
    provider = _FakeProvider([_browser_result()])
    completed = []

    async def enqueue(_sessions, _run_id, _fields, **kwargs):
        calls.append(("enqueue", kwargs))
        return _summary(plan)

    async def lease(_sessions, _run_id, _worker_id, **kwargs):
        calls.append(("lease", kwargs))
        if kwargs["task_keys"] == (plan.task_key,):
            return [SimpleNamespace(task_key=plan.task_key, lease_token="synthetic-token")]
        return []

    async def complete(_sessions, task_key, lease_token, candidates, **kwargs):
        completed.append((task_key, lease_token, candidates, kwargs))
        return SimpleNamespace(status="completed")

    monkeypatch.setattr(service, "enqueue_web_enrichment_tasks", enqueue)
    monkeypatch.setattr(service, "lease_web_enrichment_tasks", lease)
    monkeypatch.setattr(service, "_complete_task", complete)
    filters = service.TargetFilters(
        "comment", (plan.target_id,), tid=plan.tid, fid=plan.fid, parent_pid=plan.parent_post_pid
    )
    result = await service.run_field_backfill(
        object(),
        "synthetic-run",
        ("ip_location_text",),
        filters,
        browser_profile="/private/synthetic-profile",
        request_budget=_FakeBudget(),
        browser_config=SimpleNamespace(max_pages=1, max_expansions=1, timeout_seconds=30),
        provider_factory=lambda **_kwargs: provider,
    )

    assert calls[0][1]["target_ids"] == (plan.target_id,)
    assert calls[0][1]["parent_pid"] == plan.parent_post_pid
    assert calls[1][1]["task_keys"] == (plan.task_key,)
    assert provider.targets[0].target_id == str(plan.target_id)
    assert provider.targets[0].cid == str(plan.target_id)
    assert provider.targets[0].parent_post_pid == str(plan.parent_post_pid)
    assert completed[0][3]["field_outcomes"]["ip_location_text"] == {
        "status": "not_tested",
        "reason": "not_observed_in_bounded_page",
    }
    assert result.completed_count == 1
    assert provider.closed == 1


@pytest.mark.asyncio
async def test_blocked_429_stops_without_retrying_remaining_targets(monkeypatch):
    first = _plan("task-a")
    second = _plan("task-b")
    lease_calls = []
    failures = []
    provider = _FakeProvider([_browser_result("blocked", "http_429")])

    async def enqueue(*_args, **_kwargs):
        return _summary(first, second)

    async def lease(*_args, **kwargs):
        lease_calls.append(kwargs)
        return [SimpleNamespace(task_key=first.task_key, lease_token="synthetic-token")]

    async def fail(*_args, **kwargs):
        failures.append(kwargs)

    monkeypatch.setattr(service, "enqueue_web_enrichment_tasks", enqueue)
    monkeypatch.setattr(service, "lease_web_enrichment_tasks", lease)
    monkeypatch.setattr(service, "_fail_task", fail)
    result = await service.run_field_backfill(
        object(),
        "synthetic-run",
        ("ip_location_text",),
        service.TargetFilters("comment"),
        browser_profile="/private/synthetic-profile",
        request_budget=_FakeBudget(),
        browser_config=SimpleNamespace(max_pages=1, max_expansions=1, timeout_seconds=30),
        provider_factory=lambda **_kwargs: provider,
    )

    assert len(lease_calls) == 1
    assert provider.targets
    assert failures[0]["status"] == "blocked"
    assert result.stopped_reason == "http_429"
    assert result.collected_count == 1


@pytest.mark.asyncio
async def test_manual_login_releases_lease_before_confirmation_and_resumes_exact_task(monkeypatch):
    plan = _plan()
    lease_count = 0
    lease_live = False
    calls = []
    provider = _FakeProvider(
        [_browser_result("blocked", "http_auth_or_verification"), _browser_result()],
        has_manual_login_page=True,
    )

    async def enqueue(*_args, **_kwargs):
        return _summary(plan)

    async def lease(_sessions, _run, _worker, **kwargs):
        nonlocal lease_count, lease_live
        calls.append(("lease", kwargs["task_keys"]))
        lease_count += 1
        if lease_count <= 2:
            lease_live = True
            return [SimpleNamespace(task_key=plan.task_key, lease_token=f"token-{lease_count}")]
        return []

    async def fail(*_args, **_kwargs):
        nonlocal lease_live
        lease_live = False
        calls.append(("fail",))

    async def confirm(_provider, _timeout):
        assert lease_live is False
        calls.append(("confirm",))
        return True

    async def resume(_sessions, _run, **kwargs):
        assert kwargs["task_keys"] == (plan.task_key,)
        calls.append(("resume",))
        return 1

    async def complete(*_args, **_kwargs):
        nonlocal lease_live
        lease_live = False
        return SimpleNamespace(status="completed")

    monkeypatch.setattr(service, "enqueue_web_enrichment_tasks", enqueue)
    monkeypatch.setattr(service, "lease_web_enrichment_tasks", lease)
    monkeypatch.setattr(service, "_fail_task", fail)
    monkeypatch.setattr(service, "resume_web_enrichment_tasks", resume)
    monkeypatch.setattr(service, "_complete_task", complete)
    result = await service.run_field_backfill(
        object(),
        "synthetic-run",
        ("ip_location_text",),
        service.TargetFilters("comment"),
        browser_profile="/private/synthetic-profile",
        headed=True,
        wait_for_login=True,
        request_budget=_FakeBudget(),
        browser_config=SimpleNamespace(max_pages=1, max_expansions=1, timeout_seconds=30),
        provider_factory=lambda **kwargs: calls.append(("provider", kwargs["keep_login_page"])) or provider,
        login_confirmation=confirm,
    )

    assert ("provider", True) in calls
    assert calls.index(("fail",)) < calls.index(("confirm",)) < calls.index(("resume",))
    assert provider.login_page_closed == 1
    assert provider.closed == 1
    assert result.completed_count == 1
    assert result.collected_count == 2
    assert result.stopped_reason is None


@pytest.mark.asyncio
async def test_live_worker_reuses_one_headed_provider_to_drain_multiple_runs(monkeypatch):
    stop_event = asyncio.Event()
    provider = _FakeProvider([])
    provider_args = []
    lease_calls = []
    process_calls = []
    available = [
        SimpleNamespace(run_id="current-run", task_key="current-task"),
        SimpleNamespace(run_id="old-run", task_key="old-task"),
    ]

    def make_provider(**kwargs):
        provider_args.append(kwargs)
        return provider

    async def lease(_sessions, run_ids, _worker_id):
        lease_calls.append(tuple(run_ids))
        for index, lease in enumerate(available):
            if lease.run_id in run_ids:
                return available.pop(index)
        return None

    async def process(_sessions, current_provider, lease):
        process_calls.append((current_provider, lease.run_id, lease.task_key))
        if lease.run_id == "current-run":
            stop_event.set()
        return "completed", SimpleNamespace(status="ok", reason=None, diagnostics={})

    async def no_unresolved(_sessions, _run_ids):
        return 0

    async def no_retry_delay(_sessions, _run_ids):
        return None

    monkeypatch.setattr(service, "_lease_one_web_task", lease)
    monkeypatch.setattr(service, "_process_durable_web_lease", process)
    monkeypatch.setattr(service, "_count_unresolved_web_tasks", no_unresolved)
    monkeypatch.setattr(service, "_next_retry_delay", no_retry_delay)

    summary = await service.run_live_web_fallback_worker(
        object(),
        lambda: ("current-run", "old-run"),
        stop_event=stop_event,
        browser_profile="/private/synthetic-profile",
        request_budget=_FakeBudget(),
        browser_config=SimpleNamespace(max_pages=1, max_expansions=1, timeout_seconds=30),
        provider_factory=make_provider,
    )

    assert provider_args[0]["headed"] is True
    assert provider_args[0]["keep_login_page"] is True
    assert [call[1:] for call in process_calls] == [
        ("current-run", "current-task"),
        ("old-run", "old-task"),
    ]
    assert all(call[0] is provider for call in process_calls)
    assert lease_calls[-1] == ("current-run", "old-run")
    assert summary.run_ids == ("current-run", "old-run")
    assert summary.collected_count == 2
    assert summary.completed_count == 2
    assert summary.unresolved_count == 0
    assert summary.stopped_reason == "stop_requested_drained"
    assert provider.closed == 1


@pytest.mark.asyncio
async def test_login_or_stop_cancellation_cleans_up_both_waiters():
    confirmation_started = asyncio.Event()
    confirmation_cancelled = asyncio.Event()

    class BlockingStopEvent:
        def __init__(self):
            self.started = asyncio.Event()
            self.cancelled = asyncio.Event()

        def is_set(self):
            return False

        async def wait(self):
            self.started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                self.cancelled.set()
                raise

    stop_event = BlockingStopEvent()

    async def confirm(_provider, _timeout):
        confirmation_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            confirmation_cancelled.set()
            raise

    task = asyncio.create_task(
        service._confirm_login_or_stop(
            SimpleNamespace(),
            stop_event,
            confirmation=confirm,
        )
    )
    await asyncio.wait_for(
        asyncio.gather(confirmation_started.wait(), stop_event.started.wait()),
        timeout=1,
    )
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert confirmation_cancelled.is_set()
    assert stop_event.cancelled.is_set()


@pytest.mark.asyncio
async def test_web_audit_is_private_and_does_not_reconcile_or_enqueue(monkeypatch, tmp_path):
    plan = _plan()
    provider = _FakeProvider([_browser_result(identity_verified=True)])
    planner_calls = []

    async def planner(*_args, **_kwargs):
        planner_calls.append((_args, _kwargs))
        return (plan,)

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("read-only web audit must not enqueue or reconcile")

    async def snapshot(*_args):
        return {
            "status": "found",
            "source_api": "synthetic.api",
            "observed_at": "2026-01-01T00:00:00+00:00",
            "fields": {
                "ip_location_text": {
                    "canonical_value": None,
                    "csv_value": "",
                    "api_missing_reason": "not_returned_by_source",
                    "field_source_metadata": None,
                }
            },
            "urls": {},
        }

    monkeypatch.setattr(service, "plan_web_enrichment_targets", planner)
    monkeypatch.setattr(service, "enqueue_web_enrichment_tasks", forbidden)
    monkeypatch.setattr(service, "_complete_task", forbidden)
    monkeypatch.setattr(service, "_fail_task", forbidden)
    monkeypatch.setattr(service, "_read_canonical_snapshot", snapshot)
    sessions = object()
    output = await service.run_web_audit(
        sessions,
        "synthetic-run",
        ("ip_location_text",),
        service.TargetFilters(
            "comment", (plan.target_id,), tid=plan.tid, fid=plan.fid, parent_pid=plan.parent_post_pid
        ),
        output_dir=tmp_path / "reports",
        browser_profile="/private/synthetic-profile",
        request_budget=_FakeBudget(),
        browser_config=SimpleNamespace(max_pages=1, max_expansions=1, timeout_seconds=30),
        provider_factory=lambda **_kwargs: provider,
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["canonical_updated"] is False
    assert report["task_rows_written"] is False
    assert report["targets"][0]["target_id"] == str(plan.target_id)
    assert report["targets"][0]["fields"]["ip_location_text"]["field_outcome"]["status"] == "not_tested"
    assert report["targets"][0]["identity_verified"] is True
    assert report["targets"][0]["identity_status"] == "verified"
    assert planner_calls == [
        (
            (sessions, "synthetic-run", ("ip_location_text",)),
            {
                "target_type": "comment",
                "target_ids": (plan.target_id,),
                "tid": plan.tid,
                "fid": plan.fid,
                "parent_pid": plan.parent_post_pid,
                "limit": 1,
            },
        )
    ]
    assert S_IMODE(output.parent.stat().st_mode) == 0o700
    assert S_IMODE(output.stat().st_mode) == 0o600
    assert provider.closed == 1


def test_browser_configuration_error_prints_fixed_optional_install_help(monkeypatch, capsys):
    from src.research.web_fallback.browser import BrowserProviderConfigurationError

    async def fail(_args):
        raise BrowserProviderConfigurationError("must never be printed")

    monkeypatch.setattr(cli, "_run_field_backfill_cli", fail)
    result = cli.main([
        "field-backfill",
        "--run-id",
        "synthetic-run",
        "--target-type",
        "thread",
        "--tid",
        "123",
        "--field",
        "ip_location_text",
    ])
    captured = capsys.readouterr()

    assert result == 2
    assert "uv sync --group login" in captured.err
    assert "uv run playwright install chromium" in captured.err
    assert "must never be printed" not in captured.err


@pytest.mark.asyncio
async def test_web_audit_confirms_login_and_retries_target_once_without_task_writes(monkeypatch, tmp_path):
    plan = _plan()
    provider = _FakeProvider(
        [
            _browser_result("needs_login", "official_verification"),
            _browser_result(identity_verified=True),
        ],
        has_manual_login_page=True,
    )
    confirmations = []

    async def planner(*_args, **_kwargs):
        return (plan,)

    async def snapshot(*_args):
        return {"status": "found", "fields": {}, "urls": {}}

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("read-only audit cannot write task or observation data")

    async def confirm(current_provider, timeout_seconds):
        confirmations.append((current_provider, timeout_seconds))
        return True

    monkeypatch.setattr(service, "plan_web_enrichment_targets", planner)
    monkeypatch.setattr(service, "enqueue_web_enrichment_tasks", forbidden)
    monkeypatch.setattr(service, "lease_web_enrichment_tasks", forbidden)
    monkeypatch.setattr(service, "resume_web_enrichment_tasks", forbidden)
    monkeypatch.setattr(service, "_complete_task", forbidden)
    monkeypatch.setattr(service, "_fail_task", forbidden)
    monkeypatch.setattr(service, "_read_canonical_snapshot", snapshot)
    output = await service.run_web_audit(
        object(),
        "synthetic-run",
        ("ip_location_text",),
        service.TargetFilters("comment"),
        output_dir=tmp_path / "reports",
        browser_profile="/private/synthetic-profile",
        headed=True,
        wait_for_login=True,
        request_budget=_FakeBudget(),
        browser_config=SimpleNamespace(max_pages=1, max_expansions=1, timeout_seconds=30),
        provider_factory=lambda **kwargs: (
            provider if kwargs["keep_login_page"] else pytest.fail("login page not retained")
        ),
        login_confirmation=confirm,
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert len(provider.targets) == 2
    assert len(confirmations) == 1
    assert provider.login_page_closed == 1
    assert provider.closed == 1
    assert report["targets"][0]["manual_login_status"] == "confirmed_retry_once"
    assert report["targets"][0]["status"] == "ok"
    assert report["targets"][0]["identity_verified"] is True
    assert report["canonical_updated"] is False
    assert report["task_rows_written"] is False


@pytest.mark.asyncio
async def test_web_audit_login_timeout_writes_only_report_and_closes_provider(monkeypatch, tmp_path):
    plan = _plan()
    provider = _FakeProvider(
        [_browser_result("needs_login", "official_verification")],
        has_manual_login_page=True,
    )

    async def planner(*_args, **_kwargs):
        return (plan,)

    async def snapshot(*_args):
        return {"status": "found", "fields": {}, "urls": {}}

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("read-only audit timeout cannot write task data")

    async def timed_out(*_args, **_kwargs):
        raise TimeoutError

    monkeypatch.setattr(service, "plan_web_enrichment_targets", planner)
    monkeypatch.setattr(service, "enqueue_web_enrichment_tasks", forbidden)
    monkeypatch.setattr(service, "_complete_task", forbidden)
    monkeypatch.setattr(service, "_fail_task", forbidden)
    monkeypatch.setattr(service, "_confirm_manual_login", timed_out)
    monkeypatch.setattr(service, "_read_canonical_snapshot", snapshot)
    output = await service.run_web_audit(
        object(),
        "synthetic-run",
        ("ip_location_text",),
        service.TargetFilters("comment"),
        output_dir=tmp_path / "reports",
        browser_profile="/private/synthetic-profile",
        headed=True,
        wait_for_login=True,
        request_budget=_FakeBudget(),
        browser_config=SimpleNamespace(max_pages=1, max_expansions=1, timeout_seconds=30),
        provider_factory=lambda **_kwargs: provider,
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["targets"][0]["manual_login_status"] == "timed_out"
    assert report["targets"][0]["status"] == "needs_login"
    assert len(provider.targets) == 1
    assert provider.login_page_closed == 0
    assert provider.closed == 1


@pytest.mark.asyncio
async def test_web_audit_login_cancellation_closes_provider_without_report_or_writes(monkeypatch, tmp_path):
    plan = _plan()
    provider = _FakeProvider(
        [_browser_result("needs_login", "official_verification")],
        has_manual_login_page=True,
    )
    confirmation_started = asyncio.Event()

    async def planner(*_args, **_kwargs):
        return (plan,)

    async def snapshot(*_args):
        return {"status": "found", "fields": {}, "urls": {}}

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("read-only audit cancellation cannot write task data")

    async def confirm(_provider, _timeout_seconds):
        confirmation_started.set()
        await asyncio.Future()

    monkeypatch.setattr(service, "plan_web_enrichment_targets", planner)
    monkeypatch.setattr(service, "enqueue_web_enrichment_tasks", forbidden)
    monkeypatch.setattr(service, "_complete_task", forbidden)
    monkeypatch.setattr(service, "_fail_task", forbidden)
    monkeypatch.setattr(service, "_read_canonical_snapshot", snapshot)
    task = asyncio.create_task(
        service.run_web_audit(
            object(),
            "synthetic-run",
            ("ip_location_text",),
            service.TargetFilters("comment"),
            output_dir=tmp_path / "reports",
            browser_profile="/private/synthetic-profile",
            headed=True,
            wait_for_login=True,
            request_budget=_FakeBudget(),
            browser_config=SimpleNamespace(max_pages=1, max_expansions=1, timeout_seconds=30),
            provider_factory=lambda **_kwargs: provider,
            login_confirmation=confirm,
        )
    )
    await asyncio.wait_for(confirmation_started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert provider.closed == 1
    assert not list((tmp_path / "reports").glob("*.json"))


@pytest.mark.asyncio
async def test_web_audit_never_retries_429(monkeypatch, tmp_path):
    plan = _plan()
    provider = _FakeProvider(
        [_browser_result("blocked", "http_429")],
        has_manual_login_page=True,
    )

    async def planner(*_args, **_kwargs):
        return (plan,)

    async def snapshot(*_args):
        return {"status": "found", "fields": {}, "urls": {}}

    async def confirm(*_args):
        pytest.fail("429 must never enter manual-login resume")

    monkeypatch.setattr(service, "plan_web_enrichment_targets", planner)
    monkeypatch.setattr(service, "_read_canonical_snapshot", snapshot)
    output = await service.run_web_audit(
        object(),
        "synthetic-run",
        ("ip_location_text",),
        service.TargetFilters("comment"),
        output_dir=tmp_path / "reports",
        browser_profile="/private/synthetic-profile",
        headed=True,
        wait_for_login=True,
        request_budget=_FakeBudget(),
        browser_config=SimpleNamespace(max_pages=1, max_expansions=1, timeout_seconds=30),
        provider_factory=lambda **_kwargs: provider,
        login_confirmation=confirm,
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert len(provider.targets) == 1
    assert "manual_login_status" not in report["targets"][0]
    assert report["targets"][0]["reason"] == "http_429"


def test_web_audit_does_not_treat_legacy_bawu_false_as_a_known_zero():
    result = service._audit_field_comparisons(
        ("is_bawu",),
        {
            "fields": {
                "is_bawu": {
                    "canonical_value": False,
                    "csv_value": "False",
                    "api_missing_reason": None,
                    "field_source_metadata": None,
                }
            }
        },
        (),
        {},
    )

    assert result["is_bawu"]["database_value_status"] == "unknown_zero"
    assert result["is_bawu"]["conflict_with_known_database_value"] is False


@pytest.mark.asyncio
async def test_terminal_confirmation_removes_reader_on_timeout_and_cancel(monkeypatch):
    import os

    read_fd, write_fd = os.pipe()

    @dataclass
    class FakeInput:
        descriptor: int

        def fileno(self):
            return self.descriptor

    try:
        loop = asyncio.get_running_loop()
        assert await service._wait_for_terminal_enter(0.01, stream=FakeInput(read_fd)) is False
        assert loop.remove_reader(read_fd) is False
        task = asyncio.create_task(service._wait_for_terminal_enter(5, stream=FakeInput(read_fd)))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert loop.remove_reader(read_fd) is False
        os.write(write_fd, b"x")
    finally:
        os.close(read_fd)
        os.close(write_fd)
