import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from tiebameow.models.orm import Base

from src.core.models import ResearchRun, ResearchTaskScope
from src.research.models import (
    RESEARCH_TABLES,
    ResearchComment,
    ResearchContentObservation,
    ResearchFieldObservation,
    ResearchPost,
    ResearchThread,
    ResearchWebEnrichmentAttempt,
    ResearchWebEnrichmentFailure,
    ResearchWebEnrichmentTask,
)
from src.research.persistence import _content_is_bawu, _upsert_content_rows
from src.research.web_fallback import service
from src.research.web_fallback.dom import extract_dom_candidates
from src.research.web_fallback.identity import (
    ObservationCandidate,
    Target,
    make_request_context,
)
from src.research.web_fallback.network import extract_page_pc_thread_counts
from src.research.web_fallback.reconcile import (
    _finish_attempt_after_error,
    complete_web_enrichment_task,
    fail_web_enrichment_task,
    preflight_web_enrichment_task,
)
from src.research.web_fallback.tasks import (
    cancel_web_enrichment_task,
    enqueue_web_enrichment_tasks,
    lease_web_enrichment_tasks,
    resume_web_enrichment_tasks,
)


class _AsyncTransaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        self.transaction = self.session.begin()
        self.transaction.__enter__()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return self.transaction.__exit__(exc_type, exc, traceback)


class _AsyncSessionAdapter:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.session.close()

    def begin(self):
        return _AsyncTransaction(self.session)

    def get_bind(self):
        return self.session.get_bind()

    async def get(self, *args, **kwargs):
        return self.session.get(*args, **kwargs)

    async def execute(self, statement):
        return self.session.execute(statement)

    async def scalar(self, statement):
        return self.session.scalar(statement)

    async def scalars(self, statement):
        return self.session.scalars(statement)

    def add(self, instance):
        self.session.add(instance)


class _AsyncSessionMaker:
    def __init__(self, engine):
        self.engine = engine

    def __call__(self):
        return _AsyncSessionAdapter(Session(self.engine))


@pytest.fixture
def reconcile_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[ResearchRun.__table__, ResearchTaskScope.__table__, *RESEARCH_TABLES],
    )
    observed = datetime(2026, 1, 1, tzinfo=UTC)
    with Session(engine) as session, session.begin():
        session.add(
            ResearchRun(
                run_id="run-reconcile-001",
                mode="backfill",
                scope_key="synthetic",
                forums_json=[7654321],
                status="completed",
            )
        )
        session.add(
            ResearchThread(
                tid=1001,
                first_pid=2001,
                fid=7654321,
                title="same title synthetic",
                author_user_id=501,
                author_portrait="thread-portrait",
                source_api="aiotieba.get_posts",
                observed_at=observed,
                source_payload_json={},
                missing_reasons_json={},
            )
        )
        session.add(
            ResearchTaskScope(
                run_id="run-reconcile-001",
                logical_task_key="thread_replies:1001",
                generation=0,
                task_type="posts",
                scope_type="thread_replies",
                fid=7654321,
                tid=1001,
                status="completed",
            )
        )
        rows = [
            (2101, 0, {}, None, {"is_bawu": "zero_presence_unverified_in_parsed_object"}),
            (
                2102,
                0,
                {"agree_count": "zero_presence_unverified_in_parsed_object"},
                None,
                {"is_bawu": "zero_presence_unverified_in_parsed_object"},
            ),
            (2103, None, {}, None, {}),
            (2104, 0, {}, None, {}),
            (2105, 0, {}, None, {}),
        ]
        for pid, agree_count, missing, is_bawu, extra_missing in rows:
            session.add(
                ResearchPost(
                    pid=pid,
                    tid=1001,
                    fid=7654321,
                    text="same text synthetic",
                    author_user_id=777,
                    author_user_name="known-api-name",
                    author_portrait="post-portrait",
                    author_nickname="known-title" if pid == 2105 else None,
                    agree_count=agree_count,
                    disagree_count=0 if pid == 2104 else None,
                    is_bawu=is_bawu,
                    source_api="aiotieba.get_posts",
                    observed_at=observed,
                    source_payload_json={},
                    missing_reasons_json={**missing, **extra_missing},
                )
            )
        session.add_all(
            [
                ResearchComment(
                    cid=3101,
                    tid=1001,
                    parent_post_pid=2101,
                    fid=7654321,
                    text="same comment synthetic",
                    author_user_id=888,
                    source_api="aiotieba.get_comments",
                    observed_at=observed,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchComment(
                    cid=3102,
                    tid=1001,
                    parent_post_pid=2101,
                    fid=7654321,
                    text="same comment synthetic",
                    author_user_id=888,
                    source_api="aiotieba.get_comments",
                    observed_at=observed,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
            ]
        )
        session.add(
            ResearchTaskScope(
                run_id="run-reconcile-001",
                logical_task_key="post_comments:1001:2101",
                generation=0,
                task_type="comments",
                scope_type="post_comments",
                fid=7654321,
                tid=1001,
                parent_pid=2101,
                parent_generation=0,
                status="completed",
            )
        )
    yield engine, _AsyncSessionMaker(engine)
    engine.dispose()


def _identity(lease, *, author_user_id=None, fid=None, parent_post_pid="use-lease", source_kind="web_dom"):
    actual_parent = lease.parent_post_pid if parent_post_pid == "use-lease" else parent_post_pid
    if source_kind == "web_dom":
        methods = ["canonical_page_tid", "body_tid", "body_fid", "owner_scoped_header"]
        if lease.target_type == "post":
            methods.append("post_data_id")
    else:
        methods = ["page_tid", "request_tid", "known_api_tid_fid_binding", "response_author_id"]
    return SimpleNamespace(
        target_type=lease.target_type,
        target_id=str(lease.target_id),
        tid=str(lease.tid),
        pid=None if lease.pid is None else str(lease.pid),
        cid=None if lease.cid is None else str(lease.cid),
        parent_post_pid=None if actual_parent is None else str(actual_parent),
        author_user_id=str(lease.author_user_id if author_user_id is None else author_user_id),
        fid=str(lease.fid if fid is None else fid),
        match_methods=tuple(methods),
        context_id="synthetic-context",
        request_offset="0" if lease.target_type == "comment" else None,
        identity_verified=True,
    )


def _candidate(
    lease,
    field_name,
    value,
    *,
    author_user_id=None,
    fid=None,
    parent_post_pid="use-lease",
    source_kind="web_dom",
    precision="exact",
    raw_value=None,
):
    identity = _identity(
        lease,
        author_user_id=author_user_id,
        fid=fid,
        parent_post_pid=parent_post_pid,
        source_kind=source_kind,
    )
    source_url = f"https://tieba.baidu.com/p/{lease.tid}?fid={lease.fid}&token=synthetic-secret"
    source_path = "owner.head-line.synthetic"
    if source_kind == "web_network":
        source_url = (
            f"https://tieba.baidu.com/c/f/pb/nestedFloor?tid={lease.tid}&fid={lease.fid}"
            f"&pid={lease.parent_post_pid}&offset=0&token=synthetic-secret"
        )
        source_path = "data.post_list[0].author.synthetic"
    return ObservationCandidate(
        field_name=field_name,
        raw_value=value if raw_value is None else raw_value,
        normalized_value=value,
        source_kind=source_kind,
        source_path=source_path,
        source_url=source_url,
        observed_at=datetime(2026, 1, 3, tzinfo=UTC),
        identity=identity,
        parser_version="synthetic-parser-v1",
        temporal_scope="page_observation",
        precision=precision,
    )


def _page_pc_thread_result(target):
    payload = {
        "error_code": 0,
        "forum": {"id": int(target.fid)},
        "thread": {
            "id": int(target.tid),
            "post_id": 2001,
            "author": {"id": int(target.author_user_id)},
            "reply_num": 8,
        },
        "first_floor": {"id": 2001, "author_id": int(target.author_user_id)},
    }
    context = make_request_context(
        target,
        page_url=f"https://tieba.baidu.com/p/{target.tid}?pn=1",
        request_url=(
            f"https://tieba.baidu.com/c/f/pb/page_pc?kz={target.tid}&pn=1"
            "&tbs=synthetic-tbs&sign=synthetic-sign"
        ),
        request_method="POST",
        observed_at=datetime(2026, 1, 3, tzinfo=UTC),
        context_id="synthetic-page-pc-reconcile",
    )
    return extract_page_pc_thread_counts(target, json.dumps(payload), context)


async def _lease_post(maker, pid, fields):
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        fields,
        target_type="post",
        target_ids=[pid],
        tid=1001,
    )
    leases = await lease_web_enrichment_tasks(maker, "run-reconcile-001", "synthetic-worker")
    return summary, next(lease for lease in leases if lease.target_id == pid)


async def _lease_thread(maker, fields):
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        fields,
        target_type="thread",
        target_ids=[1001],
        tid=1001,
    )
    leases = await lease_web_enrichment_tasks(maker, "run-reconcile-001", "synthetic-worker")
    return summary, next(lease for lease in leases if lease.target_id == 1001)


async def _lease_comment(maker, cid, fields):
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        fields,
        target_type="comment",
        target_ids=[cid],
        tid=1001,
    )
    leases = await lease_web_enrichment_tasks(
        maker, "run-reconcile-001", "synthetic-worker", task_keys=summary.task_keys
    )
    return summary, leases[0]


def test_stale_web_task_is_ignored_only_after_current_evidence_replaces_it():
    old_observed = datetime(2026, 1, 1, tzinfo=UTC)
    current_observed = datetime(2026, 1, 2, tzinfo=UTC)
    old_task = SimpleNamespace(
        run_id="run",
        target_type="post",
        target_id=2101,
        tid=1001,
        fid=7654321,
        author_user_id=501,
        author_portrait="post-portrait",
        parent_post_pid=None,
        scope_generation=0,
        parent_scope_generation=None,
        target_observed_at=old_observed,
        requested_fields_json=["ip_location_text"],
    )
    row = SimpleNamespace(
        pid=2101,
        cid=None,
        tid=1001,
        fid=7654321,
        author_user_id=501,
        author_portrait="post-portrait",
        parent_post_pid=None,
        observed_at=current_observed,
        ip_location_text=None,
        missing_reasons_json={},
    )
    scopes = {
        ("run", "thread_replies:1001"): SimpleNamespace(generation=1, fid=7654321)
    }
    replacement = SimpleNamespace(
        **{
            **vars(old_task),
            "scope_generation": 1,
            "target_observed_at": current_observed,
            "requested_fields_json": ["ip_location_text", "author_nickname"],
        }
    )

    assert service._stale_web_task_is_superseded(old_task, row, [old_task, replacement], scopes)


def test_stale_web_task_stays_unresolved_without_covered_current_work():
    old_observed = datetime(2026, 1, 1, tzinfo=UTC)
    current_observed = datetime(2026, 1, 2, tzinfo=UTC)
    task = SimpleNamespace(
        run_id="run",
        target_type="post",
        target_id=2101,
        tid=1001,
        fid=7654321,
        author_user_id=501,
        author_portrait="post-portrait",
        parent_post_pid=None,
        scope_generation=0,
        parent_scope_generation=None,
        target_observed_at=old_observed,
        requested_fields_json=["ip_location_text"],
    )
    row = SimpleNamespace(
        pid=2101,
        cid=None,
        tid=1001,
        fid=7654321,
        author_user_id=501,
        author_portrait="post-portrait",
        parent_post_pid=None,
        observed_at=current_observed,
        ip_location_text=None,
        missing_reasons_json={},
    )
    scopes = {
        ("run", "thread_replies:1001"): SimpleNamespace(generation=1, fid=7654321)
    }
    subset = SimpleNamespace(
        **{
            **vars(task),
            "scope_generation": 1,
            "target_observed_at": current_observed,
            "requested_fields_json": ["author_nickname"],
        }
    )

    assert not service._stale_web_task_is_superseded(task, row, [subset], scopes)
    assert not service._stale_web_task_is_superseded(task, row, [], scopes)

    row.ip_location_text = "Vancouver"
    assert service._stale_web_task_is_superseded(task, row, [], scopes)

    row.ip_location_text = None
    row.author_user_id = 999
    assert not service._stale_web_task_is_superseded(task, row, [subset], scopes)


def test_same_snapshot_scope_refresh_requires_current_generation_replacement():
    observed = datetime(2026, 1, 2, tzinfo=UTC)
    task = SimpleNamespace(
        run_id="run",
        target_type="post",
        target_id=2101,
        tid=1001,
        fid=7654321,
        author_user_id=501,
        author_portrait="post-portrait",
        parent_post_pid=None,
        scope_generation=0,
        parent_scope_generation=None,
        target_observed_at=observed,
        requested_fields_json=["ip_location_text"],
    )
    row = SimpleNamespace(
        pid=2101,
        cid=None,
        tid=1001,
        fid=7654321,
        author_user_id=501,
        author_portrait="post-portrait",
        parent_post_pid=None,
        observed_at=observed,
        ip_location_text=None,
        missing_reasons_json={},
    )
    scopes = {
        ("run", "thread_replies:1001"): SimpleNamespace(generation=1, fid=7654321)
    }
    replacement = SimpleNamespace(
        **{
            **vars(task),
            "scope_generation": 1,
            "requested_fields_json": ["ip_location_text", "author_nickname"],
        }
    )

    assert not service._stale_web_task_is_superseded(task, row, [], scopes)
    assert service._stale_web_task_is_superseded(task, row, [replacement], scopes)

    replacement.requested_fields_json = ["author_nickname"]
    assert not service._stale_web_task_is_superseded(task, row, [replacement], scopes)

    row.ip_location_text = "Vancouver"
    current_scopes = {
        ("run", "thread_replies:1001"): SimpleNamespace(generation=0, fid=7654321)
    }
    assert service._stale_web_task_is_superseded(task, row, [], current_scopes)


@pytest.mark.asyncio
async def test_verified_field_applies_by_exact_post_and_source_is_sanitized(reconcile_db):
    engine, maker = reconcile_db
    summary, lease = await _lease_post(maker, 2101, ["author_nickname"])
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "author_nickname", "网页头衔")],
    )
    assert result.status == "completed"
    assert result.applied_fields == ("author_nickname",)
    with Session(engine) as session:
        row = session.get(ResearchPost, 2101)
        observation = session.scalar(
            select(ResearchFieldObservation).where(ResearchFieldObservation.task_key == summary.task_keys[0])
        )
        task = session.get(ResearchWebEnrichmentTask, summary.task_keys[0])
        assert row.author_nickname == "网页头衔"
        assert row.field_sources_json["author_nickname"]["verified_web"]["source_kind"] == "web_dom"
        assert observation.source_url == "https://tieba.baidu.com/p/1001?fid=7654321"
        assert "synthetic-secret" not in observation.source_url
        assert observation.temporal_scope == "web_observation_at_observed_time"
        assert task.status == "completed"
        assert "elapsed_seconds" in task.last_diagnostics_json


@pytest.mark.asyncio
async def test_durable_preflight_skips_task_filled_by_api_before_browser(reconcile_db):
    engine, maker = reconcile_db
    with Session(engine) as session, session.begin():
        post = session.get(ResearchPost, 2101)
        post.author_nickname = "API refreshed title"
        post.observed_at = datetime(2026, 1, 2, tzinfo=UTC)
    _summary, lease = await _lease_post(maker, 2101, ["author_nickname"])

    class Provider:
        async def collect(self, _target):
            pytest.fail("preflight-completed tasks must not visit the browser")

    status, _result = await service._process_durable_web_lease(maker, Provider(), lease)

    assert status == "completed"
    with Session(engine) as session:
        post = session.get(ResearchPost, 2101)
        task = session.get(ResearchWebEnrichmentTask, lease.task_key)
        attempt = session.get(ResearchWebEnrichmentAttempt, (lease.task_key, lease.lease_generation))
        assert post.author_nickname == "API refreshed title"
        assert task.status == "completed"
        assert task.last_diagnostics_json["status"] == "already_satisfied"
        assert task.last_diagnostics_json["pages"] == 0
        assert attempt.status == "completed"


@pytest.mark.asyncio
async def test_durable_preflight_collects_only_missing_fields_after_api_refresh(reconcile_db):
    engine, maker = reconcile_db
    with Session(engine) as session, session.begin():
        post = session.get(ResearchPost, 2101)
        post.global_growth_level = 12
        post.global_growth_icon_url = "https://growth.example.test/icon.png"
        post.observed_at = datetime(2026, 1, 2, tzinfo=UTC)
    _summary, lease = await _lease_post(
        maker,
        2101,
        ["global_growth_level", "global_growth_icon_url", "ip_location_text"],
    )
    targets = []

    class Provider:
        async def collect(self, target):
            targets.append(target)
            return SimpleNamespace(
                status="ok",
                candidates=(_candidate(lease, "ip_location_text", "Vancouver"),),
                field_outcomes={},
                diagnostics={},
            )

    status, _result = await service._process_durable_web_lease(maker, Provider(), lease)

    assert status == "completed"
    assert len(targets) == 1
    assert targets[0].requested_fields == ("ip_location_text",)
    with Session(engine) as session:
        post = session.get(ResearchPost, 2101)
        assert post.ip_location_text == "Vancouver"
        task = session.get(ResearchWebEnrichmentTask, lease.task_key)
        assert task.field_statuses_json["global_growth_level"] == {
            "status": "confirmed",
            "reason": "already_satisfied",
        }
        observations = session.scalars(
            select(ResearchFieldObservation).where(
                ResearchFieldObservation.task_key == lease.task_key,
                ResearchFieldObservation.field_name.in_(
                    ("global_growth_level", "global_growth_icon_url")
                ),
            )
        ).all()
        assert observations == []


@pytest.mark.asyncio
async def test_single_growth_field_preflight_keeps_legacy_request_scope(reconcile_db):
    engine, maker = reconcile_db
    with Session(engine) as session, session.begin():
        post = session.get(ResearchPost, 2101)
        post.global_growth_level = 12
        post.observed_at = datetime(2026, 1, 2, tzinfo=UTC)
    _summary, lease = await _lease_post(maker, 2101, ["global_growth_level"])

    class Provider:
        async def collect(self, _target):
            pytest.fail("a known single-field legacy task should be skipped")

    status, result = await service._process_durable_web_lease(maker, Provider(), lease)

    assert status == "completed"
    assert result.preflight_skipped is True
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, lease.task_key)
        assert task.requested_fields_json == ["global_growth_level"]


@pytest.mark.asyncio
async def test_preflight_satisfied_fields_are_rechecked_at_merge(reconcile_db):
    engine, maker = reconcile_db
    with Session(engine) as session, session.begin():
        post = session.get(ResearchPost, 2101)
        post.global_growth_level = 12
        post.global_growth_icon_url = "https://growth.example.test/icon.png"
        post.observed_at = datetime(2026, 1, 2, tzinfo=UTC)
    _summary, lease = await _lease_post(
        maker,
        2101,
        ["global_growth_level", "global_growth_icon_url", "ip_location_text"],
    )

    class Provider:
        async def collect(self, _target):
            with Session(engine) as session, session.begin():
                post = session.get(ResearchPost, 2101)
                post.global_growth_level = None
                post.global_growth_icon_url = None
                post.observed_at = datetime(2026, 1, 3, tzinfo=UTC)
            return SimpleNamespace(
                status="ok",
                candidates=(_candidate(lease, "ip_location_text", "Vancouver"),),
                field_outcomes={},
                diagnostics={},
            )

    status, _result = await service._process_durable_web_lease(maker, Provider(), lease)

    assert status == "failed_final"
    with Session(engine) as session:
        post = session.get(ResearchPost, 2101)
        task = session.get(ResearchWebEnrichmentTask, lease.task_key)
        assert post.ip_location_text is None
        assert task.status == "failed_final"
        assert task.last_error == "target_observed_at_changed"


@pytest.mark.asyncio
async def test_stale_thread_content_marker_can_fall_back_to_verified_list_metadata(reconcile_db):
    engine, maker = reconcile_db
    with Session(engine) as session, session.begin():
        row = session.get(ResearchThread, 1001)
        row.source_api = "aiotieba.get_threads"
        row.observed_at = datetime(2026, 1, 2, tzinfo=UTC)
        scope = session.get(ResearchTaskScope, ("run-reconcile-001", "thread_replies:1001"))
        scope.status = "pending"
        session.add(
            ResearchContentObservation(
                owner_type="thread_first_post",
                owner_id=1001,
                tid=1001,
                source_api="aiotieba.get_posts",
                observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )

    _summary, lease = await _lease_thread(maker, ["author_nickname"])
    result = await preflight_web_enrichment_task(maker, lease.task_key, lease.lease_token)

    assert result.status == "ready"
    assert result.missing_fields == ("author_nickname",)


@pytest.mark.asyncio
async def test_live_web_worker_drains_after_api_done_and_uses_its_own_budget(reconcile_db):
    _engine, maker = reconcile_db
    await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["author_nickname"],
        target_type="post",
        target_ids=[2101, 2102],
        tid=1001,
    )
    provider_targets = []
    provider_budgets = []

    class Provider:
        async def collect(self, target):
            provider_targets.append(target)
            return SimpleNamespace(
                status="ok",
                candidates=(_candidate(target, "author_nickname", f"web-{target.target_id}"),),
                field_outcomes={},
                diagnostics={"pages": 1},
            )

    async def factory(**kwargs):
        provider_budgets.append(kwargs["budget"])
        return Provider()

    stop_event = asyncio.Event()
    api_done_event = asyncio.Event()
    api_done_event.set()
    result = await service.run_live_web_fallback_worker(
        maker,
        lambda: ("run-reconcile-001",),
        stop_event=stop_event,
        api_done_event=api_done_event,
        browser_profile="synthetic-profile",
        browser_config=SimpleNamespace(rps=0.2),
        provider_factory=factory,
        headed=False,
        wait_for_login=False,
    )

    assert result.completed_count == 2
    assert result.unresolved_count == 0
    assert {target.target_id for target in provider_targets} == {"2101", "2102"}
    assert len(provider_budgets) == 1
    assert provider_budgets[0] is not None


@pytest.mark.asyncio
async def test_user_stop_interrupts_api_done_web_drain_after_current_visit(reconcile_db):
    _engine, maker = reconcile_db
    await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["author_nickname"],
        target_type="post",
        target_ids=[2101, 2102],
        tid=1001,
    )
    stop_event = asyncio.Event()
    api_done_event = asyncio.Event()
    api_done_event.set()
    visited = []

    class Provider:
        async def collect(self, target):
            visited.append(target.target_id)
            stop_event.set()
            return SimpleNamespace(
                status="ok",
                candidates=(_candidate(target, "author_nickname", f"web-{target.target_id}"),),
                field_outcomes={},
                diagnostics={"pages": 1},
            )

    result = await service.run_live_web_fallback_worker(
        maker,
        lambda: ("run-reconcile-001",),
        stop_event=stop_event,
        api_done_event=api_done_event,
        browser_profile="synthetic-profile",
        browser_config=SimpleNamespace(rps=0.2),
        provider_factory=lambda **_kwargs: Provider(),
        headed=False,
        wait_for_login=False,
    )

    assert len(visited) == 1
    assert result.completed_count == 1
    assert result.unresolved_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["drain", "live"])
async def test_stop_during_lease_wait_does_not_start_web_visit(reconcile_db, monkeypatch, entrypoint):
    _engine, maker = reconcile_db
    await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["author_nickname"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
    )
    stop_event = asyncio.Event()
    original_lease = service._lease_one_web_task

    async def lease_then_stop(*args):
        lease = await original_lease(*args)
        stop_event.set()
        return lease

    monkeypatch.setattr(service, "_lease_one_web_task", lease_then_stop)
    visited = []

    class Provider:
        async def collect(self, target):
            visited.append(target.target_id)
            return SimpleNamespace(status="failed_retryable", candidates=(), diagnostics={})

    provider = Provider()
    if entrypoint == "drain":
        result = await service.drain_pending_web_enrichment(
            maker,
            ("run-reconcile-001",),
            provider=provider,
            stop_event=stop_event,
            wait_for_login=False,
        )
    else:
        result = await service.run_live_web_fallback_worker(
            maker,
            lambda: ("run-reconcile-001",),
            stop_event=stop_event,
            browser_profile="synthetic-profile",
            browser_config=SimpleNamespace(rps=0.2),
            provider_factory=lambda **_kwargs: provider,
            headed=False,
            wait_for_login=False,
        )

    assert visited == []
    assert result.collected_count == 0
    assert result.unresolved_count == 1


@pytest.mark.asyncio
async def test_stop_during_preflight_does_not_start_web_visit(reconcile_db, monkeypatch):
    _engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["author_nickname"])
    stop_event = asyncio.Event()
    original_preflight = service._preflight_web_task

    async def preflight_then_stop(sessions, current_lease):
        result = await original_preflight(sessions, current_lease)
        stop_event.set()
        return result

    monkeypatch.setattr(service, "_preflight_web_task", preflight_then_stop)

    class Provider:
        async def collect(self, _target):
            pytest.fail("stop during preflight must prevent a new browser visit")

    status, _result = await service._process_durable_web_lease(
        maker, Provider(), lease, stop_event=stop_event
    )

    assert status == "stop_requested"
    with Session(_engine) as session:
        task = session.get(ResearchWebEnrichmentTask, lease.task_key)
        assert task.status == "running"
        assert task.lease_token == lease.lease_token


@pytest.mark.asyncio
async def test_api_refresh_during_web_visit_rejects_stale_web_observation(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["author_nickname"])

    class Provider:
        async def collect(self, _target):
            with Session(engine) as session, session.begin():
                post = session.get(ResearchPost, 2101)
                post.author_nickname = "API arrived during visit"
                post.observed_at = datetime(2026, 1, 2, tzinfo=UTC)
            return SimpleNamespace(
                status="ok",
                candidates=(_candidate(lease, "author_nickname", "stale browser title"),),
                field_outcomes={},
                diagnostics={},
            )

    status, _result = await service._process_durable_web_lease(maker, Provider(), lease)

    assert status == "failed_final"
    with Session(engine) as session:
        post = session.get(ResearchPost, 2101)
        task = session.get(ResearchWebEnrichmentTask, lease.task_key)
        assert post.author_nickname == "API arrived during visit"
        assert task.status == "failed_final"
        assert task.last_error == "target_observed_at_changed"


@pytest.mark.asyncio
async def test_partial_web_retries_use_three_then_five_seconds_and_stop_after_three_attempts(reconcile_db):
    engine, maker = reconcile_db
    _summary, first_lease = await _lease_post(
        maker, 2101, ["author_nickname", "ip_location_text"]
    )

    class PartialProvider:
        async def collect(self, target):
            assert target.requested_fields == ("author_nickname", "ip_location_text")
            return SimpleNamespace(
                status="ok",
                candidates=(_candidate(first_lease, "author_nickname", "Partial title"),),
                field_outcomes={},
                diagnostics={},
            )

    status, _result = await service._process_durable_web_lease(
        maker, PartialProvider(), first_lease
    )
    assert status == "partially_completed"
    await service._enqueue_remaining_fields(maker, first_lease)
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, first_lease.task_key)
        assert task.status == "failed_retryable"
        assert task.attempts == 1
        first_due = task.next_retry_at.replace(tzinfo=UTC)
    now = datetime.now(UTC)
    assert timedelta(seconds=2.8) <= first_due - now <= timedelta(seconds=3.2)
    assert await lease_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        "too-early-first-retry",
        task_keys=(first_lease.task_key,),
        now=first_due - timedelta(milliseconds=1),
    ) == []
    second_lease = (
        await lease_web_enrichment_tasks(
            maker,
            "run-reconcile-001",
            "second-partial-attempt",
            task_keys=(first_lease.task_key,),
            now=first_due + timedelta(milliseconds=1),
        )
    )[0]
    assert second_lease.lease_generation == 2

    with Session(engine) as session, session.begin():
        session.get(ResearchWebEnrichmentTask, first_lease.task_key).status = "partially_completed"
    await service._enqueue_remaining_fields(maker, second_lease)
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, first_lease.task_key)
        assert task.status == "failed_retryable"
        assert task.attempts == 2
        second_due = task.next_retry_at.replace(tzinfo=UTC)
    now = datetime.now(UTC)
    assert timedelta(seconds=4.8) <= second_due - now <= timedelta(seconds=5.2)
    assert await lease_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        "too-early-second-retry",
        task_keys=(first_lease.task_key,),
        now=second_due - timedelta(milliseconds=1),
    ) == []
    third_lease = (
        await lease_web_enrichment_tasks(
            maker,
            "run-reconcile-001",
            "third-partial-attempt",
            task_keys=(first_lease.task_key,),
            now=second_due + timedelta(milliseconds=1),
        )
    )[0]
    assert third_lease.lease_generation == 3

    with Session(engine) as session, session.begin():
        session.get(ResearchWebEnrichmentTask, first_lease.task_key).status = "partially_completed"
    await service._enqueue_remaining_fields(maker, third_lease)
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, first_lease.task_key)
        failures = session.scalars(
            select(ResearchWebEnrichmentFailure).where(
                ResearchWebEnrichmentFailure.task_key == first_lease.task_key,
                ResearchWebEnrichmentFailure.reason_code == "max_attempts_exhausted_after_partial",
            )
        ).all()
        assert task.status == "failed_final"
        assert task.attempts == 3
        assert len(failures) == 1


@pytest.mark.asyncio
async def test_cli_field_backfill_does_not_immediately_revisit_partial_task(reconcile_db):
    engine, maker = reconcile_db
    calls = []

    class Provider:
        async def collect(self, target):
            calls.append(target)
            return SimpleNamespace(
                status="ok",
                candidates=(_candidate(target, "author_nickname", "CLI partial title"),),
                field_outcomes={},
                diagnostics={},
            )

    summary = await service.run_field_backfill(
        maker,
        "run-reconcile-001",
        ["author_nickname", "ip_location_text"],
        service.TargetFilters(target_type="post", target_ids=(2101,), tid=1001),
        limit=1,
        max_seconds=1,
        browser_profile="synthetic-profile",
        request_budget=object(),
        browser_config=SimpleNamespace(rps=0.2),
        provider_factory=lambda **_kwargs: Provider(),
    )

    assert len(calls) == 1
    assert summary.collected_count == 1
    assert summary.partial_count == 1
    assert summary.stopped_reason == "max_seconds_exhausted"
    with Session(engine) as session:
        task = session.scalar(
            select(ResearchWebEnrichmentTask).where(
                ResearchWebEnrichmentTask.target_id == 2101,
                ResearchWebEnrichmentTask.target_type == "post",
            )
        )
        assert task.status == "failed_retryable"
        assert task.attempts == 1
        assert task.next_retry_at is not None


@pytest.mark.asyncio
async def test_reply_display_name_is_not_applied_when_mention_uid_disagrees_with_known_uid(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_comment(
        maker, 3101, ["reply_to_user_id", "reply_to_display_name"]
    )
    with Session(engine) as session, session.begin():
        session.get(ResearchComment, 3101).reply_to_user_id = 99002

    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [
            _candidate(lease, "reply_to_user_id", 99001, source_kind="web_network"),
            _candidate(
                lease,
                "reply_to_display_name",
                "Synthetic Mention",
                source_kind="web_network",
                raw_value={"mention_type": 4, "mention_uid": "99001"},
            ),
        ],
    )

    assert result.conflict_fields == ("reply_to_display_name", "reply_to_user_id")
    with Session(engine) as session:
        row = session.get(ResearchComment, 3101)
        assert row.reply_to_user_id == 99002
        assert row.reply_to_display_name is None


@pytest.mark.asyncio
async def test_reply_display_name_cannot_mismatch_uid_candidate_in_same_batch(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_comment(
        maker, 3101, ["reply_to_user_id", "reply_to_display_name"]
    )

    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [
            _candidate(lease, "reply_to_user_id", 99001, source_kind="web_network"),
            _candidate(
                lease,
                "reply_to_display_name",
                "Synthetic Mention",
                source_kind="web_network",
                raw_value={"mention_type": 4, "mention_uid": "99002"},
            ),
        ],
    )

    assert result.applied_fields == ()
    assert result.conflict_fields == ("reply_to_display_name", "reply_to_user_id")
    with Session(engine) as session:
        row = session.get(ResearchComment, 3101)
        assert row.reply_to_user_id is None
        assert row.reply_to_display_name is None


@pytest.mark.asyncio
async def test_reply_candidates_respect_existing_verified_web_uid_source(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_comment(
        maker, 3101, ["reply_to_user_id", "reply_to_display_name"]
    )
    with Session(engine) as session, session.begin():
        row = session.get(ResearchComment, 3101)
        row.field_sources_json = {"reply_to_user_id": {"verified_web": {"value": 99002}}}

    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [
            _candidate(lease, "reply_to_user_id", 99001, source_kind="web_network"),
            _candidate(
                lease,
                "reply_to_display_name",
                "Synthetic Mention",
                source_kind="web_network",
                raw_value={"mention_type": 4, "mention_uid": "99001"},
            ),
        ],
    )

    assert result.conflict_fields == ("reply_to_display_name", "reply_to_user_id")
    with Session(engine) as session:
        row = session.get(ResearchComment, 3101)
        assert row.reply_to_user_id is None
        assert row.reply_to_display_name is None


@pytest.mark.parametrize(("later_uid", "expected_status"), [(111, "applied"), (222, "conflict")])
@pytest.mark.asyncio
async def test_reply_name_only_task_binds_mention_uid_for_later_uid_tasks(
    reconcile_db, later_uid, expected_status
):
    engine, maker = reconcile_db
    _name_summary, name_lease = await _lease_comment(maker, 3101, ["reply_to_display_name"])
    name_result = await complete_web_enrichment_task(
        maker,
        name_lease.task_key,
        name_lease.lease_token,
        [
            _candidate(
                name_lease,
                "reply_to_display_name",
                "Target A",
                source_kind="web_network",
                raw_value={"mention_type": 4, "mention_uid": "111"},
            )
        ],
    )
    assert name_result.applied_fields == ("reply_to_display_name",)

    _uid_summary, uid_lease = await _lease_comment(maker, 3101, ["reply_to_user_id"])
    uid_result = await complete_web_enrichment_task(
        maker,
        uid_lease.task_key,
        uid_lease.lease_token,
        [_candidate(uid_lease, "reply_to_user_id", later_uid, source_kind="web_network")],
    )

    with Session(engine) as session:
        row = session.get(ResearchComment, 3101)
        binding = row.field_sources_json["reply_to_display_name"]["verified_web"]
        assert binding["reply_target_user_id"] == "111"
        assert row.reply_to_display_name == "Target A"
        assert row.reply_to_user_id == (111 if expected_status == "applied" else None)
        if expected_status == "applied":
            assert uid_result.applied_fields == ("reply_to_user_id",)
            assert uid_result.conflict_fields == ()
        else:
            assert uid_result.applied_fields == ()
            assert uid_result.conflict_fields == ("reply_to_user_id",)
            observation = session.scalar(
                select(ResearchFieldObservation).where(
                    ResearchFieldObservation.task_key == uid_lease.task_key,
                    ResearchFieldObservation.field_name == "reply_to_user_id",
                )
            )
            assert observation.normalized_value_json == later_uid
            assert observation.applied_status == "conflict"


@pytest.mark.asyncio
async def test_reply_name_without_mention_uid_is_saved_as_display_name_only(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_comment(maker, 3101, ["reply_to_display_name"])

    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [
            _candidate(
                lease,
                "reply_to_display_name",
                "Display Only",
                source_kind="web_network",
                raw_value={"mention_type": 4, "mention_uid": None},
            )
        ],
    )

    assert result.applied_fields == ("reply_to_display_name",)
    with Session(engine) as session:
        row = session.get(ResearchComment, 3101)
        observation = session.scalar(select(ResearchFieldObservation))
        source = row.field_sources_json["reply_to_display_name"]["verified_web"]
        assert row.reply_to_display_name == "Display Only"
        assert "reply_to_display_name" not in row.missing_reasons_json
        assert source["confidence"] == "display_name_only"
        assert source["reply_target_user_id"] is None
        assert observation.applied_status == "applied"
        assert observation.raw_value_json == {"mention_type": 4, "mention_uid": None}


@pytest.mark.asyncio
async def test_reply_name_without_mention_uid_is_audit_only_when_uid_is_known(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_comment(maker, 3101, ["reply_to_display_name"])
    with Session(engine) as session, session.begin():
        session.get(ResearchComment, 3101).reply_to_user_id = 99001

    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [
            _candidate(
                lease,
                "reply_to_display_name",
                "Display Only",
                source_kind="web_network",
                raw_value={"mention_type": 4, "mention_uid": None},
            )
        ],
    )

    assert result.applied_fields == ()
    with Session(engine) as session:
        row = session.get(ResearchComment, 3101)
        observation = session.scalar(select(ResearchFieldObservation))
        assert row.reply_to_display_name is None
        assert row.missing_reasons_json["reply_to_display_name"] == "reply_to_mention_uid_missing"
        assert observation.applied_status == "audit_only"
        assert observation.missing_reason == "reply_to_mention_uid_missing"


@pytest.mark.asyncio
async def test_display_name_only_blocks_uid_task_until_same_label_has_explicit_uid(reconcile_db):
    engine, maker = reconcile_db
    _name_summary, name_lease = await _lease_comment(maker, 3101, ["reply_to_display_name"])
    await complete_web_enrichment_task(
        maker,
        name_lease.task_key,
        name_lease.lease_token,
        [
            _candidate(
                name_lease,
                "reply_to_display_name",
                "Display Only",
                source_kind="web_network",
                raw_value={"mention_type": 4, "mention_uid": None},
            )
        ],
    )

    _uid_summary, uid_lease = await _lease_comment(maker, 3101, ["reply_to_user_id"])
    uid_result = await complete_web_enrichment_task(
        maker,
        uid_lease.task_key,
        uid_lease.lease_token,
        [_candidate(uid_lease, "reply_to_user_id", 222, source_kind="web_network")],
    )

    assert uid_result.conflict_fields == ("reply_to_user_id",)
    with Session(engine) as session:
        row = session.get(ResearchComment, 3101)
        observation = session.scalar(
            select(ResearchFieldObservation).where(
                ResearchFieldObservation.task_key == uid_lease.task_key,
                ResearchFieldObservation.field_name == "reply_to_user_id",
            )
        )
        assert row.reply_to_user_id is None
        assert row.reply_to_display_name == "Display Only"
        assert row.missing_reasons_json["reply_to_user_id"] == "reply_to_uid_unbound_display_name"
        assert observation.applied_status == "conflict"

    _upgrade_summary, upgrade_lease = await _lease_comment(
        maker, 3101, ["reply_to_user_id", "reply_to_display_name"]
    )
    upgraded = await complete_web_enrichment_task(
        maker,
        upgrade_lease.task_key,
        upgrade_lease.lease_token,
        [
            _candidate(upgrade_lease, "reply_to_user_id", 111, source_kind="web_network"),
            _candidate(
                upgrade_lease,
                "reply_to_display_name",
                "Display Only",
                source_kind="web_network",
                raw_value={"mention_type": 4, "mention_uid": "111"},
            ),
        ],
    )

    assert upgraded.applied_fields == ("reply_to_user_id",)
    assert upgraded.confirmed_fields == ("reply_to_display_name",)
    with Session(engine) as session:
        row = session.get(ResearchComment, 3101)
        name_source = row.field_sources_json["reply_to_display_name"]["verified_web"]
        assert row.reply_to_user_id == 111
        assert row.reply_to_display_name == "Display Only"
        assert name_source["confidence"] == "explicit_type4_mention"
        assert name_source["reply_target_user_id"] == "111"


@pytest.mark.asyncio
async def test_page_pc_and_dom_conflict_preserves_both_raw_observations(reconcile_db):
    engine, maker = reconcile_db
    target = Target(
        target_type="thread",
        target_id=1001,
        tid=1001,
        fid=7654321,
        author_user_id=501,
        author_portrait="thread-portrait",
        requested_fields=("reply_count",),
    )
    network_result = _page_pc_thread_result(target)
    dom_result = extract_dom_candidates(
        target,
        "https://tieba.baidu.com/p/1001?pn=1",
        """
        <html><body><div class="image-text">
          <div class="pb-content-wrap"><div class="richtext-item">
            <span tid="1001" fid="7654321" index="0"></span>
          </div></div>
          <div class="head-line user-info"><div class="name-info">
            <a class="head-name" href="https://tieba.baidu.com/home/main?id=thread-portrait">author</a>
          </div></div>
          <div class="pc-pb-first-floor-interactive"><div class="action-item">
            <svg><use href="#comment_pb"></use></svg><span class="action-number">7</span>
          </div></div>
        </div></body></html>
        """,
        observed_at=datetime(2026, 1, 4, tzinfo=UTC),
    )
    assert network_result.status == "ok"
    assert dom_result.identity_verified is True
    assert {item.normalized_value for item in dom_result.candidates} == {7}
    _summary, lease = await _lease_thread(maker, ["reply_count"])

    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [*network_result.candidates, *dom_result.candidates],
    )

    assert result.conflict_fields == ("reply_count",)
    with Session(engine) as session:
        observations = session.scalars(
            select(ResearchFieldObservation).where(
                ResearchFieldObservation.task_key == lease.task_key,
                ResearchFieldObservation.field_name == "reply_count",
            )
        ).all()
        assert {
            (
                row.source_kind,
                row.raw_value_json,
                row.normalized_value_json,
                row.observed_at.replace(tzinfo=UTC),
            )
            for row in observations
        } == {
            ("web_network", 8, 8, datetime(2026, 1, 3, tzinfo=UTC)),
            ("web_dom", "7", 7, datetime(2026, 1, 4, tzinfo=UTC)),
        }
        row = session.get(ResearchThread, 1001)
        assert row.reply_count is None
        assert all("synthetic-tbs" not in item.source_url for item in observations)
        assert all("synthetic-sign" not in item.source_url for item in observations)
        assert {item.source_url for item in observations} == {
            "https://tieba.baidu.com/c/f/pb/page_pc?kz=1001",
            "https://tieba.baidu.com/p/1001",
        }


@pytest.mark.asyncio
async def test_page_pc_reconcile_rejects_mismatched_kz(reconcile_db):
    engine, maker = reconcile_db
    target = Target(
        target_type="thread",
        target_id=1001,
        tid=1001,
        fid=7654321,
        author_user_id=501,
        requested_fields=("reply_count",),
    )
    network_result = _page_pc_thread_result(target)
    candidate = replace(
        network_result.candidates[0],
        source_url=(
            "https://tieba.baidu.com/c/f/pb/page_pc?kz=1002&pn=1"
            "&tbs=synthetic-tbs&sign=synthetic-sign"
        ),
    )
    _summary, lease = await _lease_thread(maker, ["reply_count"])

    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [candidate],
    )

    assert result.status == "identity_unverified"
    assert result.reason_code == "candidate_query_tid_mismatch"
    with Session(engine) as session:
        assert session.get(ResearchThread, 1001).reply_count is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []


@pytest.mark.asyncio
async def test_mixed_candidate_batch_with_wrong_author_has_zero_partial_updates(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["author_nickname", "global_growth_level"])
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [
            _candidate(lease, "author_nickname", "must-roll-back"),
            _candidate(lease, "global_growth_level", 8, author_user_id=999),
        ],
    )
    assert result.status == "identity_unverified"
    with Session(engine) as session:
        row = session.get(ResearchPost, 2101)
        assert row.author_nickname is None
        assert row.global_growth_level is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []
        failure = session.scalar(select(ResearchWebEnrichmentFailure))
        assert failure.reason_code == "candidate_author_user_id_mismatch"
        assert session.get(ResearchWebEnrichmentTask, lease.task_key).status == "identity_unverified"


@pytest.mark.asyncio
async def test_field_outcomes_preserve_not_tested_reason_without_marking_unsupported(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["global_growth_level"])
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [],
        field_outcomes={
            "global_growth_level": {
                "status": "not_tested",
                "reason": "numeric_level_not_exposed_in_page",
            }
        },
    )
    assert result.status == "partially_completed"
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, lease.task_key)
        row = session.get(ResearchPost, 2101)
        assert task.field_statuses_json["global_growth_level"] == {
            "status": "not_tested",
            "reason": "numeric_level_not_exposed_in_page",
        }
        web_check = row.field_sources_json["global_growth_level"]["web_check"]
        assert web_check["status"] == "not_tested"
        assert web_check["reason"] == "numeric_level_not_exposed_in_page"
        assert web_check["task_key"] == lease.task_key
        assert datetime.fromisoformat(web_check["checked_at"]).tzinfo is not None
        assert row.global_growth_level is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []


@pytest.mark.asyncio
async def test_growth_icon_satisfies_pair_without_filling_unknown_numeric_level(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(
        maker,
        2101,
        ["global_growth_level", "global_growth_icon_url"],
    )
    icon_url = "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp"
    candidate = _candidate(
        lease, "global_growth_icon_url", icon_url, source_kind="web_network"
    )
    candidate = replace(
        candidate,
        source_url=f"https://tieba.baidu.com/c/f/pb/page_pc?kz={lease.tid}&fid={lease.fid}",
        source_path="page_pc.post_list[0].author.growth_icon_url",
    )
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [candidate],
        field_outcomes={
            "global_growth_level": {
                "status": "absent_on_page",
                "reason": "global_growth_level_zero_is_unknown",
            }
        },
    )

    assert result.status == "completed"
    with Session(engine) as session:
        row = session.get(ResearchPost, 2101)
        task = session.get(ResearchWebEnrichmentTask, lease.task_key)
        assert row.global_growth_icon_url == icon_url
        assert row.global_growth_level is None
        assert task.field_statuses_json["global_growth_level"] == {
            "status": "absent_on_page",
            "reason": "global_growth_level_zero_is_unknown",
        }


@pytest.mark.asyncio
async def test_absent_field_evidence_carries_to_same_snapshot_superset_task(reconcile_db):
    engine, maker = reconcile_db
    _summary, first_lease = await _lease_post(maker, 2101, ["ip_location_text"])
    first_result = await complete_web_enrichment_task(
        maker,
        first_lease.task_key,
        first_lease.lease_token,
        [],
        field_outcomes={
            "ip_location_text": {
                "status": "absent_on_page",
                "reason": "ip_location_not_present",
            }
        },
        identity_verified=True,
    )
    assert first_result.status == "completed_with_absent_fields"

    summary = await enqueue_web_enrichment_tasks(
        maker,
        first_lease.run_id,
        ["ip_location_text", "author_nickname"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )
    assert summary.created_count == 1
    assert summary.task_keys[0] != first_lease.task_key

    new_lease = (
        await lease_web_enrichment_tasks(
            maker,
            first_lease.run_id,
            "superset-worker",
            task_keys=summary.task_keys,
        )
    )[0]
    preflight = await preflight_web_enrichment_task(
        maker, new_lease.task_key, new_lease.lease_token
    )

    assert preflight.status == "ready"
    assert preflight.missing_fields == ("author_nickname",)
    assert preflight.preserved_outcomes == {
        "ip_location_text": {
            "status": "absent_on_page",
            "reason": "ip_location_not_present",
        }
    }
    with Session(engine) as session:
        new_task = session.get(ResearchWebEnrichmentTask, new_lease.task_key)
        assert new_task.identity_verified is True
        assert new_task.field_statuses_json["ip_location_text"] == {
            "status": "absent_on_page",
            "reason": "ip_location_not_present",
        }


@pytest.mark.asyncio
async def test_applied_field_is_not_inherited_when_canonical_value_is_missing(reconcile_db):
    engine, maker = reconcile_db
    _summary, first_lease = await _lease_post(maker, 2101, ["author_nickname"])
    first_result = await complete_web_enrichment_task(
        maker,
        first_lease.task_key,
        first_lease.lease_token,
        [_candidate(first_lease, "author_nickname", "Verified title")],
    )
    assert first_result.status == "completed"
    with Session(engine) as session, session.begin():
        session.get(ResearchPost, 2101).author_nickname = None

    summary = await enqueue_web_enrichment_tasks(
        maker,
        first_lease.run_id,
        ["author_nickname", "ip_location_text"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )
    assert summary.created_count == 1
    lease = (
        await lease_web_enrichment_tasks(
            maker,
            first_lease.run_id,
            "superset-worker",
            task_keys=summary.task_keys,
        )
    )[0]
    preflight = await preflight_web_enrichment_task(maker, lease.task_key, lease.lease_token)

    assert preflight.status == "ready"
    assert set(preflight.missing_fields) == {"author_nickname", "ip_location_text"}
    assert preflight.preserved_outcomes == {}


@pytest.mark.asyncio
async def test_partial_reply_scope_accepts_only_matching_content_snapshot(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["author_nickname"])
    observed = datetime(2026, 1, 1, tzinfo=UTC)
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-reconcile-001", "thread_replies:1001"))
        scope.status = "pending"
        session.add(
            ResearchContentObservation(
                owner_type="post",
                owner_id=2101,
                tid=1001,
                source_api="aiotieba.get_posts",
                observed_at=observed,
            )
        )

    applied = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "author_nickname", "partial-scope-name")],
    )
    assert applied.applied_fields == ("author_nickname",)
    with Session(engine) as session:
        assert session.get(ResearchPost, 2101).author_nickname == "partial-scope-name"


@pytest.mark.asyncio
async def test_partial_comment_scope_requires_exact_comment_snapshot(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_comment(maker, 3101, ["author_nickname"])
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-reconcile-001", "post_comments:1001:2101"))
        scope.status = "pending"
        session.add(
            ResearchContentObservation(
                owner_type="comment",
                owner_id=3101,
                tid=1001,
                source_api="aiotieba.get_comments",
                observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )

    applied = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "author_nickname", "partial-comment-name", source_kind="web_network")],
    )
    assert applied.applied_fields == ("author_nickname",)
    with Session(engine) as session:
        assert session.get(ResearchComment, 3101).author_nickname == "partial-comment-name"


@pytest.mark.asyncio
async def test_partial_scope_rejects_mismatched_content_snapshot(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["author_nickname"])
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-reconcile-001", "thread_replies:1001"))
        scope.status = "pending"
        session.add(
            ResearchContentObservation(
                owner_type="post",
                owner_id=2101,
                tid=1001,
                source_api="aiotieba.get_posts",
                observed_at=datetime(2026, 1, 2, tzinfo=UTC),
            )
        )

    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "author_nickname", "must-not-apply")],
    )
    assert result.status == "failed_final"
    assert result.reason_code == "content_snapshot_marker_mismatch"
    with Session(engine) as session:
        assert session.get(ResearchPost, 2101).author_nickname is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []


@pytest.mark.asyncio
async def test_thread_list_metadata_fallback_requires_page_pc_creator_proof(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_thread(maker, ["global_growth_level"])
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-reconcile-001", "thread_replies:1001"))
        scope.status = "pending"
        thread = session.get(ResearchThread, 1001)
        thread.source_api = "aiotieba.get_threads"

    methods = (
        "response_forum_id",
        "response_thread_id",
        "thread_post_id_matches_first_floor_id",
        "response_author_id",
    )
    candidate = _candidate(lease, "global_growth_level", 8, source_kind="web_network")
    identity = _identity(lease, source_kind="web_network")
    identity.match_methods = methods
    candidate = replace(
        candidate,
        source_url=f"https://tieba.baidu.com/c/f/pb/page_pc?kz={lease.tid}&fid={lease.fid}",
        source_path="page_pc.thread.author.user_growth.level_id",
        temporal_scope="content_observation",
        identity=identity,
    )
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [candidate],
        identity_verified=True,
        identity_methods=methods,
    )

    assert result.applied_fields == ("global_growth_level",)
    with Session(engine) as session:
        assert session.get(ResearchThread, 1001).global_growth_level == 8


@pytest.mark.asyncio
async def test_thread_list_metadata_fallback_rejects_unproven_page_identity(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_thread(maker, ["global_growth_level"])
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-reconcile-001", "thread_replies:1001"))
        scope.status = "pending"
        thread = session.get(ResearchThread, 1001)
        thread.source_api = "aiotieba.get_threads"

    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [],
        field_outcomes={"global_growth_level": {"status": "absent_on_page", "reason": "no_icon"}},
        identity_verified=True,
        identity_methods=("page_tid", "response_author_id"),
    )

    assert result.status == "identity_unverified"
    assert result.reason_code == "thread_list_page_pc_proof_missing"
    with Session(engine) as session:
        assert session.get(ResearchThread, 1001).global_growth_level is None


@pytest.mark.asyncio
async def test_thread_list_metadata_fallback_rejects_counter_fields(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_thread(maker, ["agree_count"])
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-reconcile-001", "thread_replies:1001"))
        scope.status = "pending"
        thread = session.get(ResearchThread, 1001)
        thread.source_api = "aiotieba.get_threads"

    methods = (
        "response_forum_id",
        "response_thread_id",
        "thread_post_id_matches_first_floor_id",
        "response_author_id",
    )
    candidate = _candidate(lease, "agree_count", 9, source_kind="web_network")
    identity = _identity(lease, source_kind="web_network")
    identity.match_methods = methods
    candidate = replace(
        candidate,
        source_url=f"https://tieba.baidu.com/c/f/pb/page_pc?kz={lease.tid}&fid={lease.fid}",
        source_path="page_pc.thread.agree.agree_num",
        temporal_scope="content_observation",
        identity=identity,
    )

    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [candidate],
        identity_verified=True,
        identity_methods=methods,
    )

    assert result.status == "identity_unverified"
    assert result.reason_code == "thread_list_metadata_field_not_allowed"
    with Session(engine) as session:
        assert session.get(ResearchThread, 1001).agree_count is None


@pytest.mark.asyncio
async def test_partial_task_retry_keeps_key_and_waits_for_due_time(reconcile_db):
    engine, maker = reconcile_db
    _summary, initial_lease = await _lease_post(
        maker,
        2101,
        ["global_growth_level", "global_growth_icon_url", "ip_location_text"],
    )
    first = await complete_web_enrichment_task(
        maker,
        initial_lease.task_key,
        initial_lease.lease_token,
        [_candidate(initial_lease, "global_growth_level", 6)],
        field_outcomes={
            "ip_location_text": {
                "status": "not_tested",
                "reason": "ip_location_not_observed",
            }
        },
    )
    assert first.status == "partially_completed"

    retry_summary = await service._enqueue_remaining_fields(maker, initial_lease)
    assert retry_summary.task_keys == (initial_lease.task_key,)
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, initial_lease.task_key)
        assert task.status == "failed_retryable"
        assert task.attempts == 1
        due_at = task.next_retry_at.replace(tzinfo=UTC)

    assert await lease_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        "too-early-worker",
        task_keys=(initial_lease.task_key,),
        now=due_at - timedelta(milliseconds=1),
    ) == []
    retry_lease = (
        await lease_web_enrichment_tasks(
            maker,
            "run-reconcile-001",
            "due-worker",
            task_keys=(initial_lease.task_key,),
            now=due_at + timedelta(milliseconds=1),
        )
    )[0]
    assert retry_lease.task_key == initial_lease.task_key
    assert retry_lease.lease_generation == 2


@pytest.mark.asyncio
async def test_identity_failure_field_outcome_rejects_good_candidates_before_any_write(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["author_nickname", "global_growth_level"])
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "author_nickname", "must-not-apply")],
        field_outcomes={
            "global_growth_level": {
                "status": "identity_unverified",
                "reason": "response_fid_mismatch",
            }
        },
    )
    assert result.status == "identity_unverified"
    assert result.reason_code == "response_fid_mismatch"
    with Session(engine) as session:
        row = session.get(ResearchPost, 2101)
        task = session.get(ResearchWebEnrichmentTask, lease.task_key)
        attempt = session.get(ResearchWebEnrichmentAttempt, (lease.task_key, lease.lease_generation))
        failure = session.scalar(select(ResearchWebEnrichmentFailure))
        assert row.author_nickname is None
        assert row.global_growth_level is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []
        assert task.status == "identity_unverified"
        assert attempt.status == "identity_unverified"
        assert failure.reason_code == "response_fid_mismatch"


@pytest.mark.asyncio
async def test_known_zero_unknown_zero_null_and_confirmed_zero_are_distinct(reconcile_db):
    engine, maker = reconcile_db
    _known_summary, known_lease = await _lease_post(maker, 2104, ["agree_count"])
    known = await complete_web_enrichment_task(
        maker, known_lease.task_key, known_lease.lease_token, [_candidate(known_lease, "agree_count", 7)]
    )
    assert known.conflict_fields == ("agree_count",)

    _unknown_summary, unknown_lease = await _lease_post(maker, 2102, ["agree_count"])
    unknown = await complete_web_enrichment_task(
        maker, unknown_lease.task_key, unknown_lease.lease_token, [_candidate(unknown_lease, "agree_count", 7)]
    )
    assert unknown.applied_fields == ("agree_count",)

    _null_summary, null_lease = await _lease_post(maker, 2103, ["agree_count"])
    null_value = await complete_web_enrichment_task(
        maker, null_lease.task_key, null_lease.lease_token, [_candidate(null_lease, "agree_count", 4)]
    )
    assert null_value.applied_fields == ("agree_count",)

    _confirmed_summary, confirmed_lease = await _lease_post(maker, 2105, ["agree_count"])
    confirmed = await complete_web_enrichment_task(
        maker,
        confirmed_lease.task_key,
        confirmed_lease.lease_token,
        [_candidate(confirmed_lease, "agree_count", 0)],
    )
    assert confirmed.confirmed_fields == ("agree_count",)
    with Session(engine) as session:
        assert session.get(ResearchPost, 2104).agree_count == 0
        row = session.get(ResearchPost, 2102)
        assert row.agree_count == 7
        assert "agree_count" not in row.missing_reasons_json
        assert session.get(ResearchPost, 2103).agree_count == 4


@pytest.mark.asyncio
async def test_api_refresh_preserves_web_unknown_and_records_explicit_conflict(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["author_nickname"])
    await complete_web_enrichment_task(
        maker, lease.task_key, lease.lease_token, [_candidate(lease, "author_nickname", "web-title")]
    )
    incoming = ResearchPost(
        pid=2101,
        tid=1001,
        fid=7654321,
        author_user_id=777,
        source_api="aiotieba.get_posts",
        observed_at=datetime(2026, 1, 5, tzinfo=UTC),
        source_payload_json={"synthetic_api": "raw"},
        missing_reasons_json={
            "author_nickname": "not_exposed_by_get_posts",
            "author_user_name": "not_returned_by_source",
        },
        author_nickname=None,
        author_user_name=None,
    )
    async with maker() as session, session.begin():
        await _upsert_content_rows(session, ResearchPost, [incoming])
    with Session(engine) as session:
        row = session.get(ResearchPost, 2101)
        assert row.author_nickname == "web-title"
        assert row.author_user_name == "known-api-name"
        assert row.field_sources_json["author_user_name"]["last_known_api"]["retained_value"] == "known-api-name"

    conflicting = ResearchPost(
        pid=2101,
        tid=1001,
        fid=7654321,
        author_user_id=777,
        source_api="aiotieba.get_posts",
        observed_at=datetime(2026, 1, 6, tzinfo=UTC),
        source_payload_json={"synthetic_api": "conflict"},
        missing_reasons_json={},
        author_nickname="api-title",
    )
    async with maker() as session, session.begin():
        await _upsert_content_rows(session, ResearchPost, [conflicting])
    with Session(engine) as session:
        row = session.get(ResearchPost, 2101)
        conflict = row.field_sources_json["author_nickname"]["api_conflicts"][-1]
        assert row.author_nickname == "web-title"
        assert conflict == {
            "source_api": "aiotieba.get_posts",
            "observed_at": "2026-01-06T00:00:00+00:00",
            "value": "api-title",
        }


@pytest.mark.asyncio
async def test_unknown_api_false_bawu_can_be_filled_but_known_web_false_is_protected(reconcile_db):
    engine, maker = reconcile_db
    missing = {}
    assert _content_is_bawu(False, missing) is None
    assert missing["is_bawu"] == "zero_presence_unverified_in_parsed_object"

    _summary, true_lease = await _lease_post(maker, 2101, ["is_bawu"])
    true_result = await complete_web_enrichment_task(
        maker, true_lease.task_key, true_lease.lease_token, [_candidate(true_lease, "is_bawu", True)]
    )
    assert true_result.applied_fields == ("is_bawu",)

    _false_summary, false_lease = await _lease_post(maker, 2102, ["is_bawu"])
    false_result = await complete_web_enrichment_task(
        maker, false_lease.task_key, false_lease.lease_token, [_candidate(false_lease, "is_bawu", False)]
    )
    assert false_result.applied_fields == ("is_bawu",)
    incoming_unknown = ResearchPost(
        pid=2101,
        tid=1001,
        fid=7654321,
        author_user_id=777,
        source_api="aiotieba.get_posts",
        observed_at=datetime(2026, 1, 5, tzinfo=UTC),
        source_payload_json={"is_bawu": False},
        missing_reasons_json={"is_bawu": "zero_presence_unverified_in_parsed_object"},
        is_bawu=None,
    )
    incoming_true = ResearchPost(
        pid=2102,
        tid=1001,
        fid=7654321,
        author_user_id=777,
        source_api="aiotieba.get_posts",
        observed_at=datetime(2026, 1, 5, tzinfo=UTC),
        source_payload_json={"is_bawu": True},
        missing_reasons_json={},
        is_bawu=True,
    )
    async with maker() as session, session.begin():
        await _upsert_content_rows(session, ResearchPost, [incoming_unknown, incoming_true])
    with Session(engine) as session:
        true_row = session.get(ResearchPost, 2101)
        false_row = session.get(ResearchPost, 2102)
        assert true_row.is_bawu is True
        assert false_row.is_bawu is False
        api_conflict = false_row.field_sources_json["is_bawu"]["api_conflicts"][-1]
        assert api_conflict["value"] is True


@pytest.mark.asyncio
async def test_comment_scope_parent_and_fid_identity_are_strict(reconcile_db):
    engine, maker = reconcile_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["author_nickname"],
        target_type="comment",
        target_ids=[3101],
        tid=1001,
    )
    lease = (await lease_web_enrichment_tasks(maker, "run-reconcile-001", "synthetic-worker"))[0]
    wrong_fid = _candidate(lease, "author_nickname", "comment-name", fid=7654322, source_kind="web_network")
    result = await complete_web_enrichment_task(maker, lease.task_key, lease.lease_token, [wrong_fid])
    assert result.status == "identity_unverified"
    with Session(engine) as session:
        assert session.get(ResearchComment, 3101).author_nickname is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []
        assert session.get(ResearchWebEnrichmentTask, summary.task_keys[0]).status == "identity_unverified"

    parent_summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["author_nickname"],
        target_type="comment",
        target_ids=[3102],
        tid=1001,
        parent_pid=2101,
    )
    assert parent_summary.created_count == 1
    parent_lease = (
        await lease_web_enrichment_tasks(
            maker, "run-reconcile-001", "synthetic-worker", task_keys=parent_summary.task_keys
        )
    )[0]
    applied = await complete_web_enrichment_task(
        maker,
        parent_lease.task_key,
        parent_lease.lease_token,
        [_candidate(parent_lease, "author_nickname", "comment-author", source_kind="web_network")],
    )
    assert applied.applied_fields == ("author_nickname",)
    with Session(engine) as session:
        assert session.get(ResearchComment, 3102).author_nickname == "comment-author"


@pytest.mark.asyncio
async def test_comment_row_parent_change_rejects_entire_candidate_batch(reconcile_db):
    engine, maker = reconcile_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["author_nickname"],
        target_type="comment",
        target_ids=[3101],
        tid=1001,
    )
    lease = (
        await lease_web_enrichment_tasks(
            maker, "run-reconcile-001", "synthetic-worker", task_keys=summary.task_keys
        )
    )[0]
    with Session(engine) as session, session.begin():
        session.get(ResearchComment, 3101).parent_post_pid = 2102
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "author_nickname", "wrong-parent", source_kind="web_network")],
    )
    assert result.reason_code == "target_database_parent_changed"
    with Session(engine) as session:
        assert session.get(ResearchComment, 3101).author_nickname is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []


@pytest.mark.parametrize("column_name", ["author_user_id", "fid"])
@pytest.mark.asyncio
async def test_target_author_or_forum_row_change_is_rejected(reconcile_db, column_name):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["author_nickname"])
    with Session(engine) as session, session.begin():
        row = session.get(ResearchPost, 2101)
        setattr(row, column_name, 999 if column_name == "author_user_id" else 7654322)
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "author_nickname", "must-not-apply")],
    )
    assert result.status == "identity_unverified"
    with Session(engine) as session:
        assert session.get(ResearchPost, 2101).author_nickname is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []


@pytest.mark.asyncio
async def test_expired_cancelled_scope_and_changed_watermark_responses_are_rejected(reconcile_db):
    engine, maker = reconcile_db
    current = datetime.now(UTC)
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["author_nickname"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
    )
    lease = (
        await lease_web_enrichment_tasks(
            maker, "run-reconcile-001", "synthetic-worker", now=current, lease_seconds=1
        )
    )[0]
    rejected = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "author_nickname", "expired")],
        now=current + timedelta(seconds=2),
    )
    assert rejected.reason_code == "lease_expired"
    renewed = (
        await lease_web_enrichment_tasks(
            maker, "run-reconcile-001", "replacement-worker", now=current + timedelta(seconds=2)
        )
    )[0]
    assert renewed.task_key == summary.task_keys[0]
    assert renewed.lease_token != lease.lease_token
    assert await cancel_web_enrichment_task(maker, renewed.task_key)
    cancelled = await complete_web_enrichment_task(
        maker,
        renewed.task_key,
        renewed.lease_token,
        [_candidate(renewed, "author_nickname", "cancelled")],
    )
    assert cancelled.reason_code == "lease_token_stale"
    with Session(engine) as session:
        assert session.get(ResearchPost, 2101).author_nickname is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []


@pytest.mark.asyncio
async def test_old_scope_generation_and_target_identity_cannot_write(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["author_nickname"])
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-reconcile-001", "thread_replies:1001"))
        scope.generation = 1
    stale_scope = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "author_nickname", "old-scope")],
    )
    assert stale_scope.reason_code == "scope_generation_changed"
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-reconcile-001", "thread_replies:1001"))
        scope.generation = 0
        scope.status = "completed"
    assert await resume_web_enrichment_tasks(
        maker, "run-reconcile-001", statuses=("failed_final",), task_keys=[lease.task_key]
    ) == 1
    renewed = (
        await lease_web_enrichment_tasks(
            maker, "run-reconcile-001", "watermark-worker", task_keys=[lease.task_key]
        )
    )[0]
    with Session(engine) as session, session.begin():
        row = session.get(ResearchPost, 2101)
        row.author_user_id = 999
        row.observed_at = datetime(2026, 1, 9, tzinfo=UTC)
    stale_identity = await complete_web_enrichment_task(
        maker,
        renewed.task_key,
        renewed.lease_token,
        [_candidate(renewed, "author_nickname", "old-watermark")],
    )
    assert stale_identity.reason_code == "target_database_identity_changed"
    with Session(engine) as session:
        assert session.get(ResearchPost, 2101).author_nickname is None


@pytest.mark.asyncio
async def test_old_lease_token_cannot_finish_new_attempt_or_current_task(reconcile_db):
    engine, maker = reconcile_db
    started = datetime.now(UTC)
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["author_nickname"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
    )
    lease_a = (
        await lease_web_enrichment_tasks(
            maker,
            "run-reconcile-001",
            "old-worker",
            task_keys=summary.task_keys,
            now=started,
            lease_seconds=1,
        )
    )[0]
    lease_b = (
        await lease_web_enrichment_tasks(
            maker,
            "run-reconcile-001",
            "new-worker",
            task_keys=summary.task_keys,
            now=started + timedelta(seconds=2),
        )
    )[0]
    assert lease_b.lease_generation == lease_a.lease_generation + 1

    stale = await complete_web_enrichment_task(
        maker,
        lease_a.task_key,
        lease_a.lease_token,
        [_candidate(lease_a, "author_nickname", "old-response")],
        now=started + timedelta(seconds=3),
    )
    assert stale.reason_code == "lease_token_stale"

    await _finish_attempt_after_error(
        maker,
        lease_a.task_key,
        lease_a.lease_generation,
        lease_a.lease_token,
        "synthetic_reconcile_error",
        started + timedelta(seconds=4),
    )
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, lease_a.task_key)
        old_attempt = session.get(
            ResearchWebEnrichmentAttempt,
            (lease_a.task_key, lease_a.lease_generation),
        )
        current_attempt = session.get(
            ResearchWebEnrichmentAttempt,
            (lease_b.task_key, lease_b.lease_generation),
        )
        failure = session.scalar(select(ResearchWebEnrichmentFailure))
        assert task.status == "running"
        assert task.lease_token == lease_b.lease_token
        assert task.lease_generation == lease_b.lease_generation
        assert task.field_statuses_json == {"author_nickname": {"status": "pending", "reason": None}}
        assert old_attempt.status == "running"
        assert old_attempt.finished_at is None
        assert current_attempt.status == "running"
        assert current_attempt.finished_at is None
        assert failure.reason_code == "lease_token_stale"
        assert session.get(ResearchPost, 2101).author_nickname is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []

    accepted = await complete_web_enrichment_task(
        maker,
        lease_b.task_key,
        lease_b.lease_token,
        [_candidate(lease_b, "author_nickname", "new-response")],
        now=started + timedelta(seconds=5),
    )
    assert accepted.status == "completed"
    with Session(engine) as session:
        assert session.get(ResearchPost, 2101).author_nickname == "new-response"
        assert session.get(
            ResearchWebEnrichmentAttempt,
            (lease_b.task_key, lease_b.lease_generation),
        ).status == "completed"


@pytest.mark.asyncio
async def test_retried_response_is_observation_idempotent(reconcile_db):
    engine, maker = reconcile_db
    summary, first_lease = await _lease_post(maker, 2101, ["author_nickname"])
    retry_at = datetime.now(UTC)
    assert await fail_web_enrichment_task(
        maker,
        first_lease.task_key,
        first_lease.lease_token,
        status="failed_retryable",
        reason_code="synthetic_retry",
        retry_after=0,
        now=retry_at,
    ) == "failed_retryable"
    second_lease = (
        await lease_web_enrichment_tasks(
            maker,
            "run-reconcile-001",
            "retry-worker",
            task_keys=summary.task_keys,
            now=retry_at + timedelta(seconds=1),
        )
    )[0]
    assert second_lease.task_key == first_lease.task_key
    result = await complete_web_enrichment_task(
        maker,
        second_lease.task_key,
        second_lease.lease_token,
        [_candidate(second_lease, "author_nickname", "retried-title")],
    )
    assert result.applied_fields == ("author_nickname",)
    with Session(engine) as session:
        observations = session.scalars(
            select(ResearchFieldObservation).where(ResearchFieldObservation.task_key == first_lease.task_key)
        ).all()
        assert len(observations) == 1


def test_service_failure_mapping_preserves_terminal_not_applicable_and_unsupported_statuses():
    assert service._task_failure_status("not_applicable", "thread_deleted_or_unavailable") == "not_applicable"
    assert service._task_failure_status("unsupported_by_source", "field_unsupported") == "unsupported_by_source"
    assert service._failure_outcomes(
        ("ip_location_text",), "not_applicable", "thread_deleted_or_unavailable"
    ) == {
        "ip_location_text": {
            "status": "not_applicable",
            "reason": "thread_deleted_or_unavailable",
        }
    }
    assert service._failure_outcomes(
        ("ip_location_text",), "unsupported_by_source", "field_unsupported"
    ) == {
        "ip_location_text": {
            "status": "unsupported_by_source",
            "reason": "field_unsupported",
        }
    }


@pytest.mark.asyncio
async def test_preflight_resolves_pre_launch_ip_task_and_strips_mixed_ip_field(reconcile_db):
    from tiebameow.utils.time_utils import SHANGHAI_TZ
    from src.research.web_fallback.tasks import (
        IP_BEFORE_LAUNCH_REASON,
        TIEBA_IP_LOCATION_LAUNCH_TIME,
    )

    engine, maker = reconcile_db
    observed = datetime(2026, 1, 1, tzinfo=UTC)
    pre_launch = datetime(2021, 5, 1, 12, 0, 0, tzinfo=SHANGHAI_TZ)
    with Session(engine) as session, session.begin():
        session.add_all(
            [
                ResearchPost(
                    pid=2901,
                    tid=1001,
                    fid=7654321,
                    floor=2,
                    text="old post ip only",
                    create_time=pre_launch,
                    scraped_at=observed,
                    author_user_id=501,
                    author_portrait="p2901",
                    global_growth_level=2,
                    global_growth_icon_url="https://example.com/g.png",
                    source_api="aiotieba.get_posts",
                    observed_at=observed,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchPost(
                    pid=2902,
                    tid=1001,
                    fid=7654321,
                    floor=3,
                    text="old post mixed",
                    create_time=pre_launch,
                    scraped_at=observed,
                    author_user_id=502,
                    author_portrait="p2902",
                    source_api="aiotieba.get_posts",
                    observed_at=observed,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
            ]
        )

    ip_only_summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2901],
        only_missing_fields=True,
    )
    mixed_summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["global_growth_level", "global_growth_icon_url", "ip_location_text"],
        target_type="post",
        target_ids=[2902],
        only_missing_fields=True,
        pair_growth_fields=True,
    )
    leases = await lease_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        "preflight-cutoff-worker",
        limit=10,
        task_keys=[ip_only_summary.task_keys[0], mixed_summary.task_keys[0]],
    )
    leases_by_id = {lease.target_id: lease for lease in leases}

    ip_preflight = await preflight_web_enrichment_task(
        maker,
        leases_by_id[2901].task_key,
        leases_by_id[2901].lease_token,
        min_ip_create_time=TIEBA_IP_LOCATION_LAUNCH_TIME,
    )
    assert ip_preflight.status == "not_applicable"
    assert ip_preflight.preflight_skipped is True
    with Session(engine) as session:
        task_row = session.get(ResearchWebEnrichmentTask, leases_by_id[2901].task_key)
        assert task_row is not None and task_row.status == "not_applicable"
        assert task_row.field_statuses_json == {
            "ip_location_text": {"status": "not_applicable", "reason": IP_BEFORE_LAUNCH_REASON}
        }

    mixed_preflight = await preflight_web_enrichment_task(
        maker,
        leases_by_id[2902].task_key,
        leases_by_id[2902].lease_token,
        min_ip_create_time=TIEBA_IP_LOCATION_LAUNCH_TIME,
    )
    assert mixed_preflight.status == "ready"
    assert mixed_preflight.missing_fields == ("global_growth_icon_url", "global_growth_level")
    assert mixed_preflight.preserved_outcomes == {
        "ip_location_text": {"status": "not_applicable", "reason": IP_BEFORE_LAUNCH_REASON}
    }
