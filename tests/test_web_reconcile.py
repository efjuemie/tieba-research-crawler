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
    ResearchFieldObservation,
    ResearchPost,
    ResearchThread,
    ResearchWebEnrichmentAttempt,
    ResearchWebEnrichmentFailure,
    ResearchWebEnrichmentTask,
)
from src.research.persistence import _content_is_bawu, _upsert_content_rows
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
            (2105, None, {}, None, {}),
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
                    forum_level_name="known-title" if pid == 2105 else None,
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
        raw_value=value,
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


@pytest.mark.asyncio
async def test_verified_field_applies_by_exact_post_and_source_is_sanitized(reconcile_db):
    engine, maker = reconcile_db
    summary, lease = await _lease_post(maker, 2101, ["forum_level_name"])
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "forum_level_name", "网页头衔")],
    )
    assert result.status == "completed"
    assert result.applied_fields == ("forum_level_name",)
    with Session(engine) as session:
        row = session.get(ResearchPost, 2101)
        observation = session.scalar(
            select(ResearchFieldObservation).where(ResearchFieldObservation.task_key == summary.task_keys[0])
        )
        task = session.get(ResearchWebEnrichmentTask, summary.task_keys[0])
        assert row.forum_level_name == "网页头衔"
        assert row.field_sources_json["forum_level_name"]["verified_web"]["source_kind"] == "web_dom"
        assert observation.source_url == "https://tieba.baidu.com/p/1001?fid=7654321"
        assert "synthetic-secret" not in observation.source_url
        assert observation.temporal_scope == "web_observation_at_observed_time"
        assert task.status == "completed"
        assert "elapsed_seconds" in task.last_diagnostics_json


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
    _summary, lease = await _lease_post(maker, 2101, ["forum_level_name", "global_growth_level"])
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [
            _candidate(lease, "forum_level_name", "must-roll-back"),
            _candidate(lease, "global_growth_level", 8, author_user_id=999),
        ],
    )
    assert result.status == "identity_unverified"
    with Session(engine) as session:
        row = session.get(ResearchPost, 2101)
        assert row.forum_level_name is None
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
        assert task.field_statuses_json["global_growth_level"] == {
            "status": "not_tested",
            "reason": "numeric_level_not_exposed_in_page",
        }
        assert session.scalars(select(ResearchFieldObservation)).all() == []


@pytest.mark.asyncio
async def test_identity_failure_field_outcome_rejects_good_candidates_before_any_write(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["forum_level_name", "global_growth_level"])
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "forum_level_name", "must-not-apply")],
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
        assert row.forum_level_name is None
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

    _confirmed_summary, confirmed_lease = await _lease_post(maker, 2104, ["disagree_count"])
    confirmed = await complete_web_enrichment_task(
        maker,
        confirmed_lease.task_key,
        confirmed_lease.lease_token,
        [_candidate(confirmed_lease, "disagree_count", 0)],
    )
    assert confirmed.confirmed_fields == ("disagree_count",)
    with Session(engine) as session:
        assert session.get(ResearchPost, 2104).agree_count == 0
        row = session.get(ResearchPost, 2102)
        assert row.agree_count == 7
        assert "agree_count" not in row.missing_reasons_json
        assert session.get(ResearchPost, 2103).agree_count == 4


@pytest.mark.asyncio
async def test_api_refresh_preserves_web_unknown_and_records_explicit_conflict(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["forum_level_name"])
    await complete_web_enrichment_task(
        maker, lease.task_key, lease.lease_token, [_candidate(lease, "forum_level_name", "web-title")]
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
            "forum_level_name": "not_exposed_by_get_posts",
            "author_user_name": "not_returned_by_source",
        },
        forum_level_name=None,
        author_user_name=None,
    )
    async with maker() as session, session.begin():
        await _upsert_content_rows(session, ResearchPost, [incoming])
    with Session(engine) as session:
        row = session.get(ResearchPost, 2101)
        assert row.forum_level_name == "web-title"
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
        forum_level_name="api-title",
    )
    async with maker() as session, session.begin():
        await _upsert_content_rows(session, ResearchPost, [conflicting])
    with Session(engine) as session:
        row = session.get(ResearchPost, 2101)
        conflict = row.field_sources_json["forum_level_name"]["api_conflicts"][-1]
        assert row.forum_level_name == "web-title"
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
    _summary, lease = await _lease_post(maker, 2101, ["forum_level_name"])
    with Session(engine) as session, session.begin():
        row = session.get(ResearchPost, 2101)
        setattr(row, column_name, 999 if column_name == "author_user_id" else 7654322)
    result = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "forum_level_name", "must-not-apply")],
    )
    assert result.status == "identity_unverified"
    with Session(engine) as session:
        assert session.get(ResearchPost, 2101).forum_level_name is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []


@pytest.mark.asyncio
async def test_expired_cancelled_scope_and_changed_watermark_responses_are_rejected(reconcile_db):
    engine, maker = reconcile_db
    current = datetime.now(UTC)
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["forum_level_name"],
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
        [_candidate(lease, "forum_level_name", "expired")],
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
        [_candidate(renewed, "forum_level_name", "cancelled")],
    )
    assert cancelled.reason_code == "lease_token_stale"
    with Session(engine) as session:
        assert session.get(ResearchPost, 2101).forum_level_name is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []


@pytest.mark.asyncio
async def test_old_scope_generation_and_api_watermark_cannot_write(reconcile_db):
    engine, maker = reconcile_db
    _summary, lease = await _lease_post(maker, 2101, ["forum_level_name"])
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-reconcile-001", "thread_replies:1001"))
        scope.generation = 1
    stale_scope = await complete_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        [_candidate(lease, "forum_level_name", "old-scope")],
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
        row.observed_at = datetime(2026, 1, 9, tzinfo=UTC)
    stale_watermark = await complete_web_enrichment_task(
        maker,
        renewed.task_key,
        renewed.lease_token,
        [_candidate(renewed, "forum_level_name", "old-watermark")],
    )
    assert stale_watermark.reason_code == "target_api_watermark_changed"
    with Session(engine) as session:
        assert session.get(ResearchPost, 2101).forum_level_name is None


@pytest.mark.asyncio
async def test_old_lease_token_cannot_finish_new_attempt_or_current_task(reconcile_db):
    engine, maker = reconcile_db
    started = datetime.now(UTC)
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-reconcile-001",
        ["forum_level_name"],
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
        [_candidate(lease_a, "forum_level_name", "old-response")],
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
        assert task.field_statuses_json == {"forum_level_name": {"status": "pending", "reason": None}}
        assert old_attempt.status == "running"
        assert old_attempt.finished_at is None
        assert current_attempt.status == "running"
        assert current_attempt.finished_at is None
        assert failure.reason_code == "lease_token_stale"
        assert session.get(ResearchPost, 2101).forum_level_name is None
        assert session.scalars(select(ResearchFieldObservation)).all() == []

    accepted = await complete_web_enrichment_task(
        maker,
        lease_b.task_key,
        lease_b.lease_token,
        [_candidate(lease_b, "forum_level_name", "new-response")],
        now=started + timedelta(seconds=5),
    )
    assert accepted.status == "completed"
    with Session(engine) as session:
        assert session.get(ResearchPost, 2101).forum_level_name == "new-response"
        assert session.get(
            ResearchWebEnrichmentAttempt,
            (lease_b.task_key, lease_b.lease_generation),
        ).status == "completed"


@pytest.mark.asyncio
async def test_retried_response_is_observation_idempotent(reconcile_db):
    engine, maker = reconcile_db
    summary, first_lease = await _lease_post(maker, 2101, ["forum_level_name"])
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
        [_candidate(second_lease, "forum_level_name", "retried-title")],
    )
    assert result.applied_fields == ("forum_level_name",)
    with Session(engine) as session:
        observations = session.scalars(
            select(ResearchFieldObservation).where(ResearchFieldObservation.task_key == first_lease.task_key)
        ).all()
        assert len(observations) == 1
