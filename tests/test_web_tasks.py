from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from conftest import make_thread
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session
from tiebameow.models.orm import Base
from tiebameow.models.orm import Forum as BaseForum
from tiebameow.models.orm import Thread as BaseThread
from tiebameow.utils.time_utils import SHANGHAI_TZ

from src.core.models import ResearchRun, ResearchTaskScope
from src.research.models import (
    RESEARCH_TABLES,
    ResearchComment,
    ResearchContentObservation,
    ResearchPost,
    ResearchThread,
    ResearchWebEnrichmentAttempt,
    ResearchWebEnrichmentFailure,
    ResearchWebEnrichmentTask,
)
from src.research.web_fallback import service as web_service_mod
from src.research.web_fallback import tasks as web_tasks_mod
from src.research.web_fallback.reconcile import fail_web_enrichment_task
from src.research.web_fallback.tasks import (
    _field_outcome_resolved,
    _normalize_requested_fields,
    _resolved_web_task_status,
    cancel_web_enrichment_task,
    enqueue_web_enrichment_tasks,
    lease_web_enrichment_tasks,
    resume_web_enrichment_tasks,
)


def test_web_task_terminal_status_requires_verified_resolved_field_evidence():
    absent = {"ip_location_text": {"status": "absent_on_page", "reason": "ip_location_not_present"}}
    assert (
        _resolved_web_task_status(
            ("ip_location_text",), absent, identity_verified=True
        )
        == "completed_with_absent_fields"
    )
    assert _resolved_web_task_status(
        ("ip_location_text",), absent, identity_verified=False
    ) is None
    assert not _field_outcome_resolved(
        "ip_location_text", {"status": "needs_evidence", "reason": "ip_location_not_present"}
    )
    assert _resolved_web_task_status(
        ("ip_location_text",),
        {"ip_location_text": {"status": "not_tested", "reason": "not_observed_in_bounded_page"}},
        identity_verified=True,
    ) is None
    assert _resolved_web_task_status(
        ("global_growth_icon_url", "global_growth_level"),
        {
            "global_growth_icon_url": {"status": "absent_on_page", "reason": "no_icon"},
            "global_growth_level": {"status": "needs_evidence", "reason": "numeric_level_not_exposed_in_page"},
        },
        identity_verified=True,
    ) == "completed_with_absent_fields"
    assert _resolved_web_task_status(
        ("global_growth_icon_url", "global_growth_level"),
        {
            "global_growth_icon_url": {"status": "absent_on_page", "reason": "no_icon"},
            "global_growth_level": {"status": "needs_evidence", "reason": "ambiguous_level"},
        },
        identity_verified=True,
    ) is None
    assert _resolved_web_task_status(
        ("ip_location_text", "global_growth_icon_url", "global_growth_level"),
        {
            "ip_location_text": {"status": "absent_on_page", "reason": "not_present"},
            "global_growth_icon_url": {"status": "applied"},
        },
        identity_verified=True,
    ) is None


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


def test_new_task_fields_follow_content_specific_scope():
    assert _normalize_requested_fields(
        ["agree_count", "reply_count", "collect_count", "view_count"], "thread"
    ) == ("agree_count", "collect_count", "reply_count", "view_count")
    assert _normalize_requested_fields(["agree_count"], "post") == ("agree_count",)
    assert _normalize_requested_fields(
        ["agree_count", "global_growth_icon_url", "reply_to_display_name", "reply_to_user_id"],
        "comment",
    ) == (
        "agree_count",
        "global_growth_icon_url",
        "reply_to_display_name",
        "reply_to_user_id",
    )
    for target_type, field_name in (
        ("thread", "disagree_count"),
        ("thread", "share_count"),
        ("post", "reply_count"),
        ("post", "collect_count"),
        ("comment", "view_count"),
        ("comment", "role_name"),
        ("post", "forum_level_name"),
    ):
        with pytest.raises(ValueError, match="Unsupported web fields|Fields unsupported"):
            _normalize_requested_fields([field_name], target_type)


@pytest.fixture
def research_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[ResearchRun.__table__, ResearchTaskScope.__table__, *RESEARCH_TABLES],
    )
    with Session(engine) as session, session.begin():
        session.add(
            ResearchRun(
                run_id="run-web-001",
                mode="backfill",
                scope_key="synthetic",
                forums_json=[7654321],
                status="completed",
            )
        )
        for tid, first_pid in ((1001, 2001), (1002, 2002)):
            session.add(
                ResearchThread(
                    tid=tid,
                    first_pid=first_pid,
                    fid=7654321,
                    title="same synthetic title",
                    author_user_id=501,
                    author_portrait="synthetic-portrait",
                    source_api="aiotieba.get_posts",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                )
            )
            session.add(
                ResearchTaskScope(
                    run_id="run-web-001",
                    logical_task_key=f"thread_replies:{tid}",
                    generation=0,
                    task_type="posts",
                    scope_type="thread_replies",
                    fid=7654321,
                    tid=tid,
                    status="completed",
                )
            )
        session.add_all(
            [
                ResearchPost(
                    pid=2101,
                    tid=1001,
                    fid=7654321,
                    text="same synthetic body",
                    author_user_id=777,
                    author_portrait="synthetic-post-portrait",
                    source_api="aiotieba.get_posts",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchPost(
                    pid=2102,
                    tid=1001,
                    fid=7654321,
                    text="same synthetic body",
                    author_user_id=777,
                    author_portrait="synthetic-post-portrait",
                    source_api="aiotieba.get_posts",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchComment(
                    cid=3101,
                    tid=1001,
                    parent_post_pid=2101,
                    fid=7654321,
                    text="same synthetic comment",
                    author_user_id=888,
                    source_api="aiotieba.get_comments",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchComment(
                    cid=3102,
                    tid=1001,
                    parent_post_pid=2101,
                    fid=7654321,
                    text="same synthetic comment",
                    author_user_id=888,
                    source_api="aiotieba.get_comments",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
            ]
        )
        session.add(
            ResearchTaskScope(
                run_id="run-web-001",
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


@pytest.mark.asyncio
async def test_enqueue_deduplicates_exact_identity_field_sets_and_uses_primary_ids(research_db):
    engine, maker = research_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["author_nickname", "global_growth_level"],
        target_type="thread",
        tid=1001,
    )
    reordered = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level", "author_nickname"],
        target_type="thread",
        tid=1001,
    )
    assert summary.created_count == 1
    assert reordered.created_count == 0
    assert reordered.deduplicated_count == 1
    assert summary.task_keys == reordered.task_keys

    post_summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["author_nickname"],
        target_type="post",
        target_ids=[2102],
        tid=1001,
        fid=7654321,
    )
    comment_summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["agree_count"],
        target_type="comment",
        target_ids=[3102],
        tid=1001,
        fid=7654321,
    )
    assert post_summary.created_count == comment_summary.created_count == 1
    with Session(engine) as session:
        tasks = session.scalars(select(ResearchWebEnrichmentTask)).all()
    by_type = {task.target_type: task for task in tasks}
    assert by_type["post"].target_id == 2102
    assert by_type["comment"].target_id == 3102
    assert by_type["comment"].parent_post_pid == 2101
    assert by_type["comment"].author_user_id == 888
    assert by_type["thread"].tid == 1001


@pytest.mark.asyncio
async def test_same_names_text_and_parent_still_produce_distinct_target_ids(research_db):
    _engine, maker = research_db
    threads = await enqueue_web_enrichment_tasks(
        maker, "run-web-001", ["global_growth_level"], target_type="thread"
    )
    posts = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["author_nickname"],
        target_type="post",
        target_ids=[2101, 2102],
        tid=1001,
    )
    comments = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["agree_count"],
        target_type="comment",
        target_ids=[3101, 3102],
        tid=1001,
        parent_pid=2101,
    )
    assert threads.created_count == 2
    assert posts.created_count == 2
    assert comments.created_count == 2
    assert len(set(threads.task_keys)) == len(set(posts.task_keys)) == len(set(comments.task_keys)) == 2
    with pytest.raises(ValueError, match="parent_pid can only be used"):
        await enqueue_web_enrichment_tasks(
            maker,
            "run-web-001",
            ["global_growth_level"],
            target_type="post",
            parent_pid=2101,
        )


@pytest.mark.asyncio
async def test_scope_forum_identity_must_match_exact_target(research_db):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        thread_scope = session.get(ResearchTaskScope, ("run-web-001", "thread_replies:1001"))
        thread_scope.fid = 7654322
    thread_summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="thread",
        tid=1001,
    )
    assert thread_summary.planned_count == 0
    assert thread_summary.skipped["thread_scope_fid_mismatch"] == 1

    with Session(engine) as session, session.begin():
        thread_scope = session.get(ResearchTaskScope, ("run-web-001", "thread_replies:1001"))
        thread_scope.fid = 7654321
        comment_scope = session.get(ResearchTaskScope, ("run-web-001", "post_comments:1001:2101"))
        comment_scope.fid = 7654322
    comment_summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["agree_count"],
        target_type="comment",
        target_ids=[3101],
    )
    assert comment_summary.planned_count == 0
    assert comment_summary.skipped["comment_scope_fid_mismatch"] == 1


@pytest.mark.asyncio
async def test_unfiltered_plan_preserves_tid_and_parent_filters_across_target_types(research_db):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        session.add(
            ResearchPost(
                pid=2201,
                tid=1002,
                fid=7654321,
                text="synthetic post on second tid",
                author_user_id=777,
                source_api="aiotieba.get_posts",
                observed_at=datetime(2026, 1, 2, tzinfo=UTC),
                source_payload_json={},
                missing_reasons_json={},
            )
        )
        session.add_all(
            [
                ResearchComment(
                    cid=3201,
                    tid=1001,
                    parent_post_pid=2102,
                    fid=7654321,
                    text="synthetic second parent comment",
                    author_user_id=888,
                    source_api="aiotieba.get_comments",
                    observed_at=datetime(2026, 1, 2, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchComment(
                    cid=3202,
                    tid=1002,
                    parent_post_pid=2201,
                    fid=7654321,
                    text="synthetic second tid comment",
                    author_user_id=888,
                    source_api="aiotieba.get_comments",
                    observed_at=datetime(2026, 1, 2, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
            ]
        )
        session.add_all(
            [
                ResearchTaskScope(
                    run_id="run-web-001",
                    logical_task_key="post_comments:1001:2102",
                    generation=0,
                    task_type="comments",
                    scope_type="post_comments",
                    fid=7654321,
                    tid=1001,
                    parent_pid=2102,
                    parent_generation=0,
                    status="completed",
                ),
                ResearchTaskScope(
                    run_id="run-web-001",
                    logical_task_key="post_comments:1002:2201",
                    generation=0,
                    task_type="comments",
                    scope_type="post_comments",
                    fid=7654321,
                    tid=1002,
                    parent_pid=2201,
                    parent_generation=0,
                    status="completed",
                ),
            ]
        )

    fields = ["author_nickname", "global_growth_level"]
    all_targets = await enqueue_web_enrichment_tasks(maker, "run-web-001", fields)
    assert all_targets.planned_count == 9
    assert {target.target_type for target in all_targets.planned_targets} == {"thread", "post", "comment"}
    assert {target.tid for target in all_targets.planned_targets} == {1001, 1002}

    tid_targets = await enqueue_web_enrichment_tasks(maker, "run-web-001", fields, tid=1001)
    assert tid_targets.planned_count == 6
    assert {target.tid for target in tid_targets.planned_targets} == {1001}

    parent_targets = await enqueue_web_enrichment_tasks(maker, "run-web-001", fields, parent_pid=2101)
    comment_targets = [target for target in parent_targets.planned_targets if target.target_type == "comment"]
    assert parent_targets.planned_count == 7
    assert {target.target_id for target in comment_targets} == {3101, 3102}
    assert {target.parent_post_pid for target in comment_targets} == {2101}


@pytest.mark.asyncio
async def test_untried_target_precedes_older_due_high_priority_retry(research_db):
    engine, maker = research_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101, 2102],
        tid=1001,
    )
    now = datetime.now(UTC)
    with Session(engine) as session, session.begin():
        rows = session.scalars(select(ResearchWebEnrichmentTask)).all()
        retry = next(row for row in rows if row.target_id == 2101)
        fresh = next(row for row in rows if row.target_id == 2102)
        retry.status = "failed_retryable"
        retry.attempts = 1
        retry.priority = fresh.priority + 100
        retry.created_at = now - timedelta(days=1)
        retry.next_retry_at = now - timedelta(seconds=1)
        fresh.created_at = now

    leases = await lease_web_enrichment_tasks(
        maker, "run-web-001", "retry-order-worker", now=now, limit=2
    )
    assert [lease.target_id for lease in leases] == [2102, 2101]
    assert {lease.task_key for lease in leases} == set(summary.task_keys)


@pytest.mark.asyncio
async def test_comment_leasing_keeps_same_parent_targets_adjacent(research_db):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        session.add(
            ResearchComment(
                cid=3099,
                tid=1001,
                parent_post_pid=2102,
                fid=7654321,
                text="synthetic earlier-id comment",
                author_user_id=888,
                source_api="aiotieba.get_comments",
                observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                source_payload_json={},
                missing_reasons_json={},
            )
        )
        session.add(
            ResearchTaskScope(
                run_id="run-web-001",
                logical_task_key="post_comments:1001:2102",
                generation=0,
                task_type="comments",
                scope_type="post_comments",
                fid=7654321,
                tid=1001,
                parent_pid=2102,
                parent_generation=0,
                status="completed",
            )
        )

    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="comment",
        target_ids=[3101, 3102, 3099],
        tid=1001,
    )
    leases = await lease_web_enrichment_tasks(
        maker, "run-web-001", "parent-affinity-worker", limit=3
    )

    assert {lease.task_key for lease in leases} == set(summary.task_keys)
    assert [(lease.parent_post_pid, lease.target_id) for lease in leases] == [
        (2101, 3101),
        (2101, 3102),
        (2102, 3099),
    ]


@pytest.mark.asyncio
async def test_lease_task_keys_filter_exhausted_cleanup_and_empty_keys_are_noop(research_db):
    engine, maker = research_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="post",
        target_ids=[2101, 2102],
    )
    with Session(engine) as session, session.begin():
        for task_key in summary.task_keys:
            task = session.get(ResearchWebEnrichmentTask, task_key)
            task.status = "failed_retryable"
            task.attempts = task.max_attempts
    now = datetime.now(UTC)

    assert await lease_web_enrichment_tasks(
        maker, "run-web-001", "filter-worker", task_keys=[], now=now
    ) == []
    with Session(engine) as session:
        assert {
            task.task_key: task.status
            for task in session.scalars(
                select(ResearchWebEnrichmentTask).where(
                    ResearchWebEnrichmentTask.task_key.in_(summary.task_keys)
                )
            )
        } == dict.fromkeys(summary.task_keys, "failed_retryable")

    first_key, second_key = summary.task_keys
    assert await lease_web_enrichment_tasks(
        maker, "run-web-001", "filter-worker", task_keys=[first_key], now=now
    ) == []
    with Session(engine) as session:
        statuses = {
            task.task_key: task.status
            for task in session.scalars(
                select(ResearchWebEnrichmentTask).where(
                    ResearchWebEnrichmentTask.task_key.in_(summary.task_keys)
                )
            )
        }
    assert statuses == {first_key: "failed_final", second_key: "failed_retryable"}


@pytest.mark.asyncio
async def test_unfinished_detail_is_not_leased_then_same_task_becomes_ready(research_db):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-web-001", "thread_replies:1001"))
        scope.status = "pending"

    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="thread",
        tid=1001,
    )
    assert summary.created_count == 1
    assert await lease_web_enrichment_tasks(maker, "run-web-001", "worker-a") == []
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, summary.task_keys[0])
        assert task.status == "pending_api_detail"

    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-web-001", "thread_replies:1001"))
        scope.status = "completed"
    ready = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="thread",
        tid=1001,
    )
    assert ready.deduplicated_count == 1
    lease = (await lease_web_enrichment_tasks(maker, "run-web-001", "worker-a"))[0]
    assert lease.task_key == summary.task_keys[0]
    assert lease.requested_fields == ("global_growth_level",)


@pytest.mark.asyncio
async def test_partial_scope_can_use_exact_complete_content_marker(research_db):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-web-001", "thread_replies:1001"))
        scope.status = "pending"
        session.add(
            ResearchContentObservation(
                owner_type="post",
                owner_id=2101,
                tid=1001,
                source_api="aiotieba.get_posts",
                observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )

    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )
    assert summary.planned_targets[0].eligibility_status == "pending_web"
    assert len(await lease_web_enrichment_tasks(maker, "run-web-001", "worker-a")) == 1


@pytest.mark.asyncio
async def test_incomplete_thread_scope_can_use_verified_list_metadata_row(research_db):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-web-001", "thread_replies:1001"))
        scope.status = "pending"
        thread = session.get(ResearchThread, 1001)
        thread.source_api = "aiotieba.get_threads"

    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="thread",
        target_ids=[1001],
        tid=1001,
        only_missing_fields=True,
        pair_growth_fields=True,
    )
    assert summary.planned_targets[0].eligibility_status == "pending_web"
    assert summary.planned_targets[0].requested_fields == (
        "global_growth_icon_url",
        "global_growth_level",
    )


@pytest.mark.asyncio
async def test_incomplete_thread_list_row_does_not_enable_counter_collection(research_db):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        scope = session.get(ResearchTaskScope, ("run-web-001", "thread_replies:1001"))
        scope.status = "pending"
        thread = session.get(ResearchThread, 1001)
        thread.source_api = "aiotieba.get_threads"
        thread.agree_count = None

    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["agree_count"],
        target_type="thread",
        target_ids=[1001],
        tid=1001,
        only_missing_fields=True,
    )

    assert summary.planned_targets[0].eligibility_status == "pending_api_detail"
    assert await lease_web_enrichment_tasks(
        maker, "run-web-001", "worker-a", task_keys=summary.task_keys
    ) == []


@pytest.mark.asyncio
async def test_missing_field_filter_precedes_limit_and_growth_fallback_is_paired(research_db):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        known = session.get(ResearchPost, 2101)
        known.agree_count = 4

    limited = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["agree_count"],
        target_type="post",
        tid=1001,
        limit=1,
        only_missing_fields=True,
    )
    assert [plan.target_id for plan in limited.planned_targets] == [2102]

    growth = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
        pair_growth_fields=True,
    )
    assert growth.planned_targets[0].requested_fields == (
        "global_growth_icon_url",
        "global_growth_level",
    )


@pytest.mark.asyncio
async def test_growth_url_alone_satisfies_growth_fallback_and_partial_task_retries_are_bounded(research_db):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        post = session.get(ResearchPost, 2101)
        post.global_growth_icon_url = "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp"

    satisfied = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
        pair_growth_fields=True,
    )
    assert satisfied.planned_count == 0

    retriable = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2102],
        tid=1001,
        only_missing_fields=True,
        max_attempts=2,
    )
    lease = (await lease_web_enrichment_tasks(
        maker, "run-web-001", "worker-a", task_keys=retriable.task_keys
    ))[0]
    with Session(engine) as session, session.begin():
        task = session.get(ResearchWebEnrichmentTask, lease.task_key)
        task.status = "partially_completed"
        task.field_statuses_json = {"ip_location_text": {"status": "not_tested", "reason": "empty"}}
        task.completed_at = datetime.now(UTC)

    retry = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2102],
        tid=1001,
        only_missing_fields=True,
        max_attempts=2,
    )
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, retry.task_keys[0])
        assert task.status == "failed_retryable"
        assert task.attempts == 1
        retry_at = task.next_retry_at.replace(tzinfo=UTC)
    assert await lease_web_enrichment_tasks(
        maker,
        "run-web-001",
        "too-early-worker",
        task_keys=retry.task_keys,
        now=retry_at - timedelta(milliseconds=1),
    ) == []
    retry_lease = (await lease_web_enrichment_tasks(
        maker,
        "run-web-001",
        "worker-b",
        task_keys=retry.task_keys,
        now=retry_at + timedelta(milliseconds=1),
    ))[0]
    assert retry_lease.lease_generation == lease.lease_generation + 1
    with Session(engine) as session, session.begin():
        task = session.get(ResearchWebEnrichmentTask, retry_lease.task_key)
        task.status = "partially_completed"
        task.attempts = task.max_attempts
    exhausted = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2102],
        tid=1001,
        only_missing_fields=True,
        max_attempts=2,
    )
    assert exhausted.planned_targets[0].eligibility_status == "failed_final"


@pytest.mark.asyncio
async def test_growth_and_location_missing_predicates_handle_zero_and_blank_values(research_db):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        post = session.get(ResearchPost, 2101)
        post.global_growth_level = 0
        post.global_growth_icon_url = "  "
        post.ip_location_text = " \t"
        post.agree_count = 0
        post.missing_reasons_json = {}

    growth = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
        pair_growth_fields=True,
    )
    location = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )
    known_zero_count = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["agree_count"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )

    assert growth.planned_targets[0].requested_fields == (
        "global_growth_icon_url",
        "global_growth_level",
    )
    assert location.planned_targets[0].requested_fields == ("ip_location_text",)
    assert known_zero_count.planned_count == 0


@pytest.mark.asyncio
async def test_either_growth_value_satisfies_the_growth_pair_without_missing_reason(research_db):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        post = session.get(ResearchPost, 2101)
        post.global_growth_level = 0
        post.global_growth_icon_url = "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp"
        post.missing_reasons_json = {}

    icon_known = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
        pair_growth_fields=True,
    )
    assert icon_known.planned_count == 0

    with Session(engine) as session, session.begin():
        post = session.get(ResearchPost, 2101)
        post.global_growth_level = 5
        post.global_growth_icon_url = " "

    numeric_known = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_icon_url"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
        pair_growth_fields=True,
    )
    assert numeric_known.planned_count == 0


@pytest.mark.asyncio
async def test_leasing_keeps_same_thread_post_targets_adjacent_for_page_reuse(research_db):
    engine, maker = research_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2102, 2101],
        tid=1001,
    )
    with Session(engine) as session, session.begin():
        first = session.scalar(
            select(ResearchWebEnrichmentTask).where(
                ResearchWebEnrichmentTask.task_key == summary.task_keys[0]
            )
        )
        second = session.scalar(
            select(ResearchWebEnrichmentTask).where(
                ResearchWebEnrichmentTask.task_key == summary.task_keys[1]
            )
        )
        first.task_key = "z-same-thread-post-task"
        second.task_key = "a-same-thread-post-task"

    leases = await lease_web_enrichment_tasks(
        maker,
        "run-web-001",
        "page-reuse-worker",
        limit=2,
    )
    assert [lease.target_id for lease in leases] == [2101, 2102]


@pytest.mark.asyncio
async def test_dry_run_has_no_writes_and_filters_exact_target_scope(research_db):
    engine, maker = research_db
    dry = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="thread",
        tid=1001,
        fid=7654321,
        dry_run=True,
    )
    assert dry.planned_count == 1
    assert dry.created_count == 0
    with Session(engine) as session:
        assert session.scalars(select(ResearchWebEnrichmentTask)).all() == []
    with pytest.raises(ValueError, match="Unknown research run"):
        await enqueue_web_enrichment_tasks(maker, "missing-run", ["global_growth_level"], dry_run=True)


@pytest.mark.asyncio
async def test_expired_lease_recovers_and_cancel_or_manual_pause_does_not_auto_lease(research_db):
    engine, maker = research_db
    now = datetime.now(UTC)
    created = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="thread",
        tid=1001,
    )
    lease1 = (await lease_web_enrichment_tasks(
        maker, "run-web-001", "worker-a", now=now, lease_seconds=1
    ))[0]
    lease2 = (await lease_web_enrichment_tasks(
        maker, "run-web-001", "worker-b", now=now + timedelta(seconds=2)
    ))[0]
    assert lease2.lease_generation == lease1.lease_generation + 1
    assert lease2.lease_token != lease1.lease_token
    assert await cancel_web_enrichment_task(maker, created.task_keys[0])
    assert await lease_web_enrichment_tasks(maker, "run-web-001", "worker-c", now=now + timedelta(seconds=3)) == []
    assert await resume_web_enrichment_tasks(maker, "run-web-001", statuses=("failed_final",)) == 0


@pytest.mark.asyncio
async def test_blocked_tasks_require_explicit_resume_and_retry_is_bounded(research_db):
    _engine, maker = research_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="thread",
        tid=1001,
        max_attempts=1,
    )
    lease = (
        await lease_web_enrichment_tasks(
            maker, "run-web-001", "synthetic-worker", task_keys=summary.task_keys
        )
    )[0]
    result = await fail_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        status="blocked",
        reason_code="http_429",
        diagnostics={"requests": 9, "pages": 1, "expansions": 2, "secret_url": "https://private.invalid"},
    )
    assert result == "blocked"
    assert await lease_web_enrichment_tasks(
        maker, "run-web-001", "synthetic-worker", task_keys=summary.task_keys
    ) == []
    assert await resume_web_enrichment_tasks(
        maker, "run-web-001", statuses=("blocked",), task_keys=summary.task_keys
    ) == 1
    resumed = (
        await lease_web_enrichment_tasks(
            maker, "run-web-001", "synthetic-worker", task_keys=summary.task_keys
        )
    )[0]
    final = await fail_web_enrichment_task(
        maker,
        resumed.task_key,
        resumed.lease_token,
        status="failed_retryable",
        reason_code="transient_transport",
    )
    assert final == "failed_final"
    with Session(research_db[0]) as session:
        task = session.get(ResearchWebEnrichmentTask, summary.task_keys[0])
        assert task.attempts == 1
        attempt = session.get(ResearchWebEnrichmentAttempt, (summary.task_keys[0], 1))
        assert attempt.status == "blocked"
        assert attempt.diagnostics_json == {
            "requests": 9,
            "pages": 1,
            "expansions": 2,
            "budgeted_actions": 3,
            "http_429": 1,
            "blocked_reason": "http_429",
            "elapsed_seconds": attempt.diagnostics_json["elapsed_seconds"],
        }
        assert "secret_url" not in attempt.diagnostics_json


@pytest.mark.asyncio
async def test_security_verification_reason_persists_as_blocked_and_needs_explicit_resume(research_db):
    engine, maker = research_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="thread",
        tid=1001,
    )
    lease = (
        await lease_web_enrichment_tasks(
            maker, "run-web-001", "synthetic-worker", task_keys=summary.task_keys
        )
    )[0]

    result = await fail_web_enrichment_task(
        maker,
        lease.task_key,
        lease.lease_token,
        status="blocked",
        reason_code="security_verification",
        diagnostics={"pages": 1, "blocked_reason": "security_verification"},
    )

    assert result == "blocked"
    assert await lease_web_enrichment_tasks(
        maker, "run-web-001", "synthetic-worker", task_keys=summary.task_keys
    ) == []
    assert await resume_web_enrichment_tasks(
        maker, "run-web-001", statuses=("failed_final",), task_keys=summary.task_keys
    ) == 0
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, summary.task_keys[0])
        attempt = session.get(ResearchWebEnrichmentAttempt, (summary.task_keys[0], 1))
        assert task.status == "blocked"
        assert task.last_error == "security_verification"
        assert task.last_diagnostics_json["blocked_reason"] == "security_verification"
        assert attempt.status == "blocked"
        assert attempt.reason_code == "security_verification"
        assert attempt.diagnostics_json["blocked_reason"] == "security_verification"


@pytest.mark.asyncio
async def test_retry_delay_increases_and_stops_after_three_attempts(research_db):
    engine, maker = research_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level"],
        target_type="thread",
        tid=1001,
        max_attempts=3,
    )
    now = datetime.now(UTC)
    for attempt, delay in ((1, 3), (2, 5), (3, None)):
        lease = (
            await lease_web_enrichment_tasks(
                maker,
                "run-web-001",
                "retry-worker",
                now=now,
                task_keys=summary.task_keys,
            )
        )[0]
        status = await fail_web_enrichment_task(
            maker,
            lease.task_key,
            lease.lease_token,
            status="failed_retryable",
            reason_code="browser_task_timeout",
            now=now,
        )
        with Session(engine) as session:
            task = session.get(ResearchWebEnrichmentTask, lease.task_key)
            assert task.attempts == attempt
            if delay is None:
                assert status == "failed_final"
                assert task.next_retry_at is None
            else:
                assert status == "failed_retryable"
                assert task.next_retry_at.replace(tzinfo=UTC) == now + timedelta(seconds=delay)
        if delay is not None:
            assert await lease_web_enrichment_tasks(
                maker,
                "run-web-001",
                "too-early",
                now=now + timedelta(seconds=delay - 1),
                task_keys=summary.task_keys,
            ) == []
            now += timedelta(seconds=delay)

    assert await lease_web_enrichment_tasks(
        maker,
        "run-web-001",
        "exhausted",
        now=now + timedelta(days=1),
        task_keys=summary.task_keys,
    ) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_status", ["failed_retryable", "blocked", "failed_final"])
async def test_observed_at_refresh_creates_a_new_task_without_mutating_old_history(research_db, existing_status):
    engine, maker = research_db
    fields = ["ip_location_text"]
    original = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        fields,
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )
    task_key = original.task_keys[0]
    due_at = datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)
    attempts = 3 if existing_status == "failed_final" else 1
    next_retry_at = due_at if existing_status == "failed_retryable" else None
    with Session(engine) as session, session.begin():
        task = session.get(ResearchWebEnrichmentTask, task_key)
        task.status = existing_status
        task.attempts = attempts
        task.next_retry_at = next_retry_at
        post = session.get(ResearchPost, 2101)
        post.observed_at = datetime(2026, 1, 2, tzinfo=UTC)

    refreshed = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        fields,
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )

    assert refreshed.task_keys != (task_key,)
    assert refreshed.created_count == 1
    assert refreshed.deduplicated_count == 0
    with Session(engine) as session:
        tasks = session.scalars(select(ResearchWebEnrichmentTask)).all()
        task = session.get(ResearchWebEnrichmentTask, task_key)
        new_task = session.get(ResearchWebEnrichmentTask, refreshed.task_keys[0])
        assert len(tasks) == 2
        assert task.status == existing_status
        assert task.attempts == attempts
        assert task.next_retry_at.replace(tzinfo=UTC) == next_retry_at if next_retry_at else task.next_retry_at is None
        assert new_task.status == "pending_web"
        assert new_task.attempts == 0
        assert new_task.target_observed_at.replace(tzinfo=UTC) == datetime(2026, 1, 2, tzinfo=UTC)


@pytest.mark.asyncio
async def test_same_snapshot_retry_reuses_task_and_attempt_history(research_db):
    engine, maker = research_db
    created = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )
    task_key = created.task_keys[0]
    with Session(engine) as session, session.begin():
        task = session.get(ResearchWebEnrichmentTask, task_key)
        task.status = "failed_retryable"
        task.attempts = 1
        task.lease_generation = 1
        task.field_statuses_json = {
            "ip_location_text": {"status": "not_tested", "reason": "timeout"}
        }

    retry = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )

    assert retry.task_keys == (task_key,)
    assert retry.created_count == 0
    assert retry.deduplicated_count == 1
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, task_key)
        assert task.status == "failed_retryable"
        assert task.attempts == 1
        assert task.lease_generation == 1
        assert task.field_statuses_json["ip_location_text"]["status"] == "not_tested"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "diagnostics",
    [{"status": "already_satisfied", "pages": 0}, {"status": "ok", "pages": 1}],
)
async def test_completed_task_is_preserved_when_new_snapshot_needs_field(research_db, diagnostics):
    engine, maker = research_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )
    task_key = summary.task_keys[0]
    with Session(engine) as session, session.begin():
        task = session.get(ResearchWebEnrichmentTask, task_key)
        task.status = "completed"
        task.attempts = 1
        task.lease_generation = 1
        task.last_diagnostics_json = diagnostics
        task.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        post = session.get(ResearchPost, 2101)
        post.observed_at = datetime(2026, 1, 2, tzinfo=UTC)

    refreshed = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )

    assert refreshed.task_keys != (task_key,)
    assert refreshed.created_count == 1
    with Session(engine) as session:
        task = session.get(ResearchWebEnrichmentTask, task_key)
        new_task = session.get(ResearchWebEnrichmentTask, refreshed.task_keys[0])
        assert task.status == "completed"
        assert task.attempts == 1
        assert new_task.status == "pending_web"
        assert new_task.attempts == 0
    leases = await lease_web_enrichment_tasks(
        maker,
        "run-web-001",
        "retry-preflight-completed",
        task_keys=refreshed.task_keys,
    )
    assert len(leases) == 1
    assert leases[0].lease_generation == 1


@pytest.mark.asyncio
async def test_preflight_completed_task_at_attempt_limit_does_not_block_new_snapshot(research_db):
    engine, maker = research_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )
    old_task_key = summary.task_keys[0]
    with Session(engine) as session, session.begin():
        task = session.get(ResearchWebEnrichmentTask, old_task_key)
        task.status = "completed"
        task.attempts = 3
        task.max_attempts = 3
        task.lease_generation = 3
        task.last_diagnostics_json = {"status": "already_satisfied", "pages": 0}
        task.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        post = session.get(ResearchPost, 2101)
        post.observed_at = datetime(2026, 1, 2, tzinfo=UTC)

    refreshed = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101],
        tid=1001,
        only_missing_fields=True,
    )

    assert refreshed.task_keys != (old_task_key,)
    assert refreshed.created_count == 1
    with Session(engine) as session:
        old_task = session.get(ResearchWebEnrichmentTask, old_task_key)
        new_task = session.get(ResearchWebEnrichmentTask, refreshed.task_keys[0])
        failures = session.scalars(
            select(ResearchWebEnrichmentFailure).where(
                ResearchWebEnrichmentFailure.task_key == old_task_key,
                ResearchWebEnrichmentFailure.reason_code
                == "web_attempt_limit_exhausted_after_api_refresh",
            )
        ).all()
        assert old_task.status == "completed"
        assert old_task.attempts == 3
        assert new_task.status == "pending_web"
        assert new_task.attempts == 0
        assert failures == []


@pytest.mark.asyncio
async def test_lease_web_enrichment_tasks_filters_by_min_thread_create_time():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[
            BaseForum.__table__,
            BaseThread.__table__,
            ResearchRun.__table__,
            ResearchTaskScope.__table__,
            *RESEARCH_TABLES,
        ],
    )
    maker = _AsyncSessionMaker(engine)
    cutoff = datetime(2021, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ)
    with Session(engine) as session, session.begin():
        session.add(BaseForum(fid=7654321, fname="synthetic"))
        session.add(
            ResearchRun(
                run_id="run-web-filter",
                mode="reply_backfill",
                scope_key="7654321",
                forums_json=[{"fid": 7654321, "fname": "synthetic"}],
                status="running",
            )
        )
        session.add_all(
            [
                BaseThread.from_dto(
                    make_thread(
                        tid=1001,
                        fid=7654321,
                        fname="synthetic",
                        create_time=datetime(2020, 12, 31, 23, 59, 59, tzinfo=SHANGHAI_TZ),
                    )
                ),
                BaseThread.from_dto(
                    make_thread(
                        tid=1002,
                        fid=7654321,
                        fname="synthetic",
                        create_time=cutoff,
                    )
                ),
            ]
        )
        for tid, first_pid in ((1001, 2001), (1002, 2002)):
            session.add(
                ResearchThread(
                    tid=tid,
                    first_pid=first_pid,
                    fid=7654321,
                    title="synthetic",
                    author_user_id=501,
                    author_portrait="synthetic-portrait",
                    source_api="aiotieba.get_posts",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                )
            )
            session.add(
                ResearchTaskScope(
                    run_id="run-web-filter",
                    logical_task_key=f"thread_replies:{tid}",
                    generation=0,
                    task_type="posts",
                    scope_type="thread_replies",
                    fid=7654321,
                    tid=tid,
                    status="completed",
                )
            )

    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-filter",
        ["ip_location_text"],
        target_type="thread",
        target_ids=[1001, 1002],
        only_missing_fields=True,
    )
    assert summary.created_count == 2

    filtered = await lease_web_enrichment_tasks(
        maker,
        "run-web-filter",
        "web-worker-cutoff",
        limit=10,
        min_thread_create_time=cutoff,
    )
    assert [lease.tid for lease in filtered] == [1002]

    # Without the cutoff, the deferred pre-2021 task remains pending_web and leases cleanly.
    unfiltered = await lease_web_enrichment_tasks(
        maker,
        "run-web-filter",
        "web-worker-full",
        limit=10,
    )
    assert [lease.tid for lease in unfiltered] == [1001]


@pytest.mark.asyncio
async def test_enqueue_web_enrichment_tasks_scopes_by_target_ids_and_min_thread_create_time():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[
            BaseForum.__table__,
            BaseThread.__table__,
            ResearchRun.__table__,
            ResearchTaskScope.__table__,
            *RESEARCH_TABLES,
        ],
    )
    maker = _AsyncSessionMaker(engine)
    cutoff = datetime(2021, 1, 1, 0, 0, 0, tzinfo=SHANGHAI_TZ)
    with Session(engine) as session, session.begin():
        session.add(BaseForum(fid=7654321, fname="synthetic"))
        session.add(
            ResearchRun(
                run_id="run-scope-narrow",
                mode="reply_backfill",
                scope_key="7654321",
                forums_json=[{"fid": 7654321, "fname": "synthetic"}],
                status="running",
            )
        )
        session.add_all(
            [
                BaseThread.from_dto(
                    make_thread(
                        tid=1001,
                        fid=7654321,
                        fname="synthetic",
                        create_time=datetime(2020, 12, 31, 23, 59, 59, tzinfo=SHANGHAI_TZ),
                    )
                ),
                BaseThread.from_dto(
                    make_thread(
                        tid=1002,
                        fid=7654321,
                        fname="synthetic",
                        create_time=cutoff,
                    )
                ),
            ]
        )
        session.add_all(
            [
                ResearchThread(
                    tid=1001,
                    first_pid=2100,
                    fid=7654321,
                    title="old",
                    author_user_id=501,
                    author_portrait="p1",
                    source_api="aiotieba.get_posts",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchThread(
                    tid=1002,
                    first_pid=2200,
                    fid=7654321,
                    title="new",
                    author_user_id=502,
                    author_portrait="p2",
                    source_api="aiotieba.get_posts",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchPost(
                    pid=2101,
                    tid=1001,
                    fid=7654321,
                    floor=2,
                    author_user_id=501,
                    author_portrait="p1",
                    source_api="aiotieba.get_posts",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchPost(
                    pid=2201,
                    tid=1002,
                    fid=7654321,
                    floor=2,
                    author_user_id=502,
                    author_portrait="p2",
                    source_api="aiotieba.get_posts",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchComment(
                    cid=3101,
                    tid=1001,
                    parent_post_pid=2101,
                    fid=7654321,
                    author_user_id=501,
                    author_portrait="p1",
                    source_api="aiotieba.get_comments",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchComment(
                    cid=3201,
                    tid=1002,
                    parent_post_pid=2201,
                    fid=7654321,
                    author_user_id=502,
                    author_portrait="p2",
                    source_api="aiotieba.get_comments",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchTaskScope(
                    run_id="run-scope-narrow",
                    logical_task_key="thread_replies:1001",
                    generation=0,
                    task_type="posts",
                    scope_type="thread_replies",
                    fid=7654321,
                    tid=1001,
                    status="completed",
                ),
                ResearchTaskScope(
                    run_id="run-scope-narrow",
                    logical_task_key="thread_replies:1002",
                    generation=0,
                    task_type="posts",
                    scope_type="thread_replies",
                    fid=7654321,
                    tid=1002,
                    status="completed",
                ),
                ResearchTaskScope(
                    run_id="run-scope-narrow",
                    logical_task_key="post_comments:1001:2101",
                    generation=0,
                    task_type="comments",
                    scope_type="post_comments",
                    fid=7654321,
                    tid=1001,
                    parent_pid=2101,
                    parent_generation=0,
                    status="completed",
                ),
                ResearchTaskScope(
                    run_id="run-scope-narrow",
                    logical_task_key="post_comments:1002:2201",
                    generation=0,
                    task_type="comments",
                    scope_type="post_comments",
                    fid=7654321,
                    tid=1002,
                    parent_pid=2201,
                    parent_generation=0,
                    status="completed",
                ),
                ResearchTaskScope(
                    run_id="run-scope-narrow",
                    logical_task_key="post_comments:1002:2202",
                    generation=0,
                    task_type="comments",
                    scope_type="post_comments",
                    fid=7654321,
                    tid=1002,
                    parent_pid=2202,
                    parent_generation=0,
                    status="completed",
                ),
            ]
        )

    scope_queries: list[tuple[str, object]] = []

    def _capture_sql(_conn, _cursor, statement, parameters, _context, _executemany):
        if "FROM research_task_scope" in statement:
            scope_queries.append((statement, parameters))

    event.listen(engine, "before_cursor_execute", _capture_sql)
    try:
        post_summary = await enqueue_web_enrichment_tasks(
            maker,
            "run-scope-narrow",
            ["ip_location_text"],
            target_type="post",
            target_ids=[2201],
            only_missing_fields=True,
        )
        assert post_summary.created_count == 1
        assert [p.target_id for p in post_summary.planned_targets] == [2201]
        assert len(scope_queries) == 1
        post_sql, post_params = scope_queries[0]
        assert "research_task_scope.tid IN" in post_sql
        assert "research_task_scope.scope_type =" in post_sql
        assert "post_comments" not in str(post_params)
        assert 1002 in post_params
        assert 1001 not in post_params

        scope_queries.clear()
        comment_summary = await enqueue_web_enrichment_tasks(
            maker,
            "run-scope-narrow",
            ["ip_location_text"],
            target_type="comment",
            target_ids=[3201],
            only_missing_fields=True,
        )
        assert comment_summary.created_count == 1
        assert [p.target_id for p in comment_summary.planned_targets] == [3201]
        assert len(scope_queries) == 1
        comment_sql, comment_params = scope_queries[0]
        assert "research_task_scope.tid IN" in comment_sql
        assert "research_task_scope.parent_pid IN" in comment_sql
        assert 1002 in comment_params
        assert 2201 in comment_params
        assert 2202 not in comment_params
    finally:
        event.remove(engine, "before_cursor_execute", _capture_sql)

    filtered_discovery = await enqueue_web_enrichment_tasks(
        maker,
        "run-scope-narrow",
        ["ip_location_text"],
        only_missing_fields=True,
        min_thread_create_time=cutoff,
    )
    assert all(plan.tid == 1002 for plan in filtered_discovery.planned_targets)
    assert filtered_discovery.created_count == 1  # only thread 1002 is newly created; 2201 & 3201 already exist


@pytest.mark.asyncio
async def test_enqueue_web_enrichment_tasks_bulk_skips_noop_existing_tasks_and_keeps_transitions(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[ResearchRun.__table__, ResearchTaskScope.__table__, *RESEARCH_TABLES],
    )
    maker = _AsyncSessionMaker(engine)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    with Session(engine) as session, session.begin():
        session.add(
            ResearchRun(
                run_id="run-bulk-dedup",
                mode="backfill",
                scope_key="7654321",
                forums_json=[{"fid": 7654321, "fname": "synthetic"}],
                status="completed",
            )
        )
        session.add(
            ResearchTaskScope(
                run_id="run-bulk-dedup",
                logical_task_key="thread_replies:1001",
                generation=0,
                task_type="posts",
                scope_type="thread_replies",
                fid=7654321,
                tid=1001,
                status="completed",
            )
        )
        for pid, floor in ((2101, 2), (2102, 3), (2103, 4)):
            session.add(
                ResearchPost(
                    pid=pid,
                    tid=1001,
                    fid=7654321,
                    floor=floor,
                    author_user_id=501 + floor,
                    author_portrait=f"p{floor}",
                    source_api="aiotieba.get_posts",
                    observed_at=now,
                    source_payload_json={},
                    missing_reasons_json={},
                )
            )

    first = await enqueue_web_enrichment_tasks(
        maker,
        "run-bulk-dedup",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101, 2102, 2103],
        only_missing_fields=True,
    )
    assert first.created_count == 3

    key_2101, key_2102, key_2103 = first.task_keys
    with Session(engine) as session, session.begin():
        # 2101 stays pending_web; 2102 needs pending_api_detail -> pending_web;
        # 2103 needs completed -> pending_web.
        session.get(ResearchWebEnrichmentTask, key_2102).status = "pending_api_detail"
        task_2103 = session.get(ResearchWebEnrichmentTask, key_2103)
        task_2103.status = "completed"
        task_2103.completed_at = now

    persisted_target_ids: list[int] = []
    original_persist = web_tasks_mod._persist_enqueued_task

    async def spy_persist(session_maker, task, **kwargs):
        persisted_target_ids.append(task["target_id"])
        return await original_persist(session_maker, task, **kwargs)

    monkeypatch.setattr(web_tasks_mod, "_persist_enqueued_task", spy_persist)

    second = await enqueue_web_enrichment_tasks(
        maker,
        "run-bulk-dedup",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101, 2102, 2103],
        only_missing_fields=True,
    )
    assert second.created_count == 0
    assert second.deduplicated_count == 3
    assert persisted_target_ids == [2102, 2103]
    with Session(engine) as session:
        assert session.get(ResearchWebEnrichmentTask, key_2101).status == "pending_web"
        assert session.get(ResearchWebEnrichmentTask, key_2102).status == "pending_web"
        assert session.get(ResearchWebEnrichmentTask, key_2103).status == "pending_web"


@pytest.mark.asyncio
async def test_lease_web_enrichment_tasks_default_lease_and_worker_lease_sizing(research_db):
    _engine, maker = research_db
    summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="thread",
        target_ids=[1001],
        only_missing_fields=True,
    )
    assert summary.created_count == 1
    now = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)
    leases = await lease_web_enrichment_tasks(maker, "run-web-001", "lease-default-check", now=now)
    assert len(leases) == 1
    with Session(_engine) as session:
        task = session.get(ResearchWebEnrichmentTask, summary.task_keys[0])
        assert task.lease_until.replace(tzinfo=UTC) == now + timedelta(seconds=240)
    assert web_service_mod._web_lease_seconds(SimpleNamespace(timeout_seconds=90.0)) == 240
    assert web_service_mod._web_lease_seconds(SimpleNamespace(timeout_seconds=180.0)) == 300


@pytest.mark.asyncio
async def test_lease_web_enrichment_tasks_newest_first_groups_by_tid_desc_and_target_type_order(
    research_db,
):
    engine, maker = research_db
    with Session(engine) as session, session.begin():
        session.add_all(
            [
                ResearchPost(
                    pid=2201,
                    tid=1002,
                    fid=7654321,
                    text="newer post",
                    author_user_id=778,
                    author_portrait="synthetic-post-portrait-2",
                    source_api="aiotieba.get_posts",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchComment(
                    cid=3201,
                    tid=1002,
                    parent_post_pid=2201,
                    fid=7654321,
                    text="newer comment",
                    author_user_id=889,
                    source_api="aiotieba.get_comments",
                    observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchTaskScope(
                    run_id="run-web-001",
                    logical_task_key="post_comments:1002:2201",
                    generation=0,
                    task_type="comments",
                    scope_type="post_comments",
                    fid=7654321,
                    tid=1002,
                    parent_pid=2201,
                    parent_generation=0,
                    status="completed",
                ),
            ]
        )
    for target_type, target_ids in (
        ("thread", [1001, 1002]),
        ("post", None),
        ("comment", None),
    ):
        summary = await enqueue_web_enrichment_tasks(
            maker,
            "run-web-001",
            ["ip_location_text"],
            target_type=target_type,
            target_ids=target_ids,
            only_missing_fields=True,
        )
        assert summary.created_count > 0

    leases = await lease_web_enrichment_tasks(
        maker,
        "run-web-001",
        "newest-first-worker",
        limit=20,
        newest_first=True,
    )
    observed = [(lease.tid, lease.target_type) for lease in leases]
    assert observed == [
        (1002, "thread"),
        (1002, "post"),
        (1002, "comment"),
        (1001, "thread"),
        (1001, "post"),
        (1001, "post"),
        (1001, "comment"),
        (1001, "comment"),
    ]


@pytest.mark.asyncio
async def test_ip_location_cutoff_skips_pre_launch_enqueue_and_prunes_existing_tasks(research_db):
    from src.research.web_fallback.tasks import (
        IP_BEFORE_LAUNCH_REASON,
        TIEBA_IP_LOCATION_LAUNCH_TIME,
        prune_pre_launch_ip_web_tasks,
    )

    engine, maker = research_db
    pre_launch = datetime(2022, 4, 27, 23, 59, 0, tzinfo=SHANGHAI_TZ)
    post_launch = datetime(2022, 4, 28, 0, 1, 0, tzinfo=SHANGHAI_TZ)
    observed_at = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)

    with Session(engine) as session, session.begin():
        session.add_all(
            [
                # Old 2021 thread, with one old post (only IP missing) and one post-launch post & comment.
                ResearchThread(
                    tid=5001,
                    fid=7654321,
                    fname="synthetic",
                    title="old-thread",
                    create_time=pre_launch,
                    scraped_at=observed_at,
                    author_user_id=9001,
                    author_portrait="p-old",
                    global_growth_level=5,
                    global_growth_icon_url="https://example.com/g.png",
                    source_api="aiotieba.get_posts",
                    observed_at=observed_at,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchPost(
                    pid=6001,
                    tid=5001,
                    fid=7654321,
                    floor=2,
                    text="old post missing only ip",
                    create_time=pre_launch,
                    scraped_at=observed_at,
                    author_user_id=9002,
                    author_portrait="p-old-post",
                    global_growth_level=3,
                    global_growth_icon_url="https://example.com/g.png",
                    source_api="aiotieba.get_posts",
                    observed_at=observed_at,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchPost(
                    pid=6002,
                    tid=5001,
                    fid=7654321,
                    floor=3,
                    text="old post missing growth and ip",
                    create_time=pre_launch,
                    scraped_at=observed_at,
                    author_user_id=9003,
                    author_portrait="p-old-post2",
                    source_api="aiotieba.get_posts",
                    observed_at=observed_at,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchPost(
                    pid=6003,
                    tid=5001,
                    fid=7654321,
                    floor=4,
                    text="new reply in old thread",
                    create_time=post_launch,
                    scraped_at=observed_at,
                    author_user_id=9004,
                    author_portrait="p-new-post",
                    global_growth_level=4,
                    global_growth_icon_url="https://example.com/g.png",
                    source_api="aiotieba.get_posts",
                    observed_at=observed_at,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchComment(
                    cid=7001,
                    tid=5001,
                    parent_post_pid=6003,
                    fid=7654321,
                    text="new comment in old thread",
                    create_time=post_launch,
                    scraped_at=observed_at,
                    author_user_id=9005,
                    author_portrait="p-new-cmt",
                    global_growth_level=2,
                    global_growth_icon_url="https://example.com/g.png",
                    source_api="aiotieba.get_comments",
                    observed_at=observed_at,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchTaskScope(
                    run_id="run-web-001",
                    logical_task_key="thread_replies:5001",
                    generation=0,
                    task_type="posts",
                    scope_type="thread_replies",
                    fid=7654321,
                    tid=5001,
                    status="completed",
                ),
                ResearchTaskScope(
                    run_id="run-web-001",
                    logical_task_key="post_comments:5001:6003",
                    generation=0,
                    task_type="comments",
                    scope_type="post_comments",
                    fid=7654321,
                    tid=5001,
                    parent_pid=6003,
                    parent_generation=0,
                    status="completed",
                ),
            ]
        )

    # New enqueue with min_ip_create_time skips 5001 & 6001, strips IP from 6002, keeps 6003 & 7001.
    thread_summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level", "global_growth_icon_url", "ip_location_text"],
        target_type="thread",
        target_ids=[5001],
        only_missing_fields=True,
        pair_growth_fields=True,
        min_ip_create_time=TIEBA_IP_LOCATION_LAUNCH_TIME,
    )
    assert thread_summary.created_count == 0
    assert thread_summary.skipped.get("ip_location_before_mechanism_launch") == 1

    post_summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level", "global_growth_icon_url", "ip_location_text"],
        target_type="post",
        target_ids=[6001, 6002, 6003],
        only_missing_fields=True,
        pair_growth_fields=True,
        min_ip_create_time=TIEBA_IP_LOCATION_LAUNCH_TIME,
    )
    assert post_summary.skipped.get("ip_location_before_mechanism_launch") == 1
    assert {p.target_id: p.requested_fields for p in post_summary.planned_targets} == {
        6002: ("global_growth_icon_url", "global_growth_level"),
        6003: ("ip_location_text",),
    }

    cmt_summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="comment",
        target_ids=[7001],
        only_missing_fields=True,
        min_ip_create_time=TIEBA_IP_LOCATION_LAUNCH_TIME,
    )
    assert cmt_summary.created_count == 1

    # Also test pruning pre-existing un-filtered tasks (e.g. 5001 & 6001 enqueued before upgrade).
    legacy_thread = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="thread",
        target_ids=[5001],
        only_missing_fields=True,
    )
    legacy_post_ip = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[6001],
        only_missing_fields=True,
    )
    assert legacy_thread.created_count == 1
    assert legacy_post_ip.created_count == 1

    pruned = await prune_pre_launch_ip_web_tasks(
        maker,
        ["run-web-001"],
        min_ip_create_time=TIEBA_IP_LOCATION_LAUNCH_TIME,
    )
    assert pruned["marked_not_applicable"] == 2
    with Session(engine) as session:
        t_row = session.get(ResearchWebEnrichmentTask, legacy_thread.task_keys[0])
        p_row = session.get(ResearchWebEnrichmentTask, legacy_post_ip.task_keys[0])
        new_p_row = session.get(ResearchWebEnrichmentTask, post_summary.task_keys[1])
        assert t_row is not None and t_row.status == "not_applicable"
        assert t_row.field_statuses_json == {
            "ip_location_text": {"status": "not_applicable", "reason": IP_BEFORE_LAUNCH_REASON}
        }
        assert p_row is not None and p_row.status == "not_applicable"
        assert new_p_row is not None and new_p_row.status == "pending_web"


@pytest.mark.asyncio
async def test_prune_stale_web_tasks_and_priority_ordering_comments_and_era(research_db):
    from src.research.web_fallback.tasks import prune_stale_web_tasks

    engine, maker = research_db
    BaseThread.__table__.create(engine, checkfirst=True)
    t_old_obs = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    t_new_obs = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)
    era_cutoff = datetime(2021, 1, 1, 0, 0, tzinfo=SHANGHAI_TZ)
    pre_2021 = datetime(2020, 12, 31, 12, 0, tzinfo=SHANGHAI_TZ)
    in_2022 = datetime(2022, 6, 1, 12, 0, tzinfo=SHANGHAI_TZ)

    with Session(engine) as session, session.begin():
        # Update base fixture thread 1001 to have 2022 create_time, add 2020 thread 999 & stale 1003.
        t1001 = session.get(ResearchThread, 1001)
        assert t1001 is not None
        t1001.create_time = in_2022
        session.add_all(
            [
                BaseThread(
                    tid=999,
                    fid=7654321,
                    title="pre-2021",
                    text="old",
                    author_id=900,
                    author_level=1,
                    create_time=pre_2021,
                    last_time=pre_2021,
                    reply_num=1,
                ),
                BaseThread(
                    tid=1001,
                    fid=7654321,
                    title="synthetic-2022",
                    text="body",
                    author_id=900,
                    author_level=1,
                    create_time=in_2022,
                    last_time=in_2022,
                    reply_num=1,
                ),
                BaseThread(
                    tid=1003,
                    fid=7654321,
                    title="stale-2022",
                    text="stale",
                    author_id=901,
                    author_level=1,
                    create_time=in_2022,
                    last_time=in_2022,
                    reply_num=1,
                ),
                ResearchThread(
                    tid=999,
                    fid=7654321,
                    fname="synthetic",
                    title="pre-2021",
                    create_time=pre_2021,
                    scraped_at=t_old_obs,
                    author_user_id=900,
                    author_portrait="p999",
                    source_api="aiotieba.get_posts",
                    observed_at=t_old_obs,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchThread(
                    tid=1003,
                    fid=7654321,
                    fname="synthetic",
                    title="stale-2022",
                    create_time=in_2022,
                    scraped_at=t_old_obs,
                    author_user_id=901,
                    author_portrait="p1003",
                    source_api="aiotieba.get_posts",
                    observed_at=t_old_obs,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchPost(
                    pid=2999,
                    tid=999,
                    fid=7654321,
                    floor=2,
                    text="pre-2021-post",
                    create_time=pre_2021,
                    scraped_at=t_old_obs,
                    author_user_id=902,
                    author_portrait="p2999",
                    source_api="aiotieba.get_posts",
                    observed_at=t_old_obs,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchComment(
                    cid=3999,
                    tid=999,
                    parent_post_pid=2999,
                    fid=7654321,
                    text="pre-2021-comment",
                    create_time=pre_2021,
                    scraped_at=t_old_obs,
                    author_user_id=903,
                    source_api="aiotieba.get_comments",
                    observed_at=t_old_obs,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchTaskScope(
                    run_id="run-web-001",
                    logical_task_key="thread_replies:999",
                    generation=0,
                    task_type="posts",
                    scope_type="thread_replies",
                    fid=7654321,
                    tid=999,
                    status="completed",
                ),
                ResearchTaskScope(
                    run_id="run-web-001",
                    logical_task_key="post_comments:999:2999",
                    generation=0,
                    task_type="comments",
                    scope_type="post_comments",
                    fid=7654321,
                    tid=999,
                    parent_pid=2999,
                    parent_generation=0,
                    status="completed",
                ),
                ResearchTaskScope(
                    run_id="run-web-001",
                    logical_task_key="thread_replies:1003",
                    generation=0,
                    task_type="posts",
                    scope_type="thread_replies",
                    fid=7654321,
                    tid=1003,
                    status="completed",
                ),
            ]
        )

    # Enqueue thread 1003 (then refresh its observed_at so task becomes stale)
    stale_summary = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="thread",
        target_ids=[1003],
        only_missing_fields=True,
    )
    assert stale_summary.created_count == 1
    with Session(engine) as session, session.begin():
        t1003 = session.get(ResearchThread, 1003)
        assert t1003 is not None
        t1003.observed_at = t_new_obs

    # Enqueue mixed >=2021 (thread/post IP-only + post growth + comments) and <2021 comment.
    await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="thread",
        target_ids=[1001],
        only_missing_fields=True,
    )
    await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="post",
        target_ids=[2101],
        only_missing_fields=True,
    )
    growth_post = await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["global_growth_level", "global_growth_icon_url"],
        target_type="post",
        target_ids=[2102],
        only_missing_fields=True,
        pair_growth_fields=True,
    )
    assert growth_post.created_count == 1
    await enqueue_web_enrichment_tasks(
        maker,
        "run-web-001",
        ["ip_location_text"],
        target_type="comment",
        target_ids=[3101, 3999],
        only_missing_fields=True,
    )

    pruned_stale = await prune_stale_web_tasks(maker, ["run-web-001"])
    assert pruned_stale == 1
    with Session(engine) as session:
        stale_task = session.get(ResearchWebEnrichmentTask, stale_summary.task_keys[0])
        assert stale_task is not None
        assert stale_task.status == "failed_final"
        assert stale_task.last_error == "target_observed_at_changed"

    # Lease with era + comment priority + newest_first:
    # 1) >=2021 comment (tid=1001, cid=3101)
    # 2) >=2021 growth post (tid=1001, pid=2102)
    # 3) >=2021 IP-only thread & post (tid=1001, thread then post — NOT dropped!)
    # 4) <2021 comment (tid=999, cid=3999) last!
    leases = await lease_web_enrichment_tasks(
        maker,
        "run-web-001",
        "prio-worker",
        limit=20,
        priority_min_thread_create_time=era_cutoff,
        prioritize_comments=True,
        newest_first=True,
    )
    assert [(lease.tid, lease.target_type, lease.target_id, lease.requested_fields) for lease in leases] == [
        (1001, "comment", 3101, ("ip_location_text",)),
        (1001, "post", 2102, ("global_growth_icon_url", "global_growth_level")),
        (1001, "thread", 1001, ("ip_location_text",)),
        (1001, "post", 2101, ("ip_location_text",)),
        (999, "comment", 3999, ("ip_location_text",)),
    ]
