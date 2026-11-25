from datetime import UTC, datetime, timedelta

import pytest
from conftest import make_thread
from sqlalchemy import create_engine, select
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
