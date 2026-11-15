from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from tiebameow.models.orm import Base

from src.core.models import ResearchRun, ResearchTaskScope
from src.research.current_user_backfill import _select_targets, run_current_user_backfill
from src.research.models import RESEARCH_TABLES, ResearchComment, ResearchPost, ResearchThread


class _AsyncSession:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        self.session.close()

    async def get(self, *args, **kwargs):
        return self.session.get(*args, **kwargs)

    async def execute(self, statement):
        return self.session.execute(statement)


class _AsyncSessionMaker:
    def __init__(self, engine):
        self.engine = engine

    def __call__(self):
        return _AsyncSession(Session(self.engine))


@pytest.fixture
def user_backfill_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[ResearchRun.__table__, ResearchTaskScope.__table__, *RESEARCH_TABLES])
    now = datetime(2026, 1, 1, tzinfo=UTC)
    with Session(engine) as session, session.begin():
        session.add(
            ResearchRun(
                run_id="synthetic-user-run",
                mode="backfill",
                scope_key="synthetic",
                forums_json=[7, 8],
                status="completed",
            )
        )
        session.add_all([
            ResearchThread(
                tid=1001,
                fid=7,
                author_user_id=50,
                author_portrait="portrait-50",
                source_api="aiotieba.get_posts",
                observed_at=now,
                source_payload_json={},
            ),
            ResearchPost(
                pid=2001,
                tid=1001,
                fid=7,
                author_user_id=50,
                author_portrait="portrait-50",
                source_api="aiotieba.get_posts",
                observed_at=now,
                source_payload_json={},
            ),
            ResearchComment(
                cid=3001,
                tid=1001,
                parent_post_pid=2001,
                fid=7,
                author_user_id=50,
                author_portrait="portrait-50",
                source_api="aiotieba.get_comments",
                observed_at=now,
                source_payload_json={},
            ),
            ResearchPost(
                pid=2002,
                tid=1001,
                fid=7,
                author_user_id=60,
                author_portrait="portrait-60",
                source_api="aiotieba.get_posts",
                observed_at=now,
                source_payload_json={},
            ),
            ResearchThread(
                tid=1002,
                fid=8,
                author_user_id=99,
                author_portrait="portrait-99",
                source_api="aiotieba.get_posts",
                observed_at=now,
                source_payload_json={},
            ),
            ResearchThread(
                tid=1003,
                fid=8,
                author_user_id=100,
                source_api="aiotieba.get_posts",
                observed_at=now,
                source_payload_json={},
            ),
        ])
        session.add_all([
            ResearchTaskScope(
                run_id="synthetic-user-run",
                logical_task_key=f"thread_replies:{tid}",
                generation=0,
                task_type="post",
                scope_type="thread_replies",
                tid=tid,
                fid=fid,
                status="completed",
            )
            for tid, fid in ((1001, 7), (1002, 8), (1003, 7))
        ])
        session.add(
            ResearchTaskScope(
                run_id="synthetic-user-run",
                logical_task_key="post_comments:1001:2001",
                generation=0,
                task_type="comment",
                scope_type="post_comments",
                tid=1001,
                fid=7,
                parent_pid=2001,
                parent_generation=0,
                status="completed",
            )
        )
    yield engine, _AsyncSessionMaker(engine)
    engine.dispose()


@pytest.mark.asyncio
async def test_user_target_selection_ands_run_tid_fid_and_deduplicates_identity(user_backfill_db):
    _engine, sessions = user_backfill_db
    selected = await _select_targets(sessions, "synthetic-user-run", tid=1001, fid=7, limit=5, profile_only=False)
    assert [(target.user_id, target.fid, target.portrait) for target in selected] == [
        (50, 7, "portrait-50"),
        (60, 7, "portrait-60"),
    ]

    other_forum = await _select_targets(sessions, "synthetic-user-run", tid=None, fid=8, limit=5, profile_only=False)
    assert [(target.user_id, target.fid) for target in other_forum] == [(99, 8)]


class _ResearchClientFake:
    def __init__(
        self,
        *,
        blocked=False,
        forum_fid_mismatch_user=None,
        bawu_fid_mismatch_user=None,
        profile_response_user_override=None,
    ):
        self.calls = []
        self.blocked = blocked
        self.forum_fid_mismatch_user = forum_fid_mismatch_user
        self.bawu_fid_mismatch_user = bawu_fid_mismatch_user
        self.profile_response_user_override = profile_response_user_override

    async def get_user_info_research(self, user_id):
        self.calls.append(("profile", user_id))
        status = "blocked" if self.blocked else "available"
        return SimpleNamespace(
            source_api="aiotieba.get_user_info",
            dto=SimpleNamespace(
                status=status,
                user_id=user_id,
                response_user_id=(
                    None
                    if self.blocked
                    else self.profile_response_user_override
                    if user_id == 50 and self.profile_response_user_override is not None
                    else user_id
                ),
            ),
        )

    async def get_user_forum_info_research(self, fid, user_id, portrait):
        self.calls.append(("forum", user_id, fid, portrait))
        return SimpleNamespace(
            source_api="aiotieba.get_user_forum_info",
            dto=SimpleNamespace(
                status="available",
                user_id=user_id,
                response_user_id=user_id,
                fid=999 if user_id == self.forum_fid_mismatch_user else fid,
            ),
        )

    async def get_bawu_info_research(self, fid, user_id):
        self.calls.append(("bawu", user_id, fid))
        return SimpleNamespace(
            source_api="aiotieba.get_bawu_info",
            dto=SimpleNamespace(
                status="not_listed",
                user_id=user_id,
                response_user_id=None,
                fid=999 if user_id == self.bawu_fid_mismatch_user else fid,
            ),
        )


@pytest.mark.asyncio
async def test_current_user_backfill_persists_only_current_profile_and_forum_pages(user_backfill_db):
    _engine, sessions = user_backfill_db
    client = _ResearchClientFake(forum_fid_mismatch_user=50)
    persisted = []

    async def fake_persist(_sessions, page):
        persisted.append(page.source_api)
        return {}

    summary = await run_current_user_backfill(
        sessions,
        client,
        "synthetic-user-run",
        tid=1001,
        fid=7,
        limit=5,
        profile=True,
        forum=True,
        bawu=True,
        persist=fake_persist,
    )

    assert summary.selected_targets == 2
    assert summary.unique_users == 2
    assert summary.profile_observations == 2
    assert summary.forum_snapshots == 1
    assert summary.bawu_snapshots == 2
    assert summary.statuses["identity_unverified"] == 1
    assert client.calls.count(("profile", 50)) == 1
    assert client.calls.count(("profile", 60)) == 1
    assert persisted.count("aiotieba.get_user_info") == 2
    assert persisted.count("aiotieba.get_user_forum_info") == 1
    assert persisted.count("aiotieba.get_bawu_info") == 2


@pytest.mark.asyncio
async def test_current_user_backfill_rejects_profile_response_uid_mismatch(user_backfill_db):
    _engine, sessions = user_backfill_db
    client = _ResearchClientFake(profile_response_user_override=999)
    persisted = []

    async def fake_persist(_sessions, page):
        persisted.append(page.dto.user_id)

    summary = await run_current_user_backfill(
        sessions,
        client,
        "synthetic-user-run",
        tid=1001,
        fid=7,
        limit=5,
        profile=True,
        persist=fake_persist,
    )

    assert summary.profile_observations == 1
    assert summary.statuses["identity_unverified"] == 1
    assert persisted == [60]


@pytest.mark.asyncio
async def test_current_user_backfill_rejects_bawu_request_fid_mismatch(user_backfill_db):
    _engine, sessions = user_backfill_db
    client = _ResearchClientFake(bawu_fid_mismatch_user=50)
    persisted = []

    async def fake_persist(_sessions, page):
        persisted.append(page.dto.user_id)

    summary = await run_current_user_backfill(
        sessions,
        client,
        "synthetic-user-run",
        tid=1001,
        fid=7,
        limit=5,
        bawu=True,
        persist=fake_persist,
    )

    assert summary.bawu_snapshots == 1
    assert summary.statuses["identity_unverified"] == 1
    assert persisted == [60]


@pytest.mark.asyncio
async def test_user_backfill_stops_on_blocked_response_without_retries(user_backfill_db):
    _engine, sessions = user_backfill_db
    client = _ResearchClientFake(blocked=True)
    persisted = []

    async def fake_persist(_sessions, page):
        persisted.append(page)

    summary = await run_current_user_backfill(
        sessions,
        client,
        "synthetic-user-run",
        tid=1001,
        limit=5,
        profile=True,
        forum=True,
        persist=fake_persist,
    )
    assert summary.blocked is True
    assert summary.profile_observations == summary.forum_snapshots == 0
    assert client.calls == [("profile", 50)]
    assert persisted == []
