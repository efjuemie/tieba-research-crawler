from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from tiebameow.models.orm import Base
from tiebameow.models.orm import Post as BasePost

from src.core.models import ResearchCheckpoint, ResearchTaskScope
from src.research.client import ResearchPage
from src.research.models import (
    RESEARCH_TABLES,
    ResearchComment,
    ResearchContentObservation,
    ResearchPost,
    ResearchThread,
    ResearchUser,
)
from src.research.persistence import _upsert_content_rows, _upsert_research_threads, persist_research_page


class _AsyncSessionAdapter:
    def __init__(self, session):
        self.session = session

    def get_bind(self):
        return self.session.get_bind()

    async def execute(self, statement):
        return self.session.execute(statement)

    async def scalar(self, statement):
        return self.session.scalar(statement)

    async def scalars(self, statement):
        return self.session.scalars(statement)

    def add(self, instance):
        self.session.add(instance)


def _comment_page(*, observed_at: datetime, author_id: int, user_name: str | None) -> ResearchPage:
    user = SimpleNamespace(
        user_id=author_id,
        tieba_uid=987654,
        user_name=user_name,
        nick_name_new=None,
        portrait=None,
        level=None,
        glevel=None,
        is_bawu=None,
        icons=None,
        ip=None,
    )
    comment = SimpleNamespace(
        pid=3001,
        tid=1001,
        ppid=2001,
        fid=7654321,
        fname="合成贴吧",
        floor=1,
        create_time=1_750_000_010,
        author_id=author_id,
        user=user,
        contents=SimpleNamespace(text="合成评论", objs=[]),
        reply_to_id=0,
        agree=0,
        disagree=0,
        is_thread_author=False,
    )
    return ResearchPage(
        raw=SimpleNamespace(
            objs=[comment],
            forum=SimpleNamespace(fid=7654321, fname="合成贴吧"),
        ),
        dto=object(),
        observed_at=observed_at,
        source_api="aiotieba.get_comments",
    )


def _test_engine():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[
            BasePost.__table__,
            ResearchCheckpoint.__table__,
            ResearchTaskScope.__table__,
            *RESEARCH_TABLES,
        ],
    )
    return engine


@pytest.mark.asyncio
async def test_same_cid_with_different_known_author_rolls_back_page_and_checkpoint():
    engine = _test_engine()
    t1 = datetime(2026, 11, 12, 10, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    with Session(engine) as session, session.begin():
        session.add(
            ResearchComment(
                cid=3001,
                tid=1001,
                parent_post_pid=2001,
                fid=7654321,
                text="原评论",
                author_user_id=501,
                ip_location_text="福建省",
                ip_location_source="verified_web",
                ip_location_observed_at=t1,
                field_sources_json={
                    "ip_location_text": {
                        "verified_web": {
                            "value": "福建省",
                            "observed_at": t1.isoformat(),
                        }
                    }
                },
                source_api="aiotieba.get_comments",
                observed_at=t1,
                source_payload_json={"fixture": "before"},
                missing_reasons_json={},
            )
        )
        session.add(
            ResearchContentObservation(
                owner_type="comment",
                owner_id=3001,
                tid=1001,
                source_api="aiotieba.get_comments",
                observed_at=t1,
            )
        )
        session.add(
            ResearchTaskScope(
                run_id="synthetic-run",
                logical_task_key="post_comments:1001:2001",
                generation=0,
                task_type="comments",
                scope_type="post_comments",
                tid=1001,
                parent_pid=2001,
                status="running",
            )
        )
        session.add(
            ResearchCheckpoint(
                run_id="synthetic-run",
                task_key="post_comments:1001:2001",
                page_number=1,
                task_type="comments",
                scope_type="post_comments",
                status="running",
                attempts=1,
                has_more=True,
                next_page=2,
            )
        )

    with Session(engine) as session:
        async def _persist_page_and_checkpoint():
            with session.begin():
                await persist_research_page(
                    None,
                    _comment_page(observed_at=t2, author_id=777, user_name="新作者"),
                    session=_AsyncSessionAdapter(session),
                )
                session.execute(
                    ResearchCheckpoint.__table__.update()
                    .where(ResearchCheckpoint.task_key == "post_comments:1001:2001")
                    .values(page_number=2, next_page=3)
                )

        with pytest.raises(ValueError, match="identity changed"):
            await _persist_page_and_checkpoint()

    with Session(engine) as session:
        comment = session.scalar(select(ResearchComment).where(ResearchComment.cid == 3001))
        observation = session.scalar(
            select(ResearchContentObservation).where(
                ResearchContentObservation.owner_type == "comment",
                ResearchContentObservation.owner_id == 3001,
            )
        )
        checkpoint = session.scalar(
            select(ResearchCheckpoint).where(ResearchCheckpoint.task_key == "post_comments:1001:2001")
        )
        assert comment.author_user_id == 501
        assert comment.text == "原评论"
        assert comment.ip_location_text == "福建省"
        assert comment.field_sources_json["ip_location_text"]["verified_web"]["value"] == "福建省"
        assert comment.observed_at.replace(tzinfo=UTC) == t1
        assert observation.observed_at.replace(tzinfo=UTC) == t1
        assert checkpoint.page_number == 1
        assert checkpoint.next_page == 2
        assert session.scalar(select(ResearchUser.user_id).where(ResearchUser.user_id == 777)) is None
    engine.dispose()


@pytest.mark.asyncio
async def test_sparse_profile_retention_keeps_original_and_latest_known_source_times():
    engine = _test_engine()
    t1 = datetime(2026, 11, 12, 10, tzinfo=UTC)
    pages = [
        _comment_page(observed_at=t1, author_id=501, user_name="原作者"),
        _comment_page(observed_at=t1 + timedelta(hours=1), author_id=501, user_name=None),
        _comment_page(observed_at=t1 + timedelta(hours=2), author_id=501, user_name=None),
        _comment_page(observed_at=t1 + timedelta(hours=3), author_id=501, user_name="新作者名"),
        _comment_page(observed_at=t1 + timedelta(hours=4), author_id=501, user_name=None),
    ]
    for page in pages:
        with Session(engine) as session, session.begin():
            await persist_research_page(None, page, session=_AsyncSessionAdapter(session))

    with Session(engine) as session:
        comment = session.scalar(select(ResearchComment).where(ResearchComment.cid == 3001))
        retained = comment.field_sources_json["author_user_name"]["last_known_api"]
        assert comment.author_user_name == "新作者名"
        assert retained["retained_source_api"] == "aiotieba.get_comments"
        assert retained["retained_observed_at"] == (t1 + timedelta(hours=3)).isoformat()
        assert retained["missing_observed_at"] == (t1 + timedelta(hours=4)).isoformat()
    engine.dispose()


@pytest.mark.asyncio
async def test_api_conflicts_are_deep_copied_and_persist_for_all_content_rows():
    engine = _test_engine()
    t1 = datetime(2026, 11, 12, 10, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    source = {
        "agree_count": {
            "verified_web": {
                "observation_time": t1.isoformat(),
                "source_kind": "web_network",
                "parser_version": "synthetic-v1",
                "value": 3,
            }
        }
    }
    with Session(engine) as session, session.begin():
        session.add_all(
            [
                ResearchThread(
                    tid=1001,
                    first_pid=2001,
                    fid=7654321,
                    author_user_id=501,
                    agree_count=3,
                    field_sources_json=source,
                    source_api="aiotieba.get_threads",
                    observed_at=t1,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchPost(
                    pid=2001,
                    tid=1001,
                    fid=7654321,
                    author_user_id=501,
                    agree_count=3,
                    field_sources_json=source,
                    source_api="aiotieba.get_posts",
                    observed_at=t1,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
                ResearchComment(
                    cid=3001,
                    tid=1001,
                    parent_post_pid=2001,
                    fid=7654321,
                    author_user_id=777,
                    agree_count=3,
                    field_sources_json=source,
                    source_api="aiotieba.get_comments",
                    observed_at=t1,
                    source_payload_json={},
                    missing_reasons_json={},
                ),
            ]
        )

    with Session(engine) as session, session.begin():
        api_thread = ResearchThread(
            tid=1001,
            first_pid=2001,
            fid=7654321,
            author_user_id=501,
            agree_count=9,
            source_api="aiotieba.get_threads",
            observed_at=t2,
            source_payload_json={},
            missing_reasons_json={},
        )
        await _upsert_research_threads(_AsyncSessionAdapter(session), [api_thread], set())
        api_post = ResearchPost(
            pid=2001,
            tid=1001,
            fid=7654321,
            author_user_id=501,
            agree_count=9,
            source_api="aiotieba.get_posts",
            observed_at=t2,
            source_payload_json={},
            missing_reasons_json={},
        )
        await _upsert_content_rows(_AsyncSessionAdapter(session), ResearchPost, [api_post])
        api_comment = ResearchComment(
            cid=3001,
            tid=1001,
            parent_post_pid=2001,
            fid=7654321,
            author_user_id=777,
            agree_count=9,
            source_api="aiotieba.get_comments",
            observed_at=t2,
            source_payload_json={},
            missing_reasons_json={},
        )
        await _upsert_content_rows(_AsyncSessionAdapter(session), ResearchComment, [api_comment])

    with Session(engine) as session:
        rows = [
            session.get(ResearchThread, 1001),
            session.get(ResearchPost, 2001),
            session.get(ResearchComment, 3001),
        ]
        for row in rows:
            assert row.agree_count == 3
            assert row.field_sources_json["agree_count"]["verified_web"]["value"] == 3
            assert row.field_sources_json["agree_count"]["api_conflicts"] == [
                {
                    "source_api": row.source_api,
                    "observed_at": t2.isoformat(),
                    "value": 9,
                }
            ]
    engine.dispose()
