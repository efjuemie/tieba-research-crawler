from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session
from tiebameow.models.orm import Base
from tiebameow.models.orm import Post as BasePost

from src.research import client as research_client_module
from src.research.models import (
    RESEARCH_TABLES,
    MediaAsset,
    ResearchComment,
    ResearchContentObservation,
    ResearchFieldObservation,
    ResearchPost,
    ResearchThread,
)
from src.research.persistence import build_research_records, persist_research_page

TID = 987654321012345678
FIRST_PID = 987654321012345679
PARENT_PID = 987654321012345680
CID = 987654321012345681
FID = 7654321
AUTHOR_ID = 123456789012345678


class _SyncTransaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        self.transaction = self.session.begin()
        self.transaction.__enter__()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return self.transaction.__exit__(exc_type, exc, traceback)


class _AsyncSession:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.session.close()

    def begin(self):
        return _SyncTransaction(self.session)

    def get_bind(self):
        return self.session.get_bind()

    async def execute(self, statement):
        return self.session.execute(statement)

    async def scalar(self, statement):
        return self.session.scalar(statement)

    async def scalars(self, statement):
        return self.session.scalars(statement)


class _SessionMaker:
    def __init__(self, engine):
        self.engine = engine

    def __call__(self):
        return _AsyncSession(Session(self.engine))


class _Contents:
    def __init__(self, text, fragments=()):
        self.text = text
        self.objs = list(fragments)


class FragImage:
    src = "https://example.invalid/comment.jpg"
    origin_src = "https://example.invalid/comment-original.jpg"
    hash = "synthetic-image-hash"
    show_width = 320
    show_height = 240


def _sessionmaker():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[BasePost.__table__, *RESEARCH_TABLES])
    return engine, _SessionMaker(engine)


def _author(user_id=AUTHOR_ID, *, complete=True):
    return SimpleNamespace(
        user_id=user_id,
        user_name="preview-user" if complete else "",
        nick_name_new="preview nickname" if complete else "",
        portrait="preview-portrait" if complete else "",
        level=4 if complete else 0,
        glevel=9 if complete else 0,
        is_bawu=True if complete else False,
        ip="IP属地：浙江" if complete else "192.0.2.11",
        tieba_uid=0,
        icons=[],
    )


def _preview_page(
    observed_at: datetime,
    *,
    cid=CID,
    tid=TID,
    parent_pid=PARENT_PID,
    fid=FID,
    author_id=AUTHOR_ID,
    include_metadata=True,
    second_preview=None,
):
    thread_user = _author(777)
    thread = SimpleNamespace(
        tid=tid,
        pid=FIRST_PID,
        fid=fid,
        fname="synthetic forum",
        title="synthetic thread",
        create_time=1_750_000_000,
        author_id=thread_user.user_id,
        user=thread_user,
        contents=_Contents("thread body"),
        reply_num=2,
        agree=0,
        disagree=0,
        collect_num=0,
        view_num=0,
        share_num=0,
        type=0,
        is_share=False,
    )
    first_post = SimpleNamespace(
        pid=FIRST_PID,
        tid=tid,
        fid=fid,
        fname="synthetic forum",
        floor=1,
        create_time=1_750_000_000,
        author_id=thread_user.user_id,
        user=thread_user,
        contents=_Contents("thread body"),
        reply_num=1,
        comments=[],
    )
    preview = SimpleNamespace(
        pid=cid,
        tid=tid,
        ppid=parent_pid,
        fid=fid,
        author_id=author_id,
        user=_author(author_id, complete=include_metadata),
    )
    post_user = _author(888)
    reply_post = SimpleNamespace(
        pid=parent_pid,
        tid=tid,
        fid=fid,
        fname="synthetic forum",
        floor=2,
        create_time=1_750_000_010,
        author_id=post_user.user_id,
        user=post_user,
        contents=_Contents("reply body"),
        sign="",
        agree=0,
        disagree=0,
        reply_num=1,
        is_thread_author=False,
        comments=[preview],
    )
    posts = [first_post, reply_post]
    if second_preview is not None:
        other_parent = SimpleNamespace(**vars(reply_post))
        other_parent.pid = parent_pid + 1
        other_parent.floor = 3
        other_preview = SimpleNamespace(**vars(preview))
        other_preview.ppid = other_parent.pid
        other_parent.comments = [other_preview]
        posts.append(other_parent)
    return research_client_module.ResearchPage(
        raw=SimpleNamespace(thread=thread, objs=posts),
        dto=object(),
        observed_at=observed_at,
        source_api="aiotieba.get_posts",
    )


def _detail_page(
    observed_at: datetime,
    *,
    user=None,
    cid=CID,
    tid=TID,
    parent_pid=PARENT_PID,
    fid=FID,
    author_id=AUTHOR_ID,
):
    comment = SimpleNamespace(
        pid=cid,
        tid=tid,
        ppid=parent_pid,
        fid=fid,
        fname="synthetic forum",
        floor=1,
        create_time=1_750_000_020,
        author_id=author_id,
        user=user if user is not None else _author(author_id, complete=False),
        contents=_Contents("complete comment body", [FragImage()]),
        reply_to_id=0,
        agree=0,
        disagree=0,
        is_thread_author=False,
    )
    return research_client_module.ResearchPage(
        raw=SimpleNamespace(objs=[comment], forum=SimpleNamespace(fid=fid, fname="synthetic forum")),
        dto=object(),
        observed_at=observed_at,
        source_api="aiotieba.get_comments",
    )


def _aware(value):
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@pytest.mark.asyncio
async def test_preview_metadata_merges_before_and_after_detail_without_touching_content():
    for preview_first in (True, False):
        engine, sessionmaker = _sessionmaker()
        preview_time = datetime(2026, 1, 2, tzinfo=UTC)
        detail_time = datetime(2026, 1, 3, tzinfo=UTC)
        preview_page = _preview_page(preview_time)
        detail_page = _detail_page(detail_time)
        try:
            records = build_research_records(preview_page)
            assert records.comments == []
            assert len(records.api_field_observations) == 7
            if preview_first:
                await persist_research_page(sessionmaker, preview_page, run_id="preview-run")
                await persist_research_page(sessionmaker, preview_page, run_id="preview-run")
                with Session(engine) as database:
                    assert database.get(ResearchComment, CID) is None
                await persist_research_page(sessionmaker, detail_page)
                expected_run_id = "preview-run"
            else:
                await persist_research_page(sessionmaker, detail_page)
                await persist_research_page(sessionmaker, preview_page)
                await persist_research_page(sessionmaker, preview_page)
                expected_run_id = None

            with Session(engine) as database:
                comment = database.get(ResearchComment, CID)
                assert comment.text == "complete comment body"
                assert comment.ip_location_text == "IP属地：浙江"
                assert comment.ip_location_source == "api.comments_preview.author.ip"
                assert comment.forum_level == 4
                assert comment.global_growth_level == 9
                assert comment.is_bawu is True
                assert comment.author_user_name == "preview-user"
                assert comment.author_nickname == "preview nickname"
                assert comment.author_portrait == "preview-portrait"
                global_level_source = comment.field_sources_json["global_growth_level"]["verified_content_api"]
                assert global_level_source["source_kind"] == "api_raw"
                assert "verified_web" not in comment.field_sources_json["global_growth_level"]
                marker = database.scalar(
                    select(ResearchContentObservation).where(
                        ResearchContentObservation.owner_type == "comment",
                        ResearchContentObservation.owner_id == CID,
                    )
                )
                assert _aware(marker.observed_at) == detail_time
                media = database.scalars(
                    select(MediaAsset).where(MediaAsset.owner_type == "comment", MediaAsset.owner_id == CID)
                ).all()
                assert len(media) == 1
                assert media[0].src_url == FragImage.src
                observations = database.scalars(
                    select(ResearchFieldObservation).where(ResearchFieldObservation.target_id == CID)
                ).all()
                assert len(observations) == 7
                assert {row.source_kind for row in observations} == {"api_raw"}
                assert {row.run_id for row in observations} == {expected_run_id}
                assert all(row.task_key is None and row.source_url is None for row in observations)
                assert all(row.applied_status == "observed_only" and row.applied_at is None for row in observations)
                assert all(row.temporal_scope == "api_observation_time_not_publication_time" for row in observations)
                assert all(row.identity_evidence_json["proof"]["comment_cid"] == CID for row in observations)

            missing_detail = _detail_page(datetime(2026, 1, 4, tzinfo=UTC))
            await persist_research_page(sessionmaker, missing_detail)
            with Session(engine) as database:
                comment = database.get(ResearchComment, CID)
                assert comment.text == "complete comment body"
                assert comment.author_user_name == "preview-user"
                assert comment.forum_level == 4
                assert comment.global_growth_level == 9
                assert comment.ip_location_text == "IP属地：浙江"
                assert comment.field_sources_json["global_growth_level"]["verified_content_api"]["observed_at"] == (
                    preview_time.isoformat()
                )
        finally:
            engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity_change",
    ["parent", "author", "fid"],
)
async def test_preview_identity_conflict_rejects_page_before_any_write(identity_change):
    engine, sessionmaker = _sessionmaker()
    now = datetime(2026, 2, 1, tzinfo=UTC)
    try:
        await persist_research_page(sessionmaker, _detail_page(now))
        preview_kwargs = {}
        if identity_change == "parent":
            preview_kwargs["parent_pid"] = PARENT_PID + 1
        elif identity_change == "author":
            preview_kwargs["author_id"] = AUTHOR_ID + 1
        else:
            preview_kwargs["fid"] = FID + 1

        preview_page = _preview_page(
            datetime(2026, 2, 2, tzinfo=UTC),
            include_metadata=False,
            **preview_kwargs,
        )
        records = build_research_records(preview_page)
        assert records.api_field_observations == []
        expected_identity = (
            CID,
            TID,
            preview_kwargs.get("parent_pid", PARENT_PID),
            preview_kwargs.get("fid", FID),
            preview_kwargs.get("author_id", AUTHOR_ID),
        )
        assert records.api_preview_comment_identities == [expected_identity]
        before = _table_counts(engine)
        with pytest.raises(ValueError, match="conflicts with the existing comment CID"):
            await persist_research_page(
                sessionmaker,
                preview_page,
                run_id="conflict-run",
            )
        assert _table_counts(engine) == before
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_preview_conflicts_preserve_known_detail_metadata():
    engine, sessionmaker = _sessionmaker()
    detail_user = _author(complete=True)
    detail_user.user_name = "canonical detail name"
    detail_user.nick_name_new = "canonical detail nickname"
    detail_user.portrait = "canonical detail portrait"
    detail_user.level = 6
    detail_user.glevel = 10
    detail_user.ip = "IP属地：上海"
    try:
        await persist_research_page(
            sessionmaker,
            _detail_page(datetime(2026, 3, 1, tzinfo=UTC), user=detail_user),
        )
        await persist_research_page(
            sessionmaker,
            _preview_page(datetime(2026, 3, 2, tzinfo=UTC)),
            run_id="preview-conflict-run",
        )
        with Session(engine) as database:
            comment = database.get(ResearchComment, CID)
            assert comment.author_user_name == "canonical detail name"
            assert comment.forum_level == 6
            assert comment.global_growth_level == 10
            assert comment.ip_location_text == "IP属地：上海"
            conflict = comment.field_sources_json["forum_level"]["api_conflicts"][-1]
            assert conflict == {
                "source_kind": "api_raw",
                "source_path": "api.comments_preview.author.forum_level",
                "observed_at": "2026-03-02T00:00:00+00:00",
                "value": 4,
            }
            assert "verified_content_api" not in comment.field_sources_json["forum_level"]
    finally:
        engine.dispose()


def _table_counts(engine):
    with Session(engine) as database:
        return (
            database.scalar(select(func.count()).select_from(ResearchThread)),
            database.scalar(select(func.count()).select_from(ResearchPost)),
            database.scalar(select(func.count()).select_from(ResearchComment)),
            database.scalar(select(func.count()).select_from(ResearchContentObservation)),
            database.scalar(select(func.count()).select_from(ResearchFieldObservation)),
        )


def test_repeated_preview_cid_with_conflicting_tuple_fails_during_mapping():
    page = _preview_page(datetime(2026, 2, 3, tzinfo=UTC), second_preview=True)
    with pytest.raises(ValueError, match="conflicting identity tuples"):
        build_research_records(page)
