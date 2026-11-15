from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from aiotieba.api._classdef.contents import FragAt, FragText
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
    reply_target=None,
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
    if reply_target is not None:
        reply_name, reply_user_id = reply_target
        preview.reply_to_id = reply_user_id
        preview.contents = _Contents(
            "reply body",
            [FragText("回复 "), FragAt(f"@{reply_name}", reply_user_id), FragText("reply body")],
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
    reply_to_user_id=0,
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
        reply_to_id=reply_to_user_id,
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


@pytest.mark.asyncio
async def test_preview_reply_display_name_requires_leading_reply_prefix_and_matching_target_uid():
    engine, sessionmaker = _sessionmaker()
    observed_at = datetime(2026, 4, 2, tzinfo=UTC)
    target_uid = 987654321012345682
    preview_page = _preview_page(observed_at, reply_target=("synthetic target", target_uid))
    detail_page = _detail_page(datetime(2026, 4, 1, tzinfo=UTC), reply_to_user_id=target_uid)
    try:
        records = build_research_records(preview_page)
        target_observation = next(
            observation
            for observation in records.api_field_observations
            if observation.field_name == "reply_to_display_name"
        )
        assert target_observation.target_id == CID
        assert target_observation.normalized_value_json == "synthetic target"
        assert target_observation.raw_value_json["reply_to_user_id"] == target_uid
        await persist_research_page(sessionmaker, detail_page)
        await persist_research_page(sessionmaker, preview_page)

        non_reply_page = _preview_page(
            datetime(2026, 4, 3, tzinfo=UTC),
            reply_target=("synthetic mention", target_uid),
        )
        comment_preview = non_reply_page.raw.objs[1].comments[0]
        comment_preview.contents = _Contents(
            "a body with a mention",
            [FragText("ordinary text "), FragAt("@synthetic mention", target_uid)],
        )
        non_reply_records = build_research_records(non_reply_page)
        assert all(
            observation.field_name != "reply_to_display_name"
            for observation in non_reply_records.api_field_observations
        )

        with Session(engine) as database:
            comment = database.get(ResearchComment, CID)
            assert comment.reply_to_user_id == target_uid
            assert comment.reply_to_display_name == "synthetic target"
            assert comment.field_sources_json["reply_to_display_name"]["verified_content_api"]["source_path"] == (
                "api.comments_preview.reply_target.display_name"
            )
            assert comment.text == "complete comment body"
            marker = database.scalar(
                select(ResearchContentObservation).where(
                    ResearchContentObservation.owner_type == "comment",
                    ResearchContentObservation.owner_id == CID,
                )
            )
            assert _aware(marker.observed_at) == detail_page.observed_at
            assert database.scalar(
                select(func.count()).select_from(MediaAsset).where(
                    MediaAsset.owner_type == "comment", MediaAsset.owner_id == CID
                )
            ) == 1
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_preview_reply_name_is_suppressed_when_canonical_reply_uid_conflicts():
    engine, sessionmaker = _sessionmaker()
    target_uid = 987654321012345682
    canonical_uid = target_uid + 1
    try:
        await persist_research_page(
            sessionmaker,
            _detail_page(datetime(2026, 5, 1, tzinfo=UTC), reply_to_user_id=canonical_uid),
        )
        await persist_research_page(
            sessionmaker,
            _preview_page(
                datetime(2026, 5, 2, tzinfo=UTC),
                reply_target=("synthetic target", target_uid),
            ),
        )

        with Session(engine) as database:
            comment = database.get(ResearchComment, CID)
            assert comment.reply_to_user_id == canonical_uid
            assert comment.reply_to_display_name is None
            assert comment.missing_reasons_json["reply_to_display_name"] == "api_reply_target_uid_conflict"
            conflict = comment.field_sources_json["reply_to_display_name"]["api_conflicts"][-1]
            assert conflict["value"] == "synthetic target"
            assert comment.source_payload_json["reply_target_candidates"][-1]["raw"]["reply_to_user_id"] == target_uid
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_preview_verified_reply_uid_keeps_name_paired_across_later_detail_refresh():
    engine, sessionmaker = _sessionmaker()
    first_reply_uid = 987654321012345682
    try:
        await persist_research_page(
            sessionmaker,
            _detail_page(datetime(2026, 5, 10, tzinfo=UTC), reply_to_user_id=first_reply_uid),
        )
        await persist_research_page(
            sessionmaker,
            _preview_page(
                datetime(2026, 5, 11, tzinfo=UTC),
                reply_target=("synthetic target A", first_reply_uid),
            ),
        )
        await persist_research_page(
            sessionmaker,
            _detail_page(datetime(2026, 5, 12, tzinfo=UTC), reply_to_user_id=first_reply_uid + 1),
        )

        with Session(engine) as database:
            comment = database.get(ResearchComment, CID)
            assert comment.reply_to_user_id == first_reply_uid
            assert comment.reply_to_display_name == "synthetic target A"
            assert comment.field_sources_json["reply_to_user_id"]["verified_content_api"]["value"] == first_reply_uid
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_verified_web_reply_uid_suppresses_mismatched_detail_name_but_keeps_raw_candidate():
    engine, sessionmaker = _sessionmaker()
    observed_at = datetime(2026, 5, 20, tzinfo=UTC)
    retained_uid = 987654321012345682
    incoming_uid = retained_uid + 1
    try:
        await persist_research_page(sessionmaker, _detail_page(observed_at, reply_to_user_id=retained_uid))
        with Session(engine) as database, database.begin():
            comment = database.get(ResearchComment, CID)
            comment.reply_to_user_id = retained_uid
            comment.reply_to_display_name = None
            comment.field_sources_json = {
                "reply_to_user_id": {
                    "verified_web": {
                        "source_kind": "web_network",
                        "value": retained_uid,
                        "observed_at": observed_at.isoformat(),
                    }
                }
            }

        detail_page = _detail_page(datetime(2026, 5, 21, tzinfo=UTC), reply_to_user_id=incoming_uid)
        detail_comment = detail_page.raw.objs[0]
        detail_comment.contents = _Contents(
            "reply body",
            [FragText("回复 "), FragAt("@synthetic target B", incoming_uid), FragText("reply body")],
        )
        await persist_research_page(sessionmaker, detail_page)

        with Session(engine) as database:
            comment = database.get(ResearchComment, CID)
            assert comment.reply_to_user_id == retained_uid
            assert comment.reply_to_display_name is None
            assert comment.missing_reasons_json["reply_to_display_name"] == "api_reply_target_uid_conflict"
            assert comment.source_payload_json["reply_target_evidence"]["raw"]["reply_to_user_id"] == incoming_uid
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("bound_uid", "confidence"),
    [(987654321012345682, "explicit_prefix_type4_uid"), (None, "display_name_only")],
)
async def test_verified_web_reply_name_binding_blocks_incoming_api_uid_without_name(bound_uid, confidence):
    engine, sessionmaker = _sessionmaker()
    observed_at = datetime(2026, 5, 30, tzinfo=UTC)
    incoming_uid = (bound_uid or 987654321012345682) + 1
    try:
        await persist_research_page(sessionmaker, _detail_page(observed_at))
        with Session(engine) as database, database.begin():
            comment = database.get(ResearchComment, CID)
            comment.reply_to_display_name = "synthetic target A"
            comment.field_sources_json = {
                "reply_to_display_name": {
                    "verified_web": {
                        "source_kind": "web_network",
                        "value": "synthetic target A",
                        "reply_target_user_id": bound_uid,
                        "confidence": confidence,
                        "observed_at": observed_at.isoformat(),
                    }
                }
            }

        await persist_research_page(
            sessionmaker,
            _detail_page(datetime(2026, 5, 31, tzinfo=UTC), reply_to_user_id=incoming_uid),
        )
        await persist_research_page(
            sessionmaker,
            _preview_page(
                datetime(2026, 6, 1, tzinfo=UTC),
                reply_target=("synthetic target B", incoming_uid),
            ),
        )

        with Session(engine) as database:
            comment = database.get(ResearchComment, CID)
            assert comment.reply_to_user_id is None
            assert comment.reply_to_display_name == "synthetic target A"
            assert comment.missing_reasons_json["reply_to_user_id"] in {
                "api_reply_target_uid_conflicts_with_verified_name_binding",
                "api_reply_target_uid_unlinked_from_display_name",
            }
            assert comment.source_payload_json["reply_target_candidates"][-1]["raw"]["reply_to_user_id"] == (
                incoming_uid
            )
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_api_reply_name_binding_is_saved_and_blocks_later_mismatched_uid():
    engine, sessionmaker = _sessionmaker()
    bound_uid = 987654321012345682
    try:
        await persist_research_page(
            sessionmaker,
            _detail_page(datetime(2026, 6, 1, tzinfo=UTC)),
        )
        await persist_research_page(
            sessionmaker,
            _preview_page(
                datetime(2026, 6, 2, tzinfo=UTC),
                reply_target=("synthetic target A", bound_uid),
            ),
        )
        with Session(engine) as database, database.begin():
            comment = database.get(ResearchComment, CID)
            assert comment.field_sources_json["reply_to_display_name"]["verified_content_api"][
                "reply_target_user_id"
            ] == bound_uid
            comment.reply_to_user_id = None
            sources = dict(comment.field_sources_json)
            sources.pop("reply_to_user_id", None)
            comment.field_sources_json = sources

        await persist_research_page(
            sessionmaker,
            _detail_page(datetime(2026, 6, 3, tzinfo=UTC), reply_to_user_id=bound_uid + 1),
        )

        with Session(engine) as database:
            comment = database.get(ResearchComment, CID)
            assert comment.reply_to_user_id == bound_uid
            assert comment.reply_to_display_name == "synthetic target A"
            assert any(
                candidate["raw"].get("reply_to_user_id") == bound_uid + 1
                for candidate in comment.source_payload_json["reply_target_candidates"]
            )
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_matching_explicit_api_reply_name_and_uid_upgrade_web_display_only_name():
    engine, sessionmaker = _sessionmaker()
    target_uid = 987654321012345682
    try:
        await persist_research_page(
            sessionmaker,
            _detail_page(datetime(2026, 6, 10, tzinfo=UTC)),
        )
        with Session(engine) as database, database.begin():
            comment = database.get(ResearchComment, CID)
            comment.reply_to_display_name = "synthetic target A"
            comment.field_sources_json = {
                "reply_to_display_name": {
                    "verified_web": {
                        "source_kind": "web_network",
                        "value": "synthetic target A",
                        "reply_target_user_id": None,
                        "confidence": "display_name_only",
                    }
                }
            }

        await persist_research_page(
            sessionmaker,
            _preview_page(
                datetime(2026, 6, 11, tzinfo=UTC),
                reply_target=("synthetic target A", target_uid),
            ),
        )

        with Session(engine) as database:
            comment = database.get(ResearchComment, CID)
            assert comment.reply_to_user_id == target_uid
            assert comment.reply_to_display_name == "synthetic target A"
            assert comment.field_sources_json["reply_to_display_name"]["verified_content_api"][
                "reply_target_user_id"
            ] == target_uid
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_api_display_only_reply_name_blocks_uid_only_then_upgrades_with_matching_pair():
    engine, sessionmaker = _sessionmaker()
    target_uid = 987654321012345682
    try:
        await persist_research_page(
            sessionmaker,
            _detail_page(datetime(2026, 6, 20, tzinfo=UTC)),
        )
        display_only = _detail_page(datetime(2026, 6, 21, tzinfo=UTC))
        display_comment = display_only.raw.objs[0]
        display_comment.contents = _Contents(
            "reply body",
            [FragText("回复 "), FragAt("@synthetic target A", 0), FragText("reply body")],
        )
        await persist_research_page(sessionmaker, display_only)

        await persist_research_page(
            sessionmaker,
            _detail_page(datetime(2026, 6, 22, tzinfo=UTC), reply_to_user_id=target_uid),
        )
        with Session(engine) as database:
            comment = database.get(ResearchComment, CID)
            assert comment.reply_to_user_id is None
            assert comment.reply_to_display_name == "synthetic target A"
            assert comment.field_sources_json["reply_to_display_name"]["verified_content_api"][
                "confidence"
            ] == "display_name_only"
            assert comment.field_sources_json["reply_to_display_name"]["verified_content_api"][
                "reply_target_user_id"
            ] is None

        matched = _detail_page(datetime(2026, 6, 23, tzinfo=UTC), reply_to_user_id=target_uid)
        matched_comment = matched.raw.objs[0]
        matched_comment.contents = _Contents(
            "reply body",
            [FragText("回复 "), FragAt("@synthetic target A", target_uid), FragText("reply body")],
        )
        await persist_research_page(sessionmaker, matched)

        with Session(engine) as database:
            comment = database.get(ResearchComment, CID)
            assert comment.reply_to_user_id == target_uid
            assert comment.reply_to_display_name == "synthetic target A"
            assert comment.field_sources_json["reply_to_display_name"]["verified_content_api"][
                "reply_target_user_id"
            ] == target_uid
    finally:
        engine.dispose()


def test_growth_icon_url_requires_explicit_api_field():
    user = _author()
    user.global_growth_icon_url = "https://example.invalid/growth-icon.webp"
    page = _detail_page(datetime(2026, 6, 1, tzinfo=UTC), user=user)

    record = build_research_records(page).comments[0]
    assert record.global_growth_icon_url == user.global_growth_icon_url
    assert "global_growth_icon_url" not in record.missing_reasons_json
    assert record.source_payload_json["author"]["global_growth_icon_url"] == user.global_growth_icon_url


def test_growth_icon_url_is_marked_unavailable_without_sdk_field():
    record = build_research_records(_detail_page(datetime(2026, 6, 1, tzinfo=UTC))).comments[0]

    assert record.global_growth_icon_url is None
    assert record.missing_reasons_json["global_growth_icon_url"] == "not_exposed_by_aiotieba_parsed_content"


def test_comments_mapper_keeps_exact_reply_prefix_mention_provenance():
    target_uid = 987654321012345682
    page = _detail_page(datetime(2026, 7, 1, tzinfo=UTC), reply_to_user_id=target_uid)
    comment = page.raw.objs[0]
    comment.contents = _Contents(
        "reply body",
        [FragText("回复 "), FragAt("@synthetic target", target_uid), FragText("reply body")],
    )

    record = build_research_records(page).comments[0]

    assert record.reply_to_user_id == target_uid
    assert record.reply_to_display_name == "synthetic target"
    assert record.field_sources_json["reply_to_display_name"]["verified_content_api"]["confidence"] == (
        "explicit_leading_reply_prefix_and_type4_uid"
    )
    assert record.source_payload_json["reply_target_evidence"]["raw"]["mention"]["user_id"] == target_uid


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
