from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from aiotieba.enums import BawuType, ReqUInfo
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from tiebameow.models.orm import Base

from src.research.client import ResearchClient, ResearchRunCache
from src.research.models import ResearchUser
from src.research.persistence import (
    _author_user_id,
    _merge_research_user_batch,
    _upsert_research_users,
    build_research_records,
)


class StubResearchClient(ResearchClient):
    def __init__(self, cache, profile=None, roster=None):
        self.research_run_cache = cache
        self.profile = profile
        self.roster = roster
        self.calls = []

    async def get_user_info(self, user_id, /, *, require=ReqUInfo.ALL):
        self.calls.append(("profile", user_id, require))
        return self.profile

    async def _get_bawu_info_by_fid(self, fid):
        self.calls.append(("bawu", fid))
        return self.roster


def _profile(user_id=42):
    return SimpleNamespace(
        user_id=user_id,
        user_name="synthetic-user",
        nick_name_new="",
        nick_name_old="older-synthetic-name",
        portrait="synthetic-portrait",
        tieba_uid=987654321012345678,
        glevel=8,
        sign="must-not-persist",
        icons=["must-not-persist"],
        ip="must-not-persist",
    )


def _research_user_row(
    observed_at,
    *,
    user_name=None,
    nickname=None,
    portrait=None,
    tieba_uid=None,
    global_growth_level=None,
    payload=None,
    missing=None,
):
    return ResearchUser(
        user_id=42,
        user_name=user_name,
        nickname=nickname,
        portrait=portrait,
        tieba_uid=tieba_uid,
        global_growth_level=global_growth_level,
        first_observed_at=observed_at,
        last_observed_at=observed_at,
        source_api="synthetic.profile",
        observed_at=observed_at,
        source_payload_json=payload or {},
        missing_reasons_json=missing or {},
    )


def test_zero_author_id_is_unknown_not_an_identity_conflict():
    record = SimpleNamespace(author_id=42)
    user = SimpleNamespace(user_id=0)

    assert _author_user_id(record, user) == 42


@pytest.mark.asyncio
async def test_user_profile_uses_shared_run_cache_and_persists_allowlisted_current_fields():
    cache = ResearchRunCache()
    raw = _profile()
    first = StubResearchClient(cache, profile=raw)
    second = StubResearchClient(cache, profile=raw)

    page = await first.get_user_info_research(42)
    cached = await second.get_user_info_research(42)

    assert page is cached
    assert page.raw is raw
    assert page.source_api == "aiotieba.get_user_info"
    assert page.dto.status == "available"
    assert page.dto.response_user_id == 42
    assert page.dto.nickname == "older-synthetic-name"
    assert page.dto.global_growth_level == 8
    assert first.calls == [("profile", 42, ReqUInfo.ALL)]
    assert second.calls == []

    records = build_research_records(page)

    assert len(records.users) == 1
    user = records.users[0]
    assert user.user_name == "synthetic-user"
    assert user.nickname == "older-synthetic-name"
    assert user.tieba_uid == 987654321012345678
    assert user.global_growth_level == 8
    assert user.source_payload_json["observation_scope"] == "current_profile"
    assert "sign" not in user.source_payload_json
    assert "icons" not in user.source_payload_json
    assert "ip" not in user.source_payload_json


@pytest.mark.asyncio
async def test_profile_response_identity_mismatch_is_not_persistable():
    client = StubResearchClient(ResearchRunCache(), profile=_profile(user_id=99))

    page = await client.get_user_info_research(42)

    assert page.dto.status == "unavailable"
    assert page.dto.response_user_id == 99
    assert page.dto.missing_reasons["user_name"] == "response_user_id_mismatch"
    assert build_research_records(page).users == []


@pytest.mark.asyncio
async def test_profile_whitespace_identity_fields_are_missing_without_mutating_raw_values():
    raw = _profile()
    raw.user_name = "  "
    raw.nick_name_new = " \t"
    raw.nick_name_old = "\n"
    raw.portrait = "\t"
    client = StubResearchClient(ResearchRunCache(), profile=raw)

    page = await client.get_user_info_research(42)

    assert page.dto.user_name is None
    assert page.dto.nickname is None
    assert page.dto.portrait is None
    assert page.dto.missing_reasons["user_name"] == "not_returned_by_get_user_info_4_8_2"
    assert page.dto.missing_reasons["nickname"] == "not_returned_by_get_user_info_4_8_2"
    assert page.dto.missing_reasons["portrait"] == "not_returned_by_get_user_info_4_8_2"
    assert raw.user_name == "  "
    assert raw.nick_name_new == " \t"
    assert raw.nick_name_old == "\n"
    assert raw.portrait == "\t"


@pytest.mark.asyncio
async def test_bawu_roster_is_cached_by_fid_and_projects_only_exact_user_id():
    cache = ResearchRunCache()
    raw = SimpleNamespace(
        admin=[],
        manager=[SimpleNamespace(user_id=42), SimpleNamespace(user_id=100, user_name="same-name")],
        voice_editor=[],
        image_editor=[],
        video_editor=[],
        broadcast_editor=[],
        journal_chief_editor=[],
        journal_editor=[],
        profess_admin=[],
        fourth_admin=[],
    )
    first = StubResearchClient(cache, roster=raw)
    second = StubResearchClient(cache, roster=raw)

    matched = await first.get_bawu_info_research(7654, 42)
    not_listed = await second.get_bawu_info_research(7654, 1000)

    assert first.calls == [("bawu", 7654)]
    assert second.calls == []
    assert matched.raw is raw
    assert matched.dto.status == "available"
    assert matched.dto.response_user_id == 42
    assert matched.dto.is_bawu is True
    assert matched.dto.role_type == BawuType.MANAGER.value
    assert matched.dto.role_name == "小吧主"
    assert not_listed.dto.status == "not_listed"
    assert not_listed.dto.response_user_id is None
    assert not_listed.dto.is_bawu is None
    assert not_listed.dto.missing_reasons["is_bawu"] == "not_listed_in_observed_roster"

    records = build_research_records(matched)
    assert records.users == []
    assert len(records.user_forum_snapshots) == 1
    snapshot = records.user_forum_snapshots[0]
    assert snapshot.user_id == 42
    assert snapshot.fid == 7654
    assert snapshot.role_type == BawuType.MANAGER.value
    assert snapshot.role_name == "小吧主"
    assert snapshot.is_bawu is True
    assert snapshot.source_api == "aiotieba.get_bawu_info"

    absent_records = build_research_records(not_listed)
    absent_snapshot = absent_records.user_forum_snapshots[0]
    assert absent_snapshot.is_bawu is None
    assert absent_snapshot.status == "not_listed"
    assert absent_snapshot.missing_reasons_json["is_bawu"] == "not_listed_in_observed_roster"


@pytest.mark.parametrize("reverse_input", [False, True])
def test_research_user_batch_merge_does_not_carry_values_across_observation_times(reverse_input):
    first_at = datetime(2025, 1, 1, tzinfo=UTC)
    second_at = datetime(2025, 1, 2, tzinfo=UTC)
    latest_at = datetime(2025, 1, 3, tzinfo=UTC)
    observations = [
        _research_user_row(
            first_at,
            user_name="older-name",
            nickname="older-nickname",
            portrait="older-portrait",
            tieba_uid=101,
            global_growth_level=5,
            payload={"sample": "older"},
            missing={"observation": "older"},
        ),
        _research_user_row(
            second_at,
            user_name="newer-name",
            portrait="newer-portrait",
            tieba_uid=202,
            global_growth_level=8,
            payload={"sample": "middle"},
            missing={"observation": "middle"},
        ),
        _research_user_row(
            latest_at,
            payload={"sample": "latest-sparse"},
            missing={"user_name": "latest raw missing"},
        ),
    ]
    if reverse_input:
        observations.reverse()

    merged = _merge_research_user_batch(observations)

    assert [row.observed_at for row in merged] == [first_at, second_at, latest_at]
    assert [row.user_name for row in merged] == ["older-name", "newer-name", None]
    assert [row.nickname for row in merged] == ["older-nickname", None, None]
    assert [row.portrait for row in merged] == ["older-portrait", "newer-portrait", None]
    assert [row.tieba_uid for row in merged] == [101, 202, None]
    assert [row.global_growth_level for row in merged] == [5, 8, None]
    assert merged[-1].first_observed_at == latest_at
    assert merged[-1].last_observed_at == latest_at
    assert merged[-1].source_payload_json == {"sample": "latest-sparse"}
    assert merged[-1].missing_reasons_json == {"user_name": "latest raw missing"}


@pytest.mark.parametrize("sparse_last", [False, True])
def test_research_user_batch_merge_same_timestamp_preserves_rich_values_in_both_orders(sparse_last):
    observed_at = datetime(2025, 1, 1, tzinfo=UTC)
    rich = _research_user_row(
        observed_at,
        user_name="same-time-name",
        nickname="same-time-nickname",
        portrait="same-time-portrait",
        tieba_uid=101,
        global_growth_level=5,
        payload={"sample": "rich"},
    )
    sparse = _research_user_row(
        observed_at,
        payload={"sample": "sparse"},
        missing={"user_name": "same-time missing"},
    )
    observations = [rich, sparse] if sparse_last else [sparse, rich]

    [merged] = _merge_research_user_batch(observations)

    assert merged.user_name == "same-time-name"
    assert merged.nickname == "same-time-nickname"
    assert merged.portrait == "same-time-portrait"
    assert merged.tieba_uid == 101
    assert merged.global_growth_level == 5
    if sparse_last:
        assert merged.source_payload_json == {"sample": "sparse"}
        assert merged.missing_reasons_json["retained_last_known"]["nickname"]
    else:
        assert merged.source_payload_json == {"sample": "rich"}
        assert "retained_last_known" not in merged.missing_reasons_json


@pytest.mark.asyncio
async def test_user_batch_timestamps_do_not_backdate_database_fields_and_retained_notes_merge():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[ResearchUser.__table__])
    first_at = datetime(2025, 1, 1, tzinfo=UTC)
    database_at = datetime(2025, 1, 2, tzinfo=UTC)
    latest_at = datetime(2025, 1, 3, tzinfo=UTC)
    session = _SyncAsyncSession(engine)
    try:
        database_row = _research_user_row(
            database_at,
            user_name="database-newer-name",
            payload={"sample": "database-t2"},
        )
        older_batch_row = _research_user_row(
            first_at,
            user_name="batch-older-name",
            payload={"sample": "batch-t1"},
        )
        current_rich_row = _research_user_row(
            latest_at,
            nickname="batch-current-nickname",
            payload={"sample": "batch-t3-rich"},
        )
        current_sparse_row = _research_user_row(
            latest_at,
            payload={"sample": "batch-t3-sparse"},
            missing={"user_name": "current sparse name", "nickname": "current sparse nickname"},
        )

        with session.session.begin():
            await _upsert_research_users(session, [database_row])
            await _upsert_research_users(
                session,
                [older_batch_row, current_rich_row, current_sparse_row],
            )

        with Session(engine) as verification_session:
            user = verification_session.get(ResearchUser, 42)
            assert user is not None
            assert user.user_name == "database-newer-name"
            assert user.nickname == "batch-current-nickname"
            assert user.observed_at == latest_at.replace(tzinfo=None)
            assert user.first_observed_at == first_at.replace(tzinfo=None)
            assert user.last_observed_at == latest_at.replace(tzinfo=None)
            assert user.source_payload_json == {"sample": "batch-t3-sparse"}
            assert user.missing_reasons_json == {
                "user_name": "current sparse name",
                "nickname": "current sparse nickname",
                "retained_last_known": {
                    "nickname": "retained_last_known_due_to_missing_current_observation",
                    "user_name": "retained_last_known_due_to_missing_current_observation",
                },
            }
    finally:
        session.session.close()
        engine.dispose()


class _SyncTransaction:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        self.transaction = self.session.begin()
        self.transaction.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.transaction.__exit__(exc_type, exc_value, traceback)


class _SyncAsyncSession:
    def __init__(self, engine):
        self.session = Session(engine)

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


@pytest.mark.asyncio
async def test_user_upsert_retains_concurrently_inserted_fields_and_marks_retention(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'concurrent-user-upsert.db'}")
    Base.metadata.create_all(engine, tables=[ResearchUser.__table__])
    observed_at = datetime(2025, 1, 2, tzinfo=UTC)
    rich = _research_user_row(
        datetime(2025, 1, 1, tzinfo=UTC),
        user_name="concurrent-rich-name",
        nickname="concurrent-rich-nickname",
        portrait="concurrent-rich-portrait",
        tieba_uid=303,
        global_growth_level=9,
        payload={"sample": "concurrent-rich"},
    )
    sparse = _research_user_row(
        observed_at,
        payload={"sample": "current-sparse"},
        missing={"user_name": "missing from current response"},
    )

    class InterleavedInsertSession(_SyncAsyncSession):
        def __init__(self, engine, competing_row):
            super().__init__(engine)
            self.competing_row = competing_row
            self.inserted_concurrently = False

        async def execute(self, statement):
            statement_table = getattr(statement, "table", None)
            if not self.inserted_concurrently and getattr(statement_table, "name", None) == ResearchUser.__tablename__:
                with Session(engine) as competing_session, competing_session.begin():
                    competing_session.add(self.competing_row)
                self.inserted_concurrently = True
            return await super().execute(statement)

    try:
        session = InterleavedInsertSession(engine, rich)
        with session.session.begin():
            await _upsert_research_users(session, [sparse])
        assert session.inserted_concurrently is True

        with Session(engine) as verification_session:
            user = verification_session.get(ResearchUser, 42)
            assert user is not None
            assert user.user_name == "concurrent-rich-name"
            assert user.nickname == "concurrent-rich-nickname"
            assert user.portrait == "concurrent-rich-portrait"
            assert user.tieba_uid == 303
            assert user.global_growth_level == 9
            assert user.source_payload_json == {"sample": "current-sparse"}
            assert user.missing_reasons_json == {
                "user_name": "missing from current response",
                "retained_last_known": {
                    "user_name": "retained_last_known_due_to_missing_current_observation",
                    "nickname": "retained_last_known_due_to_missing_current_observation",
                    "portrait": "retained_last_known_due_to_missing_current_observation",
                    "tieba_uid": "retained_last_known_due_to_missing_current_observation",
                    "global_growth_level": "retained_last_known_due_to_missing_current_observation",
                },
            }
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_invalid_profile_id_does_not_call_sdk():
    client = StubResearchClient(ResearchRunCache(), profile=_profile())

    page = await client.get_user_info_research(0)

    assert page.dto.status == "unavailable"
    assert page.dto.missing_reasons["user_name"] == "invalid_user_id"
    assert client.calls == []
