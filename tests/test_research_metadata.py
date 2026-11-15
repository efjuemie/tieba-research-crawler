import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from stat import S_IMODE
from time import time_ns
from types import SimpleNamespace

import pytest
from aiotieba import Client as AiotiebaClient
from aiotieba.enums import ThreadType
from sqlalchemy import create_engine, delete, inspect, select, text
from sqlalchemy.orm import Session
from tiebameow.models.orm import Base
from tiebameow.models.orm import Post as BasePost

from src.research import __main__ as research_cli_module
from src.research import client as research_client_module
from src.research.__main__ import main as research_main
from src.research.client import ResearchClient, ResearchRunCache
from src.research.credentials import load_bduss, save_bduss, save_bduss_interactively
from src.research.migration import RESEARCH_MIGRATION_TABLES, create_research_tables
from src.research.models import (
    RESEARCH_TABLES,
    MediaAsset,
    ResearchComment,
    ResearchContentObservation,
    ResearchFieldObservation,
    ResearchPost,
    ResearchThread,
    ResearchUser,
    UserForumSnapshot,
)
from src.research.persistence import build_research_records, persist_research_page


class FragImage:
    def __init__(self, src):
        self.src = src
        self.origin_src = f"{src}?original=1"
        self.hash = "synthetic-hash"
        self.show_width = 640
        self.show_height = 480


class Contents:
    def __init__(self, text, fragments=()):
        self.text = text
        self.objs = list(fragments)


def _synthetic_user(user_id=123456789012345678, ip="203.0.113.7"):
    return SimpleNamespace(
        user_id=user_id,
        tieba_uid=987654321098765432,
        user_name="synthetic-user",
        nick_name_new="合成昵称",
        portrait="synthetic-portrait",
        level=0,
        glevel=0,
        is_bawu=False,
        icons=["https://example.invalid/icon.svg"],
        ip=ip,
    )


def _synthetic_post(pid, tid, floor, user, contents):
    return SimpleNamespace(
        pid=pid,
        tid=tid,
        fid=7654321,
        fname="合成贴吧",
        floor=floor,
        create_time=1_750_000_000,
        author_id=user.user_id,
        user=user,
        contents=contents,
        sign="",
        agree=0,
        disagree=0,
        reply_num=0,
        is_thread_author=False,
    )


def _synthetic_thread(tid, pid, user, contents):
    return SimpleNamespace(
        tid=tid,
        pid=pid,
        fid=7654321,
        fname="合成贴吧",
        title="合成主题",
        create_time=1_750_000_000,
        author_id=user.user_id,
        user=user,
        contents=contents,
        last_time=1_750_000_001,
        reply_num=0,
        agree=0,
        disagree=0,
        collect_num=0,
        view_num=0,
        share_num=0,
        type=ThreadType.ARTICLE,
        is_top=False,
        is_good=False,
        is_livepost=False,
        is_hide=False,
        is_share=False,
    )


def _get_posts_page(
    tid,
    first_pid,
    user,
    observed_at,
    first_contents,
    *,
    reply_contents=None,
    title="合成主题",
    reply_count=100,
    first_subreply_count=2,
    collect_num=0,
):
    thread = _synthetic_thread(tid, first_pid, user, first_contents)
    thread.title = title
    thread.reply_num = reply_count
    thread.collect_num = collect_num
    first_post = _synthetic_post(first_pid, tid, 1, user, first_contents)
    first_post.reply_num = first_subreply_count
    posts = [first_post]
    if reply_contents is not None:
        posts.append(_synthetic_post(first_pid + 1, tid, 2, user, reply_contents))
    return research_client_module.ResearchPage(
        raw=SimpleNamespace(thread=thread, objs=posts),
        dto=object(),
        observed_at=observed_at,
        source_api="aiotieba.get_posts",
    )


def _get_threads_page(tid, first_pid, user, observed_at, *, title, reply_count, last_time):
    thread = _synthetic_thread(tid, first_pid, user, Contents(""))
    thread.title = title
    thread.reply_num = reply_count
    thread.last_time = last_time
    return research_client_module.ResearchPage(
        raw=SimpleNamespace(objs=[thread]),
        dto=object(),
        observed_at=observed_at,
        source_api="aiotieba.get_threads",
    )


def _get_comments_page(tid, cid, parent_pid, user, observed_at, contents):
    comment = SimpleNamespace(
        pid=cid,
        tid=tid,
        ppid=parent_pid,
        fid=7654321,
        fname="合成贴吧",
        floor=1,
        create_time=1_750_000_010,
        author_id=user.user_id,
        user=user,
        contents=contents,
        reply_to_id=0,
        agree=0,
        disagree=0,
        is_thread_author=False,
    )
    return research_client_module.ResearchPage(
        raw=SimpleNamespace(objs=[comment], forum=SimpleNamespace(fid=7654321, fname="合成贴吧")),
        dto=object(),
        observed_at=observed_at,
        source_api="aiotieba.get_comments",
    )


class SyncSessionTransaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        self.transaction = self.session.begin()
        self.transaction.__enter__()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return self.transaction.__exit__(exc_type, exc, traceback)


class SQLiteAsyncSession:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.session.close()

    def begin(self):
        return SyncSessionTransaction(self.session)

    def get_bind(self):
        return self.session.get_bind()

    async def execute(self, statement):
        return self.session.execute(statement)

    async def scalar(self, statement):
        return self.session.scalar(statement)

    async def scalars(self, statement):
        return self.session.scalars(statement)


class SQLiteResearchSessionMaker:
    def __init__(self, engine):
        self.engine = engine

    def __call__(self):
        return SQLiteAsyncSession(Session(self.engine))


def _sqlite_research_sessionmaker():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[BasePost.__table__, *RESEARCH_TABLES])
    return engine, SQLiteResearchSessionMaker(engine)


class FakeAsyncConnection:
    def __init__(self, connection):
        self.connection = connection

    async def run_sync(self, function, *args, **kwargs):
        return function(self.connection, *args, **kwargs)


class FakeAsyncBegin:
    def __init__(self, engine):
        self.engine = engine

    async def __aenter__(self):
        self.context = self.engine.begin()
        connection = self.context.__enter__()
        return FakeAsyncConnection(connection)

    async def __aexit__(self, exc_type, exc, traceback):
        self.context.__exit__(exc_type, exc, traceback)


class FakeAsyncEngine:
    def __init__(self, sync_engine):
        self.sync_engine = sync_engine

    def begin(self):
        return FakeAsyncBegin(self.sync_engine)


class StubResearchClient(ResearchClient):
    def __init__(self, raw):
        self.raw = raw
        self.calls = []

    async def get_threads(self, fname_or_fid, pn=1, *, rn=30, sort=None, is_good=False):
        self.calls.append((fname_or_fid, pn, rn, sort, is_good))
        return self.raw

    async def get_posts(
        self,
        tid,
        pn=1,
        *,
        rn=30,
        sort=None,
        only_thread_author=False,
        with_comments=False,
        comment_sort_by_agree=True,
        comment_rn=4,
    ):
        self.calls.append((tid, pn, rn, sort, only_thread_author, with_comments, comment_sort_by_agree, comment_rn))
        return self.raw

    async def get_comments(self, tid, pid, pn=1, *, is_comment=False):
        self.calls.append((tid, pid, pn, is_comment))
        return self.raw


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "converter_name", "args", "kwargs", "expected_source"),
    [
        (
            "get_threads_research",
            "convert_aiotieba_threads",
            ("恐艾",),
            {"pn": 3, "rn": 20},
            "aiotieba.get_threads",
        ),
        (
            "get_posts_research",
            "convert_aiotieba_posts",
            (123456789012345678,),
            {"pn": 2, "rn": 10, "with_comments": True},
            "aiotieba.get_posts",
        ),
        (
            "get_comments_research",
            "convert_aiotieba_comments",
            (123456789012345678, 987654321098765432),
            {"pn": 4},
            "aiotieba.get_comments",
        ),
    ],
)
async def test_research_request_captures_raw_and_converts_once(
    monkeypatch,
    method_name,
    converter_name,
    args,
    kwargs,
    expected_source,
):
    raw = SimpleNamespace(page=SimpleNamespace(total_count=0))
    dto = SimpleNamespace(page=SimpleNamespace(total_count=0))
    converter_calls = []
    monkeypatch.setattr(
        research_client_module,
        converter_name,
        lambda value: converter_calls.append(value) or dto,
    )
    client = StubResearchClient(raw)

    page = await getattr(client, method_name)(*args, **kwargs)

    assert page.raw is raw
    assert page.dto is dto
    assert page.source_api == expected_source
    assert page.observed_at.tzinfo is UTC
    assert converter_calls == [raw]
    assert len(client.calls) == 1


def test_research_page_preserves_large_ids_in_raw_and_dto():
    large_id = 987654321098765432
    raw = SimpleNamespace(pid=large_id)
    dto = SimpleNamespace(pid=large_id)

    page = research_client_module.ResearchPage(
        raw=raw,
        dto=dto,
        observed_at=datetime.now(UTC),
        source_api="synthetic",
    )

    assert page.raw.pid == large_id
    assert page.dto.pid == large_id


class StubForumLookupClient(ResearchClient):
    def __init__(self, cache):
        self.research_run_cache = cache
        self.lookup_calls = []

    async def get_user_info(self, user_id, *, require):
        self.lookup_calls.append(("user_info", user_id))
        return SimpleNamespace(portrait="portrait-1", tieba_uid=112233445566778899)

    async def _get_user_forum_info_by_portrait(self, fid, portrait):
        self.lookup_calls.append(("forum_info", fid, portrait))
        return SimpleNamespace(
            user=SimpleNamespace(user_id=123456789012345678, portrait=portrait, show_name="synthetic"),
            fname="合成贴吧",
            level=0,
            level_name="",
            role_name="",
        )


@pytest.mark.asyncio
async def test_user_forum_lookup_uses_shared_run_cache():
    cache = ResearchRunCache()
    first = StubForumLookupClient(cache)
    second = StubForumLookupClient(cache)

    page_1 = await first.get_user_forum_info_research(1234, 123456789012345678)
    page_2 = await second.get_user_forum_info_research(1234, 123456789012345678)

    assert page_1 is page_2
    assert page_1.raw.user.user_id == 123456789012345678
    assert page_1.dto.forum_level == 0
    assert page_1.dto.role_name is None
    assert page_1.observed_at.tzinfo is UTC
    assert first.lookup_calls == [
        ("user_info", 123456789012345678),
        ("forum_info", 1234, "portrait-1"),
    ]
    assert second.lookup_calls == []


@pytest.mark.asyncio
async def test_sparse_user_observation_retains_last_known_values_and_latest_raw_missing_reason():
    engine, sessionmaker_ = _sqlite_research_sessionmaker()
    try:
        user_id = 123456789012345678
        first_time = datetime(2025, 1, 1, tzinfo=UTC)
        second_time = datetime(2025, 1, 2, tzinfo=UTC)
        first_user = _synthetic_user(user_id=user_id)
        first_user.glevel = 7
        first_page = _get_threads_page(
            987654321012345678,
            987654321012345679,
            first_user,
            first_time,
            title="first observation",
            reply_count=1,
            last_time=1_750_000_001,
        )
        sparse_user = SimpleNamespace(
            user_id=user_id,
            user_name=" \t",
            nick_name_new=" ",
            nick_name_old="\n",
            portrait="\t",
            tieba_uid=0,
            level=0,
            glevel=0,
            is_bawu=False,
            icons=[],
        )
        second_page = _get_threads_page(
            987654321012345678,
            987654321012345679,
            sparse_user,
            second_time,
            title="second observation",
            reply_count=2,
            last_time=1_750_000_002,
        )

        await persist_research_page(sessionmaker_, first_page)
        await persist_research_page(sessionmaker_, second_page)

        with Session(engine) as session:
            user = session.get(ResearchUser, user_id)
            assert user is not None
            assert user.user_name == "synthetic-user"
            assert user.nickname == "合成昵称"
            assert user.portrait == "synthetic-portrait"
            assert user.tieba_uid == 987654321098765432
            assert user.global_growth_level == 7
            assert user.first_observed_at == first_time.replace(tzinfo=None)
            assert user.last_observed_at == second_time.replace(tzinfo=None)
            assert user.observed_at == second_time.replace(tzinfo=None)
            assert user.source_api == "aiotieba.get_threads"
            assert user.source_payload_json["user_name"] == " \t"
            assert user.source_payload_json["nick_name_new"] == " "
            assert user.source_payload_json["tieba_uid"] == 0
            assert user.missing_reasons_json["retained_last_known"] == {
                "user_name": "retained_last_known_due_to_missing_current_observation",
                "nickname": "retained_last_known_due_to_missing_current_observation",
                "portrait": "retained_last_known_due_to_missing_current_observation",
                "tieba_uid": "retained_last_known_due_to_missing_current_observation",
                "global_growth_level": "retained_last_known_due_to_missing_current_observation",
            }
            assert user.missing_reasons_json["author_user_name"] == "not_returned_by_source"
            assert user.missing_reasons_json["author_nickname"] == "not_returned_by_source"
            assert user.missing_reasons_json["author_portrait"] == "not_returned_by_source"
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_same_page_rich_then_sparse_user_observations_keep_rich_fields():
    engine, sessionmaker_ = _sqlite_research_sessionmaker()
    try:
        user_id = 123456789012345678
        observed_at = datetime(2025, 1, 1, tzinfo=UTC)
        rich = _synthetic_user(user_id=user_id)
        rich.glevel = 7
        sparse = SimpleNamespace(
            user_id=user_id,
            user_name="",
            nick_name_new="",
            nick_name_old="",
            portrait="",
            tieba_uid=0,
            level=0,
            glevel=0,
            is_bawu=False,
            icons=[],
        )
        page = _get_posts_page(
            987654321012345678,
            987654321012345679,
            rich,
            observed_at,
            Contents("synthetic first post"),
            reply_contents=Contents("synthetic reply"),
        )
        page.raw.thread.user = rich
        page.raw.objs[0].user = rich
        page.raw.objs[1].user = sparse
        page.raw.objs[1].author_id = user_id
        assert len(build_research_records(page).users) == 2

        await persist_research_page(sessionmaker_, page)

        with Session(engine) as session:
            user = session.get(ResearchUser, user_id)
            assert user is not None
            assert user.user_name == "synthetic-user"
            assert user.nickname == "合成昵称"
            assert user.portrait == "synthetic-portrait"
            assert user.tieba_uid == 987654321098765432
            assert user.global_growth_level == 7
            assert user.source_payload_json["user_name"] == ""
            assert user.missing_reasons_json["retained_last_known"]["nickname"]
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_older_user_observation_updates_first_seen_but_not_latest_values_or_payload():
    engine, sessionmaker_ = _sqlite_research_sessionmaker()
    try:
        user_id = 123456789012345678
        old_time = datetime(2025, 1, 1, tzinfo=UTC)
        new_time = datetime(2025, 1, 2, tzinfo=UTC)
        newer_user = _synthetic_user(user_id=user_id)
        newer_user.user_name = "newer-name"
        newer_page = _get_threads_page(
            987654321012345678,
            987654321012345679,
            newer_user,
            new_time,
            title="newer observation",
            reply_count=2,
            last_time=1_750_000_002,
        )
        older_user = _synthetic_user(user_id=user_id)
        older_user.user_name = "older-name"
        older_page = _get_threads_page(
            987654321012345678,
            987654321012345679,
            older_user,
            old_time,
            title="older observation",
            reply_count=1,
            last_time=1_750_000_001,
        )

        await persist_research_page(sessionmaker_, newer_page)
        await persist_research_page(sessionmaker_, older_page)

        with Session(engine) as session:
            user = session.get(ResearchUser, user_id)
            assert user is not None
            assert user.user_name == "newer-name"
            assert user.source_payload_json["user_name"] == "newer-name"
            assert user.observed_at == new_time.replace(tzinfo=None)
            assert user.first_observed_at == old_time.replace(tzinfo=None)
            assert user.last_observed_at == new_time.replace(tzinfo=None)
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_tieba_uid_outside_signed_bigint_is_null_but_kept_as_raw_decimal_string():
    engine, sessionmaker_ = _sqlite_research_sessionmaker()
    try:
        user = _synthetic_user()
        raw_tieba_uid = 1 << 63
        user.tieba_uid = raw_tieba_uid
        page = _get_threads_page(
            987654321012345678,
            987654321012345679,
            user,
            datetime(2025, 1, 1, tzinfo=UTC),
            title="synthetic overflow case",
            reply_count=1,
            last_time=1_750_000_001,
        )

        await persist_research_page(sessionmaker_, page)

        with Session(engine) as session:
            thread = session.get(ResearchThread, 987654321012345678)
            research_user = session.get(ResearchUser, user.user_id)
            assert thread is not None
            assert research_user is not None
            assert thread.author_tieba_uid is None
            assert thread.missing_reasons_json["author_tieba_uid"] == "outside_signed_bigint_range"
            assert research_user.tieba_uid is None
            assert research_user.source_payload_json["tieba_uid"] == str(raw_tieba_uid)
            assert research_user.source_payload_json["tieba_uid_raw"] == str(raw_tieba_uid)
            assert research_user.missing_reasons_json["author_tieba_uid"] == "outside_signed_bigint_range"
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("entity", ["thread", "post", "comment"])
async def test_mismatched_record_and_user_author_ids_fail_before_any_rows_are_written(entity):
    engine, sessionmaker_ = _sqlite_research_sessionmaker()
    try:
        user = _synthetic_user(user_id=123456789012345678)
        observed_at = datetime(2025, 1, 1, tzinfo=UTC)
        if entity == "thread":
            page = _get_threads_page(
                987654321012345678,
                987654321012345679,
                user,
                observed_at,
                title="synthetic thread",
                reply_count=1,
                last_time=1_750_000_001,
            )
            page.raw.objs[0].author_id += 1
        elif entity == "post":
            page = _get_posts_page(
                987654321012345678,
                987654321012345679,
                user,
                observed_at,
                Contents("first post"),
                reply_contents=Contents("synthetic reply"),
            )
            page.raw.objs[1].author_id += 1
        else:
            page = _get_comments_page(
                987654321012345678,
                987654321012345680,
                987654321012345679,
                user,
                observed_at,
                Contents("synthetic comment"),
            )
            page.raw.objs[0].author_id += 1

        with pytest.raises(ValueError, match="author ID and parsed user ID disagree"):
            await persist_research_page(sessionmaker_, page)

        with Session(engine) as session:
            assert session.scalars(select(ResearchThread)).all() == []
            assert session.scalars(select(ResearchPost)).all() == []
            assert session.scalars(select(ResearchComment)).all() == []
            assert session.scalars(select(ResearchUser)).all() == []
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_run_cache_does_not_cache_retryable_failures():
    cache = ResearchRunCache()
    calls = 0

    async def failed_lookup():
        nonlocal calls
        calls += 1
        return research_client_module.ResearchPage(
            raw=None,
            dto=research_client_module.ResearchUserForumInfo(
                user_id=7,
                response_user_id=None,
                fid=9,
                fname=None,
                portrait=None,
                display_name=None,
                tieba_uid=None,
                forum_level=None,
                forum_level_name=None,
                role_name=None,
                status="retryable_failure",
                missing_reasons={"forum_level": "api_error"},
            ),
            observed_at=datetime.now(UTC),
            source_api="aiotieba.get_user_forum_info",
        )

    await cache.get_or_create((7, 9), failed_lookup)
    await cache.get_or_create((7, 9), failed_lookup)

    assert calls == 2


@pytest.mark.asyncio
async def test_user_forum_portrait_path_uses_limiter_without_extra_user_info(monkeypatch):
    raw = SimpleNamespace(
        user=SimpleNamespace(user_id=123456789012345678, portrait="portrait-1", show_name="synthetic"),
        fname="合成贴吧",
        level=0,
        level_name="",
        role_name="",
    )
    api_calls = []
    limited_calls = []

    async def fake_forum_info(self, fname_or_fid, id_, /):
        api_calls.append((fname_or_fid, id_))
        return raw

    async def fake_request_core(self, func, *args, **kwargs):
        limited_calls.append(func.__name__)
        return await func(self, *args, **kwargs)

    async def unexpected_user_info(*args, **kwargs):
        raise AssertionError("known portrait must not fetch user info")

    monkeypatch.setattr(AiotiebaClient, "get_user_forum_info", fake_forum_info)
    monkeypatch.setattr(ResearchClient, "_request_core", fake_request_core)
    monkeypatch.setattr(ResearchClient, "get_user_info", unexpected_user_info)
    client = ResearchClient(BDUSS="x" * 192)

    page = await client.get_user_forum_info_research(1234, 123456789012345678, "portrait-1")

    assert page.raw is raw
    assert page.dto.status == "available"
    assert page.dto.response_user_id == 123456789012345678
    assert api_calls == [(1234, "portrait-1")]
    assert limited_calls == ["_get_user_forum_info_by_portrait"]


def test_local_account_file_is_hidden_and_private(tmp_path: Path, monkeypatch, capsys):
    secret = "x" * 192
    path = tmp_path / "secrets" / "baidu-account.json"
    monkeypatch.setattr("src.research.credentials.getpass.getpass", lambda _prompt: secret)

    saved_path = save_bduss_interactively(path)

    assert saved_path == path
    assert load_bduss(path) == secret
    assert S_IMODE(path.stat().st_mode) == 0o600
    assert secret not in capsys.readouterr().out


@pytest.mark.parametrize(
    "bad_input",
    [
        "BDUSS=" + ("x" * 64) + "; STOKEN=" + ("y" * 64),
        "z" * 64,
    ],
)
def test_invalid_local_credentials_are_rejected_without_echo(tmp_path: Path, monkeypatch, capsys, bad_input):
    path = tmp_path / "secrets" / "baidu-account.json"
    monkeypatch.setattr("src.research.credentials.getpass.getpass", lambda _prompt: bad_input)

    with pytest.raises(ValueError, match="BDUSS|Cookie") as raised:
        save_bduss_interactively(path)

    assert bad_input not in str(raised.value)
    captured = capsys.readouterr()
    assert bad_input not in captured.out
    assert bad_input not in captured.err
    assert not path.exists()


def test_cookie_header_saves_only_bduss(tmp_path: Path, monkeypatch):
    bduss = "b" * 192
    cookie_header = f"BDUSS={bduss}; STOKEN={'s' * 64}"
    path = tmp_path / "secrets" / "baidu-account.json"
    monkeypatch.setattr("src.research.credentials.getpass.getpass", lambda _prompt: cookie_header)

    save_bduss_interactively(path)

    assert load_bduss(path) == bduss
    assert "STOKEN" not in path.read_text(encoding="utf-8")


def test_save_bduss_accepts_cookie_value_without_prompt(tmp_path: Path):
    bduss = "b" * 192
    path = tmp_path / "secrets" / "account.json"

    assert save_bduss(f"Cookie: BDUSS={bduss}; STOKEN={'s' * 64}", path) == path
    assert load_bduss(path) == bduss
    assert "STOKEN" not in path.read_text(encoding="utf-8")


def test_login_cli_prompts_without_echoing_or_accepting_secret_as_argument(tmp_path: Path, monkeypatch, capsys):
    bduss = "b" * 192
    path = tmp_path / "account.json"
    monkeypatch.setattr("src.research.credentials.getpass.getpass", lambda _prompt: bduss)

    result = research_main(["login", "--account-file", str(path)])

    assert result == 0
    assert load_bduss(path) == bduss
    assert bduss not in capsys.readouterr().out


def test_login_cli_does_not_echo_rejected_secret_argument(capsys):
    secret = "x" * 192

    with pytest.raises(SystemExit):
        research_main(["login", "--bduss", secret])

    captured = capsys.readouterr()
    assert secret not in captured.out
    assert secret not in captured.err


def test_export_cli_dispatches_output_and_run_id(tmp_path: Path, monkeypatch):
    calls = []

    async def fake_run(output_dir, run_id, entity_scope):
        calls.append((output_dir, run_id, entity_scope))

    monkeypatch.setattr(research_cli_module, "_run_export", fake_run)
    output_dir = tmp_path / "exports"

    result = research_main(["export", "--output", str(output_dir), "--run-id", "synthetic-run"])

    assert result == 0
    assert calls == [(output_dir, "synthetic-run", "all")]


@pytest.mark.asyncio
async def test_export_cli_orchestrates_additive_migration_quality_and_export(tmp_path: Path, monkeypatch):
    import sqlalchemy.ext.asyncio as async_ext

    import src.core.config as core_config
    import src.core.datastore as datastore
    import src.research.exporter as exporter
    import src.research.migration as migration

    calls = []
    sessionmaker = object()
    quality_rows = [{"scope_type": "thread_replies", "status": "synthetic"}]

    async def quality_generator():
        yield quality_rows[0]

    def fake_quality(sessions, run_id=None):
        calls.append(("quality", sessions, run_id))
        return quality_generator()

    class FakeEngine:
        disposed = False

        async def dispose(self):
            self.disposed = True

    engine = FakeEngine()

    async def fake_migration(received_engine):
        calls.append(("migration", received_engine))

    batch_directory = tmp_path / "exports" / "all-runs_source-synthetic-run_20261114_120000_000000_export"

    async def fake_export(received_sessions, output_dir, summaries, *, source_run_id, entity_scope):
        rows = [row async for row in summaries]
        calls.append(("export", received_sessions, output_dir, rows, source_run_id, entity_scope))
        return {"threads": batch_directory / "threads_all-runs.csv"}

    monkeypatch.setattr(core_config, "Config", lambda: SimpleNamespace(database_url="synthetic-local-database"))
    monkeypatch.setattr(async_ext, "create_async_engine", lambda url, echo: engine)
    monkeypatch.setattr(async_ext, "async_sessionmaker", lambda **kwargs: sessionmaker)
    monkeypatch.setattr(datastore, "iter_crawl_quality_summaries", fake_quality)
    monkeypatch.setattr(migration, "create_research_tables", fake_migration)
    monkeypatch.setattr(exporter, "export_research_csvs", fake_export)

    result_directory = await research_cli_module._run_export(tmp_path / "exports", "synthetic-run")

    assert calls == [
        ("migration", engine),
        ("quality", sessionmaker, "synthetic-run"),
        ("export", sessionmaker, tmp_path / "exports", quality_rows, "synthetic-run", "all"),
    ]
    assert result_directory == batch_directory
    assert engine.disposed is True


def test_cookie_without_bduss_is_not_misread_as_raw_token():
    from src.research.credentials import parse_bduss

    cookie_value = "Cookie: STOKEN=" + ("x" * 178)

    with pytest.raises(ValueError, match="BDUSS") as raised:
        parse_bduss(cookie_value)

    assert cookie_value not in str(raised.value)


def test_research_client_parses_cookie_before_constructing_upstream_client(monkeypatch):
    bduss = "b" * 192
    values = []
    monkeypatch.setattr(
        research_client_module.Client,
        "__init__",
        lambda _self, *args, **kwargs: values.append(kwargs),
    )

    ResearchClient(BDUSS=f"Cookie: BDUSS={bduss}; STOKEN={'s' * 64}")

    assert values == [{"BDUSS": bduss}]


@pytest.mark.parametrize("bad_input", ["Cookie: " + "x" * 220, "z" * 64])
def test_research_client_rejects_invalid_bduss_before_upstream_error_can_leak(bad_input, capsys):
    with pytest.raises(ValueError, match="BDUSS|Cookie") as raised:
        ResearchClient(BDUSS=bad_input)

    captured = capsys.readouterr()
    assert bad_input not in str(raised.value)
    assert bad_input not in captured.out
    assert bad_input not in captured.err


@pytest.mark.asyncio
async def test_research_models_migration_is_additive_and_has_no_base_foreign_keys(tmp_path: Path):
    engine = create_engine(f"sqlite:///{tmp_path / 'research-migration.db'}")
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE thread (tid BIGINT PRIMARY KEY, title TEXT NOT NULL)"))
            connection.execute(text("INSERT INTO thread (tid, title) VALUES (123456789012345678, 'legacy')"))

        await create_research_tables(FakeAsyncEngine(engine))

        with engine.connect() as connection:
            assert (
                connection.execute(text("SELECT title FROM thread WHERE tid = 123456789012345678")).scalar_one()
                == "legacy"
            )

        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())
        assert "thread" in table_names
        assert {table.name for table in RESEARCH_MIGRATION_TABLES} <= table_names
        for table in RESEARCH_MIGRATION_TABLES:
            assert inspector.get_foreign_keys(table.name) == []
    finally:
        engine.dispose()


def test_posts_mapper_keeps_full_first_post_and_separates_reply_rows():
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user()
    image = FragImage("https://example.invalid/first.jpg")
    first_contents = Contents('first line\n"quoted"', [image])
    first_post = _synthetic_post(first_pid, tid, 1, user, first_contents)
    first_post.reply_num = 2
    reply_one = _synthetic_post(first_pid + 1, tid, 2, user, Contents("reply 1", [image]))
    reply_two = _synthetic_post(first_pid + 2, tid, 3, user, Contents("reply 2", [image]))
    thread = _synthetic_thread(tid, first_pid, user, first_contents)
    thread.reply_num = 100
    observed_at = datetime.now(UTC)
    page = research_client_module.ResearchPage(
        raw=SimpleNamespace(thread=thread, objs=[first_post, reply_one, reply_two]),
        dto=object(),
        observed_at=observed_at,
        source_api="aiotieba.get_posts",
    )

    records = build_research_records(page)

    assert len(records.threads) == 1
    assert records.threads[0].tid == tid
    assert records.threads[0].first_pid == first_pid
    assert records.threads[0].body_text == 'first line\n"quoted"'
    assert records.threads[0].reply_count == 100
    assert records.threads[0].source_payload_json["first_post"]["subreply_count_reported"] == 2
    assert records.threads[0].source_payload_json["first_post"]["observed_at"] == observed_at.isoformat()
    assert records.threads[0].source_payload_json["first_post"]["subreply_count_reported_raw"] == 2
    assert records.threads[0].source_payload_json["first_post"]["source"] == "aiotieba.get_posts.objs[].reply_num"
    assert records.threads[0].collect_count is None
    assert records.threads[0].missing_reasons_json["collect_count"] == "zero_presence_unverified_in_parsed_object"
    assert records.threads[0].source_payload_json["counts"]["collect_count"] == 0
    assert records.threads[0].forum_level is None
    assert records.threads[0].source_payload_json["author"]["level"] == 0
    assert [post.pid for post in records.posts] == [first_pid + 1, first_pid + 2]
    assert all(post.is_thread_author is False for post in records.posts)
    assert all(post.subreply_count_reported is None for post in records.posts)
    assert all(post.forum_level is None for post in records.posts)
    assert len(records.users) == 3  # duplicate identity observations are deduped before insertion
    assert all("ip" not in user_row.source_payload_json for user_row in records.users)
    assert records.posts[0].ip_location_text is None
    assert records.posts[0].missing_reasons_json["ip_location_text"] == "source_value_is_ip_address_not_public_location"
    assert len(records.media_assets) == 3
    assert {(row.owner_type, row.owner_id) for row in records.content_observations} == {
        ("thread_first_post", tid),
        ("post", first_pid + 1),
        ("post", first_pid + 2),
    }


def test_first_post_zero_comment_count_is_not_confused_with_thread_reply_count():
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user()
    first_post = _synthetic_post(first_pid, tid, 1, user, Contents("first"))
    first_post.reply_num = 0
    thread = _synthetic_thread(tid, first_pid, user, Contents("first"))
    thread.reply_num = 100
    page = research_client_module.ResearchPage(
        raw=SimpleNamespace(thread=thread, objs=[first_post]),
        dto=object(),
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )

    record = build_research_records(page).threads[0]

    first_post_source = record.source_payload_json["first_post"]
    assert record.reply_count == 100
    assert first_post_source["subreply_count_reported_raw"] == 0
    assert first_post_source["subreply_count_reported"] is None
    assert first_post_source["missing_reason"] == "zero_presence_unverified_in_parsed_object"
    assert (
        record.missing_reasons_json["first_post.subreply_count_reported"] == "zero_presence_unverified_in_parsed_object"
    )


def test_first_floor_post_pid_backfills_missing_thread_pid():
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user()
    first_post = _synthetic_post(first_pid, tid, 1, user, Contents("first"))
    thread = _synthetic_thread(tid, 0, user, Contents(""))
    thread.fid = 0
    first_post.tid = 0
    first_post.fid = 0
    page = research_client_module.ResearchPage(
        raw=SimpleNamespace(
            thread=thread,
            objs=[first_post],
            forum=SimpleNamespace(fid=7654321, fname="合成贴吧"),
        ),
        dto=object(),
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )

    records = build_research_records(page)

    assert records.threads[0].first_pid == first_pid
    assert records.threads[0].fid == 7654321
    assert "first_pid" not in records.threads[0].missing_reasons_json


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("different_floor_one_pids", "multiple floor=1 post IDs"),
        ("foreign_tid", "TID/FID does not match"),
        ("foreign_fid", "TID/FID does not match"),
        ("different_author", "post author does not match"),
    ],
)
def test_get_posts_rejects_conflicting_floor_one_identity(case, message):
    tid = 987654321012345678
    first_pid = 987654321012345679
    thread_user = _synthetic_user(user_id=111)
    thread = _synthetic_thread(tid, first_pid, thread_user, Contents("first"))
    first_post = _synthetic_post(first_pid, tid, 1, _synthetic_user(user_id=111), Contents("first"))
    posts = [first_post]

    if case == "different_floor_one_pids":
        posts.append(_synthetic_post(first_pid + 1, tid, 1, _synthetic_user(user_id=111), Contents("duplicate")))
    elif case == "foreign_tid":
        first_post.tid = tid + 100
    elif case == "foreign_fid":
        first_post.fid = thread.fid + 100
    elif case == "different_author":
        first_post.author_id = 222
        first_post.user = _synthetic_user(user_id=222)

    page = research_client_module.ResearchPage(
        raw=SimpleNamespace(thread=thread, objs=posts),
        dto=object(),
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )

    with pytest.raises(ValueError, match=message):
        build_research_records(page)


@pytest.mark.parametrize("floor", [None, 0])
def test_unknown_floor_does_not_promote_raw_thread_pid_to_first_post(floor):
    tid = 987654321012345678
    post_pid = 987654321012345679
    user = _synthetic_user()
    thread = _synthetic_thread(tid, post_pid, user, Contents("not authoritative"))
    page_head = _synthetic_post(post_pid, tid, floor, user, Contents("ordinary page post"))
    page = research_client_module.ResearchPage(
        raw=SimpleNamespace(thread=thread, objs=[page_head]),
        dto=object(),
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )

    records = build_research_records(page)

    assert records.threads == []
    assert [post.pid for post in records.posts] == [post_pid]
    assert ("thread_first_post", tid) not in {
        (row.owner_type, row.owner_id) for row in records.content_observations
    }


def test_ip_location_text_keeps_region_and_observation_source():
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user(ip="湖北")
    thread = _synthetic_thread(tid, first_pid, user, Contents("first"))
    reply = _synthetic_post(first_pid + 1, tid, 2, user, Contents("reply"))
    observed_at = datetime.now(UTC)
    page = research_client_module.ResearchPage(
        raw=SimpleNamespace(thread=thread, objs=[reply]),
        dto=object(),
        observed_at=observed_at,
        source_api="aiotieba.get_posts",
    )

    records = build_research_records(page)

    post_snapshot = next(
        snapshot
        for snapshot in records.user_forum_snapshots
        if snapshot.ip_location_source == "aiotieba.get_posts.author.ip"
    )
    assert post_snapshot.ip_location_text == "湖北"
    assert post_snapshot.observed_at == observed_at
    assert all(
        snapshot.ip_location_source != "aiotieba.get_posts.thread.author.ip"
        for snapshot in records.user_forum_snapshots
    )
    assert records.threads == []  # this page did not return the first-post object
    assert records.posts[0].ip_location_text == "湖北"
    assert records.posts[0].ip_location_source == "aiotieba.get_posts.author.ip"
    assert records.posts[0].ip_location_observed_at == observed_at
    assert records.users[0].source_payload_json["ip_location_text"] == "湖北"
    assert "ip" not in records.users[0].source_payload_json


def test_user_forum_json_zero_level_is_retained_as_explicit_value():
    user_id = 123456789012345678
    observed_at = datetime.now(UTC)
    raw = SimpleNamespace(
        user=SimpleNamespace(user_id=user_id, portrait="synthetic-portrait", show_name="synthetic-display"),
        fname="合成贴吧",
        level=0,
        level_name="新用户",
        role_name="",
    )
    dto = research_client_module.ResearchUserForumInfo(
        user_id=user_id,
        response_user_id=user_id,
        fid=7654321,
        fname="合成贴吧",
        portrait="synthetic-portrait",
        display_name="synthetic-display",
        tieba_uid=None,
        forum_level=0,
        forum_level_name="新用户",
        role_name=None,
        status="available",
        missing_reasons={},
    )
    page = research_client_module.ResearchPage(
        raw=raw,
        dto=dto,
        observed_at=observed_at,
        source_api="aiotieba.get_user_forum_info",
    )

    records = build_research_records(page)

    assert len(records.user_forum_snapshots) == 1
    snapshot = records.user_forum_snapshots[0]
    assert snapshot.forum_level == 0
    assert snapshot.missing_reasons_json.get("forum_level") is None
    assert snapshot.observed_at == observed_at
    assert snapshot.display_name == "synthetic-display"
    assert "user_name" not in snapshot.source_payload_json["user"]


def test_comments_mapper_preserves_first_post_parent_and_reply_target_semantics():
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user()
    comments = [
        SimpleNamespace(
            pid=987654321012345680,
            tid=tid,
            ppid=first_pid,
            fid=7654321,
            fname="合成贴吧",
            floor=1,
            create_time=1_750_000_010,
            author_id=user.user_id,
            user=user,
            contents=Contents("floor one child"),
            reply_to_id=0,
            agree=0,
            disagree=0,
            is_thread_author=False,
        ),
        SimpleNamespace(
            pid=987654321012345681,
            tid=tid,
            ppid=first_pid,
            fid=7654321,
            fname="合成贴吧",
            floor=2,
            create_time=1_750_000_011,
            author_id=user.user_id,
            user=user,
            contents=Contents("replied to a user"),
            reply_to_id=998877665544332211,
            agree=0,
            disagree=0,
            is_thread_author=False,
        ),
    ]
    page = research_client_module.ResearchPage(
        raw=SimpleNamespace(objs=comments, forum=SimpleNamespace(fid=7654321, fname="合成贴吧")),
        dto=object(),
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_comments",
    )

    records = build_research_records(page)

    assert len(records.comments) == 2
    assert all(comment.parent_post_pid == first_pid for comment in records.comments)
    assert records.comments[0].reply_to_id_raw == 0
    assert records.comments[0].reply_to_user_id is None
    assert records.comments[0].missing_reasons_json["reply_to_user_id"] == "aiotieba_zero_sentinel"
    assert records.comments[1].reply_to_user_id == 998877665544332211
    assert records.comments[1].reply_target_source == "aiotieba_parsed_reply_prefix"
    assert records.comments[1].reply_target_confidence == "parsed_from_reply_prefix"
    assert all(comment.agree_count is None for comment in records.comments)


@pytest.mark.asyncio
async def test_research_persistence_deduplicates_batch_identities_and_replays_idempotently():
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user()
    image = FragImage("https://example.invalid/reply.jpg")
    first_post = _synthetic_post(first_pid, tid, 1, user, Contents("first", [image]))
    first_post.reply_num = 2
    duplicate_post = _synthetic_post(first_pid + 1, tid, 2, user, Contents("same post", [image]))
    second_post = _synthetic_post(first_pid + 2, tid, 3, user, Contents("second post", [image]))
    thread = _synthetic_thread(tid, first_pid, user, Contents("first", [image]))
    page = research_client_module.ResearchPage(
        raw=SimpleNamespace(thread=thread, objs=[first_post, duplicate_post, duplicate_post, second_post]),
        dto=object(),
        observed_at=datetime.now(UTC),
        source_api="aiotieba.get_posts",
    )
    engine, sessionmaker = _sqlite_research_sessionmaker()
    try:
        first_counts = await persist_research_page(sessionmaker, page)
        second_counts = await persist_research_page(sessionmaker, page)

        assert first_counts == second_counts
        assert first_counts["threads"] == 1
        assert first_counts["posts"] == 2
        assert first_counts["users"] == 1
        assert first_counts["media_assets"] == 3
        with Session(engine) as database:
            assert database.scalar(select(ResearchThread).where(ResearchThread.tid == tid)) is not None
            assert len(database.scalars(select(ResearchPost)).all()) == 2
            assert len(database.scalars(select(MediaAsset)).all()) == 3
            observations = database.scalars(select(ResearchContentObservation)).all()
            assert {(row.owner_type, row.owner_id) for row in observations} == {
                ("thread_first_post", tid),
                ("post", first_pid + 1),
                ("post", first_pid + 2),
            }
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_later_posts_page_replay_does_not_clear_first_post_body_or_media():
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user(user_id=111, ip="福建")
    image = FragImage("https://example.invalid/first-post.jpg")
    observed_at = datetime.now(UTC)
    later_observed_at = observed_at + timedelta(hours=1)
    first_contents = Contents('first post\n"complete"', [image])
    first_post = _synthetic_post(first_pid, tid, 1, user, first_contents)
    first_post.reply_num = 2
    first_thread = _synthetic_thread(tid, first_pid, user, first_contents)
    first_thread.reply_num = 100
    first_page = research_client_module.ResearchPage(
        raw=SimpleNamespace(
            thread=first_thread,
            objs=[first_post],
        ),
        dto=object(),
        observed_at=observed_at,
        source_api="aiotieba.get_posts",
    )

    later_user = _synthetic_user(user_id=222, ip="上海")
    later_reply = _synthetic_post(
        first_pid + 1,
        tid,
        31,
        _synthetic_user(user_id=333),
        Contents("floor 31 body", [FragImage("https://example.invalid/floor-31.jpg")]),
    )
    second_later_reply = _synthetic_post(
        first_pid + 2,
        tid,
        32,
        _synthetic_user(user_id=444),
        Contents("floor 32 body"),
    )
    later_thread = _synthetic_thread(tid, later_reply.pid, later_user, Contents("untrusted page header"))
    later_thread.reply_num = 300
    later_page = research_client_module.ResearchPage(
        raw=SimpleNamespace(
            thread=later_thread,
            objs=[later_reply, second_later_reply],
        ),
        dto=object(),
        observed_at=later_observed_at,
        source_api="aiotieba.get_posts",
    )

    first_records = build_research_records(first_page)
    later_records = build_research_records(later_page)
    assert first_records.threads[0].body_text == 'first post\n"complete"'
    assert first_records.media_assets[0].owner_type == "thread"
    assert first_records.media_assets[0].src_url == image.src
    assert later_records.threads == []
    assert ("thread_first_post", tid) not in {
        (row.owner_type, row.owner_id) for row in later_records.content_observations
    }
    assert all(asset.owner_type != "thread" for asset in later_records.media_assets)
    assert [post.pid for post in later_records.posts] == [later_reply.pid, second_later_reply.pid]
    assert [post.text for post in later_records.posts] == ["floor 31 body", "floor 32 body"]
    assert all(snapshot.user_id != later_user.user_id for snapshot in later_records.user_forum_snapshots)

    engine, sessionmaker = _sqlite_research_sessionmaker()
    try:
        await persist_research_page(sessionmaker, first_page)
        await persist_research_page(sessionmaker, later_page)
        await persist_research_page(sessionmaker, later_page)

        with Session(engine) as database:
            stored_thread = database.scalar(select(ResearchThread).where(ResearchThread.tid == tid))
            assert stored_thread.body_text == 'first post\n"complete"'
            assert stored_thread.first_pid == first_pid
            assert stored_thread.author_user_id == user.user_id
            assert stored_thread.ip_location_text == "福建"
            assert stored_thread.reply_count == 100
            assert stored_thread.observed_at.replace(tzinfo=UTC) == observed_at
            assert stored_thread.source_payload_json["first_post"]["observed_at"] == observed_at.isoformat()
            thread_media = database.scalars(
                select(MediaAsset).where(MediaAsset.owner_type == "thread", MediaAsset.owner_id == tid)
            ).all()
            assert len(thread_media) == 1
            assert thread_media[0].src_url == image.src
            marker = database.scalar(
                select(ResearchContentObservation).where(
                    ResearchContentObservation.owner_type == "thread_first_post",
                    ResearchContentObservation.owner_id == tid,
                )
            )
            assert marker.observed_at.replace(tzinfo=UTC) == observed_at
            stored_posts = database.scalars(select(ResearchPost).order_by(ResearchPost.pid)).all()
            assert [post.pid for post in stored_posts] == [later_reply.pid, second_later_reply.pid]
            assert [post.text for post in stored_posts] == ["floor 31 body", "floor 32 body"]
            floor_31_media = database.scalars(
                select(MediaAsset).where(MediaAsset.owner_type == "post", MediaAsset.owner_id == later_reply.pid)
            ).all()
            assert len(floor_31_media) == 1
            assert floor_31_media[0].src_url == "https://example.invalid/floor-31.jpg"
            assert database.scalar(
                select(UserForumSnapshot.user_id).where(UserForumSnapshot.user_id == later_user.user_id)
            ) is None
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_changed_explicit_first_floor_pid_is_still_rejected():
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user(user_id=111)
    first_time = datetime(2026, 11, 12, 10, tzinfo=UTC)
    changed_time = first_time + timedelta(hours=1)

    def page(pid, observed_at, body):
        first_post = _synthetic_post(pid, tid, 1, user, Contents(body))
        return research_client_module.ResearchPage(
            raw=SimpleNamespace(
                thread=_synthetic_thread(tid, pid, user, Contents(body)),
                objs=[first_post],
            ),
            dto=object(),
            observed_at=observed_at,
            source_api="aiotieba.get_posts",
        )

    engine, sessionmaker = _sqlite_research_sessionmaker()
    try:
        await persist_research_page(sessionmaker, page(first_pid, first_time, "original first body"))
        with pytest.raises(ValueError, match="ResearchThread.first_pid identity changed"):
            await persist_research_page(sessionmaker, page(first_pid + 100, changed_time, "changed body"))

        with Session(engine) as database:
            stored_thread = database.scalar(select(ResearchThread).where(ResearchThread.tid == tid))
            assert stored_thread.first_pid == first_pid
            assert stored_thread.body_text == "original first body"
            assert stored_thread.observed_at.replace(tzinfo=UTC) == first_time
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_stored_first_pid_filters_unknown_floor_duplicate_by_tid_only():
    tid = 987654321012345678
    other_tid = tid + 100
    first_pid = 987654321012345679
    page_head_pid = first_pid + 1
    t1 = datetime(2026, 11, 12, 10, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    t3 = t2 + timedelta(hours=1)
    thread_user = _synthetic_user(user_id=111)
    first_image = FragImage("https://example.invalid/canonical-first.jpg")
    first_contents = Contents("canonical first body", [first_image])
    first_post = _synthetic_post(first_pid, tid, 1, thread_user, first_contents)
    first_post.reply_num = 2
    first_thread = _synthetic_thread(tid, first_pid, thread_user, first_contents)
    first_thread.reply_num = 100
    first_page = research_client_module.ResearchPage(
        raw=SimpleNamespace(thread=first_thread, objs=[first_post]),
        dto=object(),
        observed_at=t1,
        source_api="aiotieba.get_posts",
    )

    duplicate_post = _synthetic_post(
        first_pid,
        tid,
        0,
        _synthetic_user(user_id=222),
        Contents("unknown-floor duplicate", [FragImage("https://example.invalid/duplicate.jpg")]),
    )
    preview_cid = first_pid + 50
    preview_user = _synthetic_user(user_id=555)
    duplicate_post.comments = [
        SimpleNamespace(
            pid=preview_cid,
            tid=tid,
            ppid=first_pid,
            fid=7654321,
            fname="合成贴吧",
            author_id=preview_user.user_id,
            user=preview_user,
            contents=Contents("preview on canonical first post"),
            reply_to_id=0,
            agree=0,
            disagree=0,
            is_thread_author=False,
        )
    ]
    page_head = _synthetic_post(
        page_head_pid,
        tid,
        31,
        _synthetic_user(user_id=333),
        Contents("ordinary page head", [FragImage("https://example.invalid/page-head.jpg")]),
    )
    later_thread = _synthetic_thread(tid, page_head_pid, _synthetic_user(user_id=999), Contents("page header"))
    later_thread.reply_num = 300
    later_page = research_client_module.ResearchPage(
        raw=SimpleNamespace(thread=later_thread, objs=[duplicate_post, page_head]),
        dto=object(),
        observed_at=t2,
        source_api="aiotieba.get_posts",
    )

    other_tid_post = _synthetic_post(
        first_pid,
        other_tid,
        0,
        _synthetic_user(user_id=444),
        Contents("same pid in another thread", [FragImage("https://example.invalid/other-thread.jpg")]),
    )
    other_thread = _synthetic_thread(other_tid, first_pid, _synthetic_user(user_id=777), Contents("other header"))
    other_tid_page = research_client_module.ResearchPage(
        raw=SimpleNamespace(thread=other_thread, objs=[other_tid_post]),
        dto=object(),
        observed_at=t3,
        source_api="aiotieba.get_posts",
    )

    engine, sessionmaker = _sqlite_research_sessionmaker()
    try:
        await persist_research_page(sessionmaker, first_page)
        result = await persist_research_page(sessionmaker, later_page)
        assert result["posts"] == 1
        assert result["media_assets"] == 1
        await persist_research_page(sessionmaker, other_tid_page)

        with Session(engine) as database:
            stored_thread = database.scalar(select(ResearchThread).where(ResearchThread.tid == tid))
            assert stored_thread.first_pid == first_pid
            assert stored_thread.body_text == "canonical first body"
            assert stored_thread.reply_count == 100
            assert stored_thread.observed_at.replace(tzinfo=UTC) == t1

            original_tid_posts = database.scalars(
                select(ResearchPost).where(ResearchPost.tid == tid).order_by(ResearchPost.pid)
            ).all()
            assert [post.pid for post in original_tid_posts] == [page_head_pid]
            same_pid_other_tid = database.scalar(select(ResearchPost).where(ResearchPost.pid == first_pid))
            assert same_pid_other_tid.tid == other_tid

            observations = database.scalars(
                select(ResearchContentObservation).where(ResearchContentObservation.owner_type == "post")
            ).all()
            assert ("post", first_pid, tid) not in {
                (row.owner_type, row.owner_id, row.tid) for row in observations
            }
            assert ("post", first_pid, other_tid) in {
                (row.owner_type, row.owner_id, row.tid) for row in observations
            }

            assets = database.scalars(
                select(MediaAsset).where(MediaAsset.owner_type == "post", MediaAsset.owner_id == first_pid)
            ).all()
            assert [(asset.tid, asset.src_url) for asset in assets] == [
                (other_tid, "https://example.invalid/other-thread.jpg")
            ]
            first_marker = database.scalar(
                select(ResearchContentObservation).where(
                    ResearchContentObservation.owner_type == "thread_first_post",
                    ResearchContentObservation.owner_id == tid,
                )
            )
            assert first_marker.observed_at.replace(tzinfo=UTC) == t1

            preview_observations = database.scalars(
                select(ResearchFieldObservation).where(
                    ResearchFieldObservation.target_type == "comment",
                    ResearchFieldObservation.target_id == preview_cid,
                    ResearchFieldObservation.parent_post_pid == first_pid,
                )
            ).all()
            assert preview_observations
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_known_first_pid_filter_keeps_existing_duplicate_history():
    tid = 987654321012345678
    first_pid = 987654321012345679
    t0 = datetime(2026, 11, 12, 9, tzinfo=UTC)
    t1 = t0 + timedelta(hours=1)
    t2 = t1 + timedelta(hours=1)
    user = _synthetic_user(user_id=111)

    historical_post = _synthetic_post(
        first_pid,
        tid,
        0,
        user,
        Contents("historical duplicate", [FragImage("https://example.invalid/historical.jpg")]),
    )
    historical_page = research_client_module.ResearchPage(
        raw=SimpleNamespace(
            thread=_synthetic_thread(tid, first_pid, user, Contents("header")),
            objs=[historical_post],
        ),
        dto=object(),
        observed_at=t0,
        source_api="aiotieba.get_posts",
    )
    first_floor = _synthetic_post(first_pid, tid, 1, user, Contents("canonical first"))
    first_floor_page = research_client_module.ResearchPage(
        raw=SimpleNamespace(
            thread=_synthetic_thread(tid, first_pid, user, Contents("canonical first")),
            objs=[first_floor],
        ),
        dto=object(),
        observed_at=t1,
        source_api="aiotieba.get_posts",
    )
    repeated_unknown_floor = _synthetic_post(
        first_pid,
        tid,
        0,
        user,
        Contents("new duplicate value", [FragImage("https://example.invalid/new-duplicate.jpg")]),
    )
    repeated_page = research_client_module.ResearchPage(
        raw=SimpleNamespace(
            thread=_synthetic_thread(tid, first_pid + 100, user, Contents("header")),
            objs=[repeated_unknown_floor],
        ),
        dto=object(),
        observed_at=t2,
        source_api="aiotieba.get_posts",
    )

    engine, sessionmaker = _sqlite_research_sessionmaker()
    try:
        await persist_research_page(sessionmaker, historical_page)
        await persist_research_page(sessionmaker, first_floor_page)
        await persist_research_page(sessionmaker, repeated_page)

        with Session(engine) as database:
            stored_post = database.scalar(select(ResearchPost).where(ResearchPost.pid == first_pid))
            assert stored_post.tid == tid
            assert stored_post.text == "historical duplicate"
            marker = database.scalar(
                select(ResearchContentObservation).where(
                    ResearchContentObservation.owner_type == "post",
                    ResearchContentObservation.owner_id == first_pid,
                )
            )
            assert marker.observed_at.replace(tzinfo=UTC) == t0
            media = database.scalars(
                select(MediaAsset).where(MediaAsset.owner_type == "post", MediaAsset.owner_id == first_pid)
            ).all()
            assert [(asset.src_url, asset.observed_at.replace(tzinfo=UTC)) for asset in media] == [
                ("https://example.invalid/historical.jpg", t0)
            ]
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_newer_thread_list_preserves_detail_body_first_pid_and_source_payload():
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user(ip="湖北")
    t1 = datetime(2026, 11, 12, 10, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    image = FragImage("https://example.invalid/detail.jpg")
    detail_page = _get_posts_page(
        tid,
        first_pid,
        user,
        t1,
        Contents("authoritative first post", [image]),
        reply_count=100,
        first_subreply_count=3,
    )
    list_page = _get_threads_page(
        tid,
        first_pid,
        user,
        t2,
        title="new list title",
        reply_count=77,
        last_time=1_750_000_111,
    )

    engine, sessionmaker = _sqlite_research_sessionmaker()
    try:
        await persist_research_page(sessionmaker, detail_page)
        await persist_research_page(sessionmaker, list_page)

        with Session(engine) as database:
            thread = database.scalar(select(ResearchThread).where(ResearchThread.tid == tid))
            marker = database.scalar(
                select(ResearchContentObservation).where(
                    ResearchContentObservation.owner_type == "thread_first_post",
                    ResearchContentObservation.owner_id == tid,
                )
            )
            media = database.scalars(
                select(MediaAsset).where(MediaAsset.owner_type == "thread", MediaAsset.owner_id == tid)
            ).all()

            assert thread.first_pid == first_pid
            assert thread.body_text == "authoritative first post"
            assert thread.contents_raw
            assert thread.title == "new list title"
            assert thread.reply_count == 77
            assert thread.collect_count is None
            assert thread.last_time_raw == 1_750_000_111
            assert thread.source_api == "aiotieba.get_threads"
            assert thread.observed_at.replace(tzinfo=UTC) == t2
            assert marker.source_api == "aiotieba.get_posts"
            assert marker.observed_at.replace(tzinfo=UTC) == t1
            assert thread.source_payload_json["first_post"]["subreply_count_reported"] == 3
            assert thread.source_payload_json["first_post"]["observed_at"] == t1.isoformat()
            assert thread.source_payload_json["counts"]["collect_count"] == 0
            assert thread.source_payload_json["author"]["ip_location_text"] == "湖北"
            assert thread.ip_location_text == "湖北"
            assert thread.ip_location_source == "aiotieba.get_posts.thread.author.ip"
            assert thread.ip_location_observed_at.replace(tzinfo=UTC) == t1
            assert "body_text" not in thread.missing_reasons_json
            assert len(media) == 1
            assert media[0].src_url == image.src
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw_collect_count", "normalized_collect_count", "collect_missing_reason"),
    [
        (17, 17, None),
        (0, None, "zero_presence_unverified_in_parsed_object"),
    ],
)
async def test_older_detail_fills_detail_only_fields_without_reversing_newer_thread_metadata(
    raw_collect_count,
    normalized_collect_count,
    collect_missing_reason,
):
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user(ip="湖北")
    t1 = datetime(2026, 11, 12, 10, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    list_user = SimpleNamespace(**vars(user))
    del list_user.ip
    list_page = _get_threads_page(
        tid,
        first_pid,
        list_user,
        t2,
        title="latest list title",
        reply_count=81,
        last_time=1_750_000_222,
    )
    detail_page = _get_posts_page(
        tid,
        first_pid,
        user,
        t1,
        Contents("detail fetched after list", []),
        title="older detail title",
        reply_count=100,
        first_subreply_count=4,
        collect_num=raw_collect_count,
    )

    engine, sessionmaker = _sqlite_research_sessionmaker()
    try:
        await persist_research_page(sessionmaker, list_page)
        await persist_research_page(sessionmaker, detail_page)

        with Session(engine) as database:
            thread = database.scalar(select(ResearchThread).where(ResearchThread.tid == tid))
            marker = database.scalar(
                select(ResearchContentObservation).where(
                    ResearchContentObservation.owner_type == "thread_first_post",
                    ResearchContentObservation.owner_id == tid,
                )
            )

            assert thread.body_text == "detail fetched after list"
            assert thread.first_pid == first_pid
            assert thread.title == "latest list title"
            assert thread.reply_count == 81
            assert thread.collect_count == normalized_collect_count
            assert thread.last_time_raw == 1_750_000_222
            assert thread.source_api == "aiotieba.get_threads"
            assert thread.observed_at.replace(tzinfo=UTC) == t2
            assert thread.ip_location_text == "湖北"
            assert thread.ip_location_source == "aiotieba.get_posts.thread.author.ip"
            assert thread.ip_location_observed_at.replace(tzinfo=UTC) == t1
            assert marker.observed_at.replace(tzinfo=UTC) == t1
            assert thread.source_payload_json["title"] == "latest list title"
            assert thread.source_payload_json["first_post"]["observed_at"] == t1.isoformat()
            assert thread.source_payload_json["counts"]["collect_count"] == raw_collect_count
            assert thread.source_payload_json["author"]["ip_location_text"] == "湖北"
            assert thread.missing_reasons_json.get("collect_count") == collect_missing_reason
            assert "ip_location_text" not in thread.missing_reasons_json
            assert "body_text" not in thread.missing_reasons_json
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_newer_detail_does_not_clear_list_only_thread_fields():
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user()
    t1 = datetime(2026, 11, 12, 10, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    list_page = _get_threads_page(
        tid,
        first_pid,
        user,
        t1,
        title="list title",
        reply_count=25,
        last_time=1_750_000_333,
    )
    list_page.raw.objs[0].is_top = True
    list_page.raw.objs[0].is_good = True
    detail_page = _get_posts_page(
        tid,
        first_pid,
        user,
        t2,
        Contents("newer detail body", []),
        title="newer detail title",
        reply_count=40,
    )

    engine, sessionmaker = _sqlite_research_sessionmaker()
    try:
        await persist_research_page(sessionmaker, list_page)
        await persist_research_page(sessionmaker, detail_page)

        with Session(engine) as database:
            thread = database.scalar(select(ResearchThread).where(ResearchThread.tid == tid))
            assert thread.body_text == "newer detail body"
            assert thread.title == "newer detail title"
            assert thread.reply_count == 40
            assert thread.last_time_raw == 1_750_000_333
            assert thread.is_top is True
            assert thread.is_good is True
            assert thread.source_api == "aiotieba.get_posts"
            assert thread.observed_at.replace(tzinfo=UTC) == t2
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_latest_empty_content_snapshots_reject_old_media_replays_for_all_owners():
    tid = 987654321012345678
    first_pid = 987654321012345679
    comment_id = 987654321012345681
    user = _synthetic_user()
    t1 = datetime(2026, 11, 12, 10, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    old_image = FragImage("https://example.invalid/old.jpg")
    old_posts = _get_posts_page(
        tid,
        first_pid,
        user,
        t1,
        Contents("old first post", [old_image]),
        reply_contents=Contents("old reply", [old_image]),
    )
    new_posts = _get_posts_page(
        tid,
        first_pid,
        user,
        t2,
        Contents("new first post", []),
        reply_contents=Contents("new reply", []),
    )
    old_comments = _get_comments_page(
        tid,
        comment_id,
        first_pid + 1,
        user,
        t1,
        Contents("old comment", [old_image]),
    )
    new_comments = _get_comments_page(
        tid,
        comment_id,
        first_pid + 1,
        user,
        t2,
        Contents("new comment", []),
    )

    engine, sessionmaker = _sqlite_research_sessionmaker()
    try:
        await persist_research_page(sessionmaker, old_posts)
        await persist_research_page(sessionmaker, old_comments)
        await persist_research_page(sessionmaker, new_posts)
        await persist_research_page(sessionmaker, new_comments)
        await persist_research_page(sessionmaker, old_posts)
        await persist_research_page(sessionmaker, old_comments)

        with Session(engine) as database:
            thread = database.scalar(select(ResearchThread).where(ResearchThread.tid == tid))
            reply = database.scalar(select(ResearchPost).where(ResearchPost.pid == first_pid + 1))
            comment = database.scalar(select(ResearchComment).where(ResearchComment.cid == comment_id))
            markers = {
                (row.owner_type, row.owner_id): row.observed_at.replace(tzinfo=UTC)
                for row in database.scalars(select(ResearchContentObservation)).all()
            }

            assert thread.body_text == "new first post"
            assert reply.text == "new reply"
            assert comment.text == "new comment"
            assert database.scalars(select(MediaAsset)).all() == []
            assert markers == {
                ("thread_first_post", tid): t2,
                ("post", first_pid + 1): t2,
                ("comment", comment_id): t2,
            }
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_injected_sqlite_session_leaves_all_research_changes_to_outer_rollback():
    tid = 987654321012345678
    first_pid = 987654321012345679
    user = _synthetic_user()
    t1 = datetime(2026, 11, 12, 10, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    image = FragImage("https://example.invalid/rollback.jpg")
    old_page = _get_posts_page(
        tid,
        first_pid,
        user,
        t1,
        Contents("committed old body", [image]),
        reply_contents=Contents("committed old reply", [image]),
    )
    new_page = _get_posts_page(
        tid,
        first_pid,
        user,
        t2,
        Contents("uncommitted new body", []),
        reply_contents=Contents("uncommitted new reply", []),
    )
    engine, sessionmaker = _sqlite_research_sessionmaker()
    try:
        await persist_research_page(sessionmaker, old_page)
        outer_session = Session(engine)
        try:
            class RollbackOuterTransactionError(Exception):
                pass

            try:
                with outer_session.begin():
                    injected = SQLiteAsyncSession(outer_session)
                    await persist_research_page(sessionmaker, new_page, session=injected)
                    assert outer_session.in_transaction()
                    assert outer_session.scalar(
                        select(ResearchThread.body_text).where(ResearchThread.tid == tid)
                    ) == "uncommitted new body"
                    assert outer_session.scalar(
                        select(ResearchPost.text).where(ResearchPost.pid == first_pid + 1)
                    ) == "uncommitted new reply"
                    assert outer_session.scalars(select(MediaAsset)).all() == []
                    marker = outer_session.scalar(
                        select(ResearchContentObservation).where(
                            ResearchContentObservation.owner_type == "thread_first_post",
                            ResearchContentObservation.owner_id == tid,
                        )
                    )
                    assert marker.observed_at.replace(tzinfo=UTC) == t2
                    raise RollbackOuterTransactionError
            except RollbackOuterTransactionError:
                pass
            else:
                pytest.fail("The synthetic exception should leave the outer transaction uncommitted.")

            assert not outer_session.in_transaction()
        finally:
            outer_session.close()

        with Session(engine) as database:
            thread = database.scalar(select(ResearchThread).where(ResearchThread.tid == tid))
            reply = database.scalar(select(ResearchPost).where(ResearchPost.pid == first_pid + 1))
            marker = database.scalar(
                select(ResearchContentObservation).where(
                    ResearchContentObservation.owner_type == "thread_first_post",
                    ResearchContentObservation.owner_id == tid,
                )
            )
            media = database.scalars(select(MediaAsset).where(MediaAsset.tid == tid)).all()

            assert thread.body_text == "committed old body"
            assert reply.text == "committed old reply"
            assert marker.observed_at.replace(tzinfo=UTC) == t1
            assert {asset.src_url for asset in media} == {image.src}
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_postgresql_injected_session_outer_rollback_restores_research_snapshot():
    if os.environ.get("TIEBA_RESEARCH_METADATA_TEST_PG") != "1":
        pytest.skip("Set TIEBA_RESEARCH_METADATA_TEST_PG=1 to run on the dedicated test database.")

    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from src.core.config import Config

    database_url = str(Config().database_url)
    if make_url(database_url).database != "tieba_research_metadata_test_20261114":
        pytest.fail("PostgreSQL persistence rollback test requires the dedicated research metadata test database.")

    tid = 7_000_000_000_000_000_000 + time_ns() % 1_000_000_000_000_000
    first_pid = tid + 1
    user = _synthetic_user(tid + 2)
    t1 = datetime(2026, 11, 12, 10, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    image = FragImage("https://example.invalid/rollback-pg.jpg")
    old_page = _get_posts_page(
        tid,
        first_pid,
        user,
        t1,
        Contents("committed old body", [image]),
        reply_contents=Contents("committed old reply", [image]),
    )
    new_page = _get_posts_page(
        tid,
        first_pid,
        user,
        t2,
        Contents("uncommitted new body", []),
        reply_contents=Contents("uncommitted new reply", []),
    )
    engine = create_async_engine(database_url, echo=False)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def cleanup():
        async with sessions() as session, session.begin():
            await session.execute(delete(MediaAsset).where(MediaAsset.tid == tid))
            await session.execute(delete(ResearchContentObservation).where(ResearchContentObservation.tid == tid))
            await session.execute(delete(ResearchComment).where(ResearchComment.tid == tid))
            await session.execute(delete(ResearchPost).where(ResearchPost.tid == tid))
            await session.execute(delete(ResearchThread).where(ResearchThread.tid == tid))
            await session.execute(delete(ResearchUser).where(ResearchUser.user_id == user.user_id))
            await session.execute(delete(UserForumSnapshot).where(UserForumSnapshot.user_id == user.user_id))

    class RollbackOuterTransactionError(Exception):
        pass

    try:
        await create_research_tables(engine)
        await cleanup()
        await persist_research_page(sessions, old_page)

        async with sessions() as outer_session:
            try:
                async with outer_session.begin():
                    await persist_research_page(sessions, new_page, session=outer_session)
                    assert outer_session.in_transaction()
                    assert await outer_session.scalar(
                        select(ResearchThread.body_text).where(ResearchThread.tid == tid)
                    ) == "uncommitted new body"
                    assert await outer_session.scalar(
                        select(ResearchPost.text).where(ResearchPost.pid == first_pid + 1)
                    ) == "uncommitted new reply"
                    assert await outer_session.scalar(
                        select(MediaAsset.owner_id).where(MediaAsset.tid == tid).limit(1)
                    ) is None
                    marker = await outer_session.scalar(
                        select(ResearchContentObservation).where(
                            ResearchContentObservation.owner_type == "thread_first_post",
                            ResearchContentObservation.owner_id == tid,
                        )
                    )
                    assert marker.observed_at.replace(tzinfo=UTC) == t2
                    raise RollbackOuterTransactionError
            except RollbackOuterTransactionError:
                pass
            assert not outer_session.in_transaction()

        async with sessions() as session:
            thread = await session.scalar(select(ResearchThread).where(ResearchThread.tid == tid))
            reply = await session.scalar(select(ResearchPost).where(ResearchPost.pid == first_pid + 1))
            marker = await session.scalar(
                select(ResearchContentObservation).where(
                    ResearchContentObservation.owner_type == "thread_first_post",
                    ResearchContentObservation.owner_id == tid,
                )
            )
            media = (
                await session.scalars(select(MediaAsset).where(MediaAsset.tid == tid))
            ).all()

        assert thread.body_text == "committed old body"
        assert reply.text == "committed old reply"
        assert marker.observed_at.replace(tzinfo=UTC) == t1
        assert {asset.src_url for asset in media} == {image.src}
    finally:
        await cleanup()
        await engine.dispose()


@pytest.mark.asyncio
async def test_postgresql_concurrent_content_snapshots_keep_newest_owner_state():
    if os.environ.get("TIEBA_RESEARCH_METADATA_TEST_PG") != "1":
        pytest.skip("Set TIEBA_RESEARCH_METADATA_TEST_PG=1 to run on the dedicated test database.")

    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from src.core.config import Config

    database_url = str(Config().database_url)
    if make_url(database_url).database != "tieba_research_metadata_test_20261114":
        pytest.fail("PostgreSQL persistence race test requires the dedicated research metadata test database.")

    tid = 7_000_000_000_000_000_000 + time_ns() % 1_000_000_000_000_000
    first_pid = tid + 1
    reply_pid = first_pid + 1
    user = _synthetic_user(tid + 2)
    t1 = datetime(2026, 11, 12, 10, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    image = FragImage("https://example.invalid/concurrent-old.jpg")
    old_page = _get_posts_page(
        tid,
        first_pid,
        user,
        t1,
        Contents("older first post", [image]),
        reply_contents=Contents("older reply", [image]),
    )
    new_page = _get_posts_page(
        tid,
        first_pid,
        user,
        t2,
        Contents("newer first post", []),
        reply_contents=Contents("newer reply", []),
    )
    engine = create_async_engine(database_url, echo=False)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def cleanup():
        async with sessions() as session, session.begin():
            await session.execute(delete(MediaAsset).where(MediaAsset.tid == tid))
            await session.execute(delete(ResearchContentObservation).where(ResearchContentObservation.tid == tid))
            await session.execute(delete(ResearchComment).where(ResearchComment.tid == tid))
            await session.execute(delete(ResearchPost).where(ResearchPost.tid == tid))
            await session.execute(delete(ResearchThread).where(ResearchThread.tid == tid))
            await session.execute(delete(ResearchUser).where(ResearchUser.user_id == user.user_id))
            await session.execute(delete(UserForumSnapshot).where(UserForumSnapshot.user_id == user.user_id))

    marker_locked = asyncio.Event()
    release_old = asyncio.Event()
    new_attempting = asyncio.Event()

    class GatedSession:
        def __init__(self, session, *, is_old):
            self.session = session
            self.is_old = is_old
            self.did_pause = False

        async def __aenter__(self):
            await self.session.__aenter__()
            return self

        async def __aexit__(self, *args):
            return await self.session.__aexit__(*args)

        def begin(self):
            return self.session.begin()

        def get_bind(self):
            return self.session.get_bind()

        async def scalar(self, statement):
            return await self.session.scalar(statement)

        async def execute(self, statement):
            table = getattr(getattr(statement, "table", None), "name", None)
            if table == ResearchContentObservation.__tablename__:
                if self.is_old and not self.did_pause:
                    result = await self.session.execute(statement)
                    self.did_pause = True
                    marker_locked.set()
                    await release_old.wait()
                    return result
                if not self.is_old:
                    new_attempting.set()
            return await self.session.execute(statement)

    def old_sessionmaker():
        return GatedSession(sessions(), is_old=True)

    def new_sessionmaker():
        return GatedSession(sessions(), is_old=False)

    try:
        await create_research_tables(engine)
        await cleanup()
        old_task = asyncio.create_task(persist_research_page(old_sessionmaker, old_page))
        try:
            await asyncio.wait_for(marker_locked.wait(), timeout=15)
            new_task = asyncio.create_task(persist_research_page(new_sessionmaker, new_page))
            await asyncio.wait_for(new_attempting.wait(), timeout=15)
        finally:
            release_old.set()
        await asyncio.wait_for(asyncio.gather(old_task, new_task), timeout=30)

        async with sessions() as session:
            thread = await session.scalar(select(ResearchThread).where(ResearchThread.tid == tid))
            post = await session.scalar(select(ResearchPost).where(ResearchPost.pid == reply_pid))
            markers = {
                (row.owner_type, row.owner_id): row.observed_at.astimezone(UTC)
                for row in (await session.scalars(select(ResearchContentObservation))).all()
                if row.tid == tid
            }
            media = (await session.scalars(select(MediaAsset).where(MediaAsset.tid == tid))).all()

        assert thread.body_text == "newer first post"
        assert post.text == "newer reply"
        assert media == []
        assert markers == {
            ("thread_first_post", tid): t2,
            ("post", reply_pid): t2,
        }
    finally:
        release_old.set()
        await cleanup()
        await engine.dispose()
