import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from stat import S_IMODE
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from tiebameow.models.orm import Base
from tiebameow.models.orm import Comment as BaseComment
from tiebameow.models.orm import Post as BasePost
from tiebameow.models.orm import Thread as BaseThread
from tiebameow.models.orm import User as BaseUser

from src.research.exporter import (
    COMMENT_FIELDS,
    CRAWL_QUALITY_FIELDS,
    MEDIA_ASSET_FIELDS,
    POST_FIELDS,
    THREAD_FIELDS,
    USER_FIELDS,
    USER_FORUM_SNAPSHOT_FIELDS,
    ResearchExportValidationError,
    export_research_csvs,
)
from src.research.models import (
    RESEARCH_TABLES,
    MediaAsset,
    ResearchComment,
    ResearchPost,
    ResearchThread,
    ResearchUser,
    UserForumSnapshot,
)


class AsyncResultFromSync:
    def __init__(self, result, *, drop_first=False):
        self._result = result
        self._rows = iter(result.yield_per(1000))
        if drop_first:
            next(self._rows, None)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._rows)
        except StopIteration:
            raise StopAsyncIteration from None

    async def close(self):
        self._result.close()


class AsyncSessionFromSync:
    def __init__(self, session, *, drop_first_stream=False):
        self._session = session
        self._drop_first_stream = drop_first_stream

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None

    def get_bind(self):
        return SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))

    async def scalar(self, statement):
        return self._session.scalar(statement)

    async def stream(self, statement):
        result = self._session.execute(statement)
        drop_first = self._drop_first_stream
        self._drop_first_stream = False
        return AsyncResultFromSync(result, drop_first=drop_first)


def create_export_database():
    engine = create_engine("sqlite:///:memory:")
    base_tables = [BaseUser.__table__, BaseThread.__table__, BasePost.__table__, BaseComment.__table__]
    Base.metadata.create_all(engine, tables=base_tables + list(RESEARCH_TABLES))
    return engine, Session(engine)


def make_base_thread(tid: int, created_at: datetime):
    return BaseThread(
        tid=tid,
        create_time=created_at,
        title="synthetic thread",
        text="synthetic body",
        contents=None,
        last_time=created_at,
        reply_num=0,
        author_level=0,
        fid=7,
        author_id=1,
    )


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        return list(reader.fieldnames or []), list(reader)


@pytest.mark.asyncio
async def test_export_writes_all_seven_csvs_without_losing_legacy_or_research_rows(tmp_path):
    large_user_id = 987654321098765432
    research_tid = 876543210987654321
    legacy_tid = 876543210987654322
    research_only_tid = 876543210987654323
    research_post_pid = 765432109876543210
    first_pid = 765432109876543211
    research_only_pid = 765432109876543212
    legacy_first_pid = 765432109876543213
    research_comment_cid = 654321098765432109
    legacy_comment_cid = 654321098765432110
    research_only_comment_cid = 654321098765432111
    now = datetime(2025, 5, 6, 7, 8, 9, tzinfo=UTC)
    long_text = '第一行，含逗号和"引号"\n第二行中文；不截断。'

    engine = create_engine("sqlite:///:memory:")
    base_tables = [BaseUser.__table__, BaseThread.__table__, BasePost.__table__, BaseComment.__table__]
    Base.metadata.create_all(engine, tables=base_tables + list(RESEARCH_TABLES))
    database = Session(engine)
    database.add_all([
        BaseUser(user_id=large_user_id, portrait="legacy-portrait", user_name="legacy-user", nick_name="旧昵称"),
        BaseUser(user_id=123456789012345678, portrait="p-2", user_name="legacy-2", nick_name="昵称2"),
        BaseThread(
            tid=research_tid,
            create_time=now,
            title="base title",
            text="base first-post text",
            contents=None,
            last_time=now,
            reply_num=0,
            author_level=2,
            fid=7,
            author_id=large_user_id,
        ),
        BaseThread(
            tid=legacy_tid,
            create_time=now,
            title="legacy only",
            text='legacy body, with "quotes"\nand newline',
            contents=None,
            last_time=now,
            reply_num=0,
            author_level=0,
            fid=7,
            author_id=123456789012345678,
        ),
        BasePost(
            pid=research_post_pid,
            create_time=now,
            text="old base post",
            contents=None,
            floor=2,
            reply_num=0,
            author_level=1,
            tid=research_tid,
            fid=7,
            author_id=large_user_id,
        ),
        BasePost(
            pid=legacy_first_pid,
            create_time=now,
            text="legacy first post",
            contents=None,
            floor=1,
            reply_num=0,
            author_level=0,
            tid=legacy_tid,
            fid=7,
            author_id=123456789012345678,
        ),
        BaseComment(
            cid=legacy_comment_cid,
            create_time=now,
            text="legacy comment",
            contents=None,
            author_level=0,
            reply_to_id=0,
            pid=legacy_first_pid,
            tid=legacy_tid,
            fid=7,
            author_id=123456789012345678,
        ),
        ResearchThread(
            tid=research_tid,
            first_pid=first_pid,
            fid=7,
            fname="测试吧",
            title="研究标题",
            body_text=long_text,
            contents_raw=[{"type": "text", "text": long_text}],
            create_time=now,
            author_user_id=large_user_id,
            author_user_name="research-user",
            author_nickname="研究昵称",
            reply_count=None,
            source_api="aiotieba.get_posts",
            observed_at=now,
            source_payload_json={"reply_num": 0},
            missing_reasons_json={"reply_count": "zero_presence_unverified_in_parsed_object"},
        ),
        ResearchThread(
            tid=research_only_tid,
            first_pid=research_only_pid,
            title="extension only",
            body_text="extension-only body",
            source_api="aiotieba.get_posts",
            observed_at=now,
            source_payload_json={},
            missing_reasons_json={},
        ),
        ResearchPost(
            pid=research_post_pid,
            tid=research_tid,
            fid=7,
            floor=2,
            text=long_text,
            author_user_id=large_user_id,
            author_user_name="research-user",
            author_nickname="研究昵称",
            forum_level=3,
            subreply_count_reported=None,
            source_api="aiotieba.get_posts",
            observed_at=now,
            source_payload_json={"reply_num": 0},
            missing_reasons_json={"subreply_count_reported": "zero_presence_unverified_in_parsed_object"},
        ),
        ResearchPost(
            pid=research_only_pid,
            tid=research_only_tid,
            floor=2,
            text="research-only reply",
            source_api="aiotieba.get_posts",
            observed_at=now,
            source_payload_json={},
            missing_reasons_json={},
        ),
        ResearchComment(
            cid=research_comment_cid,
            tid=research_tid,
            parent_post_pid=first_pid,
            parent_post_author_user_id=large_user_id,
            text="comment to exact user",
            author_user_id=123456789012345678,
            reply_to_user_id=large_user_id,
            reply_to_id_raw=large_user_id,
            reply_target_source="explicit_api_user_id",
            source_api="aiotieba.get_comments",
            observed_at=now,
            source_payload_json={},
            missing_reasons_json={},
        ),
        ResearchComment(
            cid=research_only_comment_cid,
            tid=research_only_tid,
            parent_post_pid=research_only_pid,
            reply_to_user_id=None,
            reply_to_id_raw=0,
            reply_target_source="aiotieba_zero_sentinel",
            text="research-only comment",
            source_api="aiotieba.get_comments",
            observed_at=now,
            source_payload_json={"reply_to_id": 0},
            missing_reasons_json={"reply_to_user_id": "aiotieba_zero_sentinel"},
        ),
        ResearchUser(
            user_id=large_user_id,
            user_name="research-user",
            nickname="研究昵称",
            first_observed_at=now,
            last_observed_at=now,
            source_api="aiotieba.get_posts",
            observed_at=now,
            source_payload_json={},
            missing_reasons_json={},
        ),
        ResearchUser(
            user_id=111111111111111111,
            user_name="extension-only-user",
            nickname="扩展用户",
            first_observed_at=now,
            last_observed_at=now,
            source_api="aiotieba.get_comments",
            observed_at=now,
            source_payload_json={},
            missing_reasons_json={},
        ),
        UserForumSnapshot(
            user_id=large_user_id,
            fid=7,
            source_api="aiotieba.get_user_forum_info",
            observed_at=now,
            response_user_id=large_user_id,
            fname="测试吧",
            display_name="吧内显示名",
            forum_level=0,
            forum_level_name="初级会员",
            role_name="吧务",
            status="available",
            source_payload_json={"level": 0},
            missing_reasons_json={},
        ),
        UserForumSnapshot(
            user_id=large_user_id,
            fid=7,
            source_api="aiotieba.get_posts",
            observed_at=now,
            response_user_id=large_user_id,
            forum_level=3,
            ip_location_text="合成地域文案",
            ip_location_source="aiotieba.get_posts.author.ip",
            status="available",
            source_payload_json={},
            missing_reasons_json={},
        ),
        MediaAsset(
            owner_type="post",
            owner_id=research_post_pid,
            position=0,
            tid=research_tid,
            media_type="image",
            media_id="image-1",
            src_url="https://example.test/image-1.jpg",
            api_fragment_type="img",
            raw_fragment_json={"type": "image", "url": "https://example.test/image-1.jpg"},
            source_api="aiotieba.get_posts",
            observed_at=now,
        ),
        MediaAsset(
            owner_type="post",
            owner_id=research_post_pid,
            position=1,
            tid=research_tid,
            media_type="video",
            media_id="video-2",
            source_url="https://example.test/video-2.mp4",
            api_fragment_type="video",
            raw_fragment_json={"type": "video", "url": "https://example.test/video-2.mp4"},
            source_api="aiotieba.get_posts",
            observed_at=now,
        ),
    ])
    database.commit()

    async def quality_rows():
        yield {
            "run_id": "synthetic-run",
            "scope_type": "thread_replies",
            "fid": 7,
            "tid": research_tid,
            "parent_pid": None,
            "parent_kind": None,
            "reported_reply_num": 0,
            "unique_saved_replies": 2,
            "total_pages": 1,
            "success_pages": 1,
            "failure_pages": 0,
            "status": "completed",
            "last_checked": now,
            "count_mismatch": True,
            "reason": "synthetic-count-mismatch",
        }

    output_dir = tmp_path / "exports"

    def async_sessionmaker_():
        return AsyncSessionFromSync(database)

    paths = await export_research_csvs(
        async_sessionmaker_,
        output_dir,
        quality_rows(),
        source_run_id="synthetic-run",
    )

    expected_csv_names = {
        "threads",
        "posts",
        "comments",
        "users",
        "user_forum_snapshots",
        "media_assets",
        "crawl_quality_report",
    }
    batch_prefix = paths["threads"].stem.removeprefix("threads_")
    assert set(paths) == {*expected_csv_names, "manifest"}
    assert {paths[name].name for name in expected_csv_names} == {
        f"{name}_{batch_prefix}.csv" for name in expected_csv_names
    }
    assert paths["manifest"].name == f"manifest_{batch_prefix}.json"
    assert "all-runs_source-synthetic-run_" in batch_prefix
    assert str(large_user_id) not in batch_prefix
    assert S_IMODE(output_dir.stat().st_mode) == 0o700
    assert S_IMODE(paths["threads"].parent.stat().st_mode) == 0o700
    for path in paths.values():
        assert path.is_file()
        assert S_IMODE(path.stat().st_mode) == 0o600

    thread_headers, threads = read_csv(paths["threads"])
    post_headers, posts = read_csv(paths["posts"])
    comment_headers, comments = read_csv(paths["comments"])
    user_headers, users = read_csv(paths["users"])
    snapshot_headers, snapshots = read_csv(paths["user_forum_snapshots"])
    media_headers, media = read_csv(paths["media_assets"])
    quality_headers, quality = read_csv(paths["crawl_quality_report"])

    assert thread_headers == list(THREAD_FIELDS)
    assert post_headers == list(POST_FIELDS)
    assert comment_headers == list(COMMENT_FIELDS)
    assert user_headers == list(USER_FIELDS)
    assert snapshot_headers == list(USER_FORUM_SNAPSHOT_FIELDS)
    assert media_headers == list(MEDIA_ASSET_FIELDS)
    assert quality_headers == list(CRAWL_QUALITY_FIELDS)
    assert (len(threads), len(posts), len(comments), len(users), len(snapshots), len(media), len(quality)) == (
        3,
        3,
        3,
        3,
        2,
        2,
        1,
    )

    threads_by_id = {row["tid"]: row for row in threads}
    assert len(threads_by_id) == len(threads)
    assert threads_by_id[str(research_tid)]["first_pid"] == str(first_pid)
    assert threads_by_id[str(research_tid)]["body_text"] == long_text
    assert threads_by_id[str(research_tid)]["reply_count"] == ""
    assert json.loads(threads_by_id[str(research_tid)]["source_payload_json"])["reply_num"] == 0
    assert threads_by_id[str(legacy_tid)]["record_source"] == "legacy_base_unverified"
    assert threads_by_id[str(legacy_tid)]["body_text"] == 'legacy body, with "quotes"\nand newline'
    assert threads_by_id[str(legacy_tid)]["first_pid"] == str(legacy_first_pid)
    assert json.loads(threads_by_id[str(legacy_tid)]["missing_reasons_json"])["record_source"]

    posts_by_id = {row["pid"]: row for row in posts}
    assert len(posts_by_id) == len(posts)
    assert posts_by_id[str(research_post_pid)]["text"] == long_text
    assert posts_by_id[str(research_post_pid)]["subreply_count_reported"] == ""
    assert json.loads(posts_by_id[str(research_post_pid)]["source_payload_json"])["reply_num"] == 0
    assert posts_by_id[str(legacy_first_pid)]["is_first_post"] == "true"
    assert posts_by_id[str(legacy_first_pid)]["record_source"] == "legacy_base_unverified"
    for row in posts:
        assert row["tid"] in threads_by_id

    comments_by_id = {row["cid"]: row for row in comments}
    assert len(comments_by_id) == len(comments)
    assert comments_by_id[str(research_comment_cid)]["reply_to_user_id"] == str(large_user_id)
    assert comments_by_id[str(research_comment_cid)]["parent_post_pid"] == str(first_pid)
    assert comments_by_id[str(research_only_comment_cid)]["reply_to_user_id"] == ""
    assert json.loads(comments_by_id[str(research_only_comment_cid)]["source_payload_json"])["reply_to_id"] == 0
    assert comments_by_id[str(legacy_comment_cid)]["reply_to_user_id"] == "0"
    for row in comments:
        if row["parent_post_pid"] in posts_by_id:
            parent = posts_by_id[row["parent_post_pid"]]
            assert parent["tid"] == row["tid"]
        else:
            assert threads_by_id[row["tid"]]["first_pid"] == row["parent_post_pid"]

    users_by_id = {row["user_id"]: row for row in users}
    assert len(users_by_id) == len(users)
    assert users_by_id[str(large_user_id)]["nickname"] == "研究昵称"
    assert users_by_id[str(123456789012345678)]["record_source"] == "legacy_base_unverified"
    snapshots_by_api = {row["source_api"]: row for row in snapshots}
    assert snapshots_by_api["aiotieba.get_user_forum_info"]["ip_location_source"] == ""
    assert snapshots_by_api["aiotieba.get_posts"]["ip_location_source"] == "aiotieba.get_posts.author.ip"
    assert media[0]["owner_id"] == str(research_post_pid)
    assert {row["position"] for row in media} == {"0", "1"}
    assert quality[0]["count_mismatch"] == "true"
    assert quality[0]["tid"] == str(research_tid)
    manifest_text = paths["manifest"].read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)
    assert manifest["export_id"] in batch_prefix
    assert manifest["source_run_id_context_only"] == "synthetic-run"
    assert manifest["entity_scope"] == "all_database_rows"
    assert manifest["validation_status"] == "passed"
    assert manifest["quality_evidence_status"] == "present_not_collection_completeness_proof"
    assert manifest["database_row_counts"]["comments"] == len(comments)
    assert manifest["quality_checkpoint_summary_count"] == 1
    assert manifest["csv_row_counts"]["comments"] == len(comments)
    assert manifest["csv_row_counts"]["crawl_quality_report"] == len(quality)
    assert str(large_user_id) not in manifest_text

    database.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_export_writes_header_only_quality_report_when_no_summaries(tmp_path):
    engine, database = create_export_database()

    def async_sessionmaker_():
        return AsyncSessionFromSync(database)

    paths = await export_research_csvs(async_sessionmaker_, tmp_path / "empty", ())

    headers, rows = read_csv(paths["crawl_quality_report"])
    assert headers == list(CRAWL_QUALITY_FIELDS)
    assert len(rows) == 1
    assert rows[0]["status"] == "unknown"
    assert "all-runs" in rows[0]["reason"]
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    assert manifest["quality_evidence_status"] == "unknown_not_tested"
    assert manifest["quality_checkpoint_summary_count"] == 0
    assert manifest["csv_row_counts"]["crawl_quality_report"] == 1
    assert manifest["database_row_counts"] == dict.fromkeys(
        ("threads", "posts", "comments", "users", "user_forum_snapshots", "media_assets"), 0
    )
    assert read_csv(paths["threads"])[0] == list(THREAD_FIELDS)
    assert read_csv(paths["posts"])[0] == list(POST_FIELDS)
    assert read_csv(paths["comments"])[0] == list(COMMENT_FIELDS)
    assert read_csv(paths["users"])[0] == list(USER_FIELDS)
    assert read_csv(paths["user_forum_snapshots"])[0] == list(USER_FORUM_SNAPSHOT_FIELDS)
    assert read_csv(paths["media_assets"])[0] == list(MEDIA_ASSET_FIELDS)
    database.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_export_failure_preserves_old_batch_and_successful_exports_get_unique_names(tmp_path):
    engine, database = create_export_database()

    def async_sessionmaker_():
        return AsyncSessionFromSync(database)

    output_dir = tmp_path / "exports"
    first = await export_research_csvs(async_sessionmaker_, output_dir, ())
    first_batch_dir = first["threads"].parent
    original_thread_csv = first["threads"].read_bytes()
    original_manifest = first["manifest"].read_bytes()

    async def failing_quality_rows():
        yield {"scope_type": "thread_replies", "status": "incomplete", "reason": "synthetic"}
        raise RuntimeError("synthetic checkpoint read failure")

    with pytest.raises(RuntimeError, match="synthetic checkpoint read failure"):
        await export_research_csvs(async_sessionmaker_, output_dir, failing_quality_rows())

    assert first["threads"].read_bytes() == original_thread_csv
    assert first["manifest"].read_bytes() == original_manifest
    assert list(output_dir.iterdir()) == [first_batch_dir]

    second = await export_research_csvs(async_sessionmaker_, output_dir, ())
    assert second["threads"].parent != first_batch_dir
    assert second["threads"].name != first["threads"].name
    assert first["threads"].read_bytes() == original_thread_csv
    assert {path.name for path in output_dir.iterdir()} == {first_batch_dir.name, second["threads"].parent.name}

    database.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_export_rejects_csv_count_mismatch_before_publishing(tmp_path):
    engine, database = create_export_database()
    database.add(make_base_thread(7654321, datetime(2025, 5, 6, 7, 8, 9, tzinfo=UTC)))
    database.commit()

    def async_sessionmaker_():
        return AsyncSessionFromSync(database, drop_first_stream=True)

    output_dir = tmp_path / "exports"
    with pytest.raises(ResearchExportValidationError, match="threads database and staged row counts differ"):
        await export_research_csvs(async_sessionmaker_, output_dir, ())

    assert list(output_dir.iterdir()) == []
    database.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_export_rejects_duplicate_thread_ids_without_leaking_ids(tmp_path):
    engine, database = create_export_database()
    database.add_all([
        make_base_thread(7654321, datetime(2025, 5, 6, 7, 8, 9, tzinfo=UTC)),
        make_base_thread(7654321, datetime(2025, 5, 6, 7, 9, 9, tzinfo=UTC)),
    ])
    database.commit()

    def async_sessionmaker_():
        return AsyncSessionFromSync(database)

    output_dir = tmp_path / "exports"
    with pytest.raises(ResearchExportValidationError) as error:
        await export_research_csvs(async_sessionmaker_, output_dir, ())

    assert "duplicate primary keys" in str(error.value)
    assert "7654321" not in str(error.value)
    assert list(output_dir.iterdir()) == []
    database.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_export_rejects_post_without_an_exported_thread(tmp_path):
    engine, database = create_export_database()
    orphan_tid = 765432199
    database.add(
        BasePost(
            pid=1234567,
            create_time=datetime(2025, 5, 6, 7, 8, 9, tzinfo=UTC),
            text="synthetic orphan post",
            contents=None,
            floor=2,
            reply_num=0,
            author_level=0,
            tid=orphan_tid,
            fid=7,
            author_id=1,
        )
    )
    database.commit()

    def async_sessionmaker_():
        return AsyncSessionFromSync(database)

    output_dir = tmp_path / "exports"
    with pytest.raises(ResearchExportValidationError) as error:
        await export_research_csvs(async_sessionmaker_, output_dir, ())

    assert "post rows do not resolve" in str(error.value)
    assert str(orphan_tid) not in str(error.value)
    assert list(output_dir.iterdir()) == []
    database.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_export_rejects_comment_without_parent_post_or_thread_first_post(tmp_path):
    engine, database = create_export_database()
    now = datetime(2025, 5, 6, 7, 8, 9, tzinfo=UTC)
    database.add_all([
        make_base_thread(7654321, now),
        BasePost(
            pid=1234567,
            create_time=now,
            text="synthetic first post",
            contents=None,
            floor=1,
            reply_num=0,
            author_level=0,
            tid=7654321,
            fid=7,
            author_id=1,
        ),
        BaseComment(
            cid=2345678,
            create_time=now,
            text="synthetic orphan comment",
            contents=None,
            author_level=0,
            reply_to_id=1,
            scrape_time=now,
            pid=3456789,
            tid=7654321,
            fid=7,
            author_id=1,
        ),
    ])
    database.commit()

    def async_sessionmaker_():
        return AsyncSessionFromSync(database)

    output_dir = tmp_path / "exports"
    with pytest.raises(ResearchExportValidationError) as error:
        await export_research_csvs(async_sessionmaker_, output_dir, ())

    assert "comment rows do not resolve" in str(error.value)
    assert "3456789" not in str(error.value)
    assert list(output_dir.iterdir()) == []
    database.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_export_validates_csv_field_larger_than_default_limit_without_truncation(tmp_path):
    engine, database = create_export_database()
    now = datetime(2025, 5, 6, 7, 8, 9, tzinfo=UTC)
    large_body = "长" * 140_000
    database.add(
        ResearchThread(
            tid=7654321,
            title="synthetic long body",
            body_text=large_body,
            source_api="synthetic",
            observed_at=now,
        )
    )
    database.commit()

    def async_sessionmaker_():
        return AsyncSessionFromSync(database)

    original_field_limit = csv.field_size_limit()
    paths = await export_research_csvs(async_sessionmaker_, tmp_path / "exports", ())

    exported_bytes = paths["threads"].read_bytes()
    assert large_body.encode("utf-8") in exported_bytes
    assert json.loads(paths["manifest"].read_text(encoding="utf-8"))["csv_row_counts"]["threads"] == 1
    assert csv.field_size_limit() == original_field_limit

    database.close()
    engine.dispose()
