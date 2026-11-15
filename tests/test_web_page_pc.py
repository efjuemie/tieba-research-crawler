from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from src.research.web_fallback.identity import Target, make_request_context
from src.research.web_fallback.network import (
    extract_page_pc_comment,
    extract_page_pc_thread_counts,
)

TID = "81001"
FID = "120"
POST_ID = "45002"
CID = "9007199254740993"
AUTHOR_ID = "77003"
OBSERVED_AT = datetime(2026, 11, 15, 3, 0, tzinfo=UTC)


def _thread_target(**changes):
    values = {
        "target_type": "thread",
        "target_id": TID,
        "tid": TID,
        "fid": FID,
        "author_user_id": AUTHOR_ID,
        "requested_fields": (
            "reply_count",
            "agree_count",
            "disagree_count",
            "collect_count",
            "share_count",
            "view_count",
        ),
    }
    values.update(changes)
    return Target(**values)


def _comment_target(**changes):
    values = {
        "target_type": "comment",
        "target_id": CID,
        "tid": TID,
        "fid": FID,
        "cid": CID,
        "parent_post_pid": POST_ID,
        "author_user_id": AUTHOR_ID,
        "requested_fields": ("ip_location_text", "agree_count", "disagree_count"),
    }
    values.update(changes)
    return Target(**values)


def _context(target, *, page_url=None, request_url=None, request_method="POST", post_data=None):
    return make_request_context(
        target,
        page_url=page_url or f"https://tieba.baidu.com/p/{TID}?pn=1",
        request_url=request_url or "https://tieba.baidu.com/c/f/pb/page_pc?sign=synthetic",
        request_method=request_method,
        post_data=post_data or f"kz={TID}&pn=1",
        observed_at=OBSERVED_AT,
        context_id="synthetic-page-pc",
    )


def _payload():
    return {
        "error_code": 0,
        "forum": {"id": int(FID)},
        "thread": {
            "id": int(TID),
            "post_id": int(POST_ID),
            "author": {"id": int(AUTHOR_ID)},
            "reply_num": 8,
            "agree": {"agree_num": 5, "disagree_num": 2},
            "collect_num": 0,
            "share_num": 1,
        },
        "first_floor": {"id": int(POST_ID), "author_id": int(AUTHOR_ID)},
        "post_list": [
            {
                "id": int(POST_ID),
                "sub_post_list": {
                    "pid": int(POST_ID),
                    "sub_post_list": [
                        {
                            "id": int(CID),
                            "author_id": int(AUTHOR_ID),
                            "agree": {"agree_num": 3, "disagree_num": 0},
                        }
                    ],
                },
            }
        ],
        "user_list": [{"id": int(AUTHOR_ID), "ip_address": "合成地区"}],
    }


def _extract(parser, payload, target, context=None):
    text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    return parser(target, text, context or _context(target))


def test_page_pc_thread_counts_require_full_identity_and_keep_explicit_zero():
    result = _extract(extract_page_pc_thread_counts, _payload(), _thread_target())

    assert result.status == "ok"
    assert result.identity_verified is True
    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert {name: value.normalized_value for name, value in candidates.items()} == {
        "reply_count": 8,
        "agree_count": 5,
        "disagree_count": 2,
        "collect_count": 0,
        "share_count": 1,
    }
    assert candidates["collect_count"].raw_value == 0
    assert candidates["collect_count"].source_path == "page_pc.thread.collect_num"
    assert candidates["agree_count"].source_path == "page_pc.thread.agree.agree_num"
    assert candidates["collect_count"].source_url == "https://tieba.baidu.com/c/f/pb/page_pc"
    assert candidates["collect_count"].observed_at == OBSERVED_AT
    assert candidates["collect_count"].identity.fid == FID
    assert "response_author_id" in candidates["collect_count"].identity.match_methods
    assert result.field_outcomes["view_count"]["reason"] == "not_observed_in_response"


@pytest.mark.parametrize(
    ("path", "replacement", "reason"),
    [
        (("forum", "id"), "121", "response_fid_mismatch"),
        (("thread", "id"), "81002", "response_tid_mismatch"),
        (("thread", "post_id"), "45003", "response_first_floor_id_mismatch"),
        (("thread", "author", "id"), "77004", "response_author_id_mismatch"),
        (("first_floor", "author_id"), "77004", "response_author_id_mismatch"),
    ],
)
def test_page_pc_thread_rejects_identity_conflicts(path, replacement, reason):
    payload = _payload()
    current = payload
    for key in path[:-1]:
        current = current[key]
    current[path[-1]] = int(replacement)

    result = _extract(extract_page_pc_thread_counts, payload, _thread_target())

    assert result.status == "identity_unverified"
    assert result.reason == reason
    assert result.candidates == ()


def test_page_pc_rejects_duplicate_json_keys_and_invalid_id_types():
    target = _thread_target()
    duplicate = _extract(
        extract_page_pc_thread_counts,
        '{"error_code":0,"forum":{"id":120,"id":121}}',
        target,
    )
    invalid_id = _payload()
    invalid_id["forum"]["id"] = True
    invalid = _extract(extract_page_pc_thread_counts, invalid_id, target)

    assert duplicate.reason == "duplicate_json_key"
    assert duplicate.candidates == ()
    assert invalid.reason == "page_pc_identity_id_invalid"
    assert invalid.candidates == ()


def test_page_pc_context_requires_same_origin_post_and_kz_thread_id():
    target = _thread_target()
    context = _context(target)

    assert context.request_tid == TID
    assert "sign" not in context.request_url
    assert context.page_origin == "https://tieba.baidu.com"
    for changed in (
        replace(context, request_method="GET"),
        replace(context, request_url="https://www.tieba.baidu.com/c/f/pb/page_pc"),
        _context(target, post_data="kz=81002&pn=1"),
    ):
        result = _extract(extract_page_pc_thread_counts, _payload(), target, changed)
        assert result.status == "identity_unverified"
        assert result.candidates == ()


def test_page_pc_comment_requires_exact_parent_cid_and_unique_user_id():
    target = _comment_target()
    result = _extract(extract_page_pc_comment, _payload(), target)

    assert result.status == "ok"
    assert result.identity_verified is True
    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert candidates["ip_location_text"].normalized_value == "合成地区"
    assert candidates["ip_location_text"].source_path == "page_pc.user_list[0].ip_address"
    assert candidates["agree_count"].normalized_value == 3
    assert candidates["disagree_count"].normalized_value == 0
    assert candidates["disagree_count"].identity.cid == CID
    assert "unique_response_user_id" in candidates["agree_count"].identity.match_methods


def test_page_pc_missing_target_cid_does_not_bind_same_author_elsewhere():
    payload = _payload()
    payload["post_list"][0]["sub_post_list"]["sub_post_list"].clear()

    result = _extract(extract_page_pc_comment, payload, _comment_target())

    assert result.status == "identity_unverified"
    assert result.reason == "cid_missing_from_response"
    assert result.candidates == ()


def test_page_pc_comment_page_without_target_parent_is_not_a_identity_failure():
    payload = _payload()
    payload["post_list"] = []

    result = _extract(extract_page_pc_comment, payload, _comment_target())

    assert result.status == "not_tested"
    assert result.reason == "parent_post_missing_from_response"
    assert result.candidates == ()


def test_page_pc_rejects_target_cid_found_under_a_different_parent():
    payload = _payload()
    payload["post_list"][0]["id"] = 45003
    payload["post_list"][0]["sub_post_list"]["pid"] = 45003

    result = _extract(extract_page_pc_comment, payload, _comment_target())

    assert result.status == "identity_unverified"
    assert result.reason == "response_cid_parent_mismatch"
    assert result.candidates == ()


@pytest.mark.parametrize(
    ("path", "key", "value"),
    [
        (("forum",), "fid", 121),
        (("thread",), "tid", 81002),
        (("thread",), "forum_id", 121),
        (("post_list", 0), "post_id", 45003),
        (("post_list", 0, "sub_post_list", "sub_post_list", 0), "thread_id", 81002),
        (("post_list", 0, "sub_post_list", "sub_post_list", 0), "forum_id", 121),
        (("post_list", 0, "sub_post_list", "sub_post_list", 0), "parent_post_id", 45003),
    ],
)
def test_page_pc_comment_rejects_conflicting_optional_identity_aliases(path, key, value):
    payload = _payload()
    current = payload
    for part in path:
        current = current[part]
    current[key] = value

    result = _extract(extract_page_pc_comment, payload, _comment_target())

    assert result.status == "identity_unverified"
    assert result.candidates == ()


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("parent_pid", "response_parent_pid_mismatch"),
        ("author", "response_author_id_mismatch"),
        ("duplicate_user", "duplicate_response_user_id"),
    ],
)
def test_page_pc_comment_rejects_parent_author_and_user_ambiguity(change, reason):
    payload = _payload()
    comment = payload["post_list"][0]["sub_post_list"]["sub_post_list"][0]
    if change == "parent_pid":
        payload["post_list"][0]["sub_post_list"]["pid"] = 45003
    elif change == "author":
        comment["author_id"] = 77004
    else:
        payload["user_list"].append(dict(payload["user_list"][0]))

    result = _extract(extract_page_pc_comment, payload, _comment_target())

    assert result.status == "identity_unverified"
    assert result.reason == reason
    assert result.candidates == ()
