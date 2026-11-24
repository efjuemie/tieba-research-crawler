from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from src.research.web_fallback.identity import Target, make_request_context
from src.research.web_fallback.network import (
    extract_from_parent_snapshot,
    extract_page_pc_comment,
    extract_page_pc_post,
    extract_page_pc_thread_counts,
    verify_parent_snapshot,
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


def _post_target(**changes):
    values = {
        "target_type": "post",
        "target_id": "45003",
        "tid": TID,
        "fid": FID,
        "pid": "45003",
        "author_user_id": AUTHOR_ID,
        "requested_fields": (
            "ip_location_text",
            "global_growth_level",
            "global_growth_icon_url",
            "agree_count",
            "disagree_count",
            "share_count",
        ),
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


def _post_payload(*, ip_address="合成地区", growth_level=4, include_growth_icon=True):
    user = {
        "id": int(AUTHOR_ID),
        "ip_address": ip_address,
        "user_growth": {"level_id": growth_level},
    }
    if include_growth_icon:
        user["user_show_info"] = {
            "feed_head": {
                "main_data": [
                    {
                        "business_info_map": {"icon_tip": "贴吧成长等级"},
                        "icon": {"url": "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp"},
                    }
                ]
            }
        }
    return {
        "error_code": 0,
        "forum": {"id": int(FID)},
        "thread": {"id": int(TID)},
        "post_list": [
            {
                "id": 45003,
                "author_id": int(AUTHOR_ID),
                "agree": {"agree_num": 6, "disagree_num": 99},
                "share_num": 88,
            }
        ],
        "user_list": [user],
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


def test_page_pc_thread_growth_url_uses_only_structured_growth_icon():
    payload = _payload()
    payload["thread"]["author"]["user_show_info"] = {
        "feed_head": {
            "main_data": [
                {
                    "business_info_map": {"icon_tip": "本吧头衔"},
                    "icon": {"url": "https://tieba-ares.cdn.bcebos.com/synthetic/forum.webp"},
                },
                {
                    "business_info_map": {"icon_tip": "贴吧成长等级"},
                    "icon": {"url": "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp"},
                },
            ]
        }
    }
    target = _thread_target(requested_fields=("global_growth_icon_url",))

    result = _extract(extract_page_pc_thread_counts, payload, target)

    [candidate] = result.candidates
    assert candidate.field_name == "global_growth_icon_url"
    assert candidate.normalized_value == "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp"
    assert candidate.identity.author_user_id == AUTHOR_ID


def test_page_pc_thread_extracts_growth_level_and_ip_from_verified_thread_author():
    payload = _payload()
    payload["thread"]["author"].update(
        user_growth={"level_id": 6},
        ip_address=" 浙江 ",
    )
    target = _thread_target(requested_fields=("global_growth_level", "ip_location_text"))

    result = _extract(extract_page_pc_thread_counts, payload, target)
    candidates = {candidate.field_name: candidate for candidate in result.candidates}

    assert candidates["global_growth_level"].normalized_value == 6
    assert candidates["global_growth_level"].source_path == (
        "page_pc.thread.author.user_growth.level_id"
    )
    assert candidates["ip_location_text"].normalized_value == "浙江"
    assert candidates["ip_location_text"].source_path == "page_pc.thread.author.ip_address"
    assert all(candidate.identity.author_user_id == AUTHOR_ID for candidate in candidates.values())


def test_page_pc_thread_unknown_growth_zero_and_bare_ip_are_not_candidates():
    payload = _payload()
    payload["thread"]["author"].update(
        user_growth={"level_id": 0},
        ip_address="192.0.2.8",
    )
    target = _thread_target(requested_fields=("global_growth_level", "ip_location_text"))

    result = _extract(extract_page_pc_thread_counts, payload, target)

    assert "global_growth_level" not in {candidate.field_name for candidate in result.candidates}
    assert result.field_outcomes["global_growth_level"] == {
        "status": "absent_on_page",
        "reason": "global_growth_level_zero_is_unknown",
    }
    assert "ip_location_text" not in {candidate.field_name for candidate in result.candidates}
    assert result.field_outcomes["ip_location_text"] == {
        "status": "needs_evidence",
        "reason": "bare_ip_not_region_label",
    }


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
    assert candidates["ip_location_text"].source_url == "https://tieba.baidu.com/c/f/pb/page_pc"
    assert candidates["agree_count"].normalized_value == 3
    assert candidates["disagree_count"].normalized_value == 0
    assert candidates["disagree_count"].identity.cid == CID
    assert "unique_response_user_id" in candidates["agree_count"].identity.match_methods


def test_verified_page_pc_parent_snapshot_checks_page_pid_and_sibling_author():
    anchor = _comment_target(requested_fields=("ip_location_text",))
    sibling_cid = "9007199254740994"
    sibling_author = "77004"
    sibling = _comment_target(
        target_id=sibling_cid,
        cid=sibling_cid,
        author_user_id=sibling_author,
        requested_fields=("ip_location_text",),
    )
    payload = _payload()
    sibling_comment = deepcopy(payload["post_list"][0]["sub_post_list"]["sub_post_list"][0])
    sibling_comment["id"] = int(sibling_cid)
    sibling_comment["author_id"] = int(sibling_author)
    payload["post_list"][0]["sub_post_list"]["sub_post_list"].append(sibling_comment)
    payload["user_list"].append({"id": int(sibling_author), "ip_address": "浙江"})
    response_text = json.dumps(payload, ensure_ascii=False)
    context = _context(anchor, page_url=f"https://tieba.baidu.com/p/{TID}?pid={POST_ID}")

    snapshot = verify_parent_snapshot(anchor, response_text, context)
    assert snapshot is not None
    assert snapshot.request_context.target == anchor

    result = extract_from_parent_snapshot(sibling, snapshot)
    assert result.status == "ok"
    candidate = next(candidate for candidate in result.candidates if candidate.field_name == "ip_location_text")
    assert candidate.normalized_value == "浙江"
    assert candidate.identity.cid == sibling_cid
    assert candidate.identity.author_user_id == sibling_author

    wrong_page_parent = _context(anchor, page_url=f"https://tieba.baidu.com/p/{TID}?pid=45003")
    assert verify_parent_snapshot(anchor, response_text, wrong_page_parent) is None


def test_page_pc_comment_extracts_growth_from_exact_user_without_nested_request():
    payload = _payload()
    user = payload["user_list"][0]
    user["user_growth"] = {"level_id": 5}
    user["user_show_info"] = {
        "feed_head": {
            "main_data": [
                {
                    "business_info_map": {"icon_tip": "贴吧成长等级"},
                    "icon": {"url": "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp"},
                }
            ]
        }
    }
    target = _comment_target(requested_fields=("global_growth_level", "global_growth_icon_url"))

    result = _extract(extract_page_pc_comment, payload, target)

    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert result.identity_verified is True
    assert candidates["global_growth_level"].normalized_value == 5
    assert candidates["global_growth_level"].source_path == "page_pc.user_list[0].user_growth.level_id"
    assert candidates["global_growth_icon_url"].normalized_value == (
        "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp"
    )
    assert candidates["global_growth_icon_url"].source_path.startswith("page_pc.user_list[0].")


def test_page_pc_comment_missing_growth_keeps_exact_cid_identity():
    target = _comment_target(requested_fields=("global_growth_level", "global_growth_icon_url"))

    result = _extract(extract_page_pc_comment, _payload(), target)

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.candidates == ()
    assert result.identity_evidence.cid == CID
    assert result.field_outcomes["global_growth_level"] == {
        "status": "not_tested",
        "reason": "not_observed_in_response",
    }
    assert result.field_outcomes["global_growth_icon_url"] == {
        "status": "not_tested",
        "reason": "growth_icon_not_exposed_in_response",
    }


def test_page_pc_comment_extracts_exact_reply_prefix_name_and_uid():
    payload = _payload()
    comment = payload["post_list"][0]["sub_post_list"]["sub_post_list"][0]
    comment["content"] = [
        {"type": 0, "text": "回复 "},
        {"type": 4, "text": "Synthetic Mention", "uid": 99001},
    ]
    target = _comment_target(requested_fields=("reply_to_display_name", "reply_to_user_id"))

    result = _extract(extract_page_pc_comment, payload, target)

    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert candidates["reply_to_display_name"].normalized_value == "Synthetic Mention"
    assert candidates["reply_to_display_name"].raw_value == {
        "mention_type": 4,
        "mention_uid": "99001",
    }
    assert candidates["reply_to_user_id"].normalized_value == 99001
    assert candidates["reply_to_display_name"].source_path.endswith("content[1].text")
    assert candidates["reply_to_user_id"].source_path.endswith("content[1].uid")
    assert candidates["reply_to_display_name"].identity.cid == CID


def test_page_pc_comment_keeps_display_only_mention_when_uid_is_unavailable():
    payload = _payload()
    comment = payload["post_list"][0]["sub_post_list"]["sub_post_list"][0]
    comment["content"] = [
        {"type": 0, "text": "回复 "},
        {"type": 4, "text": "Synthetic Mention", "uid": 0},
    ]
    target = _comment_target(requested_fields=("reply_to_display_name", "reply_to_user_id"))

    result = _extract(extract_page_pc_comment, payload, target)

    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert candidates["reply_to_display_name"].raw_value == {
        "mention_type": 4,
        "mention_uid": None,
    }
    assert candidates["reply_to_display_name"].normalized_value == "Synthetic Mention"
    assert "reply_to_user_id" not in candidates
    assert result.field_outcomes["reply_to_user_id"] == {
        "status": "absent_on_page",
        "reason": "reply_mention_uid_not_valid",
    }


def test_page_pc_comment_marks_missing_reply_prefix_and_unknown_growth_zero():
    payload = _payload()
    payload["user_list"][0]["user_growth"] = {"level_id": 0}
    target = _comment_target(
        requested_fields=(
            "global_growth_level",
            "global_growth_icon_url",
            "reply_to_display_name",
            "reply_to_user_id",
        )
    )

    result = _extract(extract_page_pc_comment, payload, target)

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.candidates == ()
    assert result.identity_evidence is not None
    assert result.identity_evidence.cid == _comment_target().cid
    assert result.identity_evidence.author_user_id == _comment_target().author_user_id
    assert "response_cid" in result.identity_evidence.match_methods
    assert "response_author_id" in result.identity_evidence.match_methods
    assert "response_forum_id" in result.identity_evidence.match_methods
    assert "response_sub_post_list_pid" in result.identity_evidence.match_methods
    assert result.field_outcomes["global_growth_level"] == {
        "status": "absent_on_page",
        "reason": "global_growth_level_zero_is_unknown",
    }
    assert result.field_outcomes["global_growth_icon_url"]["reason"] == "growth_icon_not_exposed_in_response"
    for field in ("reply_to_display_name", "reply_to_user_id"):
        assert result.field_outcomes[field] == {
            "status": "absent_on_page",
            "reason": "reply_prefix_not_present",
        }


def test_page_pc_comment_marks_empty_author_ip_as_absent():
    payload = _payload()
    payload["user_list"][0]["ip_address"] = "  "
    target = _comment_target(requested_fields=("ip_location_text",))

    result = _extract(extract_page_pc_comment, payload, target)

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.candidates == ()
    assert result.field_outcomes["ip_location_text"] == {
        "status": "absent_on_page",
        "reason": "ip_location_empty_in_response",
    }


def test_page_pc_comment_rejects_reply_candidates_when_comment_author_uid_mismatches():
    payload = _payload()
    comment = payload["post_list"][0]["sub_post_list"]["sub_post_list"][0]
    comment["author_id"] = 77004
    comment["content"] = [
        {"type": 0, "text": "回复 "},
        {"type": 4, "text": "Synthetic Mention", "uid": 99001},
    ]
    target = _comment_target(requested_fields=("reply_to_display_name", "reply_to_user_id"))

    result = _extract(extract_page_pc_comment, payload, target)

    assert result.status == "identity_unverified"
    assert result.reason == "response_author_id_mismatch"
    assert result.candidates == ()


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


def test_page_pc_post_uses_exact_pid_and_unique_user_metadata():
    result = _extract(extract_page_pc_post, _post_payload(), _post_target())

    assert result.status == "ok"
    assert result.identity_verified is True
    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert {name: candidate.normalized_value for name, candidate in candidates.items()} == {
        "ip_location_text": "合成地区",
        "global_growth_level": 4,
        "global_growth_icon_url": "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp",
        "agree_count": 6,
    }
    assert candidates["ip_location_text"].source_path == "page_pc.user_list[0].ip_address"
    assert candidates["global_growth_level"].source_path == "page_pc.user_list[0].user_growth.level_id"
    assert candidates["agree_count"].source_path == "page_pc.post_list[0].agree.agree_num"
    assert candidates["agree_count"].identity.pid == "45003"
    assert "unique_response_user_id" in candidates["agree_count"].identity.match_methods
    assert "disagree_count" not in candidates
    assert "share_count" not in candidates


def test_page_pc_post_empty_metadata_keeps_verified_identity_and_unknown_zero():
    payload = _post_payload(ip_address="  ", growth_level=0, include_growth_icon=False)
    payload["post_list"][0].pop("agree")
    result = _extract(
        extract_page_pc_post,
        payload,
        _post_target(),
    )

    assert result.status == "ok"
    assert result.identity_verified is True
    assert result.candidates == ()
    assert result.field_outcomes["ip_location_text"]["reason"] == "ip_location_empty_in_response"
    assert result.field_outcomes["global_growth_level"] == {
        "status": "absent_on_page",
        "reason": "global_growth_level_zero_is_unknown",
    }
    assert result.field_outcomes["global_growth_icon_url"]["reason"] == "growth_icon_not_exposed_in_response"
    assert result.field_outcomes["agree_count"]["reason"] == "not_observed_in_response"


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda payload: payload["forum"].update(id=121), "response_fid_mismatch"),
        (lambda payload: payload["thread"].update(id=81002), "response_tid_mismatch"),
        (lambda payload: payload["post_list"][0].update(pid=45004), "response_post_id_mismatch"),
        (lambda payload: payload["post_list"][0].update(author_id=77004), "response_author_id_mismatch"),
        (lambda payload: payload["user_list"].append(dict(payload["user_list"][0])), "duplicate_response_user_id"),
    ],
)
def test_page_pc_post_rejects_identity_mismatches(mutate, reason):
    payload = _post_payload()
    mutate(payload)

    result = _extract(extract_page_pc_post, payload, _post_target())

    assert result.status == "identity_unverified"
    assert result.reason == reason
    assert result.candidates == ()


def test_page_pc_post_does_not_borrow_user_from_another_pid_and_missing_pid_can_continue():
    payload = _post_payload()
    payload["post_list"][0]["id"] = 45004

    result = _extract(extract_page_pc_post, payload, _post_target())

    assert result.status == "not_tested"
    assert result.reason == "target_post_missing_from_response"
    assert result.candidates == ()


def test_page_pc_post_request_requires_exact_thread_context_and_safe_post_route():
    target = _post_target()
    context = _context(target)
    for changed in (
        _context(target, post_data="kz=81002&pn=1"),
        replace(context, request_method="GET"),
        replace(context, request_url="https://tieba.baidu.com/c/f/pb/other"),
        _context(target, request_url="http://tieba.baidu.com/c/f/pb/page_pc?sign=synthetic"),
    ):
        result = _extract(extract_page_pc_post, _post_payload(), target, changed)
        assert result.status == "identity_unverified"
        assert result.candidates == ()


def test_page_pc_post_rejects_duplicate_target_pid():
    payload = _post_payload()
    payload["post_list"].append(dict(payload["post_list"][0]))

    result = _extract(extract_page_pc_post, payload, _post_target())

    assert result.status == "identity_unverified"
    assert result.reason == "duplicate_response_post_id"
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
