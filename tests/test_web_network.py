from __future__ import annotations

import json
from urllib.parse import urlencode

import pytest

from src.research.web_fallback.dom import extract_comment_author_growth
from src.research.web_fallback.identity import Target, make_request_context
from src.research.web_fallback.network import extract_nested_floor

TID = "81001"
FID = "120"
POST_ID = "45002"
CID = "9007199254740993"
AUTHOR_ID = "77003"


def _target(**changes) -> Target:
    values = {
        "target_type": "comment",
        "target_id": CID,
        "tid": TID,
        "fid": FID,
        "cid": CID,
        "parent_post_pid": POST_ID,
        "author_user_id": AUTHOR_ID,
        "author_portrait": "tb.1.synthetic",
        "requested_fields": (
            "author_user_name",
            "author_nickname",
            "author_portrait",
            "ip_location_text",
            "agree_count",
            "disagree_count",
            "reply_count",
            "global_growth_level",
            "forum_level",
            "forum_level_name",
        ),
    }
    values.update(changes)
    return Target(**values)


def _context(target: Target | None = None, *, post_id: str = POST_ID, fid: str = FID):
    target = target or _target()
    return make_request_context(
        target,
        page_url=f"https://tieba.baidu.com/p/{TID}?pn=1",
        request_url="https://tieba.baidu.com/c/f/pb/nestedFloor?tbs=secret&sign=secret&token=secret",
        request_method="POST",
        post_data=f"thread_id={TID}&post_id={post_id}&fid={fid}&offset=40",
        context_id="request-capture-1",
    )


def _payload(
    *,
    cid: str = CID,
    author_id=AUTHOR_ID,
    nested_author_id=AUTHOR_ID,
    fid: str = FID,
    ip_address: str = "黑龙江",
    error_code: int = 0,
):
    forum_schema = "tiebaapp://forum/level?" + urlencode({"params": json.dumps({"forumId": int(FID)})})
    return {
        "error_code": error_code,
        "data": {
            "post_list": [
                {
                    "id": int(cid),
                    "fid": int(fid),
                    "author_id": author_id,
                    "author": {
                        "id": nested_author_id,
                        "name": "synthetic-user",
                        "name_show": "重复昵称",
                        "portrait": "tb.1.synthetic",
                        "ip_address": ip_address,
                        "level_id": 0,
                        "user_growth": {"level_id": 6},
                    },
                    "agree": {"agree_num": 0, "disagree_num": 0},
                    "user_show_info": {
                        "feed_head": {
                            "main_data": [
                                {
                                    "custom": {
                                        "style": "forum_level",
                                        "data": json.dumps({"level_id": "8", "level_name": "本吧头衔"}),
                                    },
                                    "schema": forum_schema,
                                }
                            ]
                        }
                    },
                }
            ]
        }
    }


def _extract(payload, *, target: Target | None = None, context=None, status=200):
    target = target or _target()
    return extract_nested_floor(
        target,
        payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False),
        context or _context(target),
        response_status=status,
    )


def _comment_author_growth_html():
    return f"""
    <html><body>
      <div class="pb-content-wrap">
        <div class="richtext-item"><span tid="{TID}" fid="{FID}" index="1"></span></div>
      </div>
      <div class="pb-comment-item" data-id="{POST_ID}">
        <div class="comment-content"><div class="lzl-wrapper">
          <div class="pb-lzl-item" data-cid="9007199254740994">
            <div class="head-line user-info">
              <div class="name-info">
                <a class="head-name" href="https://tieba.baidu.com/home/main?id=tb.1.synthetic">作者</a>
              </div>
              <span class="icon_tshows tooltip__reference" aria-describedby="growth-tip">
                <img class="icon_tshows_img" src="https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp">
              </span>
            </div>
            <div id="growth-tip">贴吧成长等级：Lv. 6</div>
          </div>
        </div></div>
      </div>
    </body></html>
    """


def test_nested_floor_uses_python_raw_json_ids_and_explicit_zero_presence():
    result = _extract(_payload())

    assert result.status == "ok"
    assert result.identity_verified is True
    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert candidates["author_nickname"].raw_value == "重复昵称"
    assert candidates["ip_location_text"].raw_value == "黑龙江"
    assert candidates["agree_count"].raw_value == 0
    assert candidates["agree_count"].normalized_value == 0
    assert candidates["disagree_count"].normalized_value == 0
    assert candidates["global_growth_level"].normalized_value == 6
    assert candidates["forum_level"].normalized_value == 8
    assert candidates["forum_level_name"].normalized_value == "本吧头衔"
    assert candidates["agree_count"].source_url == "https://tieba.baidu.com/c/f/pb/nestedFloor"
    assert candidates["author_nickname"].identity.request_offset == "40"
    assert candidates["author_nickname"].identity.context_id == "request-capture-1"
    assert candidates["author_nickname"].identity.author_user_id == AUTHOR_ID
    assert candidates["author_nickname"].identity.fid == FID
    assert "request_parent_post_pid" in candidates["author_nickname"].identity.match_methods
    assert "response_cid" in candidates["author_nickname"].identity.match_methods
    assert "response_author_id" in candidates["author_nickname"].identity.match_methods
    assert "response_forum_id" not in candidates["author_nickname"].identity.match_methods
    assert "response_parent_post_id" not in candidates["author_nickname"].identity.match_methods
    assert "request_fid" in candidates["author_nickname"].identity.match_methods
    assert "response_fid" in candidates["author_nickname"].identity.match_methods
    assert "author_nickname" not in result.field_outcomes
    assert result.field_outcomes["reply_count"] == {
        "status": "not_tested",
        "reason": "not_observed_in_response",
    }


def test_nested_floor_extracts_reply_target_only_from_exact_prefix_and_type4_mention():
    target = _target(requested_fields=("reply_to_display_name", "reply_to_user_id"))
    payload = _payload()
    payload["data"]["post_list"][0]["content"] = [
        {"type": 0, "text": "回复 "},
        {"type": 4, "text": "Synthetic Mention", "uid": 99001},
        {"type": 0, "text": " synthetic body"},
    ]

    result = _extract(payload, target=target)

    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert candidates["reply_to_display_name"].normalized_value == "Synthetic Mention"
    assert candidates["reply_to_display_name"].raw_value == {
        "mention_type": 4,
        "mention_uid": "99001",
    }
    assert candidates["reply_to_user_id"].normalized_value == 99001
    assert candidates["reply_to_display_name"].identity.cid == CID
    assert candidates["reply_to_display_name"].identity.parent_post_pid == POST_ID


def test_nested_floor_reply_display_name_only_requires_type4_mention_with_exact_prefix():
    target = _target(requested_fields=("reply_to_display_name", "reply_to_user_id"))
    payload = _payload()
    payload["data"]["post_list"][0]["content"] = [
        {"type": 0, "text": "回复 "},
        {"type": 4, "text": "Synthetic Mention", "uid": 0},
    ]
    result = _extract(payload, target=target)
    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert candidates["reply_to_display_name"].normalized_value == "Synthetic Mention"
    assert candidates["reply_to_display_name"].raw_value["mention_uid"] is None
    assert "reply_to_user_id" not in candidates

    payload["data"]["post_list"][0]["content"][0]["text"] = "回复"
    not_exact = _extract(payload, target=target)
    assert not_exact.candidates == ()


@pytest.mark.parametrize("asset_path", ["synthetic/growth.png", "synthetic/growth-asset"])
def test_nested_floor_growth_icon_url_requires_structured_growth_semantics(asset_path):
    target = _target(requested_fields=("global_growth_icon_url",))
    payload = _payload()
    payload["data"]["post_list"][0]["author"]["user_show_info"] = {
        "feed_head": {
            "main_data": [
                {
                    "business_info_map": {"icon_tip": "本吧头衔"},
                    "icon": {"url": "https://tieba-ares.cdn.bcebos.com/synthetic/forum.webp"},
                },
                {
                    "business_info_map": {"icon_tip": "贴吧成长等级"},
                    "icon": {"url": f"https://tieba-ares.cdn.bcebos.com/{asset_path}?size=1"},
                },
            ]
        }
    }

    result = _extract(payload, target=target)

    [candidate] = result.candidates
    assert candidate.field_name == "global_growth_icon_url"
    assert candidate.normalized_value == (
        f"https://tieba-ares.cdn.bcebos.com/{asset_path}?size=1"
    )


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"cid": "9007199254740994"}, "cid_missing_from_response"),
        ({"author_id": "77004", "nested_author_id": "77004"}, "response_author_id_mismatch"),
        ({"author_id": "77003", "nested_author_id": "77004"}, "response_author_id_disagreement"),
        ({"author_id": "malformed", "nested_author_id": "malformed"}, "invalid_response_author_id"),
        ({"fid": "121"}, "response_fid_mismatch"),
    ],
)
def test_nested_floor_identity_mismatch_never_emits_candidates(kwargs, reason):
    result = _extract(_payload(**kwargs))

    assert result.identity_verified is False
    assert result.reason == reason
    assert result.candidates == ()


@pytest.mark.parametrize(
    ("alias", "value", "reason"),
    [
        ("tid", "9999", "response_tid_mismatch"),
        ("thread_id", "9999", "response_tid_mismatch"),
        ("forum_id", "9999", "response_fid_mismatch"),
        ("parent_post_id", "9999", "response_parent_pid_mismatch"),
        ("pid", "9999", "response_parent_identity_mismatch"),
        ("post_id", "9999", "response_parent_identity_mismatch"),
    ],
)
def test_nested_floor_rejects_exposed_conflicting_context_aliases(alias, value, reason):
    payload = _payload()
    payload["data"]["post_list"][0][alias] = value

    result = _extract(payload)

    assert result.status == "identity_unverified"
    assert result.reason == reason
    assert result.identity_evidence is None
    assert result.candidates == ()


def test_nested_floor_identity_composes_with_comment_author_growth_without_body_parent_proof():
    target = _target(requested_fields=("global_growth_level", "global_growth_icon_url"))
    payload = _payload()
    payload["data"]["post_list"][0]["author"].pop("user_growth")
    network_result = _extract(payload, target=target)
    assert network_result.status == "ok"
    assert network_result.candidates == ()
    assert network_result.identity_evidence is not None
    assert "request_parent_post_pid" in network_result.identity_evidence.match_methods
    assert "response_cid" in network_result.identity_evidence.match_methods
    assert "response_author_id" in network_result.identity_evidence.match_methods
    assert "response_parent_post_id" not in network_result.identity_evidence.match_methods
    assert "response_sub_post_list_pid" not in network_result.identity_evidence.match_methods

    dom_result = extract_comment_author_growth(
        target,
        f"https://tieba.baidu.com/p/{TID}?pid=123&pn=74",
        _comment_author_growth_html(),
        network_result.identity_evidence,
    )
    candidates = {candidate.field_name: candidate for candidate in dom_result.candidates}

    assert dom_result.status == "ok"
    assert dom_result.identity_verified is True
    assert candidates["global_growth_level"].normalized_value == 6
    assert candidates["global_growth_icon_url"].normalized_value == (
        "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp"
    )
    assert all(candidate.source_url == f"https://tieba.baidu.com/p/{TID}" for candidate in candidates.values())
    assert all("request_parent_post_pid" in c.identity.match_methods for c in candidates.values())
    assert all("same_author_profile_growth" in c.identity.match_methods for c in candidates.values())


@pytest.mark.parametrize(
    ("error_code", "reason"),
    [(1, "response_error_code_nonzero"), (-1, "response_error_code_nonzero")],
)
def test_nonzero_nested_floor_error_code_blocks_matching_response_candidate(error_code, reason):
    result = _extract(_payload(error_code=error_code))

    assert result.status == "dom_changed"
    assert result.reason == reason
    assert result.identity_verified is False
    assert result.candidates == ()


def test_missing_or_invalid_nested_floor_error_code_is_not_treated_as_success():
    payload = _payload()
    del payload["error_code"]
    missing = _extract(payload)
    invalid = _extract({**payload, "error_code": "0"})

    assert missing.reason == "response_error_code_missing"
    assert invalid.reason == "invalid_response_error_code"
    assert missing.candidates == invalid.candidates == ()


@pytest.mark.parametrize("missing", ["author_id", "author.id"])
@pytest.mark.parametrize("value", ["missing", None])
def test_both_response_author_ids_are_required(missing, value):
    payload = _payload()
    item = payload["data"]["post_list"][0]
    field = item if missing == "author_id" else item["author"]
    key = "author_id" if missing == "author_id" else "id"
    if value == "missing":
        del field[key]
    else:
        field[key] = None

    result = _extract(payload)

    assert result.status == "identity_unverified"
    assert result.reason == "response_author_id_missing"
    assert result.candidates == ()
    assert result.field_outcomes["author_nickname"]["status"] == "identity_unverified"


def test_missing_network_fids_use_only_the_known_tid_fid_binding_method():
    target = _target()
    payload = _payload()
    item = payload["data"]["post_list"][0]
    del item["fid"]
    context = make_request_context(
        target,
        page_url=f"https://tieba.baidu.com/p/{TID}",
        request_url="https://tieba.baidu.com/c/f/pb/nestedFloor",
        request_method="POST",
        post_data=f"thread_id={TID}&post_id={POST_ID}&offset=40",
    )

    result = _extract(payload, target=target, context=context)

    assert result.identity_verified is True
    evidence = result.candidates[0].identity
    assert evidence.fid == FID
    assert "known_api_tid_fid_binding" in evidence.match_methods
    assert "request_fid" not in evidence.match_methods
    assert "response_fid" not in evidence.match_methods


def test_wrong_request_parent_or_thread_context_rejects_response():
    target = _target()
    wrong_parent = _context(target, post_id="45003")
    result = _extract(_payload(), target=target, context=wrong_parent)

    assert result.status == "identity_unverified"
    assert result.reason == "request_post_id_mismatch"
    assert result.candidates == ()


def test_bare_ip_address_is_not_an_ip_location_candidate():
    result = _extract(_payload(ip_address="192.0.2.8"))

    assert result.status == "ok"
    assert "ip_location_text" not in {candidate.field_name for candidate in result.candidates}


def test_region_label_that_contains_a_bare_ip_is_not_a_location_candidate():
    result = _extract(_payload(ip_address="IP属地：192.0.2.8"))

    assert "ip_location_text" not in {candidate.field_name for candidate in result.candidates}


def test_duplicate_identity_json_keys_are_rejected():
    target = _target()
    context = _context(target)
    text = '{"data":{"post_list":[{"id":9007199254740993,"id":9007199254740994}]}}'

    result = extract_nested_floor(target, text, context)

    assert result.status == "identity_unverified"
    assert result.reason == "duplicate_json_key"
    assert result.candidates == ()


def test_missing_count_fields_are_not_filled_with_zero():
    payload = _payload()
    del payload["data"]["post_list"][0]["agree"]["disagree_num"]

    result = _extract(payload)
    fields = {candidate.field_name for candidate in result.candidates}

    assert "agree_count" in fields
    assert "disagree_count" not in fields


def test_generic_author_level_zero_is_not_used_as_forum_or_growth_level():
    payload = _payload()
    payload["data"]["post_list"][0]["author"].pop("user_growth")

    result = _extract(payload)
    candidates = {candidate.field_name: candidate for candidate in result.candidates}

    assert "global_growth_level" not in candidates
    assert candidates["forum_level"].normalized_value == 8


def test_nested_floor_keeps_zero_growth_unknown_and_explicit_zero_agree_count():
    payload = _payload()
    payload["data"]["post_list"][0]["author"]["user_growth"]["level_id"] = 0

    result = _extract(payload)

    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert "global_growth_level" not in candidates
    assert result.field_outcomes["global_growth_level"] == {
        "status": "absent_on_page",
        "reason": "global_growth_level_zero_is_unknown",
    }
    assert candidates["agree_count"].normalized_value == 0
