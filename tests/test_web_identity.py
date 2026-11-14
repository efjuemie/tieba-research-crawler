from __future__ import annotations

import pytest

from src.research.web_fallback.identity import (
    DuplicateJSONKeyError,
    IdentityError,
    Target,
    canonical_thread_id_from_url,
    make_identity_evidence,
    make_request_context,
    parse_json_unique,
    parse_positive_id,
    portrait_token_from_profile_href,
    sanitize_source_url,
    validate_request_context,
)


def _comment_target(**changes) -> Target:
    values = {
        "target_type": "comment",
        "target_id": "9007199254740993",
        "tid": "81001",
        "fid": "120",
        "cid": "9007199254740993",
        "parent_post_pid": "45002",
        "author_user_id": "77003",
        "author_portrait": "tb.1.synthetic",
        "requested_fields": ("author_nickname",),
    }
    values.update(changes)
    return Target(**values)


@pytest.mark.parametrize(
    "value",
    [True, False, 1.0, 0, "0", "01", "+1", " 1", "-1", (1 << 63), str(1 << 63)],
)
def test_positive_id_rejects_noncanonical_or_unsafe_values(value):
    with pytest.raises(IdentityError):
        parse_positive_id(value)


def test_positive_id_preserves_large_exact_python_int_and_string():
    large = (1 << 53) + 1

    assert parse_positive_id(large) == str(large)
    assert parse_positive_id(str(large)) == str(large)


def test_target_is_frozen_and_enforces_type_specific_identity_shape():
    target = _comment_target()

    assert target.cid == "9007199254740993"
    with pytest.raises((AttributeError, TypeError)):
        target.cid = "9007199254740994"
    with pytest.raises(IdentityError, match="post_target_shape_mismatch"):
        Target("post", "1", "2", pid="3")


def test_duplicate_json_keys_are_rejected_at_every_depth():
    with pytest.raises(DuplicateJSONKeyError, match="duplicate_json_key"):
        parse_json_unique('{"data":{"id":1,"id":2}}')


def test_request_context_uses_natural_thread_post_fields_and_drops_signatures():
    target = _comment_target()
    context = make_request_context(
        target,
        page_url="https://tieba.baidu.com/p/81001?pn=1",
        request_url="https://tieba.baidu.com/c/f/pb/nestedFloor?tbs=sensitive&sign=secret&token=secret2",
        request_method="POST",
        post_data="thread_id=81001&post_id=45002&fid=120&offset=20",
        context_id="natural-1",
    )

    result = validate_request_context(target, context)

    assert result.identity_verified is True
    assert context.request_tid == "81001"
    assert context.request_post_id == "45002"
    assert context.request_offset == "20"
    assert "tbs" not in context.request_url
    assert "sign" not in context.request_url
    assert "token" not in context.request_url
    evidence = make_identity_evidence(target, ("request_tid",), context)
    assert evidence.fid == target.fid
    assert evidence.context_id == "natural-1"


@pytest.mark.parametrize(
    ("page_url", "post_data", "reason"),
    [
        ("https://tieba.baidu.com/p/81002", "thread_id=81001&post_id=45002", "request_tid_mismatch"),
        ("https://tieba.baidu.com/p/81001", "thread_id=81001&post_id=45003", "request_post_id_mismatch"),
        ("https://tieba.baidu.com/p/81001", "thread_id=81001&post_id=45002&fid=121", "request_fid_mismatch"),
        ("https://tieba.baidu.com/p/81001", "thread_id=81001&thread_id=81002&post_id=45002", "conflicting_request_tid"),
    ],
)
def test_request_context_rejects_scope_mismatch(page_url, post_data, reason):
    target = _comment_target()
    context = make_request_context(
        target,
        page_url=page_url,
        request_url="https://tieba.baidu.com/c/f/pb/nestedFloor",
        request_method="POST",
        post_data=post_data,
    )

    result = validate_request_context(target, context)

    assert result.identity_verified is False
    assert result.reason == reason


@pytest.mark.parametrize(
    ("request_url", "request_method", "reason"),
    [
        ("https://tieba.baidu.com/c/f/pb/nestedFloor", "GET", "request_method_mismatch"),
        ("https://tieba.baidu.com/c/f/forum/other", "POST", "request_route_mismatch"),
    ],
)
def test_request_context_requires_natural_nested_floor_route(request_url, request_method, reason):
    target = _comment_target()
    context = make_request_context(
        target,
        page_url="https://tieba.baidu.com/p/81001",
        request_url=request_url,
        request_method=request_method,
        post_data="thread_id=81001&post_id=45002",
    )

    result = validate_request_context(target, context)

    assert result.identity_verified is False
    assert result.reason == reason


def test_profile_token_requires_official_home_main_href_and_non_numeric_token():
    assert portrait_token_from_profile_href(
        "https://tieba.baidu.com/home/main?id=tb.1.synthetic&fr=pb"
    ) == "tb.1.synthetic"
    assert portrait_token_from_profile_href("https://example.com/home/main?id=tb.1.synthetic") is None
    assert portrait_token_from_profile_href("https://tieba.baidu.com/home/main?id=77003") is None
    assert portrait_token_from_profile_href("https://tieba.baidu.com/home/main?id=one&id=two") is None
    assert portrait_token_from_profile_href("http://tieba.baidu.com/home/main?id=tb.1.synthetic") is None
    assert portrait_token_from_profile_href("https://tieba.baidu.com:444/home/main?id=tb.1.synthetic") is None


def test_source_url_sanitizer_removes_sensitive_query_names_case_insensitively():
    result = sanitize_source_url(
        "https://tieba.baidu.com/p/1?pn=2&TBS=x&sign=y&token=z&access_token=a&BDUSS=b&unknown=c#fragment"
    )

    assert result == "https://tieba.baidu.com/p/1?pn=2"


@pytest.mark.parametrize(
    "url",
    [
        "http://tieba.baidu.com/p/1",
        "https://example.com/p/1",
        "https://user:password@tieba.baidu.com/p/1",
        "https://tieba.baidu.com:8443/p/1",
    ],
)
def test_untrusted_tieba_origin_is_not_a_source_or_page_identity(url):
    assert sanitize_source_url(url) == ""
    assert canonical_thread_id_from_url(url) is None
