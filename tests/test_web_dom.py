from __future__ import annotations

import pytest
from lxml import html

from src.research.web_fallback.dom import extract_dom_candidates
from src.research.web_fallback.identity import Target

TID = "81001"
FID = "120"
POST_ID = "9007199254740993"
AUTHOR_ID = "77003"
PORTRAIT = "tb.1.synthetic-owner"


def _target(target_type="thread", **changes):
    values = {
        "target_type": target_type,
        "target_id": TID if target_type == "thread" else POST_ID,
        "tid": TID,
        "fid": FID,
        "pid": POST_ID if target_type == "post" else None,
        "author_user_id": AUTHOR_ID,
        "author_portrait": PORTRAIT,
        "requested_fields": (
            "author_user_id",
            "role_name",
            "is_bawu",
            "forum_level_name",
            "forum_level_icon_ref",
            "forum_level",
            "global_growth_level",
            "global_growth_icon_url",
            "ip_location_text",
            "agree_count",
            "reply_count",
            "collect_count",
            "share_count",
        ),
    }
    values.update(changes)
    return Target(**values)


def _header(*, label="owner", portrait=PORTRAIT, nested=False):
    return f"""
      <div class="head-line user-info">
        <div class="head-info"><a class="name-info-link">
          <div class="name-info">
            <a class="head-name" href="https://tieba.baidu.com/home/main?id={portrait}">{label}</a>
            <span class="icon_bawu tooltip__reference" aria-describedby="role-tip-{label}"></span>
            <span class="icon_level tooltip__reference" aria-describedby="title-tip-{label}">
              <svg><use href="#level_12"></use></svg>
            </span>
            <span class="tag">太保</span>
            <span class="icon_tshows tooltip__reference" aria-describedby="growth-tip-{label}">
              <img class="icon_tshows_img" src="https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp?size=1">
            </span>
          </div>
        </a></div>
      </div>
      <div id="role-tip-{label}">小吧主</div>
      <div id="title-tip-{label}">本吧头衔</div>
      <div id="growth-tip-{label}">贴吧成长等级</div>
    """


def _thread_html(*, tid=TID, fid=FID, owner_portrait=PORTRAIT, nested=True, agree="0", location="广东"):
    nested_markup = ""
    if nested:
        nested_markup = f"""
        <div class="lzl-wrapper">
          <div class="pb-lzl-item">
          {_header(label="nested", portrait=PORTRAIT).replace("#level_12", "#level_7")}
            <div class="pc-pb-comments-desc">
              <div class="comment-desc-left"><span class="ip-address">嵌套地区</span></div>
            </div>
          </div>
        </div>
        """
    return f"""
    <html><body>
      <div class="image-text">
        <div class="pb-content-wrap">
          <div class="richtext-item"><span tid="{tid}" fid="{fid}" index="0"></span></div>
        </div>
        {_header(label="owner", portrait=owner_portrait)}
        <div class="desc-info"><span class="ip-address">{location}</span></div>
        <div class="pc-pb-first-floor-interactive">
          <div class="action-item">
            <svg><use href="#agree_pb"></use></svg><span class="action-number">{agree}</span>
          </div>
          <div class="action-item"><svg><use href="#comment_pb"></use></svg><span class="action-number">32</span></div>
          <div class="action-item"><svg><use href="#collect"></use></svg><span class="action-number">收藏</span></div>
          <div class="action-item"><svg><use href="#share_pb"></use></svg><span class="action-number">1.2万</span></div>
        </div>
        {nested_markup}
      </div>
    </body></html>
    """


def _post_html(*, pid=POST_ID, tid=TID, fid=FID, owner_portrait=PORTRAIT):
    return f"""
    <html><body>
      <div class="pb-content-wrap">
        <div class="richtext-item"><span tid="{tid}" fid="{fid}" index="1"></span></div>
      </div>
      <div class="pb-comment-item" data-id="{pid}">
        {_header(label="post-owner", portrait=owner_portrait)}
        <div class="comment-content">
          <div class="pc-pb-comments-desc">
            <div class="comment-desc-left"><span>第31楼</span><span>2026-11-15</span><span>黑龙江</span></div>
          </div>
        </div>
        <div class="lzl-wrapper">
          <div class="pb-lzl-item">
            {_header(label="nested", portrait=PORTRAIT)}
            <div class="pc-pb-comments-desc">
              <div class="comment-desc-left"><span class="ip-address">嵌套地区</span></div>
            </div>
          </div>
        </div>
      </div>
    </body></html>
    """


def test_thread_dom_uses_nearest_owner_and_tooltip_semantics():
    result = extract_dom_candidates(
        _target(),
        f"https://tieba.baidu.com/p/{TID}?pn=1&sign=private",
        _thread_html(),
    )

    assert result.status == "ok"
    assert result.identity_verified is True
    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert "author_user_id" not in candidates
    assert candidates["role_name"].normalized_value == "小吧主"
    assert candidates["is_bawu"].normalized_value is True
    assert candidates["forum_level_name"].normalized_value == "太保"
    assert candidates["forum_level_icon_ref"].raw_value == "#level_12"
    assert candidates["forum_level"].normalized_value == 12
    assert "global_growth_level" not in candidates
    assert candidates["global_growth_icon_url"].normalized_value == (
        "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp?size=1"
    )
    assert candidates["ip_location_text"].normalized_value == "广东"
    assert candidates["agree_count"].normalized_value == 0
    assert candidates["reply_count"].normalized_value == 32
    assert "collect_count" not in candidates
    assert candidates["share_count"].raw_value == "1.2万"
    assert candidates["share_count"].normalized_value == "12000.0"
    assert candidates["share_count"].precision == "approximate"
    assert candidates["ip_location_text"].source_url == f"https://tieba.baidu.com/p/{TID}?pn=1"
    assert candidates["ip_location_text"].identity.fid == FID
    assert "body_fid" in candidates["ip_location_text"].identity.match_methods
    assert "agree_count" not in result.field_outcomes
    assert result.field_outcomes["collect_count"] == {
        "status": "needs_evidence",
        "reason": "zero_presence_unverified",
    }
    assert result.field_outcomes["global_growth_level"] == {
        "status": "needs_evidence",
        "reason": "numeric_level_not_exposed_in_page",
    }


def test_action_labels_are_not_counts_or_explicit_zero():
    result = extract_dom_candidates(_target(), f"https://tieba.baidu.com/p/{TID}", _thread_html(agree="赞"))
    fields = {candidate.field_name for candidate in result.candidates}

    assert "agree_count" not in fields
    assert "collect_count" not in fields


@pytest.mark.parametrize(
    "ambiguous_action",
    [
        '<use href="#agree_pb"></use><use href="#agree_pb"></use>',
        '<use href="#agree_pb"></use><use href="#share_pb"></use>',
        '<use href="#agree_pb"></use><span class="action-number">1</span>',
    ],
)
def test_action_count_refuses_multiple_counter_symbols_or_number_nodes(ambiguous_action):
    document = html.fromstring(_thread_html(nested=False))
    interactive = document.xpath(
        "//*[contains(concat(' ', normalize-space(@class), ' '), ' pc-pb-first-floor-interactive ')]"
    )[0]
    for child in list(interactive):
        interactive.remove(child)
    interactive.append(
        html.fromstring(
            f'<div class="action-item"><svg>{ambiguous_action}</svg>'
            '<span class="action-number">0</span></div>'
        )
    )
    fixture = html.tostring(document, encoding="unicode")
    target = _target(requested_fields=("agree_count", "share_count"))
    result = extract_dom_candidates(target, f"https://tieba.baidu.com/p/{TID}", fixture)
    fields = {candidate.field_name for candidate in result.candidates}

    assert "agree_count" not in fields
    assert "share_count" not in fields


def test_dom_ip_location_rejects_a_literal_ip_inside_label_text():
    result = extract_dom_candidates(
        _target(),
        f"https://tieba.baidu.com/p/{TID}",
        _thread_html(location="IP属地：192.0.2.8"),
    )
    fields = {candidate.field_name for candidate in result.candidates}

    assert "ip_location_text" not in fields


def test_post_dom_reads_only_a_unique_region_from_owner_comment_desc_spans():
    result = extract_dom_candidates(
        _target("post", requested_fields=("ip_location_text",)),
        f"https://tieba.baidu.com/p/{TID}",
        _post_html(),
    )

    assert [candidate.normalized_value for candidate in result.candidates] == ["黑龙江"]


def test_post_dom_rejects_multiple_unclassed_region_spans():
    fixture = _post_html().replace(
        "<span>黑龙江</span>", "<span>黑龙江</span><span>广东</span>"
    )
    result = extract_dom_candidates(
        _target("post", requested_fields=("ip_location_text",)),
        f"https://tieba.baidu.com/p/{TID}",
        fixture,
    )

    assert "ip_location_text" not in {candidate.field_name for candidate in result.candidates}


def test_post_dom_does_not_treat_arbitrary_text_as_a_region():
    fixture = _post_html().replace("黑龙江", "验证码测试市")
    result = extract_dom_candidates(
        _target("post", requested_fields=("ip_location_text",)),
        f"https://tieba.baidu.com/p/{TID}",
        fixture,
    )

    assert "ip_location_text" not in {candidate.field_name for candidate in result.candidates}


def test_thread_portrait_exception_only_binds_verified_owner_header():
    target = _target()
    valid = extract_dom_candidates(target, f"https://tieba.baidu.com/p/{TID}", _thread_html())
    mismatch = extract_dom_candidates(
        target,
        f"https://tieba.baidu.com/p/{TID}",
        _thread_html(owner_portrait="tb.1.other-user"),
    )

    assert valid.identity_verified is True
    assert valid.candidates
    assert mismatch.identity_verified is False
    assert mismatch.reason == "owner_author_not_verified"
    assert mismatch.candidates == ()


def test_nested_comment_portrait_inside_parent_header_cannot_bind_parent_author():
    fixture = f"""
    <html><body><div class="image-text">
      <div class="pb-content-wrap">
        <div class="richtext-item"><span tid="{TID}" fid="{FID}" index="0"></span></div>
      </div>
      <div class="head-line user-info"><div class="name-info">
        <div class="pb-lzl-item"><div class="head-line user-info"><div class="name-info">
          <a class="head-name" href="https://tieba.baidu.com/home/main?id={PORTRAIT}">same user</a>
        </div></div></div>
      </div></div>
      <div class="pc-pb-first-floor-interactive"></div>
    </div></body></html>
    """
    result = extract_dom_candidates(_target(), f"https://tieba.baidu.com/p/{TID}", fixture)

    assert result.status == "identity_unverified"
    assert result.reason == "owner_author_not_verified"
    assert result.candidates == ()


def test_forum_level_requires_one_owner_icon_with_own_tooltip_and_canonical_fragment():
    no_tooltip = _thread_html().replace(
        'class="icon_level tooltip__reference" aria-describedby="title-tip-owner"',
        'class="icon_level"',
    )
    result = extract_dom_candidates(_target(), f"https://tieba.baidu.com/p/{TID}", no_tooltip)
    fields = {candidate.field_name for candidate in result.candidates}
    assert "forum_level" not in fields
    assert "forum_level_icon_ref" not in fields

    duplicate_icon = _thread_html(nested=False).replace(
        '<span class="tag">太保</span>',
        '<span class="tag">太保</span>'
        '<span class="icon_level tooltip__reference" aria-describedby="duplicate-tip">'
        '<svg><use href="#level_8"></use></svg></span><div id="duplicate-tip">本吧头衔</div>',
    )
    duplicate = extract_dom_candidates(_target(), f"https://tieba.baidu.com/p/{TID}", duplicate_icon)
    assert "forum_level" not in {candidate.field_name for candidate in duplicate.candidates}

    no_fid = extract_dom_candidates(
        _target(fid=None),
        f"https://tieba.baidu.com/p/{TID}",
        _thread_html(nested=False),
    )
    no_fid_fields = {candidate.field_name for candidate in no_fid.candidates}
    assert "forum_level" not in no_fid_fields
    assert "forum_level_name" not in no_fid_fields


def test_self_tooltip_growth_requires_an_explicit_numeric_value():
    fixture = _thread_html(nested=False).replace(
        "贴吧成长等级</div>", "贴吧成长等级：20</div>"
    )
    result = extract_dom_candidates(_target(), f"https://tieba.baidu.com/p/{TID}", fixture)
    candidates = {candidate.field_name: candidate for candidate in result.candidates}

    assert candidates["global_growth_level"].normalized_value == 20


def test_growth_icon_url_requires_its_own_official_growth_tooltip_and_cdn():
    target = _target(requested_fields=("global_growth_icon_url",))
    no_tooltip = _thread_html(nested=False).replace(
        'aria-describedby="growth-tip-owner"', 'aria-describedby="missing-tip"'
    )
    unverified = extract_dom_candidates(target, f"https://tieba.baidu.com/p/{TID}", no_tooltip)
    assert "global_growth_icon_url" not in {candidate.field_name for candidate in unverified.candidates}

    wrong_host = _thread_html(nested=False).replace(
        "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp?size=1",
        "https://example.com/synthetic/growth.webp",
    )
    rejected = extract_dom_candidates(target, f"https://tieba.baidu.com/p/{TID}", wrong_host)
    assert "global_growth_icon_url" not in {candidate.field_name for candidate in rejected.candidates}


@pytest.mark.parametrize("asset_path", ["synthetic/growth.png", "synthetic/growth-asset"])
def test_growth_icon_url_accepts_official_assets_without_webp_extension(asset_path):
    target = _target(requested_fields=("global_growth_icon_url",))
    expected = f"https://tieba-ares.cdn.bcebos.com/{asset_path}"
    fixture = _thread_html(nested=False).replace(
        "https://tieba-ares.cdn.bcebos.com/synthetic/growth.webp?size=1",
        expected,
    )

    result = extract_dom_candidates(target, f"https://tieba.baidu.com/p/{TID}", fixture)

    [candidate] = result.candidates
    assert candidate.normalized_value == expected


def test_post_dom_requires_exact_data_id_and_scopes_header_and_location_to_owner():
    target = _target("post", requested_fields=("role_name", "ip_location_text", "author_user_id"))
    result = extract_dom_candidates(
        target,
        f"https://tieba.baidu.com/p/{TID}",
        _post_html(),
    )

    assert result.identity_verified is True
    candidates = {candidate.field_name: candidate for candidate in result.candidates}
    assert candidates["ip_location_text"].normalized_value == "黑龙江"
    assert candidates["role_name"].normalized_value == "小吧主"
    assert "author_user_id" not in candidates
    assert all(candidate.identity.pid == POST_ID for candidate in candidates.values())


def test_post_data_id_mismatch_and_page_thread_mismatch_fail_closed():
    target = _target("post")
    wrong_post = extract_dom_candidates(target, f"https://tieba.baidu.com/p/{TID}", _post_html(pid="9007199254740994"))
    wrong_tid = extract_dom_candidates(target, "https://tieba.baidu.com/p/81002", _post_html())

    assert wrong_post.identity_verified is False
    assert wrong_post.reason == "post_data_id_missing"
    assert wrong_post.candidates == ()
    assert wrong_tid.reason == "page_tid_mismatch"
    assert wrong_tid.candidates == ()


def test_fid_mismatch_and_numeric_author_id_absence_do_not_create_candidates():
    target = _target()
    wrong_fid = extract_dom_candidates(
        target,
        f"https://tieba.baidu.com/p/{TID}",
        _thread_html(fid="121"),
    )

    assert wrong_fid.identity_verified is False
    assert wrong_fid.reason == "body_fid_mismatch"
    assert wrong_fid.candidates == ()


def test_nested_comment_dom_without_cid_is_never_bound_by_name_or_order():
    target = Target(
        target_type="comment",
        target_id="9007199254740993",
        tid=TID,
        fid=FID,
        cid="9007199254740993",
        parent_post_pid=POST_ID,
        author_user_id=AUTHOR_ID,
        author_portrait=PORTRAIT,
        requested_fields=("ip_location_text", "author_nickname"),
    )
    result = extract_dom_candidates(target, f"https://tieba.baidu.com/p/{TID}", _post_html())

    assert result.status == "identity_unverified"
    assert result.reason == "cid_not_exposed_in_observed_dom"
    assert result.candidates == ()
