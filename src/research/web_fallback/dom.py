"""Owner-scoped extraction from captured modern PC Tieba HTML."""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from lxml import html

from .identity import (
    IdentityError,
    ObservationCandidate,
    Target,
    canonical_thread_id_from_url,
    make_identity_evidence,
    parse_positive_id,
    portrait_token_from_profile_href,
    sanitize_source_url,
)

_OWNER_BOUNDARIES = {"image-text", "pb-comment-item", "pb-lzl-item"}
_COUNTER_SYMBOLS = {
    "agree_pb": "agree_count",
    "comment_pb": "reply_count",
    "collect": "collect_count",
    "share_pb": "share_count",
}


@dataclass(frozen=True)
class DOMExtractionResult:
    status: str
    reason: str | None
    identity_verified: bool
    candidates: tuple[ObservationCandidate, ...]
    field_outcomes: dict[str, dict[str, str]] = field(default_factory=dict)


def extract_dom_candidates(
    target: Target,
    page_url: str,
    html_text: str,
    *,
    observed_at: datetime | None = None,
) -> DOMExtractionResult:
    """Extract requested values only after binding an owner to target identity."""
    if target.target_type == "comment":
        return _failed_dom_result(target, "identity_unverified", "cid_not_exposed_in_observed_dom")
    try:
        document = html.fromstring(html_text)
    except (ValueError, TypeError):
        return _failed_dom_result(target, "dom_changed", "html_parse_error")

    page_tid = canonical_thread_id_from_url(page_url)
    if page_tid != target.tid:
        return _failed_dom_result(target, "identity_unverified", "page_tid_mismatch")
    owner, owner_reason = _target_owner(document, target)
    if owner is None:
        return _failed_dom_result(target, "dom_changed", owner_reason)
    _context_root, content_context = _context_scope_for_owner(owner)
    if not content_context:
        return _failed_dom_result(target, "dom_changed", "thread_context_not_exposed")
    if any(tid is None for tid, _fid in content_context):
        return _failed_dom_result(target, "identity_unverified", "body_identity_malformed")
    if any(tid != target.tid for tid, _fid in content_context):
        return _failed_dom_result(target, "identity_unverified", "body_tid_mismatch")
    fids = {fid for _tid, fid in content_context if fid is not None}
    if target.fid is not None and any(fid != target.fid for fid in fids):
        return _failed_dom_result(target, "identity_unverified", "body_fid_mismatch")

    if not _owner_author_matches(owner, target):
        return _failed_dom_result(target, "identity_unverified", "owner_author_not_verified")

    methods = ["canonical_page_tid", "body_tid"]
    if target.fid is not None and any(fid == target.fid for _tid, fid in content_context):
        methods.append("body_fid")
    if target.target_type == "post":
        methods.append("post_data_id")
    methods.extend(("owner_scoped_header", "profile_link_portrait_matches_api"))
    evidence = make_identity_evidence(target, tuple(methods))
    observed_at = observed_at or datetime.now(UTC)
    candidates: list[ObservationCandidate] = []
    field_outcomes = {
        field_name: {"status": "not_tested", "reason": "not_observed_in_bounded_page"}
        for field_name in target.requested_fields
    }

    def mark(field_name: str, status: str, reason: str) -> None:
        if field_name in field_outcomes:
            field_outcomes[field_name] = {"status": status, "reason": reason}

    def add(
        field_name: str,
        raw_value: Any,
        normalized_value: Any,
        source_path: str,
        *,
        precision: str = "exact",
    ) -> None:
        if field_name not in target.requested_fields:
            return
        field_outcomes.pop(field_name, None)
        candidates.append(
            ObservationCandidate(
                field_name=field_name,
                raw_value=raw_value,
                normalized_value=normalized_value,
                source_kind="web_dom",
                source_path=source_path,
                source_url=sanitize_source_url(page_url),
                observed_at=observed_at,
                identity=evidence,
                temporal_scope="page_observation",
                precision=precision,
            )
        )

    header = _own_header(owner)
    if header is not None:
        _extract_role(document, header, add, mark)
        fid_verified = target.fid is not None and any(
            fid == target.fid for _tid, fid in content_context
        )
        _extract_forum_title(document, header, add, mark, fid_verified=fid_verified)
        _extract_global_growth(document, header, add, mark)
    _extract_location(owner, target, add, mark)
    if target.target_type == "thread":
        _extract_thread_actions(owner, add, mark)
    return DOMExtractionResult("ok", None, True, tuple(candidates), field_outcomes)


def _failed_dom_result(target: Target, status: str, reason: str) -> DOMExtractionResult:
    outcome_status = "identity_unverified" if status == "identity_unverified" else "not_tested"
    return DOMExtractionResult(
        status,
        reason,
        False,
        (),
        {field: {"status": outcome_status, "reason": reason} for field in target.requested_fields},
    )


def _target_owner(document, target: Target):
    if target.target_type == "post":
        matches = [
            element
            for element in document.xpath(
                "//*[contains(concat(' ', normalize-space(@class), ' '), ' pb-comment-item ')]"
            )
            if element.get("data-id") == target.pid
        ]
        if len(matches) != 1:
            reason = "post_data_id_missing" if not matches else "duplicate_post_owner"
            return None, reason
        try:
            if parse_positive_id(matches[0].get("data-id")) != target.pid:
                return None, "post_data_id_mismatch"
        except IdentityError:
            return None, "post_data_id_invalid"
        return matches[0], None

    interactive_nodes = document.xpath(
        "//*[contains(concat(' ', normalize-space(@class), ' '), ' pc-pb-first-floor-interactive ')]"
    )
    owners = []
    for interactive in interactive_nodes:
        owner = _nearest_class_ancestor(interactive, "image-text")
        if owner is None:
            continue
        if _thread_context(owner):
            owners.append(owner)
    matching = [owner for owner in owners if any(tid == target.tid for tid, _fid in _thread_context(owner))]
    if len(matching) != 1:
        if not matching and len(owners) == 1:
            return owners[0], None
        reason = "first_floor_owner_missing" if not matching else "duplicate_first_floor_owner"
        return None, reason
    return matching[0], None


def target_dom_structure_ready(target: Target, page_url: str, html_text: str) -> bool:
    """Whether the target owner and its current thread context have rendered."""
    if canonical_thread_id_from_url(page_url) != target.tid:
        return False
    try:
        document = html.fromstring(html_text)
    except (ValueError, TypeError):
        return False
    owner, _reason = _target_owner(document, target)
    if owner is None:
        return False
    _context_root, contexts = _context_scope_for_owner(owner)
    if not contexts or any(tid is None for tid, _fid in contexts):
        return False
    return True


def _context_scope_for_owner(owner):
    current = owner
    while current is not None:
        contexts = _thread_context(current)
        if contexts:
            return current, contexts
        current = current.getparent()
    return None, []


def _thread_context(root) -> list[tuple[str | None, str | None]]:
    contexts: list[tuple[str | None, str | None]] = []
    content_wraps = [root] if _has_class(root, "pb-content-wrap") else []
    content_wraps.extend(node for node in _owned_descendants(root) if _has_class(node, "pb-content-wrap"))
    for content_wrap in content_wraps:
        richtext_items = [node for node in _owned_descendants(content_wrap) if _has_class(node, "richtext-item")]
        for richtext_item in richtext_items:
            identity_nodes = []
            partial_identity = False
            for node in _self_and_owned_descendants(richtext_item):
                if (
                    node is not richtext_item
                    and _nearest_class_ancestor(node.getparent(), "richtext-item") is not richtext_item
                ):
                    continue
                identity_attributes = {"tid", "fid", "index"}.intersection(node.attrib)
                if len(identity_attributes) == 3:
                    identity_nodes.append(node)
                elif identity_attributes:
                    partial_identity = True
            if len(identity_nodes) != 1:
                if partial_identity or identity_nodes:
                    contexts.append((None, None))
                continue
            node = identity_nodes[0]
            try:
                tid = parse_positive_id(node.get("tid"))
                fid = parse_positive_id(node.get("fid"))
            except IdentityError:
                contexts.append((None, None))
                continue
            contexts.append((tid, fid))
    return contexts


def _nearest_class_ancestor(node, class_name: str):
    current = node
    while current is not None:
        if _has_class(current, class_name):
            return current
        current = current.getparent()
    return None


def _owner_author_matches(owner, target: Target) -> bool:
    if target.author_user_id is None or not target.author_portrait:
        return False
    header = _own_header(owner)
    if header is None:
        return False
    name_infos = [node for node in _owned_descendants(header) if _has_class(node, "name-info")]
    if len(name_infos) != 1:
        return False
    name_info = name_infos[0]
    links = [
        node
        for node in _owned_descendants(name_info)
        if _tag_name(node) == "a"
        and _has_class(node, "head-name")
        and node.get("href")
        and _nearest_class_ancestor(node.getparent(), "name-info") is name_info
    ]
    if len(links) != 1:
        return False
    return portrait_token_from_profile_href(links[0].get("href", "")) == target.author_portrait


def _own_header(owner):
    if _has_class(owner, "pb-comment-item"):
        headers = [node for node in owner if _has_class(node, "head-line") and _has_class(node, "user-info")]
        return headers[0] if len(headers) == 1 else None
    headers = [
        node
        for node in _owned_descendants(owner)
        if _has_class(node, "head-line") and _has_class(node, "user-info")
    ]
    return headers[0] if len(headers) == 1 else None


def _owned_descendants(owner):
    for node in owner.iterdescendants():
        if any(_has_class(node, boundary) for boundary in _OWNER_BOUNDARIES):
            continue
        ancestor = node.getparent()
        nested = False
        while ancestor is not None and ancestor is not owner:
            if any(_has_class(ancestor, boundary) for boundary in _OWNER_BOUNDARIES):
                nested = True
                break
            ancestor = ancestor.getparent()
        if not nested:
            yield node


def _self_and_owned_descendants(owner):
    yield owner
    yield from _owned_descendants(owner)


def _extract_role(document, header, add, mark) -> None:
    matches = []
    for ref in _owned_elements_with_class(header, "tooltip__reference"):
        tooltip_text = _tooltip_text(document, ref)
        if not tooltip_text or not re.search(r"(?:吧主|吧务)", tooltip_text):
            continue
        matches.append(tooltip_text)
    if len(matches) == 1:
        add("role_name", matches[0], matches[0], "owner.head-line.tooltip_text")
        add("is_bawu", matches[0], True, "owner.head-line.tooltip_text")
    elif matches:
        for field_name in ("role_name", "is_bawu"):
            mark(field_name, "needs_evidence", "ambiguous_role_tooltip")
    else:
        for field_name in ("role_name", "is_bawu"):
            mark(field_name, "absent_on_page", "official_role_tooltip_not_observed")


def _extract_forum_title(document, header, add, mark, *, fid_verified: bool) -> None:
    fields = ("forum_level", "forum_level_name", "forum_level_icon_ref")
    icons = _owned_elements_with_class(header, "icon_level")
    if not icons:
        for field_name in fields:
            mark(field_name, "absent_on_page", "forum_level_icon_not_present")
        return
    qualifying = []
    for icon in icons:
        refs = _owned_elements_with_class(icon, "tooltip__reference")
        title_refs = [ref for ref in refs if _tooltip_text(document, ref) == "本吧头衔"]
        if len(title_refs) == 1:
            qualifying.append(icon)
    if len(qualifying) != 1:
        reason = "icon_semantics_unverified" if not qualifying else "ambiguous_forum_level_icon"
        for field_name in fields:
            mark(field_name, "needs_evidence", reason)
        return

    icon = qualifying[0]
    name_info = _nearest_class_ancestor(icon, "name-info")
    if not fid_verified:
        mark("forum_level_name", "needs_evidence", "fid_context_not_verified")
        mark("forum_level", "needs_evidence", "fid_context_not_verified")
    elif name_info is not None:
        tags = [node for node in _owned_descendants(name_info) if _has_class(node, "tag")]
        if len(tags) == 1:
            title = _text(tags[0])
            if title:
                add("forum_level_name", title, title, "owner.head-line.icon_level.tag")
        else:
            mark("forum_level_name", "absent_on_page", "forum_level_name_not_observed")
    else:
        mark("forum_level_name", "absent_on_page", "forum_level_name_not_observed")
    uses = [node for node in _owned_descendants(icon) if _tag_name(node) == "use"]
    if len(uses) != 1:
        mark("forum_level_icon_ref", "needs_evidence", "icon_use_not_unique")
        if fid_verified:
            mark("forum_level", "needs_evidence", "icon_use_not_unique")
        return
    use = uses[0]
    href = use.get("href") or use.get("{http://www.w3.org/1999/xlink}href") or use.get("xlink:href")
    if not href:
        mark("forum_level_icon_ref", "needs_evidence", "icon_use_reference_missing")
        if fid_verified:
            mark("forum_level", "needs_evidence", "icon_use_reference_missing")
        return
    add("forum_level_icon_ref", href, href, "owner.head-line.icon_level.svg_use")
    match = re.fullmatch(r"#level_([1-9][0-9]*)", href)
    if match and fid_verified:
        add("forum_level", match.group(1), int(match.group(1)), "owner.head-line.icon_level.svg_use.fragment")
    elif fid_verified:
        mark("forum_level", "needs_evidence", "icon_semantics_unverified")


def _extract_global_growth(document, header, add, mark) -> None:
    icons = _owned_elements_with_class(header, "icon_tshows")
    if not icons:
        mark("global_growth_level", "absent_on_page", "global_growth_icon_not_present")
        return
    matches = []
    for icon in icons:
        for ref in _owned_elements_with_class(icon, "tooltip__reference"):
            tooltip_text = _tooltip_text(document, ref)
            if not tooltip_text or "贴吧成长等级" not in tooltip_text:
                continue
            matches.append(tooltip_text)
    if len(matches) != 1:
        reason = "icon_semantics_unverified" if not matches else "ambiguous_growth_tooltip"
        mark("global_growth_level", "needs_evidence", reason)
        return
    match = re.search(r"贴吧成长等级\s*[:：]?\s*(?:Lv\.?\s*)?([0-9]+)", matches[0], re.IGNORECASE)
    if match:
        level = int(match.group(1))
        add("global_growth_level", matches[0], level, "owner.head-line.icon_tshows.tooltip_text")
    else:
        mark("global_growth_level", "needs_evidence", "numeric_level_not_exposed_in_page")


def _extract_location(owner, target: Target, add, mark) -> None:
    if target.target_type == "thread":
        nodes = [node for node in _owned_descendants(owner) if _has_class(node, "ip-address")]
        if len(nodes) != 1:
            status = "absent_on_page" if not nodes else "needs_evidence"
            reason = "ip_location_not_present" if not nodes else "ambiguous_ip_location_nodes"
            mark("ip_location_text", status, reason)
            return
        value = _text(nodes[0])
        if value and not _is_literal_ip(value):
            add("ip_location_text", value, value, "owner.ip-address")
        else:
            mark("ip_location_text", "needs_evidence", "bare_ip_not_region_label")
        return
    else:
        contents = [node for node in owner if _has_class(node, "comment-content")]
        if len(contents) != 1:
            mark("ip_location_text", "not_tested", "comment_location_structure_not_exposed")
            return
        descs = [child for child in contents[0] if _has_class(child, "pc-pb-comments-desc")]
        if len(descs) != 1:
            mark("ip_location_text", "not_tested", "comment_location_structure_not_exposed")
            return
        desc = descs[0]
        left = next(
            (
                node
                for node in desc
                if _has_class(node, "comment-desc-left")
            ),
            None,
        )
        if left is None:
            mark("ip_location_text", "not_tested", "comment_location_structure_not_exposed")
            return
        nodes = _owned_elements_with_class(left, "ip-address")
        if len(nodes) == 1:
            value = _text(nodes[0])
            if value and not _is_literal_ip(value):
                add(
                    "ip_location_text",
                    value,
                    value,
                    "owner.comment-content.pc-pb-comments-desc.comment-desc-left.ip-address",
                )
            else:
                mark("ip_location_text", "needs_evidence", "bare_ip_not_region_label")
            return
        if nodes:
            mark("ip_location_text", "needs_evidence", "ambiguous_ip_location_nodes")
            return
        value = _unique_region_from_comment_desc(left)
        if value is not None:
            add(
                "ip_location_text",
                value,
                value,
                "owner.comment-content.pc-pb-comments-desc.comment-desc-left.region_text",
            )
        else:
            details = _comment_desc_values(left)
            if details:
                mark("ip_location_text", "needs_evidence", "region_format_unrecognized")
            else:
                mark("ip_location_text", "absent_on_page", "ip_location_not_present")
        return


def _extract_thread_actions(owner, add, mark) -> None:
    interactive = next(
        (node for node in _owned_descendants(owner) if _has_class(node, "pc-pb-first-floor-interactive")),
        None,
    )
    if interactive is None:
        for field_name in _COUNTER_SYMBOLS.values():
            mark(field_name, "not_tested", "interaction_section_not_exposed")
        return
    seen_fields = set()
    for action in _owned_descendants(interactive):
        if not _has_class(action, "action-item") or _nearest_class_ancestor(action.getparent(), "action-item"):
            continue
        known_fields = []
        action_nodes = [
            node
            for node in _owned_descendants(action)
            if _nearest_class_ancestor(node.getparent(), "action-item") is action
        ]
        for use in action_nodes:
            if _tag_name(use) != "use":
                continue
            href = use.get("href") or use.get("{http://www.w3.org/1999/xlink}href") or use.get("xlink:href")
            if href:
                symbol = href.rsplit("#", 1)[-1]
                if symbol in _COUNTER_SYMBOLS:
                    known_fields.append(_COUNTER_SYMBOLS[symbol])
        fields = set(known_fields)
        if len(known_fields) != 1 or len(fields) != 1:
            for field_name in fields:
                seen_fields.add(field_name)
                mark(field_name, "needs_evidence", "counter_markup_ambiguous")
            continue
        field = next(iter(fields))
        seen_fields.add(field)
        numbers = [
            node
            for node in action_nodes
            if _has_class(node, "action-number")
        ]
        if len(numbers) != 1:
            reason = "counter_markup_ambiguous" if numbers else "zero_presence_unverified"
            mark(field, "needs_evidence", reason)
            continue
        number = numbers[0]
        raw = _text(number)
        parsed = _parse_count(raw)
        if parsed is not None:
            normalized, precision = parsed
            add(field, raw, normalized, f"owner.first-floor.action-item.{field}", precision=precision)
        else:
            mark(field, "needs_evidence", "zero_presence_unverified")
    for field_name in _COUNTER_SYMBOLS.values():
        if field_name not in seen_fields:
            mark(field_name, "absent_on_page", "counter_action_not_present")


_KNOWN_REGION_NAMES = {
    "北京", "天津", "上海", "重庆", "北京市", "天津市", "上海市", "重庆市", "河北", "山西", "辽宁",
    "吉林", "黑龙江", "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南",
    "广东", "海南", "四川", "贵州", "云南", "陕西", "甘肃", "青海", "台湾", "内蒙古", "广西",
    "西藏", "宁夏", "新疆", "香港", "澳门", "香港特别行政区", "澳门特别行政区",
}


def _unique_region_from_comment_desc(left) -> str | None:
    """Accept only one direct span that has an explicit Chinese region form."""
    regions = [value for value in _comment_desc_values(left) if _is_region_label(value)]
    return regions[0] if len(regions) == 1 else None


def _comment_desc_values(left) -> list[str]:
    values = []
    for child in left:
        if _tag_name(child) != "span":
            continue
        value = _text(child)
        if not value or re.fullmatch(r"(?:第)?[0-9]+楼", value):
            continue
        if re.fullmatch(r"[0-9]{4}[-/.][0-9]{1,2}[-/.][0-9]{1,2}(?:\s+[0-9:]+)?", value):
            continue
        if re.fullmatch(r"[0-9]{1,2}:[0-9]{2}", value):
            continue
        values.append(value)
    return values


def _is_region_label(value: str) -> bool:
    normalized = re.sub(r"^IP属地\s*[:：]\s*", "", value)
    if _is_literal_ip(normalized):
        return False
    return normalized in _KNOWN_REGION_NAMES or bool(
        re.fullmatch(r"[\u4e00-\u9fff]{2,}(?:省|自治区|特别行政区)", normalized)
    )


def _parse_count(raw: str) -> tuple[int | str, str] | None:
    compact = raw.replace(",", "").replace(" ", "")
    if re.fullmatch(r"(?:0|[1-9][0-9]*)", compact):
        return int(compact), "exact"
    match = re.fullmatch(r"(0|[1-9][0-9]*)(?:\.([0-9]+))?(万|w|W)", compact)
    if not match:
        return None
    try:
        value = Decimal(match.group(1) + (f".{match.group(2)}" if match.group(2) else "")) * Decimal(10_000)
    except InvalidOperation:
        return None
    return format(value, "f"), "approximate"


def _tooltip_text(document, reference) -> str | None:
    describedby = reference.get("aria-describedby")
    if not describedby:
        return None
    ids = describedby.split()
    if len(ids) != 1:
        return None
    matches = document.xpath("//*[@id=$value]", value=ids[0])
    if len(matches) != 1:
        return None
    return _text(matches[0])


def _owned_elements_with_class(root, class_name: str):
    matches = [root] if _has_class(root, class_name) else []
    matches.extend(node for node in _owned_descendants(root) if _has_class(node, class_name))
    return matches


def _has_class(node, class_name: str) -> bool:
    return class_name in (node.get("class") or "").split()


def _tag_name(node) -> str | None:
    tag = getattr(node, "tag", None)
    return tag.lower() if isinstance(tag, str) else None


def _text(node) -> str:
    return " ".join(" ".join(node.itertext()).split())


def _is_literal_ip(value: str) -> bool:
    tokens = re.findall(r"(?<![\w])[0-9A-Fa-f:.]{3,}(?![\w])", value)
    for token in tokens:
        try:
            ipaddress.ip_address(token.strip("[]"))
        except ValueError:
            continue
        return True
    return False
