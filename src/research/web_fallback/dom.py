"""Owner-scoped extraction from captured modern PC Tieba HTML."""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from lxml import html

from .identity import (
    IdentityError,
    IdentityEvidence,
    ObservationCandidate,
    Target,
    canonical_growth_icon_url,
    canonical_thread_id_from_url,
    make_identity_evidence,
    parse_positive_id,
    portrait_token_from_profile_href,
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
    try:
        document = html.fromstring(html_text)
    except (ValueError, TypeError):
        return _failed_dom_result(target, "dom_changed", "html_parse_error")

    page_tid = canonical_thread_id_from_url(page_url)
    if page_tid != target.tid:
        return _failed_dom_result(target, "identity_unverified", "page_tid_mismatch")
    owner, owner_reason = _target_owner(document, target)
    if owner is None:
        comment_identity_reasons = {
            "parent_post_data_id_missing",
            "duplicate_parent_post_owner",
            "cid_not_exposed_in_observed_dom",
            "comment_data_id_invalid",
            "cid_missing_from_observed_dom",
            "duplicate_comment_owner",
        }
        status = (
            "identity_unverified"
            if target.target_type == "comment" and owner_reason in comment_identity_reasons
            else "dom_changed"
        )
        return _failed_dom_result(target, status, owner_reason)
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
    elif target.target_type == "comment":
        methods.extend(("parent_post_data_id", "comment_data_id"))
    methods.extend(("owner_scoped_header", "profile_link_portrait_matches_api"))
    evidence = make_identity_evidence(target, tuple(methods))
    observed_at = observed_at or datetime.now(UTC)
    source_url = _canonical_dom_page_url(target)
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
                source_url=source_url,
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


def extract_comment_author_growth(
    target: Target,
    page_url: str,
    html_text: str,
    network_identity: IdentityEvidence | None,
    *,
    observed_at: datetime | None = None,
) -> DOMExtractionResult:
    """Use an exact network comment identity to read only its author's shared growth attributes."""
    growth_fields = tuple(
        field_name
        for field_name in ("global_growth_level", "global_growth_icon_url")
        if field_name in target.requested_fields
    )
    analysis_fields = ("global_growth_level", "global_growth_icon_url")
    if not growth_fields:
        return DOMExtractionResult("ok", None, False, (), {})

    reason = _comment_growth_network_identity_reason(target, page_url, network_identity)
    if reason is not None:
        return _comment_growth_failed_result(growth_fields, "identity_unverified", reason)
    try:
        document = html.fromstring(html_text)
    except (ValueError, TypeError):
        return _comment_growth_failed_result(growth_fields, "dom_changed", "html_parse_error")

    parent_matches = []
    for element in document.xpath(
        "//*[contains(concat(' ', normalize-space(@class), ' '), ' pb-comment-item ')]"
    ):
        try:
            if parse_positive_id(element.get("data-id")) == target.parent_post_pid:
                parent_matches.append(element)
        except IdentityError:
            continue
    if len(parent_matches) != 1:
        reason = "parent_post_data_id_missing" if not parent_matches else "duplicate_parent_post_owner"
        return _comment_growth_failed_result(growth_fields, "identity_unverified", reason)
    parent = parent_matches[0]

    _context_root, contexts = _context_scope_for_owner(parent)
    if not contexts or any(tid is None for tid, _fid in contexts):
        return _comment_growth_failed_result(
            growth_fields, "identity_unverified", "thread_context_not_exposed"
        )
    if any(tid != target.tid for tid, _fid in contexts):
        return _comment_growth_failed_result(growth_fields, "identity_unverified", "body_tid_mismatch")
    if target.fid is not None and any(fid is not None and fid != target.fid for _tid, fid in contexts):
        return _comment_growth_failed_result(growth_fields, "identity_unverified", "body_fid_mismatch")

    owners, owner_reason = _nested_comment_owners(parent)
    if owners is None:
        return _comment_growth_failed_result(growth_fields, "identity_unverified", owner_reason)
    for owner in owners:
        raw_ids = [
            owner.get(name)
            for name in ("data-cid", "data-comment-id", "data-id", "cid")
            if owner.get(name) is not None
        ]
        if not raw_ids:
            continue
        try:
            ids = {parse_positive_id(value) for value in raw_ids}
        except IdentityError:
            return _comment_growth_failed_result(
                growth_fields, "identity_unverified", "comment_data_id_invalid"
            )
        if len(ids) != 1:
            return _comment_growth_failed_result(
                growth_fields, "identity_unverified", "comment_data_id_invalid"
            )
        if target.cid in ids:
            return _comment_growth_failed_result(
                growth_fields, "identity_unverified", "comment_cid_exposed_in_dom"
            )

    matching_headers = []
    for owner in owners:
        header = _own_header(owner)
        if header is not None and _header_matches_portrait(header, target.author_portrait):
            matching_headers.append(header)
    if not matching_headers:
        return _comment_growth_failed_result(
            growth_fields, "ok", "author_profile_header_not_observed", identity_verified=True
        )

    identity = replace(
        network_identity,
        match_methods=(*network_identity.match_methods, "same_author_profile_growth"),
    )
    observed_at = observed_at or datetime.now(UTC)
    source_url = _canonical_dom_page_url(target)
    source_prefix = (
        f"owner.pb-comment-item[data-id={target.parent_post_pid}].comment-content."
        "lzl-wrapper.pb-lzl-item.same-author-head-line"
    )

    def collect_header_growth(header):
        local_candidates = {}
        local_outcomes = {
            field_name: {"status": "not_tested", "reason": "growth_not_observed_in_author_header"}
            for field_name in analysis_fields
        }

        def add(field_name, raw_value, normalized_value, source_path, *, precision="exact"):
            if field_name not in local_outcomes:
                return
            local_outcomes.pop(field_name, None)
            local_candidates[field_name] = ObservationCandidate(
                field_name=field_name,
                raw_value=raw_value,
                normalized_value=normalized_value,
                source_kind="web_dom",
                source_path=source_path,
                source_url=source_url,
                observed_at=observed_at,
                identity=identity,
                temporal_scope="page_observation",
                precision=precision,
            )

        def mark(field_name, status, outcome_reason):
            if field_name in local_outcomes:
                local_outcomes[field_name] = {"status": status, "reason": outcome_reason}

        _extract_global_growth(document, header, add, mark, source_prefix=source_prefix)
        return local_candidates, local_outcomes

    header_results = [collect_header_growth(header) for header in matching_headers]

    outcomes = {
        field_name: {"status": "not_tested", "reason": "growth_not_observed_in_author_header"}
        for field_name in growth_fields
    }
    candidates = []
    level_candidates = [
        result_candidates["global_growth_level"]
        for result_candidates, _result_outcomes in header_results
        if "global_growth_level" in result_candidates
    ]
    level_values = {candidate.normalized_value for candidate in level_candidates}
    level_has_zero = any(
        result_outcomes.get("global_growth_level", {}).get("reason")
        == "global_growth_level_zero_is_unknown"
        for _result_candidates, result_outcomes in header_results
    )
    level_tooltip_ambiguous = any(
        result_outcomes.get("global_growth_level", {}).get("reason") == "ambiguous_growth_tooltip"
        for _result_candidates, result_outcomes in header_results
    )
    semantic_growth_conflict = (
        len(level_values) > 1 or level_tooltip_ambiguous or (level_has_zero and level_values)
    )
    for field_name in growth_fields:
        field_candidates = [
            result_candidates[field_name]
            for result_candidates, _result_outcomes in header_results
            if field_name in result_candidates
        ]
        values = {candidate.normalized_value for candidate in field_candidates}
        has_zero = any(
            result_outcomes.get(field_name, {}).get("reason") == "global_growth_level_zero_is_unknown"
            for _result_candidates, result_outcomes in header_results
        )
        conflicting_reason = any(
            result_outcomes.get(field_name, {}).get("reason")
            in (
                {"ambiguous_growth_tooltip"}
                if field_name == "global_growth_level"
                else {"ambiguous_growth_tooltip", "ambiguous_growth_icon_image"}
            )
            for _result_candidates, result_outcomes in header_results
        )
        if (
            len(values) > 1
            or conflicting_reason
            or (field_name == "global_growth_level" and (has_zero and values or semantic_growth_conflict))
        ):
            outcomes[field_name] = {
                "status": "needs_evidence",
                "reason": "same_author_growth_values_conflict",
            }
        elif values:
            candidate = field_candidates[0]
            candidates.append(candidate)
            outcomes.pop(field_name, None)
        elif field_name == "global_growth_level" and has_zero:
            outcomes[field_name] = {
                "status": "absent_on_page",
                "reason": "global_growth_level_zero_is_unknown",
            }
        else:
            observed_outcomes = [
                result_outcomes[field_name]
                for _result_candidates, result_outcomes in header_results
                if field_name in result_outcomes
            ]
            needs_evidence = next(
                (outcome for outcome in observed_outcomes if outcome["status"] == "needs_evidence"),
                None,
            )
            if needs_evidence is not None:
                outcomes[field_name] = needs_evidence
            elif observed_outcomes:
                outcomes[field_name] = observed_outcomes[0]

    return DOMExtractionResult("ok", None, True, tuple(candidates), outcomes)


def _failed_dom_result(target: Target, status: str, reason: str) -> DOMExtractionResult:
    outcome_status = "identity_unverified" if status == "identity_unverified" else "not_tested"
    return DOMExtractionResult(
        status,
        reason,
        False,
        (),
        {field: {"status": outcome_status, "reason": reason} for field in target.requested_fields},
    )


def _canonical_dom_page_url(target: Target) -> str:
    return f"https://tieba.baidu.com/p/{target.tid}"


def _comment_growth_network_identity_reason(
    target: Target,
    page_url: str,
    network_identity: IdentityEvidence | None,
) -> str | None:
    if (
        target.target_type != "comment"
        or target.cid is None
        or target.parent_post_pid is None
        or target.author_user_id is None
        or target.fid is None
        or not target.author_portrait
    ):
        return "comment_target_identity_missing"
    if canonical_thread_id_from_url(page_url) != target.tid:
        return "page_tid_mismatch"
    if not isinstance(network_identity, IdentityEvidence) or not network_identity.identity_verified:
        return "network_comment_identity_missing"
    expected = (
        (network_identity.target_type, target.target_type),
        (network_identity.target_id, target.cid),
        (network_identity.tid, target.tid),
        (network_identity.pid, None),
        (network_identity.cid, target.cid),
        (network_identity.parent_post_pid, target.parent_post_pid),
        (network_identity.author_user_id, target.author_user_id),
        (network_identity.fid, target.fid),
    )
    if any(actual != (str(value) if value is not None else None) for actual, value in expected):
        return "network_comment_identity_mismatch"
    methods = set(network_identity.match_methods)
    required_methods = {"page_tid", "request_tid", "response_cid", "response_author_id"}
    parent_methods = {
        "request_parent_post_pid",
        "response_parent_post_id",
        "response_sub_post_list_pid",
    }
    forum_methods = {
        "response_forum_id",
        "response_fid",
        "request_fid",
        "known_api_tid_fid_binding",
    }
    if (
        not required_methods.issubset(methods)
        or not parent_methods.intersection(methods)
        or not forum_methods.intersection(methods)
        or not network_identity.context_id
    ):
        return "network_comment_identity_proof_incomplete"
    return None


def _comment_growth_failed_result(
    fields: tuple[str, ...],
    status: str,
    reason: str,
    *,
    identity_verified: bool = False,
) -> DOMExtractionResult:
    outcome_status = "identity_unverified" if status == "identity_unverified" else "not_tested"
    return DOMExtractionResult(
        status,
        reason,
        identity_verified,
        (),
        {field_name: {"status": outcome_status, "reason": reason} for field_name in fields},
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
    if target.target_type == "comment":
        parent_matches = [
            element
            for element in document.xpath(
                "//*[contains(concat(' ', normalize-space(@class), ' '), ' pb-comment-item ')]"
            )
            if element.get("data-id") == target.parent_post_pid
        ]
        if len(parent_matches) != 1:
            reason = "parent_post_data_id_missing" if not parent_matches else "duplicate_parent_post_owner"
            return None, reason
        parent = parent_matches[0]
        contents = [node for node in parent if _has_class(node, "comment-content")]
        if len(contents) != 1:
            return None, "comment_content_not_unique"
        wrappers = [node for node in contents[0] if _has_class(node, "lzl-wrapper")]
        if not wrappers:
            wrappers = [node for node in parent if _has_class(node, "lzl-wrapper")]
        if len(wrappers) != 1:
            return None, "comment_wrapper_not_unique"
        owners = [node for node in wrappers[0] if _has_class(node, "pb-lzl-item")]
        if not owners:
            return None, "comment_owner_not_present"
        exposed_ids = False
        matching = []
        invalid_ids = False
        invalid_target_alias = False
        for owner in owners:
            raw_ids = [
                owner.get(name)
                for name in ("data-cid", "data-comment-id", "data-id", "cid")
                if owner.get(name) is not None
            ]
            if not raw_ids:
                continue
            exposed_ids = True
            ids = set()
            malformed_alias = False
            for value in raw_ids:
                try:
                    ids.add(parse_positive_id(value))
                except IdentityError:
                    malformed_alias = True
            if malformed_alias or len(ids) != 1:
                invalid_ids = True
                if target.cid in ids:
                    invalid_target_alias = True
                continue
            if next(iter(ids)) == target.cid:
                matching.append(owner)
        if invalid_target_alias:
            return None, "comment_data_id_invalid"
        if len(matching) == 1:
            return matching[0], None
        if len(matching) > 1:
            return None, "duplicate_comment_owner"
        if not exposed_ids:
            return None, "cid_not_exposed_in_observed_dom"
        if invalid_ids:
            return None, "comment_data_id_invalid"
        return None, "cid_missing_from_observed_dom"

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


def _nested_comment_owners(parent):
    contents = [node for node in parent if _has_class(node, "comment-content")]
    if len(contents) != 1:
        return None, "comment_content_not_unique"
    wrappers = [node for node in contents[0] if _has_class(node, "lzl-wrapper")]
    if not wrappers:
        wrappers = [node for node in parent if _has_class(node, "lzl-wrapper")]
    if len(wrappers) != 1:
        return None, "comment_wrapper_not_unique"
    owners = [node for node in wrappers[0] if _has_class(node, "pb-lzl-item")]
    if not owners:
        return None, "comment_owner_not_present"
    return owners, None


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
    return header is not None and _header_matches_portrait(header, target.author_portrait)


def _header_matches_portrait(header, portrait: str) -> bool:
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
    return portrait_token_from_profile_href(links[0].get("href", "")) == portrait


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


def _extract_global_growth(document, header, add, mark, *, source_prefix="owner.head-line") -> None:
    icons = _owned_elements_with_class(header, "icon_tshows")
    if not icons:
        mark("global_growth_level", "absent_on_page", "global_growth_icon_not_present")
        mark("global_growth_icon_url", "absent_on_page", "global_growth_icon_not_present")
        return
    semantic_icons = []
    for icon in icons:
        tooltip_texts = set()
        for ref in _owned_elements_with_class(icon, "tooltip__reference"):
            tooltip_texts.update(
                tooltip_text
                for tooltip_text in _growth_tooltip_texts(document, icon, ref)
                if "贴吧成长等级" in tooltip_text
            )
        if tooltip_texts:
            semantic_icons.append((icon, tooltip_texts))
    if len(semantic_icons) != 1:
        reason = "icon_semantics_unverified" if not semantic_icons else "ambiguous_growth_tooltip"
        mark("global_growth_level", "needs_evidence", reason)
        mark("global_growth_icon_url", "needs_evidence", reason)
        return
    icon, tooltip_texts = semantic_icons[0]
    tooltip_texts = sorted(tooltip_texts)
    levels = {
        int(match.group(1))
        for tooltip_text in tooltip_texts
        for match in re.finditer(
            r"贴吧成长等级\s*[:：]?\s*(?:Lv\.?\s*)?([0-9]+)",
            tooltip_text,
            re.IGNORECASE,
        )
    }
    if len(levels) == 1:
        level = next(iter(levels))
        if level > 0:
            tooltip_text = next(
                text
                for text in tooltip_texts
                if re.search(
                    rf"贴吧成长等级\s*[:：]?\s*(?:Lv\.?\s*)?{level}(?![0-9])",
                    text,
                    re.IGNORECASE,
                )
            )
            add(
                "global_growth_level",
                tooltip_text,
                level,
                f"{source_prefix}.icon_tshows.tooltip_text",
            )
        else:
            mark("global_growth_level", "absent_on_page", "global_growth_level_zero_is_unknown")
    elif len(levels) > 1:
        mark("global_growth_level", "needs_evidence", "ambiguous_growth_tooltip")
    else:
        mark("global_growth_level", "needs_evidence", "numeric_level_not_exposed_in_page")

    images = _owned_elements_with_class(icon, "icon_tshows_img")
    image_urls = {}
    for image in images:
        for attribute in ("src", "data-src", "data-original"):
            canonical = canonical_growth_icon_url(image.get(attribute))
            if canonical is not None:
                image_urls.setdefault(canonical, attribute)
    if len(image_urls) == 1:
        url, attribute = next(iter(image_urls.items()))
        add(
            "global_growth_icon_url",
            url,
            url,
            f"{source_prefix}.icon_tshows.icon_tshows_img.{attribute}",
        )
    else:
        reason = "growth_icon_image_not_found" if not image_urls else "ambiguous_growth_icon_image"
        mark("global_growth_icon_url", "needs_evidence", reason)


def _extract_location(owner, target: Target, add, mark) -> None:
    if target.target_type == "thread":
        descs = [node for node in _owned_descendants(owner) if _has_class(node, "desc-info")]
        if len(descs) != 1:
            status = "absent_on_page" if not descs else "needs_evidence"
            reason = "ip_location_not_present" if not descs else "ambiguous_thread_desc_info"
            mark("ip_location_text", status, reason)
            return
        nodes = _owned_elements_with_class(descs[0], "ip-address")
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
    if target.target_type == "comment":
        nodes = _owned_elements_with_class(owner, "ip-address")
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
    if target.target_type == "post":
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
        if re.fullmatch(
            r"(?:[0-9]{4}[-/.])?[0-9]{1,2}[-/.][0-9]{1,2}"
            r"(?:\s+[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)?",
            value,
        ):
            continue
        if re.fullmatch(r"[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?", value):
            continue
        if re.fullmatch(
            r"(?:刚刚|今天|昨天|前天|[0-9]+(?:秒|分钟|分|小时|天|周|星期|个月|月|年)前)"
            r"(?:\s+[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)?",
            value,
        ):
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


def _growth_tooltip_texts(document, icon, reference) -> set[str]:
    texts = set()
    for node in {icon, reference}:
        for attribute in (
            "title",
            "aria-label",
            "data-tooltip",
            "data-tooltip-content",
            "data-tippy-content",
            "data-original-title",
        ):
            value = node.get(attribute)
            if value and value.strip():
                texts.add(" ".join(value.split()))
    described = _tooltip_text(document, reference)
    if described:
        texts.add(described)
    for root in {icon, reference}:
        for tooltip in _owned_elements_with_class(root, "tooltip"):
            value = _text(tooltip)
            if value:
                texts.add(value)
    ancestor = _nearest_class_ancestor(reference.getparent(), "tooltip")
    if ancestor is not None:
        ancestor_icons = _owned_elements_with_class(ancestor, "icon_tshows")
        if len(ancestor_icons) == 1 and ancestor_icons[0] is icon:
            value = _text(ancestor)
            if value:
                texts.add(value)
    return texts


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
