"""Python-side parsing of naturally observed Tieba web responses."""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qsl, urlparse

from .identity import (
    DuplicateJSONKeyError,
    IdentityError,
    ImmutableRequestContext,
    ObservationCandidate,
    Target,
    make_identity_evidence,
    parse_json_unique,
    parse_positive_id,
    validate_request_context,
)

NESTED_FLOOR_PATH = "/c/f/pb/nestedFloor"
USER_CARD_PATH = "/c/u/pc/userCard"


@dataclass(frozen=True)
class NetworkExtractionResult:
    status: str
    reason: str | None
    identity_verified: bool
    candidates: tuple[ObservationCandidate, ...]
    field_outcomes: dict[str, dict[str, str]] = field(default_factory=dict)


def extract_nested_floor(
    target: Target,
    response_text: str,
    request_context: ImmutableRequestContext,
    *,
    observed_at: datetime | None = None,
    response_status: int = 200,
) -> NetworkExtractionResult:
    """Extract values displayed at observation time for the exact CID and author."""
    def failed(status: str, reason: str) -> NetworkExtractionResult:
        outcome_status = {
            "blocked": "blocked",
            "identity_unverified": "identity_unverified",
        }.get(status, "not_tested")
        outcomes = {
            field_name: {"status": outcome_status, "reason": reason}
            for field_name in target.requested_fields
        }
        return NetworkExtractionResult(status, reason, False, (), outcomes)

    if response_status == 429:
        return failed("blocked", "http_429")
    if response_status in {401, 403}:
        return failed("blocked", "http_auth_or_verification")
    if response_status != 200:
        return failed("dom_changed", f"http_{response_status}")
    if target.target_type != "comment":
        return failed("identity_unverified", "nested_floor_requires_comment_target")

    context_check = validate_request_context(target, request_context)
    if not context_check.identity_verified:
        return failed("identity_unverified", context_check.reason or "request_context_invalid")

    try:
        payload = parse_json_unique(response_text)
    except DuplicateJSONKeyError:
        return failed("identity_unverified", "duplicate_json_key")
    except (ValueError, TypeError):
        return failed("dom_changed", "response_json_parse_error")
    if not isinstance(payload, dict):
        return failed("dom_changed", "response_root_not_object")
    error_code = payload.get("error_code")
    if isinstance(error_code, bool) or not isinstance(error_code, int):
        reason = "response_error_code_missing" if "error_code" not in payload else "invalid_response_error_code"
        return failed("dom_changed", reason)
    if error_code != 0:
        return failed("dom_changed", "response_error_code_nonzero")
    data = payload.get("data")
    post_list = data.get("post_list") if isinstance(data, dict) else None
    if not isinstance(post_list, list):
        return failed("dom_changed", "post_list_missing")

    matching_items: list[tuple[int, dict[str, Any]]] = []
    for index, item in enumerate(post_list):
        if not isinstance(item, dict) or "id" not in item:
            continue
        try:
            cid = parse_positive_id(item["id"])
        except IdentityError:
            continue
        if cid == target.cid:
            matching_items.append((index, item))
    if len(matching_items) != 1:
        reason = "cid_missing_from_response" if not matching_items else "duplicate_target_cid"
        return failed("identity_unverified", reason)

    index, item = matching_items[0]
    item_fid = _optional_id(item, "fid")
    if item.get("fid") is not None and item_fid is None:
        return failed("identity_unverified", "invalid_response_fid")
    if target.fid is not None and item_fid is not None and item_fid != target.fid:
        return failed("identity_unverified", "response_fid_mismatch")
    if request_context.request_fid is not None and target.fid is not None and request_context.request_fid != target.fid:
        return failed("identity_unverified", "request_fid_mismatch")
    if item_fid is not None and request_context.request_fid is not None and item_fid != request_context.request_fid:
        return failed("identity_unverified", "request_response_fid_mismatch")

    author = item.get("author")
    if not isinstance(author, dict):
        return failed("identity_unverified", "response_author_missing")
    if item.get("author_id") is None or author.get("id") is None:
        return failed("identity_unverified", "response_author_id_missing")
    try:
        author_id = parse_positive_id(item["author_id"])
        nested_author_id = parse_positive_id(author["id"])
    except (IdentityError, KeyError):
        return failed("identity_unverified", "invalid_response_author_id")
    if author_id != nested_author_id:
        return failed("identity_unverified", "response_author_id_disagreement")
    if target.author_user_id is None or author_id != target.author_user_id:
        return failed("identity_unverified", "response_author_id_mismatch")

    identity_methods = ["page_tid", "request_tid", "request_parent_post_pid", "response_cid", "response_author_id"]
    if request_context.request_fid is not None:
        identity_methods.append("request_fid")
    if item_fid is not None:
        identity_methods.append("response_fid")
    if target.fid is not None and request_context.request_fid is None and item_fid is None:
        identity_methods.append("known_api_tid_fid_binding")
    identity = make_identity_evidence(target, tuple(identity_methods), request_context)
    observed_at = observed_at or datetime.now(UTC)
    candidates: list[ObservationCandidate] = []
    field_outcomes = {
        field_name: {"status": "not_tested", "reason": "not_observed_in_response"}
        for field_name in target.requested_fields
    }

    def add(field: str, raw: Any, path: str, *, normalized: Any | None = None) -> None:
        if field not in target.requested_fields or raw is None:
            return
        field_outcomes.pop(field, None)
        if normalized is None:
            normalized = raw
        candidates.append(
            ObservationCandidate(
                field_name=field,
                raw_value=raw,
                normalized_value=normalized,
                source_kind="web_network",
                source_path=path,
                source_url=request_context.request_url,
                observed_at=observed_at,
                identity=identity,
                temporal_scope="content_observation",
            )
        )

    base = f"data.post_list[{index}]"
    for api_key, field_name in (
        ("name", "author_user_name"),
        ("name_show", "author_nickname"),
        ("portrait", "author_portrait"),
    ):
        value = author.get(api_key)
        if isinstance(value, str) and value:
            add(field_name, value, f"{base}.author.{api_key}")

    region = author.get("ip_address")
    if isinstance(region, str) and region.strip() and not _is_literal_ip(region.strip()):
        add("ip_location_text", region, f"{base}.author.ip_address")

    agree = item.get("agree")
    if isinstance(agree, dict):
        for key, field_name in (("agree_num", "agree_count"), ("disagree_num", "disagree_count")):
            if key not in agree:
                continue
            normalized = _numeric_count(agree[key])
            if normalized is not None:
                add(field_name, agree[key], f"{base}.agree.{key}", normalized=normalized)

    growth = author.get("user_growth")
    if isinstance(growth, dict) and "level_id" in growth:
        normalized_growth = _nonnegative_integer(growth["level_id"])
        if normalized_growth is not None:
            add(
                "global_growth_level",
                growth["level_id"],
                f"{base}.author.user_growth.level_id",
                normalized=normalized_growth,
            )

    _add_forum_level_candidates(target, item, base, add)
    return NetworkExtractionResult("ok", None, True, tuple(candidates), field_outcomes)


def _add_forum_level_candidates(target: Target, item: dict[str, Any], base: str, add) -> None:
    if target.fid is None:
        return
    show_info = item.get("user_show_info")
    feed_head = show_info.get("feed_head") if isinstance(show_info, dict) else None
    main_data = feed_head.get("main_data") if isinstance(feed_head, dict) else None
    if not isinstance(main_data, list):
        return
    for item_index, entry in enumerate(main_data):
        custom = entry.get("custom") if isinstance(entry, dict) else None
        if not isinstance(custom, dict) or custom.get("style") != "forum_level":
            continue
        data_text = custom.get("data")
        schema = entry.get("schema")
        if not isinstance(data_text, str) or not isinstance(schema, str):
            continue
        try:
            custom_data = parse_json_unique(data_text)
            forum_id = _forum_id_from_schema(schema)
        except (ValueError, TypeError):
            continue
        if not isinstance(custom_data, dict) or forum_id != target.fid:
            continue
        forum_path = f"{base}.user_show_info.feed_head.main_data[{item_index}].custom.data"
        if "level_id" in custom_data:
            level = _nonnegative_integer(custom_data["level_id"])
            if level is not None:
                add("forum_level", custom_data["level_id"], f"{forum_path}.level_id", normalized=level)
        if isinstance(custom_data.get("level_name"), str) and custom_data["level_name"]:
            add("forum_level_name", custom_data["level_name"], f"{forum_path}.level_name")
        return


def _forum_id_from_schema(schema: str) -> str | None:
    parsed = urlparse(schema)
    encoded = [
        value
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key in {"params", "paramsJSON"}
    ]
    if len(encoded) != 1:
        return None
    params = parse_json_unique(encoded[0])
    if not isinstance(params, dict) or "forumId" not in params:
        return None
    try:
        return parse_positive_id(params["forumId"])
    except IdentityError:
        return None


def _optional_id(value: dict[str, Any], key: str) -> str | None:
    if key not in value or value[key] is None:
        return None
    try:
        return parse_positive_id(value[key])
    except IdentityError:
        return None


def _numeric_count(value: Any) -> int | None:
    return _nonnegative_integer(value)


def _nonnegative_integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, str) and re.fullmatch(r"(?:0|[1-9][0-9]*)", value):
        return int(value)
    return None


def _is_literal_ip(value: str) -> bool:
    tokens = re.findall(r"(?<![\w])[0-9A-Fa-f:.]{3,}(?![\w])", value)
    for token in tokens:
        try:
            ipaddress.ip_address(token.strip("[]"))
        except ValueError:
            continue
        return True
    return False
