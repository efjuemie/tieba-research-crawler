"""Strict identity and provenance contracts for the web fallback."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

MAX_SIGNED_64 = (1 << 63) - 1
PARSER_VERSION = "web-fallback-v1"
TargetType = Literal["thread", "post", "comment"]
SourceKind = Literal["web_network", "web_dom"]
_SAFE_ID_QUERY_KEYS = {"tid", "thread_id", "kz", "fid", "pid", "post_id", "cid"}
_SAFE_PAGE_QUERY_KEYS = {"pn", "offset", "see_lz"}
PAGE_PC_PATH = "/c/f/pb/page_pc"


class IdentityError(ValueError):
    """An identifier or identity context is unsafe to associate."""


class DuplicateJSONKeyError(ValueError):
    """JSON with a repeated key is ambiguous for identity-sensitive parsing."""


def parse_positive_id(value: Any) -> str:
    """Return a canonical positive signed-64 ID, rejecting coercions and JS numbers."""
    if isinstance(value, bool):
        raise IdentityError("boolean_id")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value):
        number = int(value)
    else:
        raise IdentityError("noncanonical_id")
    if not 1 <= number <= MAX_SIGNED_64:
        raise IdentityError("id_out_of_signed64_range")
    return str(number)


def parse_json_unique(text: str) -> Any:
    """Parse JSON in Python while rejecting every duplicate object key."""

    def pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise DuplicateJSONKeyError("duplicate_json_key")
            result[key] = value
        return result

    return json.loads(text, object_pairs_hook=pairs_hook)


@dataclass(frozen=True)
class Target:
    target_type: TargetType
    target_id: str | int
    tid: str | int
    fid: str | int | None = None
    pid: str | int | None = None
    cid: str | int | None = None
    parent_post_pid: str | int | None = None
    author_user_id: str | int | None = None
    author_portrait: str | None = None
    requested_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.target_type not in {"thread", "post", "comment"}:
            raise IdentityError("invalid_target_type")
        for name in ("target_id", "tid", "fid", "pid", "cid", "parent_post_pid", "author_user_id"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, parse_positive_id(value))
        if self.target_type == "thread":
            if self.target_id != self.tid or self.pid is not None or self.cid is not None:
                raise IdentityError("thread_target_shape_mismatch")
        elif self.target_type == "post":
            if self.pid is None or self.target_id != self.pid or self.cid is not None:
                raise IdentityError("post_target_shape_mismatch")
        elif self.target_type == "comment":
            if (
                self.cid is None
                or self.target_id != self.cid
                or self.parent_post_pid is None
                or self.pid is not None
            ):
                raise IdentityError("comment_target_shape_mismatch")
        if self.author_portrait is not None and not isinstance(self.author_portrait, str):
            raise IdentityError("invalid_author_portrait")
        fields = tuple(self.requested_fields)
        if any(not isinstance(field, str) or not field for field in fields):
            raise IdentityError("invalid_requested_field")
        if len(set(fields)) != len(fields):
            raise IdentityError("duplicate_requested_field")
        object.__setattr__(self, "requested_fields", fields)


@dataclass(frozen=True)
class IdentityEvidence:
    target_type: TargetType
    target_id: str
    tid: str
    pid: str | None
    cid: str | None
    parent_post_pid: str | None
    author_user_id: str | None
    match_methods: tuple[str, ...]
    context_id: str | None = None
    request_offset: str | None = None
    identity_verified: bool = True
    fid: str | None = None


@dataclass(frozen=True)
class ObservationCandidate:
    """A value shown by a page at observation time, never proof of historical state."""

    field_name: str
    raw_value: Any
    normalized_value: Any
    source_kind: SourceKind
    source_path: str
    source_url: str
    observed_at: datetime
    identity: IdentityEvidence
    parser_version: str = PARSER_VERSION
    temporal_scope: str = "content_observation"
    precision: str = "exact"


@dataclass(frozen=True)
class IdentityCheck:
    identity_verified: bool
    reason: str | None
    evidence: IdentityEvidence | None = None


@dataclass(frozen=True)
class ImmutableRequestContext:
    """Safe identity-only snapshot of one natural browser request."""

    target: Target
    page_tid: str | None
    request_tid: str | None
    request_post_id: str | None
    request_fid: str | None
    request_offset: str | None
    request_url: str
    request_method: str
    observed_at: datetime
    context_id: str
    reason: str | None = None
    page_origin: str | None = None


def canonical_thread_id_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    if not _is_trusted_tieba_url(parsed):
        return None
    match = re.fullmatch(r"/p/([0-9]+)(?:/)?", parsed.path)
    if not match:
        return None
    try:
        return parse_positive_id(match.group(1))
    except IdentityError:
        return None


def sanitize_source_url(url: str) -> str:
    parsed = urlparse(url)
    if not _is_trusted_tieba_url(parsed):
        return ""
    safe_query: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        normalized_key = key.lower()
        if normalized_key in _SAFE_ID_QUERY_KEYS:
            try:
                safe_query.append((normalized_key, parse_positive_id(value)))
            except IdentityError:
                continue
        elif normalized_key in {"pn", "offset"} and re.fullmatch(r"(?:0|[1-9][0-9]*)", value):
            safe_query.append((normalized_key, value))
        elif normalized_key == "see_lz" and value in {"0", "1"}:
            safe_query.append((normalized_key, value))
    host = (parsed.hostname or "").lower()
    return urlunparse(("https", host, parsed.path, "", urlencode(safe_query), ""))


def _is_trusted_tieba_url(parsed, *, allow_www: bool = True) -> bool:
    allowed_hosts = {"tieba.baidu.com", "www.tieba.baidu.com"} if allow_www else {"tieba.baidu.com"}
    if parsed.scheme != "https" or (parsed.hostname or "").lower() not in allowed_hosts:
        return False
    if parsed.username is not None or parsed.password is not None:
        return False
    try:
        port = parsed.port
    except ValueError:
        return False
    return port is None or port == 443


def portrait_token_from_profile_href(href: str) -> str | None:
    parsed = urlparse(href)
    if (
        not _is_trusted_tieba_url(parsed, allow_www=False)
        or parsed.path.rstrip("/") != "/home/main"
    ):
        return None
    values = [value for key, value in parse_qsl(parsed.query, keep_blank_values=True) if key == "id"]
    if len(values) != 1 or not values[0] or values[0].isdigit():
        return None
    return values[0]


def make_request_context(
    target: Target,
    *,
    page_url: str,
    request_url: str,
    request_method: str,
    post_data: str | None = None,
    observed_at: datetime | None = None,
    context_id: str = "natural-request",
) -> ImmutableRequestContext:
    """Snapshot only identity fields from a natural request; omit signature data."""
    pairs: list[tuple[str, str]] = []
    parsed_url = urlparse(request_url)
    pairs.extend(parse_qsl(parsed_url.query, keep_blank_values=True))
    reason: str | None = None
    if post_data:
        try:
            if post_data.lstrip().startswith("{"):
                payload = parse_json_unique(post_data)
                if not isinstance(payload, dict):
                    reason = "request_body_not_object"
                else:
                    pairs.extend(
                        (key, str(value))
                        for key, value in payload.items()
                        if key.lower() in {"tid", "thread_id", "kz", "fid", "pid", "post_id", "offset"}
                    )
            else:
                pairs.extend(parse_qsl(post_data, keep_blank_values=True))
        except (ValueError, TypeError):
            reason = "request_body_parse_error"

    values: dict[str, list[str]] = {"tid": [], "fid": [], "post": [], "offset": []}
    for key, value in pairs:
        if key.lower() in {"tid", "thread_id", "kz"}:
            values["tid"].append(value)
        elif key.lower() == "fid":
            values["fid"].append(value)
        elif key.lower() in {"pid", "post_id"}:
            values["post"].append(value)
        elif key.lower() == "offset":
            values["offset"].append(value)

    def one_id(key: str) -> str | None:
        nonlocal reason
        raw_values = values[key]
        if not raw_values:
            return None
        try:
            canonical = [parse_positive_id(value) for value in raw_values]
        except IdentityError:
            reason = reason or f"invalid_request_{key}"
            return None
        if len(set(canonical)) != 1:
            reason = reason or f"conflicting_request_{key}"
            return None
        return canonical[0]

    page_tid = canonical_thread_id_from_url(page_url)
    if page_tid is None:
        reason = reason or "page_tid_unavailable"
    request_tid = one_id("tid")
    request_fid = one_id("fid")
    request_post_id = one_id("post")
    request_offset: str | None = None
    if values["offset"]:
        offsets = values["offset"]
        if len(set(offsets)) != 1 or not re.fullmatch(r"(?:0|[1-9][0-9]*)", offsets[0]):
            reason = reason or "invalid_request_offset"
        else:
            request_offset = offsets[0]
    return ImmutableRequestContext(
        target=target,
        page_tid=page_tid,
        request_tid=request_tid,
        request_post_id=request_post_id,
        request_fid=request_fid,
        request_offset=request_offset,
        request_url=sanitize_source_url(request_url),
        request_method=request_method.upper(),
        observed_at=observed_at or datetime.now(UTC),
        context_id=context_id,
        reason=reason,
        page_origin=_trusted_origin(page_url),
    )


def _trusted_origin(url: str) -> str | None:
    parsed = urlparse(url)
    if not _is_trusted_tieba_url(parsed):
        return None
    return f"https://{(parsed.hostname or '').lower()}"


def validate_request_context(target: Target, context: ImmutableRequestContext) -> IdentityCheck:
    if context.reason:
        return IdentityCheck(False, context.reason)
    if target != context.target:
        return IdentityCheck(False, "request_context_target_mismatch")
    if context.request_method != "POST":
        return IdentityCheck(False, "request_method_mismatch")
    parsed_request_url = urlparse(context.request_url)
    if (
        parsed_request_url.scheme != "https"
        or (parsed_request_url.hostname or "").lower() != "tieba.baidu.com"
        or parsed_request_url.path != "/c/f/pb/nestedFloor"
    ):
        return IdentityCheck(False, "request_route_mismatch")
    if context.page_tid != target.tid or context.request_tid != target.tid:
        return IdentityCheck(False, "request_tid_mismatch")
    expected_post_id = target.pid if target.target_type == "post" else target.parent_post_pid
    if expected_post_id is not None and context.request_post_id != expected_post_id:
        return IdentityCheck(False, "request_post_id_mismatch")
    if target.fid is not None and context.request_fid is not None and context.request_fid != target.fid:
        return IdentityCheck(False, "request_fid_mismatch")
    return IdentityCheck(True, None)


def validate_page_pc_request_context(target: Target, context: ImmutableRequestContext) -> IdentityCheck:
    if context.reason:
        return IdentityCheck(False, context.reason)
    if target != context.target:
        return IdentityCheck(False, "request_context_target_mismatch")
    if target.target_type not in {"thread", "comment"}:
        return IdentityCheck(False, "page_pc_target_type_mismatch")
    if context.request_method != "POST":
        return IdentityCheck(False, "request_method_mismatch")
    parsed_request = urlparse(context.request_url)
    if (
        parsed_request.scheme != "https"
        or (parsed_request.hostname or "").lower() not in {"tieba.baidu.com", "www.tieba.baidu.com"}
        or parsed_request.username is not None
        or parsed_request.password is not None
        or parsed_request.path != PAGE_PC_PATH
        or context.page_origin != _trusted_origin(context.request_url)
    ):
        return IdentityCheck(False, "page_pc_request_context_mismatch")
    if context.page_tid != target.tid:
        return IdentityCheck(False, "page_tid_mismatch")
    if context.request_tid != target.tid:
        return IdentityCheck(False, "request_tid_mismatch")
    if target.fid is None:
        return IdentityCheck(False, "target_fid_missing")
    if context.request_fid is not None and context.request_fid != target.fid:
        return IdentityCheck(False, "request_fid_mismatch")
    return IdentityCheck(True, None)


def make_identity_evidence(
    target: Target,
    methods: tuple[str, ...],
    context: ImmutableRequestContext | None = None,
) -> IdentityEvidence:
    return IdentityEvidence(
        target_type=target.target_type,
        target_id=str(target.target_id),
        tid=str(target.tid),
        pid=str(target.pid) if target.pid is not None else None,
        cid=str(target.cid) if target.cid is not None else None,
        parent_post_pid=str(target.parent_post_pid) if target.parent_post_pid is not None else None,
        author_user_id=str(target.author_user_id) if target.author_user_id is not None else None,
        match_methods=methods,
        context_id=context.context_id if context is not None else None,
        request_offset=context.request_offset if context is not None else None,
        fid=str(target.fid) if target.fid is not None else None,
    )
