"""Privacy-limited counters persisted from bounded browser attempts."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

_COUNT_KEYS = frozenset(
    {
        "requests",
        "nested_floor_requests",
        "user_card_requests",
        "pages",
        "expansions",
        "budgeted_actions",
        "http_429",
        "auth_pauses",
        "parent_snapshot_hits",
        "parent_snapshot_misses",
    }
)
_FLOAT_KEYS = frozenset({"elapsed_seconds"})
_STATUS_KEYS = frozenset(
    {
        "status",
        "blocked_reason",
        "user_card_status",
        "user_card_reason",
        "transport_error_type",
        "transport_component",
        "operation_phase",
    }
)
_TRANSPORT_ERROR_TYPES = frozenset(
    {"ConnectionResetError", "BrokenPipeError", "EOFError", "IncompleteReadError"}
)
_TRANSPORT_COMPONENTS = frozenset(
    {
        "browser_context_start",
        "page_create",
        "page_navigation",
        "page_readiness",
        "comment_expansion",
        "playwright_response_body",
        "playwright_response_capture",
        "cached_page_close",
        "web_provider_collect",
    }
)
_READINESS_SELECTOR_KEYS = (
    "first_floor_interactive",
    "image_text_owner",
    "pb_content_wrap",
    "richtext_item",
    "l_post",
    "data_field",
    "data_pid",
    "identity",
    "comment_owner",
    "comment_content",
    "lzl_wrapper",
    "show_more_lzl",
    "nested_owner",
)
_READINESS_ENUMS = {
    "readiness_diagnostic_status": frozenset({"ok", "unavailable", "gate"}),
    "readiness_url_class": frozenset({"official_thread", "official_gate", "other"}),
    "readiness_document_state": frozenset({"loading", "interactive", "complete", "unknown"}),
    "readiness_step": frozenset({"content_snapshot", "target_check", "selector_wait", "unknown"}),
    "readiness_gate_reason": frozenset({"needs_login", "blocked", "security_verification"}),
    "readiness_resource_request_budget": frozenset({"unknown"}),
}
_READINESS_INT_KEYS = frozenset(
    {
        "readiness_main_document_status",
        "readiness_budget_pages_limit",
        "readiness_budget_expansions_limit",
        "readiness_requests",
        "readiness_observed_subresource_requests",
        "readiness_page_pc_requests",
        "readiness_nested_floor_requests",
        "readiness_user_card_requests",
        "readiness_budgeted_actions",
        *(f"readiness_{name}_{metric}" for name in _READINESS_SELECTOR_KEYS for metric in ("count", "visible")),
    }
)
_READINESS_BOOL_KEYS = frozenset(
    {
        "readiness_headed",
        "readiness_main_document_status_known",
        "readiness_budget_pages_exhausted",
        "readiness_budget_expansions_exhausted",
    }
)


def sanitize_page_readiness_diagnostics(values: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep only fixed, non-content page-readiness facts."""
    if not values:
        return {}
    sanitized: dict[str, Any] = {}
    for key, allowed in _READINESS_ENUMS.items():
        value = values.get(key)
        if isinstance(value, str) and value in allowed:
            sanitized[key] = value
    for key in _READINESS_INT_KEYS:
        value = values.get(key)
        if type(value) is int and value >= 0:
            if key != "readiness_main_document_status" or 100 <= value <= 599:
                sanitized[key] = value
    for key in _READINESS_BOOL_KEYS:
        value = values.get(key)
        if type(value) is bool:
            sanitized[key] = value
    return sanitized


def sanitize_web_diagnostics(values: Mapping[str, Any] | None) -> dict[str, int | float | str | bool]:
    """Keep only known bounded counters and status labels; never persist URLs or raw errors."""
    if not values:
        return {}
    sanitized: dict[str, int | float | str | bool] = {}
    for key in _COUNT_KEYS | _FLOAT_KEYS:
        value = values.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int) and value >= 0:
            sanitized[key] = value
        elif key in _FLOAT_KEYS and isinstance(value, (float, int)) and value >= 0:
            sanitized[key] = round(float(value), 3)
    for key in _STATUS_KEYS:
        value = values.get(key)
        if isinstance(value, str) and value and len(value) <= 64 and all(
            char.isalnum() or char in "_.-" for char in value
        ):
            if key == "transport_error_type" and value not in _TRANSPORT_ERROR_TYPES:
                continue
            if key == "transport_component" and value not in _TRANSPORT_COMPONENTS:
                continue
            sanitized[key] = value
    if values.get("manual_gate_detected") is True:
        sanitized["manual_gate_detected"] = True
    error_at = values.get("transport_error_at")
    if isinstance(error_at, str) and len(error_at) <= 40:
        try:
            parsed = datetime.fromisoformat(error_at)
        except ValueError:
            pass
        else:
            if parsed.tzinfo is not None:
                sanitized["transport_error_at"] = parsed.astimezone(UTC).isoformat()
    if "budgeted_actions" not in sanitized:
        page_actions = sanitized.get("pages", 0)
        expansions = sanitized.get("expansions", 0)
        if page_actions or expansions:
            sanitized["budgeted_actions"] = page_actions + expansions
    blocked_reason = sanitized.get("blocked_reason")
    if blocked_reason == "http_429":
        sanitized.setdefault("http_429", 1)
    if blocked_reason in {"http_auth_or_verification", "needs_login", "auth_required"}:
        sanitized.setdefault("auth_pauses", 1)
    sanitized.update(sanitize_page_readiness_diagnostics(values))
    return sanitized


def with_elapsed_seconds(
    values: Mapping[str, Any] | None,
    leased_at: datetime | None,
    finished_at: datetime,
) -> dict[str, int | float | str | bool]:
    sanitized = sanitize_web_diagnostics(values)
    if leased_at is not None:
        start = leased_at.replace(tzinfo=UTC) if leased_at.tzinfo is None else leased_at.astimezone(UTC)
        end = finished_at.replace(tzinfo=UTC) if finished_at.tzinfo is None else finished_at.astimezone(UTC)
        sanitized["elapsed_seconds"] = round(max(0.0, (end - start).total_seconds()), 3)
    return sanitized


__all__ = ["sanitize_web_diagnostics", "with_elapsed_seconds"]
