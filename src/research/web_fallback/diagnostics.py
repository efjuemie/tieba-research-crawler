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
    }
)
_FLOAT_KEYS = frozenset({"elapsed_seconds"})
_STATUS_KEYS = frozenset({"status", "blocked_reason", "user_card_status", "user_card_reason"})


def sanitize_web_diagnostics(values: Mapping[str, Any] | None) -> dict[str, int | float | str]:
    """Keep only known bounded counters and status labels; never persist URLs or raw errors."""
    if not values:
        return {}
    sanitized: dict[str, int | float | str] = {}
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
            sanitized[key] = value
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
    return sanitized


def with_elapsed_seconds(
    values: Mapping[str, Any] | None,
    leased_at: datetime | None,
    finished_at: datetime,
) -> dict[str, int | float | str]:
    sanitized = sanitize_web_diagnostics(values)
    if leased_at is not None:
        start = leased_at.replace(tzinfo=UTC) if leased_at.tzinfo is None else leased_at.astimezone(UTC)
        end = finished_at.replace(tzinfo=UTC) if finished_at.tzinfo is None else finished_at.astimezone(UTC)
        sanitized["elapsed_seconds"] = round(max(0.0, (end - start).total_seconds()), 3)
    return sanitized


__all__ = ["sanitize_web_diagnostics", "with_elapsed_seconds"]
