"""Validation helpers for persisted API error codes."""


def normalize_checkpoint_error_code(value: object) -> int | None:
    """Return a signed 32-bit integer code, discarding non-platform values."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if not -(2**31) <= value <= 2**31 - 1:
        return None
    return int(value)
