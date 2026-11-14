"""Build canonical Tieba thread URLs from validated identifiers."""

from __future__ import annotations

import re

_MAX_SIGNED_BIGINT = 2**63 - 1
_CANONICAL_POSITIVE_DECIMAL = re.compile(r"[1-9][0-9]*\Z")


def canonical_thread_url(tid: object) -> str | None:
    """Return a canonical thread URL for a positive signed-BIGINT TID.

    Only exact Python integers and canonical ASCII decimal strings are accepted.
    In particular, booleans and floats are rejected so a JavaScript ``Number``
    cannot silently round an identifier before a URL is built.
    """
    if type(tid) is int:
        if not 1 <= tid <= _MAX_SIGNED_BIGINT:
            return None
        canonical_tid = str(tid)
    elif isinstance(tid, str):
        if len(tid) > 19 or _CANONICAL_POSITIVE_DECIMAL.fullmatch(tid) is None:
            return None
        if int(tid) > _MAX_SIGNED_BIGINT:
            return None
        canonical_tid = tid
    else:
        return None

    return f"https://tieba.baidu.com/p/{canonical_tid}"
