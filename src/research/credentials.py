"""Local-only credentials for optional forum-info lookups."""

from __future__ import annotations

import getpass
import json
import os
import tempfile
from http.cookies import SimpleCookie
from pathlib import Path
from typing import TextIO


def default_account_file() -> Path:
    """Return the ignored local account file path inside this checkout."""
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "secrets" / "baidu-account.json"


def save_bduss_interactively(path: str | Path | None = None) -> Path:
    """Prompt without echo and atomically save BDUSS to an ignored file."""
    value = getpass.getpass("BDUSS or Cookie header (input is hidden; saved locally only): ")
    return save_bduss(value, path)


def save_bduss(value: str, path: str | Path | None = None) -> Path:
    """Safely save raw BDUSS or a Cookie header; only BDUSS is persisted."""
    destination = Path(path) if path is not None else default_account_file()
    if not destination.is_absolute():
        destination = Path(__file__).resolve().parents[2] / destination

    bduss = parse_bduss(value)

    parent_was_missing = not destination.parent.exists()
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if parent_was_missing:
        try:
            destination.parent.chmod(0o700)
        except OSError:
            pass

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=".baidu-account-",
            suffix=".tmp",
            delete=False,
        ) as account_file:
            temp_path = Path(account_file.name)
            _write_account_json(account_file, bduss)
            account_file.flush()
            os.fsync(account_file.fileno())

        temp_path.chmod(0o600)
        temp_path.replace(destination)
        destination.chmod(0o600)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()

    return destination


def load_bduss(path: str | Path) -> str:
    """Read BDUSS only from the explicitly configured local account file."""
    account_path = Path(path)
    if not account_path.is_absolute():
        account_path = Path(__file__).resolve().parents[2] / account_path

    try:
        payload = json.loads(account_path.read_text(encoding="utf-8"))
        bduss = payload.get("BDUSS") if isinstance(payload, dict) else None
    except (OSError, json.JSONDecodeError):
        raise ValueError("Account file must be readable JSON containing a non-empty BDUSS value.") from None

    if not isinstance(bduss, str):
        raise ValueError("Account file must contain a valid BDUSS value.")
    return parse_bduss(bduss)


def parse_bduss(value: str) -> str:
    """Accept a 192-character BDUSS or extract it from a Cookie header."""
    candidate = value.strip()
    is_cookie_header = candidate.lower().startswith("cookie:")
    if candidate.lower().startswith("cookie:"):
        candidate = candidate.split(":", 1)[1].strip()
    is_cookie_header = is_cookie_header or candidate.startswith("BDUSS=") or ";" in candidate
    if is_cookie_header:
        cookie = SimpleCookie()
        try:
            cookie.load(candidate)
        except Exception:
            raise ValueError("Input must be a 192-character BDUSS or a Cookie header containing BDUSS.") from None
        morsel = cookie.get("BDUSS")
        if morsel is None:
            raise ValueError("Cookie header must contain BDUSS.")
        candidate = morsel.value

    if len(candidate) != 192:
        raise ValueError("BDUSS must be exactly 192 characters; input was not saved.")
    return candidate


def _write_account_json(file: TextIO, bduss: str) -> None:
    json.dump({"BDUSS": bduss}, file, ensure_ascii=False)
    file.write("\n")
