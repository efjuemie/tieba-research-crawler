from __future__ import annotations

import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.core.config import WebFallbackConfig


def test_web_fallback_defaults_are_bounded_and_serialized_in_examples():
    settings = WebFallbackConfig()
    assert settings.max_pages == 1
    assert settings.max_expansions == 1
    assert settings.timeout_seconds == 30.0

    root = Path(__file__).resolve().parents[1]
    for name in ("config.example.toml", "config.docker.example.toml"):
        with (root / name).open("rb") as stream:
            sample = tomllib.load(stream)
        assert sample["web_fallback"] == {
            "max_pages": 1,
            "max_expansions": 1,
            "timeout_seconds": 30.0,
        }


@pytest.mark.parametrize(
    "values",
    [
        {"max_pages": 2},
        {"max_expansions": 2},
        {"max_expansions": -1},
        {"timeout_seconds": 0},
        {"timeout_seconds": 121},
    ],
)
def test_web_fallback_config_rejects_unbounded_settings(values):
    with pytest.raises(ValidationError):
        WebFallbackConfig(**values)
