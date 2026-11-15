from __future__ import annotations

import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.core.config import WebFallbackConfig


def test_web_fallback_defaults_are_bounded_and_serialized_in_examples():
    settings = WebFallbackConfig()
    assert settings.browser_profile == "data/web-fallback-profile"
    assert settings.max_pages == 1
    assert settings.max_expansions == 10
    assert settings.timeout_seconds == 90.0

    root = Path(__file__).resolve().parents[1]
    for name in ("config.example.toml", "config.docker.example.toml"):
        with (root / name).open("rb") as stream:
            sample = tomllib.load(stream)
        assert sample["web_fallback"] == {
            "browser_profile": "data/web-fallback-profile",
            "max_pages": 1,
            "max_expansions": 10,
            "timeout_seconds": 90.0,
        }


def test_web_fallback_config_keeps_explicit_lower_search_limits():
    settings = WebFallbackConfig(max_expansions=1, timeout_seconds=30.0)

    assert settings.max_expansions == 1
    assert settings.timeout_seconds == 30.0


@pytest.mark.parametrize(
    ("max_pages", "max_expansions"),
    [(2, 2), (10, 10)],
)
def test_web_fallback_config_accepts_bounded_pagination(max_pages, max_expansions):
    settings = WebFallbackConfig(max_pages=max_pages, max_expansions=max_expansions)

    assert settings.max_pages == max_pages
    assert settings.max_expansions == max_expansions


@pytest.mark.parametrize(
    "values",
    [
        {"max_pages": 0},
        {"max_expansions": -1},
        {"max_pages": 11},
        {"max_expansions": 11},
        {"timeout_seconds": 0},
        {"timeout_seconds": 121},
    ],
)
def test_web_fallback_config_rejects_out_of_bounds_settings(values):
    with pytest.raises(ValidationError):
        WebFallbackConfig(**values)
