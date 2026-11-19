"""科研回溯配置边界测试。"""

import pytest
from pydantic import ValidationError

from src.core.config import RateLimitConfig, TiebaConfig


def test_zero_backfill_page_limit_is_explicitly_unbounded():
    assert TiebaConfig().max_backfill_pages == 0

    config = TiebaConfig(max_backfill_pages=0)

    assert config.max_backfill_pages == 0


def test_negative_backfill_page_limit_is_rejected():
    with pytest.raises(ValidationError):
        TiebaConfig(max_backfill_pages=-1)


def test_global_request_rate_limit_has_a_positive_conservative_default():
    assert RateLimitConfig().global_rps == 0.5
    assert RateLimitConfig().cooldown_seconds_429 == 5
    with pytest.raises(ValidationError):
        RateLimitConfig(global_rps=0)
