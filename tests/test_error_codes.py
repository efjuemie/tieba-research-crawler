from enum import IntEnum

import pytest

from src.core.error_codes import normalize_checkpoint_error_code


class SyntheticApiErrorCode(IntEnum):
    AUTH_REQUIRED = 4011
    ZERO = 0


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, 0),
        (-1, -1),
        (4011, 4011),
        (429, 429),
        (220034, 220034),
        (-(2**31), -(2**31)),
        (2**31 - 1, 2**31 - 1),
        (SyntheticApiErrorCode.AUTH_REQUIRED, 4011),
        (SyntheticApiErrorCode.ZERO, 0),
    ],
)
def test_normalize_checkpoint_error_code_preserves_valid_integer_values(value, expected):
    assert normalize_checkpoint_error_code(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        False,
        1.0,
        "429",
        "rvf5",
        b"429",
        [],
        {},
        object(),
        -(2**31) - 1,
        2**31,
    ],
    ids=[
        "none",
        "true",
        "false",
        "float",
        "numeric-string",
        "sqlalchemy-doc-code",
        "bytes",
        "list",
        "mapping",
        "object",
        "underflow",
        "overflow",
    ],
)
def test_normalize_checkpoint_error_code_discards_unknown_or_out_of_range_values(value):
    assert normalize_checkpoint_error_code(value) is None
