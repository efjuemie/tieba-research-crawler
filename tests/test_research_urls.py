import pytest

from src.research.urls import canonical_thread_url


@pytest.mark.parametrize(
    ("tid", "expected"),
    [
        (1, "https://tieba.baidu.com/p/1"),
        ("1", "https://tieba.baidu.com/p/1"),
        (9_007_199_254_740_993, "https://tieba.baidu.com/p/9007199254740993"),
        ("9223372036854775807", "https://tieba.baidu.com/p/9223372036854775807"),
    ],
)
def test_canonical_thread_url_accepts_positive_signed_bigint_ids(tid, expected):
    assert canonical_thread_url(tid) == expected


@pytest.mark.parametrize(
    "tid",
    [
        None,
        True,
        False,
        1.0,
        9_007_199_254_740_992.0,
        -1,
        0,
        "",
        "0",
        "00",
        "01",
        "+1",
        "-1",
        " 1",
        "1 ",
        "١",
        "https://evil.example/p/1",
        "9223372036854775808",
        9_223_372_036_854_775_808,
        "9" * 10000,
    ],
)
def test_canonical_thread_url_rejects_invalid_or_unsafe_ids(tid):
    assert canonical_thread_url(tid) is None
