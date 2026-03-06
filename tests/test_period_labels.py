from __future__ import annotations

from fp_wraptr.viz.period_labels import format_period_label


def test_format_period_label_quarter_tokens() -> None:
    assert format_period_label("2025.2") == "2025Q2"
    assert format_period_label("2025.4") == "2025Q4"


def test_format_period_label_non_quarter_tokens() -> None:
    assert format_period_label("2025.5") == "2025P5"
    assert format_period_label("2025.10") == "2025P10"


def test_format_period_label_passthrough() -> None:
    assert format_period_label("foo") == "foo"
    assert format_period_label("  foo  ") == "foo"

