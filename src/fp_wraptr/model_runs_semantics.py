"""Shared transform and run-comparison semantics for the static run explorer."""

from __future__ import annotations

import math

TRANSFORM_LEVEL = "level"
TRANSFORM_PCT_OF = "pct_of"
TRANSFORM_LVL_CHANGE = "lvl_change"
TRANSFORM_PCT_CHANGE = "pct_change"

COMPARE_NONE = "none"
COMPARE_DIFF_VS_RUN = "diff_vs_run"
COMPARE_PCT_DIFF_VS_RUN = "pct_diff_vs_run"

DEFAULT_TRANSFORM_DENOMINATOR = "GDP"

VALID_TRANSFORM_MODES = {
    TRANSFORM_LEVEL,
    TRANSFORM_PCT_OF,
    TRANSFORM_LVL_CHANGE,
    TRANSFORM_PCT_CHANGE,
}

VALID_COMPARE_MODES = {
    COMPARE_NONE,
    COMPARE_DIFF_VS_RUN,
    COMPARE_PCT_DIFF_VS_RUN,
}


def coerce_float(value: object) -> float:
    """Best-effort float coercion used by exported series math."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def pct_value(numerator: float, denominator: float) -> float:
    """Return percentage-of-denominator or NaN for invalid denominators."""
    if not (math.isfinite(numerator) and math.isfinite(denominator)):
        return float("nan")
    if denominator == 0.0:
        return float("nan")
    return 100.0 * numerator / denominator


def level_change_value(current: float, previous: float) -> float:
    """Return absolute difference between two values."""
    if not (math.isfinite(current) and math.isfinite(previous)):
        return float("nan")
    return current - previous


def pct_change_value(current: float, previous: float) -> float:
    """Return 100 * (current / previous - 1), mirroring Run Panels."""
    if not (math.isfinite(current) and math.isfinite(previous)):
        return float("nan")
    if previous == 0.0:
        return float("nan")
    return 100.0 * (current / previous - 1.0)


def transform_series(
    *,
    mode: str,
    level_values: list[float],
    denominator_values: list[float] | None = None,
) -> list[float]:
    """Apply one exported-series transform using Run Panels semantics."""
    selected_mode = str(mode or TRANSFORM_LEVEL).strip().lower() or TRANSFORM_LEVEL
    if selected_mode == TRANSFORM_PCT_OF:
        if not isinstance(denominator_values, list):
            return [float("nan")] * len(level_values)
        out: list[float] = []
        for idx, numer in enumerate(level_values):
            denom = denominator_values[idx] if idx < len(denominator_values) else float("nan")
            out.append(pct_value(numer, denom))
        return out
    if selected_mode == TRANSFORM_LVL_CHANGE:
        out: list[float] = []
        for idx, current in enumerate(level_values):
            if idx == 0:
                out.append(float("nan"))
                continue
            out.append(level_change_value(current, level_values[idx - 1]))
        return out
    if selected_mode == TRANSFORM_PCT_CHANGE:
        out = []
        for idx, current in enumerate(level_values):
            if idx == 0:
                out.append(float("nan"))
                continue
            out.append(pct_change_value(current, level_values[idx - 1]))
        return out
    return list(level_values)


def apply_run_comparison(
    *,
    mode: str,
    values: list[float],
    reference_values: list[float] | None,
) -> list[float]:
    """Apply a post-transform run comparison using Run Panels semantics."""
    selected_mode = str(mode or COMPARE_NONE).strip().lower() or COMPARE_NONE
    if selected_mode == COMPARE_NONE:
        return list(values)
    if not isinstance(reference_values, list):
        return [float("nan")] * len(values)
    out: list[float] = []
    for idx, current in enumerate(values):
        reference = reference_values[idx] if idx < len(reference_values) else float("nan")
        if selected_mode == COMPARE_DIFF_VS_RUN:
            out.append(level_change_value(current, reference))
        elif selected_mode == COMPARE_PCT_DIFF_VS_RUN:
            out.append(pct_change_value(current, reference))
        else:
            out.append(current)
    return out
