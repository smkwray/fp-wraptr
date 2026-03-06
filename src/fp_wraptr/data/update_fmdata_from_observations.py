"""Merge normalized quarterly observations into an existing fmdata dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


class FmdataUpdateError(RuntimeError):
    """Raised when fmdata update inputs are invalid or unsupported."""


def _parse_period(period: str) -> tuple[int, int]:
    text = str(period).strip()
    try:
        year_text, quarter_text = text.split(".")
        year = int(year_text)
        quarter = int(quarter_text)
    except Exception as exc:  # pragma: no cover - defensive parse guard
        raise FmdataUpdateError(f"Invalid period '{period}' (expected YYYY.Q)") from exc
    if quarter not in {1, 2, 3, 4}:
        raise FmdataUpdateError(f"Invalid period '{period}' (quarter must be 1..4)")
    return year, quarter


def _format_period(year: int, quarter: int) -> str:
    return f"{year}.{quarter}"


def _next_period(period: str) -> str:
    year, quarter = _parse_period(period)
    quarter += 1
    if quarter > 4:
        year += 1
        quarter = 1
    return _format_period(year, quarter)


def _periods_between(start: str, end: str) -> list[str]:
    sy, sq = _parse_period(start)
    ey, eq = _parse_period(end)
    periods: list[str] = []
    y, q = sy, sq
    while (y, q) <= (ey, eq):
        periods.append(_format_period(y, q))
        q += 1
        if q > 4:
            q = 1
            y += 1
    return periods


def _period_gt(left: str, right: str) -> bool:
    return _parse_period(left) > _parse_period(right)


@dataclass(frozen=True)
class UpdateFmdataResult:
    sample_start: str
    sample_end_before: str
    sample_end_after: str
    series: dict[str, list[float]]
    report: dict[str, Any]


def update_fmdata_from_observations(
    *,
    parsed_fmdata: dict[str, Any],
    observations: dict[str, pd.Series],
    end_period: str,
    replace_history: bool = False,
    extend_sample: bool = False,
    allow_carry_forward: bool = False,
) -> UpdateFmdataResult:
    """Merge normalized observation series into parsed fmdata.

    Args:
        parsed_fmdata: Output of `parse_fm_data_text` / `parse_fm_data`.
        observations: Dict mapping variable -> quarterly Series indexed by FP periods ("YYYY.Q").
        end_period: Requested update end bound.
        replace_history: If True, apply updates within the existing history window too.
        extend_sample: If True, extend the fmdata sample_end to `end_period` if needed.
        allow_carry_forward: If True, fill missing values in extended periods by carrying forward
            the previous period value.
    """
    sample_start = str(parsed_fmdata.get("sample_start", "")).strip()
    sample_end_before = str(parsed_fmdata.get("sample_end", "")).strip()
    if not sample_start or not sample_end_before:
        raise FmdataUpdateError("Parsed fmdata missing sample_start/sample_end")

    _parse_period(sample_start)
    _parse_period(sample_end_before)
    _parse_period(end_period)

    if _period_gt(sample_start, end_period):
        raise FmdataUpdateError(f"end_period {end_period} is before sample_start {sample_start}")

    if _period_gt(end_period, sample_end_before) and not extend_sample:
        raise FmdataUpdateError(
            f"end_period {end_period} is after fmdata sample_end {sample_end_before}. "
            "Pass --extend-sample to extend the fmdata sample."
        )

    sample_end_after = (
        end_period
        if extend_sample and _period_gt(end_period, sample_end_before)
        else sample_end_before
    )

    base_periods = _periods_between(sample_start, sample_end_before)
    out_periods = _periods_between(sample_start, sample_end_after)

    blocks = parsed_fmdata.get("blocks")
    if not isinstance(blocks, list) or not blocks:
        raise FmdataUpdateError("Parsed fmdata contains no LOAD blocks")

    # Preserve file series order by iterating LOAD blocks.
    base_series: dict[str, list[float]] = {}
    series_order: list[str] = []
    expected_len = len(base_periods)
    for block in blocks:
        if not isinstance(block, dict):
            continue
        name = str(block.get("name", "")).strip().upper()
        if not name:
            continue
        values = block.get("values", [])
        if not isinstance(values, list):
            continue
        float_values = [float(v) for v in values]
        if len(float_values) != expected_len:
            raise FmdataUpdateError(
                f"Series '{name}' length {len(float_values)} does not match sample length {expected_len}"
            )
        if name in base_series:
            # Some stock FM datasets repeat LOAD blocks (for example GNPD/USROW).
            # These appear to be redundant in practice; tolerate duplicates only if identical.
            if base_series[name] != float_values:
                raise FmdataUpdateError(
                    f"Multiple LOAD blocks found for variable '{name}' with non-identical values"
                )
            continue
        series_order.append(name)
        base_series[name] = float_values

    if not base_series:
        raise FmdataUpdateError("Parsed fmdata contains no usable series values")

    update_start = sample_start if replace_history else _next_period(sample_end_before)

    updated_cells = 0
    carried_cells = 0
    missing_cells = 0
    missing_variables: set[str] = set()
    variables_with_updates: set[str] = set()
    variables_with_carry: set[str] = set()

    merged: dict[str, list[float]] = {}

    for name in series_order:
        original = base_series[name]
        new_values: list[float | None] = [*original]
        if len(out_periods) > len(base_periods):
            new_values.extend([None] * (len(out_periods) - len(base_periods)))

        obs = observations.get(name)
        for idx, period in enumerate(out_periods):
            # Only apply updates within update window.
            apply_update = not _period_gt(update_start, period) and not _period_gt(
                period, end_period
            )
            if apply_update and obs is not None and period in obs.index:
                raw = obs.loc[period]
                if pd.notna(raw):
                    new_values[idx] = float(raw)
                    updated_cells += 1
                    variables_with_updates.add(name)
                    continue

            # Fill newly-extended periods (after base end) if still missing.
            if idx >= len(base_periods) and new_values[idx] is None:
                if allow_carry_forward:
                    if idx == 0 or new_values[idx - 1] is None:
                        missing_cells += 1
                        missing_variables.add(name)
                    else:
                        new_values[idx] = float(new_values[idx - 1])
                        carried_cells += 1
                        variables_with_carry.add(name)
                else:
                    missing_cells += 1
                    missing_variables.add(name)

            # Defensive: ensure no missing values survive inside base sample.
            if idx < len(base_periods) and new_values[idx] is None:
                missing_cells += 1
                missing_variables.add(name)

        if any(v is None for v in new_values):
            # Replace the first None with a sentinel in the error for debugging.
            missing_idx = next(i for i, v in enumerate(new_values) if v is None)
            missing_period = out_periods[missing_idx]
            raise FmdataUpdateError(
                f"Variable '{name}' has missing value at {missing_period}. "
                "Use --allow-carry-forward or expand source mappings."
            )

        merged[name] = [float(v) for v in new_values]  # type: ignore[arg-type]

    if missing_cells and not allow_carry_forward:
        preview = ", ".join(sorted(missing_variables)[:10])
        raise FmdataUpdateError(
            f"Missing values in extended sample (missing_cells={missing_cells}). "
            f"Variables with missing data (sample): {preview}. "
            "Pass --allow-carry-forward to carry forward prior values."
        )

    report: dict[str, Any] = {
        "schema_version": 1,
        "sample_start": sample_start,
        "sample_end_before": sample_end_before,
        "sample_end_after": sample_end_after,
        "end_period": end_period,
        "replace_history": bool(replace_history),
        "extend_sample": bool(extend_sample),
        "allow_carry_forward": bool(allow_carry_forward),
        "updated_cells": int(updated_cells),
        "carried_cells": int(carried_cells),
        "missing_cells": int(missing_cells),
        "variables_total": len(series_order),
        "variables_with_updates": sorted(variables_with_updates),
        "variables_with_carry_forward": sorted(variables_with_carry),
        "missing_variables": sorted(missing_variables),
    }

    return UpdateFmdataResult(
        sample_start=sample_start,
        sample_end_before=sample_end_before,
        sample_end_after=sample_end_after,
        series=merged,
        report=report,
    )
