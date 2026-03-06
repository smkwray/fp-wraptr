"""Diff/audit helpers for comparing two fmdata.txt files."""

from __future__ import annotations

import bisect
import json
from pathlib import Path
from typing import Any

from fp_wraptr.io.input_parser import parse_fm_data

__all__ = ["FmdataDiffError", "diff_fmdata_files"]


class FmdataDiffError(RuntimeError):
    """Raised when fmdata diff inputs are invalid."""


def _parse_period(period: str) -> tuple[int, int]:
    text = str(period).strip()
    try:
        year_text, quarter_text = text.split(".")
        year = int(year_text)
        quarter = int(quarter_text)
    except Exception as exc:
        raise FmdataDiffError(f"Invalid period '{period}' (expected YYYY.Q)") from exc
    if quarter not in {1, 2, 3, 4}:
        raise FmdataDiffError(f"Invalid period '{period}' (quarter must be 1..4)")
    return year, quarter


def _period_to_ordinal(period: str) -> int:
    year, quarter = _parse_period(period)
    return year * 4 + (quarter - 1)


def _ordinal_to_period(ordinal: int) -> str:
    year = int(ordinal // 4)
    quarter = int((ordinal % 4) + 1)
    return f"{year}.{quarter}"


def _next_period(period: str) -> str:
    return _ordinal_to_period(_period_to_ordinal(period) + 1)


def _periods_between(start: str, end: str) -> list[str]:
    start_ord = _period_to_ordinal(start)
    end_ord = _period_to_ordinal(end)
    if end_ord < start_ord:
        raise FmdataDiffError(f"Invalid window '{start}..{end}' (end must be >= start).")
    return [_ordinal_to_period(i) for i in range(start_ord, end_ord + 1)]


def _series_by_period(parsed: dict[str, Any]) -> dict[str, dict[str, float]]:
    blocks = parsed.get("blocks")
    if not isinstance(blocks, list):
        return {}

    result: dict[str, dict[str, float]] = {}
    for block in blocks:
        if not isinstance(block, dict):
            continue
        var = str(block.get("name", "")).strip().upper()
        start = str(block.get("sample_start", "")).strip()
        values = block.get("values")
        if not var or not start or not isinstance(values, list):
            continue

        cursor = _period_to_ordinal(start)
        points = result.setdefault(var, {})
        for raw in values:
            try:
                value = float(raw)
            except Exception:
                cursor += 1
                continue
            points[_ordinal_to_period(cursor)] = value
            cursor += 1
    return result


def _load_update_classification(
    report_path: Path | None,
) -> tuple[set[str], set[str], str | None, str | None]:
    if report_path is None:
        return set(), set(), None, None
    if not report_path.exists():
        raise FmdataDiffError(f"data_update_report.json not found: {report_path}")
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise FmdataDiffError(f"Failed to parse data update report: {report_path}") from exc
    merge = payload.get("fmdata_merge")
    if not isinstance(merge, dict):
        merge = {}
    updated = {
        str(name).strip().upper()
        for name in (merge.get("variables_with_updates") or [])
        if str(name).strip()
    }
    carried = {
        str(name).strip().upper()
        for name in (merge.get("variables_with_carry_forward") or [])
        if str(name).strip()
    }
    start_period = str(payload.get("start_period", "")).strip() or None
    end_period = str(payload.get("end_period", "")).strip() or None
    return updated, carried, start_period, end_period


def _resolve_window(
    *,
    scope: str,
    start_period: str | None,
    end_period: str | None,
    base_sample_end: str,
    updated_sample_end: str,
    report_start: str | None,
    report_end: str | None,
) -> tuple[str, str]:
    if start_period and end_period:
        return start_period, end_period
    if start_period and not end_period:
        return start_period, start_period
    if end_period and not start_period:
        return end_period, end_period

    normalized_scope = str(scope).strip().lower()
    if normalized_scope == "sample_end":
        return updated_sample_end, updated_sample_end
    if normalized_scope == "update_window":
        if report_start and report_end:
            return report_start, report_end
        default_start = _next_period(base_sample_end)
        return default_start, updated_sample_end
    if normalized_scope == "all":
        start = min(base_sample_end, updated_sample_end, key=_period_to_ordinal)
        end = max(base_sample_end, updated_sample_end, key=_period_to_ordinal)
        return start, end

    raise FmdataDiffError(
        f"Unsupported scope '{scope}' (expected sample_end, update_window, or all)."
    )


def diff_fmdata_files(
    *,
    base_fmdata: Path | str,
    updated_fmdata: Path | str,
    scope: str = "sample_end",
    start_period: str | None = None,
    end_period: str | None = None,
    data_update_report: Path | str | None = None,
    epsilon: float = 1e-12,
) -> dict[str, Any]:
    """Compare two fmdata files and report changed variables over a target window."""
    base_path = Path(base_fmdata)
    updated_path = Path(updated_fmdata)
    if not base_path.exists():
        raise FmdataDiffError(f"Base fmdata not found: {base_path}")
    if not updated_path.exists():
        raise FmdataDiffError(f"Updated fmdata not found: {updated_path}")

    base_parsed = parse_fm_data(base_path)
    updated_parsed = parse_fm_data(updated_path)

    base_sample_end = str(base_parsed.get("sample_end", "")).strip()
    updated_sample_end = str(updated_parsed.get("sample_end", "")).strip()
    if not base_sample_end or not updated_sample_end:
        raise FmdataDiffError("Unable to resolve sample_end from one or both fmdata files.")

    report_path = Path(data_update_report) if data_update_report is not None else None
    updated_vars, carried_vars, report_start, report_end = _load_update_classification(report_path)

    window_start, window_end = _resolve_window(
        scope=scope,
        start_period=start_period,
        end_period=end_period,
        base_sample_end=base_sample_end,
        updated_sample_end=updated_sample_end,
        report_start=report_start,
        report_end=report_end,
    )
    window_periods = _periods_between(window_start, window_end)
    window_ordinals = [_period_to_ordinal(period) for period in window_periods]

    base_series = _series_by_period(base_parsed)
    updated_series = _series_by_period(updated_parsed)
    variables = sorted({*base_series.keys(), *updated_series.keys()})

    records: list[dict[str, Any]] = []

    for var in variables:
        base_points = base_series.get(var, {})
        updated_points = updated_series.get(var, {})
        base_ord_map = {_period_to_ordinal(period): value for period, value in base_points.items()}
        base_ord_keys = sorted(base_ord_map.keys())
        updated_ord_map = {
            _period_to_ordinal(period): value for period, value in updated_points.items()
        }

        changed_periods: list[str] = []
        max_abs_delta: float | None = None
        sample_end_abs_delta: float | None = None
        sample_end_base_value: float | None = None
        sample_end_updated_value: float | None = None

        for period_ord in window_ordinals:
            updated_value = updated_ord_map.get(period_ord)
            if updated_value is None:
                continue

            base_value = base_ord_map.get(period_ord)
            if base_value is None and base_ord_keys:
                idx = bisect.bisect_right(base_ord_keys, period_ord) - 1
                if idx >= 0:
                    base_value = base_ord_map[base_ord_keys[idx]]

            if period_ord == _period_to_ordinal(updated_sample_end):
                sample_end_base_value = base_value
                sample_end_updated_value = updated_value

            if base_value is None:
                changed = True
                delta = None
            else:
                delta = abs(float(updated_value) - float(base_value))
                changed = delta > max(float(epsilon), 0.0)

            if not changed:
                continue

            changed_periods.append(_ordinal_to_period(period_ord))
            if delta is not None:
                if max_abs_delta is None or delta > max_abs_delta:
                    max_abs_delta = delta
                if period_ord == _period_to_ordinal(updated_sample_end):
                    sample_end_abs_delta = delta

        if not changed_periods:
            continue

        if (
            sample_end_abs_delta is None
            and sample_end_base_value is not None
            and sample_end_updated_value is not None
        ):
            sample_end_abs_delta = abs(
                float(sample_end_updated_value) - float(sample_end_base_value)
            )

        if var in updated_vars:
            change_type = "updated"
        elif var in carried_vars:
            change_type = "carried_forward"
        else:
            change_type = "unknown"

        records.append({
            "variable": var,
            "change_type": change_type,
            "changed_period_count": len(changed_periods),
            "first_changed_period": changed_periods[0],
            "last_changed_period": changed_periods[-1],
            "changed_periods_preview": changed_periods[:8],
            "sample_end_base_value": sample_end_base_value,
            "sample_end_updated_value": sample_end_updated_value,
            "sample_end_abs_delta": sample_end_abs_delta,
            "max_abs_delta": max_abs_delta,
        })

    def _delta_sort(value: float | None) -> float:
        if value is None:
            return -1.0
        return float(value)

    records.sort(
        key=lambda item: (
            -_delta_sort(item.get("sample_end_abs_delta")),
            -_delta_sort(item.get("max_abs_delta")),
            str(item.get("variable", "")),
        )
    )

    return {
        "schema_version": 1,
        "base_fmdata": str(base_path),
        "updated_fmdata": str(updated_path),
        "scope": str(scope).strip().lower(),
        "window_start": window_start,
        "window_end": window_end,
        "window_period_count": len(window_periods),
        "window_periods": window_periods,
        "sample_end_base": base_sample_end,
        "sample_end_updated": updated_sample_end,
        "data_update_report": str(report_path) if report_path is not None else "",
        "changed_variable_count": len(records),
        "records": records,
    }
