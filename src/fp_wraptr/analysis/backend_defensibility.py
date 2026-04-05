"""Backend-agreement reports for fp.exe, fppy, and fp-r.

This is intentionally simpler than full parity gating. The goal is to produce
one stable artifact showing where backends are close, near-zero sensitive, or
need review on economically meaningful variables.
"""

from __future__ import annotations

import csv
import itertools
import json
import math
from pathlib import Path
from typing import Any

from fp_wraptr.io.loadformat import read_loadformat

ENGINE_ORDER = ("fpexe", "fppy", "fp-r")


def _parse_period_key(token: str) -> tuple[int, int]:
    text = str(token).strip()
    if "." not in text:
        return (0, 0)
    year_text, quarter_text = text.split(".", 1)
    try:
        return (int(year_text), int(quarter_text))
    except ValueError:
        return (0, 0)


def _normalize_engine_paths(engine_paths: dict[str, Path | str]) -> dict[str, Path]:
    normalized: dict[str, Path] = {}
    for engine in ENGINE_ORDER:
        raw = engine_paths.get(engine)
        if raw in (None, ""):
            raise ValueError(f"Missing required engine path for {engine}")
        path = Path(raw).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Missing LOADFORMAT path for {engine}: {path}")
        normalized[engine] = path
    return normalized


def _load_engine_series(
    engine_paths: dict[str, Path],
    *,
    start: str | None = None,
    end: str | None = None,
) -> tuple[list[str], dict[str, dict[str, float]]]:
    frames: dict[str, dict[str, dict[str, float]]] = {}
    common_periods: set[str] | None = None
    for engine, path in engine_paths.items():
        periods, series = read_loadformat(path)
        period_rows: dict[str, dict[str, float]] = {}
        for idx, period in enumerate(periods):
            row: dict[str, float] = {}
            for name, values in series.items():
                if idx >= len(values):
                    continue
                value = values[idx]
                if isinstance(value, (int, float)):
                    row[str(name)] = float(value)
            period_rows[str(period)] = row
        frames[engine] = period_rows
        engine_periods = set(period_rows.keys())
        common_periods = engine_periods if common_periods is None else (common_periods & engine_periods)

    ordered_periods = sorted(common_periods or set(), key=_parse_period_key)
    if start:
        ordered_periods = [period for period in ordered_periods if _parse_period_key(period) >= _parse_period_key(start)]
    if end:
        ordered_periods = [period for period in ordered_periods if _parse_period_key(period) <= _parse_period_key(end)]

    aligned: dict[str, dict[str, float]] = {}
    for engine, period_rows in frames.items():
        aligned[engine] = {}
        for period in ordered_periods:
            aligned[engine][period] = period_rows[period]
    return ordered_periods, aligned


def _common_variables(aligned: dict[str, dict[str, dict[str, float]]], periods: list[str]) -> list[str]:
    if not periods:
        return []
    common: set[str] | None = None
    for engine in ENGINE_ORDER:
        engine_vars = set.intersection(*(set(aligned[engine][period].keys()) for period in periods))
        common = engine_vars if common is None else (common & engine_vars)
    return sorted(common or set())


def _safe_rel(abs_diff: float, scale: float, floor: float) -> float:
    denom = max(abs(scale), float(floor))
    if not math.isfinite(denom) or denom <= 0:
        return math.nan
    return abs_diff / denom


def build_backend_defensibility_report(
    engine_paths: dict[str, Path | str],
    *,
    start: str | None = None,
    end: str | None = None,
    variables: list[str] | None = None,
    focus_variables: list[str] | None = None,
    rel_scale_floor: float = 1.0,
    zero_band: float = 1e-6,
    close_rel_threshold: float = 1e-3,
    close_abs_threshold: float = 1.1e-3,
) -> dict[str, Any]:
    normalized_paths = _normalize_engine_paths(engine_paths)
    periods, aligned = _load_engine_series(normalized_paths, start=start, end=end)
    common_variables = _common_variables(aligned, periods)
    if variables:
        requested = {str(item).strip() for item in variables if str(item).strip()}
        common_variables = [name for name in common_variables if name in requested]

    pair_rows: dict[str, list[dict[str, Any]]] = {}
    summary_rows: list[dict[str, Any]] = []
    for left, right in itertools.combinations(ENGINE_ORDER, 2):
        pair_rows[f"{left}__vs__{right}"] = []

    for variable in common_variables:
        first_diff_period = ""
        max_abs_diff = -1.0
        max_abs_pair = ""
        max_abs_period = ""
        max_rel_diff = -1.0
        max_rel_pair = ""
        max_rel_period = ""
        scale_at_max = math.nan
        values_at_max: dict[str, float] = {}
        pairwise_snapshot: dict[str, float] = {}

        for period in periods:
            values = {engine: float(aligned[engine][period][variable]) for engine in ENGINE_ORDER}
            for left, right in itertools.combinations(ENGINE_ORDER, 2):
                left_value = values[left]
                right_value = values[right]
                abs_diff = abs(left_value - right_value)
                rel_diff = _safe_rel(abs_diff, max(abs(left_value), abs(right_value)), rel_scale_floor)
                pair_key = f"{left}__vs__{right}"
                pair_rows[pair_key].append({
                    "period": period,
                    "variable": variable,
                    "left_engine": left,
                    "right_engine": right,
                    "left_value": left_value,
                    "right_value": right_value,
                    "abs_diff": abs_diff,
                    "rel_diff": rel_diff,
                })
                if abs_diff > 0 and not first_diff_period:
                    first_diff_period = period
                if abs_diff > max_abs_diff:
                    max_abs_diff = abs_diff
                    max_abs_pair = pair_key
                    max_abs_period = period
                    scale_at_max = max(abs(left_value), abs(right_value))
                    values_at_max = values
                    pairwise_snapshot = {
                        "left_value": left_value,
                        "right_value": right_value,
                    }
                if math.isfinite(rel_diff) and rel_diff > max_rel_diff:
                    max_rel_diff = rel_diff
                    max_rel_pair = pair_key
                    max_rel_period = period

        if max_abs_diff < 0:
            continue

        if math.isfinite(scale_at_max) and scale_at_max <= zero_band:
            classification = "near_zero"
        elif max_abs_diff <= close_abs_threshold or (
            math.isfinite(max_rel_diff) and max_rel_diff <= close_rel_threshold
        ):
            classification = "close"
        else:
            classification = "review"

        row = {
            "variable": variable,
            "first_diff_period": first_diff_period,
            "max_abs_diff": max_abs_diff,
            "max_abs_pair": max_abs_pair,
            "max_abs_period": max_abs_period,
            "max_rel_diff": max_rel_diff if math.isfinite(max_rel_diff) else math.nan,
            "max_rel_pct": (max_rel_diff * 100.0) if math.isfinite(max_rel_diff) else math.nan,
            "max_rel_pair": max_rel_pair,
            "max_rel_period": max_rel_period,
            "scale_at_max": scale_at_max,
            "classification": classification,
            "fpexe_at_max": values_at_max.get("fpexe"),
            "fppy_at_max": values_at_max.get("fppy"),
            "fpr_at_max": values_at_max.get("fp-r"),
            "pair_left_at_max": pairwise_snapshot.get("left_value"),
            "pair_right_at_max": pairwise_snapshot.get("right_value"),
        }
        summary_rows.append(row)

    summary_rows.sort(
        key=lambda item: (
            0 if item["classification"] == "review" else 1 if item["classification"] == "close" else 2,
            float(item["max_abs_diff"]),
            float(item["max_rel_pct"]) if math.isfinite(float(item["max_rel_pct"])) else -1.0,
        ),
        reverse=True,
    )

    pair_summaries: dict[str, dict[str, Any]] = {}
    for pair_key, rows in pair_rows.items():
        best_by_var: dict[str, dict[str, Any]] = {}
        for row in rows:
            current = best_by_var.get(row["variable"])
            if current is None or float(row["abs_diff"]) > float(current["abs_diff"]):
                best_by_var[row["variable"]] = row
        top_rows = sorted(
            best_by_var.values(),
            key=lambda item: (float(item["abs_diff"]), float(item["rel_diff"]) if math.isfinite(float(item["rel_diff"])) else -1.0),
            reverse=True,
        )
        pair_summaries[pair_key] = {
            "max_abs_diff": float(top_rows[0]["abs_diff"]) if top_rows else 0.0,
            "top_variables": top_rows[:25],
        }

    focus_set = [item.strip() for item in (focus_variables or []) if str(item).strip()]
    focus_rows = [row for row in summary_rows if row["variable"] in focus_set] if focus_set else []

    return {
        "engine_paths": {engine: str(path) for engine, path in normalized_paths.items()},
        "start": start,
        "end": end,
        "common_period_count": len(periods),
        "common_variable_count": len(common_variables),
        "close_abs_threshold": float(close_abs_threshold),
        "close_rel_threshold": float(close_rel_threshold),
        "rel_scale_floor": float(rel_scale_floor),
        "zero_band": float(zero_band),
        "summary_rows": summary_rows,
        "focus_rows": focus_rows,
        "pair_summaries": pair_summaries,
    }


def write_backend_defensibility_report(
    report: dict[str, Any],
    *,
    output_dir: Path | str,
) -> tuple[Path, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_json = out_dir / "backend_defensibility_report.json"
    summary_csv = out_dir / "backend_defensibility_summary.csv"

    summary_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    fieldnames = [
        "variable",
        "classification",
        "first_diff_period",
        "max_abs_diff",
        "max_abs_pair",
        "max_abs_period",
        "max_rel_pct",
        "max_rel_pair",
        "max_rel_period",
        "scale_at_max",
        "fpexe_at_max",
        "fppy_at_max",
        "fpr_at_max",
    ]
    with summary_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in report.get("summary_rows", []):
            writer.writerow({name: row.get(name, "") for name in fieldnames})

    return summary_json, summary_csv
