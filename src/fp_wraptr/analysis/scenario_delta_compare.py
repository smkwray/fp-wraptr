"""Compare scenario effects between two engines against a shared baseline."""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any

from fp_wraptr.io.loadformat import read_loadformat


def _parse_period_key(token: str) -> tuple[int, int]:
    text = str(token).strip()
    if "." not in text:
        return (0, 0)
    year_text, quarter_text = text.split(".", 1)
    try:
        return (int(year_text), int(quarter_text))
    except ValueError:
        return (0, 0)


def _normalize_path(path: Path | str, *, label: str) -> Path:
    normalized = Path(path).expanduser().resolve()
    if not normalized.exists():
        raise FileNotFoundError(f"Missing LOADFORMAT path for {label}: {normalized}")
    return normalized


def _load_payload(
    path: Path,
) -> tuple[list[str], dict[str, list[float]]]:
    periods, series = read_loadformat(path)
    return list(periods), {name: list(values) for name, values in series.items()}


def _safe_rel(abs_diff: float, scale: float, floor: float) -> float:
    denom = max(abs(scale), float(floor))
    if not math.isfinite(denom) or denom <= 0:
        return math.nan
    return abs_diff / denom


def build_scenario_delta_compare_report(
    *,
    baseline_left: Path | str,
    scenario_left: Path | str,
    baseline_right: Path | str,
    scenario_right: Path | str,
    left_label: str = "left",
    right_label: str = "right",
    variables: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    rel_scale_floor: float = 1.0,
    zero_band: float = 1e-6,
    close_rel_threshold: float = 1e-3,
    close_abs_threshold: float = 1.1e-3,
) -> dict[str, Any]:
    normalized_paths = {
        "baseline_left": _normalize_path(baseline_left, label=f"{left_label} baseline"),
        "scenario_left": _normalize_path(scenario_left, label=f"{left_label} scenario"),
        "baseline_right": _normalize_path(baseline_right, label=f"{right_label} baseline"),
        "scenario_right": _normalize_path(scenario_right, label=f"{right_label} scenario"),
    }
    payloads = {name: _load_payload(path) for name, path in normalized_paths.items()}

    common_periods = set(payloads["baseline_left"][0])
    for periods, _series in payloads.values():
        common_periods &= set(periods)
    ordered_periods = sorted(common_periods, key=_parse_period_key)
    if start:
        ordered_periods = [period for period in ordered_periods if _parse_period_key(period) >= _parse_period_key(start)]
    if end:
        ordered_periods = [period for period in ordered_periods if _parse_period_key(period) <= _parse_period_key(end)]

    common_variables: set[str] | None = None
    for _periods, series in payloads.values():
        names = set(series.keys())
        common_variables = names if common_variables is None else (common_variables & names)
    requested = {item.strip() for item in (variables or []) if str(item).strip()}
    ordered_variables = sorted(
        name for name in (common_variables or set()) if not requested or name in requested
    )

    index_lookup: dict[str, dict[str, int]] = {}
    for name, (periods, _series) in payloads.items():
        index_lookup[name] = {period: idx for idx, period in enumerate(periods)}

    summary_rows: list[dict[str, Any]] = []
    for variable in ordered_variables:
        baseline_max_abs_diff = -1.0
        baseline_max_period = ""
        scenario_max_abs_diff = -1.0
        scenario_max_period = ""
        delta_max_abs_diff = -1.0
        delta_max_period = ""
        scale_at_max = math.nan
        delta_left_at_max = math.nan
        delta_right_at_max = math.nan
        baseline_left_at_max = math.nan
        scenario_left_at_max = math.nan
        baseline_right_at_max = math.nan
        scenario_right_at_max = math.nan
        delta_max_rel = -1.0

        for period in ordered_periods:
            i_bl = index_lookup["baseline_left"][period]
            i_sl = index_lookup["scenario_left"][period]
            i_br = index_lookup["baseline_right"][period]
            i_sr = index_lookup["scenario_right"][period]
            bl = float(payloads["baseline_left"][1][variable][i_bl])
            sl = float(payloads["scenario_left"][1][variable][i_sl])
            br = float(payloads["baseline_right"][1][variable][i_br])
            sr = float(payloads["scenario_right"][1][variable][i_sr])

            baseline_abs = abs(bl - br)
            scenario_abs = abs(sl - sr)
            delta_left = sl - bl
            delta_right = sr - br
            delta_abs = abs(delta_left - delta_right)
            delta_rel = _safe_rel(delta_abs, max(abs(delta_left), abs(delta_right)), rel_scale_floor)

            if baseline_abs > baseline_max_abs_diff:
                baseline_max_abs_diff = baseline_abs
                baseline_max_period = period
            if scenario_abs > scenario_max_abs_diff:
                scenario_max_abs_diff = scenario_abs
                scenario_max_period = period
            if delta_abs > delta_max_abs_diff:
                delta_max_abs_diff = delta_abs
                delta_max_period = period
                scale_at_max = max(abs(delta_left), abs(delta_right))
                delta_left_at_max = delta_left
                delta_right_at_max = delta_right
                baseline_left_at_max = bl
                scenario_left_at_max = sl
                baseline_right_at_max = br
                scenario_right_at_max = sr
            if math.isfinite(delta_rel):
                delta_max_rel = max(delta_max_rel, delta_rel)

        if delta_max_abs_diff < 0:
            continue

        if math.isfinite(scale_at_max) and scale_at_max <= zero_band:
            classification = "near_zero"
        elif delta_max_abs_diff <= close_abs_threshold or (
            math.isfinite(delta_max_rel) and delta_max_rel <= close_rel_threshold
        ):
            classification = "close"
        else:
            classification = "review"

        summary_rows.append(
            {
                "variable": variable,
                "classification": classification,
                "baseline_max_abs_diff": baseline_max_abs_diff,
                "baseline_max_period": baseline_max_period,
                "scenario_max_abs_diff": scenario_max_abs_diff,
                "scenario_max_period": scenario_max_period,
                "delta_max_abs_diff": delta_max_abs_diff,
                "delta_max_period": delta_max_period,
                "delta_max_rel_pct": (delta_max_rel * 100.0) if math.isfinite(delta_max_rel) else math.nan,
                "scale_at_max": scale_at_max,
                f"{left_label}_delta_at_max": delta_left_at_max,
                f"{right_label}_delta_at_max": delta_right_at_max,
                f"{left_label}_baseline_at_max": baseline_left_at_max,
                f"{left_label}_scenario_at_max": scenario_left_at_max,
                f"{right_label}_baseline_at_max": baseline_right_at_max,
                f"{right_label}_scenario_at_max": scenario_right_at_max,
            }
        )

    summary_rows.sort(
        key=lambda item: (
            0 if item["classification"] == "review" else 1 if item["classification"] == "close" else 2,
            float(item["delta_max_abs_diff"]),
            float(item["scenario_max_abs_diff"]),
        ),
        reverse=True,
    )

    return {
        "engine_labels": {"left": left_label, "right": right_label},
        "paths": {key: str(path) for key, path in normalized_paths.items()},
        "start": start,
        "end": end,
        "common_period_count": len(ordered_periods),
        "common_variable_count": len(ordered_variables),
        "close_abs_threshold": float(close_abs_threshold),
        "close_rel_threshold": float(close_rel_threshold),
        "rel_scale_floor": float(rel_scale_floor),
        "zero_band": float(zero_band),
        "summary_rows": summary_rows,
    }


def write_scenario_delta_compare_report(
    report: dict[str, Any],
    *,
    output_dir: Path | str,
) -> tuple[Path, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "scenario_delta_compare_report.json"
    csv_path = out_dir / "scenario_delta_compare_summary.csv"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    left_label = str(dict(report.get("engine_labels", {})).get("left", "left"))
    right_label = str(dict(report.get("engine_labels", {})).get("right", "right"))
    fieldnames = [
        "variable",
        "classification",
        "baseline_max_abs_diff",
        "baseline_max_period",
        "scenario_max_abs_diff",
        "scenario_max_period",
        "delta_max_abs_diff",
        "delta_max_period",
        "delta_max_rel_pct",
        "scale_at_max",
        f"{left_label}_delta_at_max",
        f"{right_label}_delta_at_max",
        f"{left_label}_baseline_at_max",
        f"{left_label}_scenario_at_max",
        f"{right_label}_baseline_at_max",
        f"{right_label}_scenario_at_max",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in report.get("summary_rows", []):
            writer.writerow({name: row.get(name, "") for name in fieldnames})

    return json_path, csv_path
