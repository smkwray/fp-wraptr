"""Focused per-period series comparison across fp.exe, fp-py, and fp-r."""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any

from fp_wraptr.analysis.backend_defensibility import _load_engine_series, _normalize_engine_paths


def _safe_rel_pct(left: float, right: float, floor: float) -> float:
    denom = max(abs(float(left)), abs(float(right)), float(floor))
    if not math.isfinite(denom) or denom <= 0:
        return math.nan
    return abs(float(left) - float(right)) / denom * 100.0


def build_focused_series_compare_report(
    engine_paths: dict[str, Path | str],
    *,
    variables: list[str],
    start: str | None = None,
    end: str | None = None,
    rel_scale_floor: float = 1.0,
) -> dict[str, Any]:
    normalized_paths = _normalize_engine_paths(engine_paths)
    periods, aligned = _load_engine_series(normalized_paths, start=start, end=end)
    requested = [str(item).strip() for item in variables if str(item).strip()]
    if not requested:
        raise ValueError("At least one variable is required")

    rows: list[dict[str, Any]] = []
    summary: dict[str, dict[str, Any]] = {}
    for variable in requested:
        summary[variable] = {
            "max_abs_fpr_vs_fppy": 0.0,
            "max_abs_fpr_vs_fpexe": 0.0,
            "max_abs_fppy_vs_fpexe": 0.0,
            "first_period_present": "",
        }
        for period in periods:
            values: dict[str, float | None] = {}
            for engine in ("fpexe", "fppy", "fp-r"):
                value = aligned[engine].get(period, {}).get(variable)
                values[engine] = float(value) if isinstance(value, (int, float)) else None
            if all(value is None for value in values.values()):
                continue
            if not summary[variable]["first_period_present"]:
                summary[variable]["first_period_present"] = period

            fpexe = values["fpexe"]
            fppy = values["fppy"]
            fpr = values["fp-r"]
            abs_fpr_vs_fppy = abs(float(fpr) - float(fppy)) if fpr is not None and fppy is not None else math.nan
            abs_fpr_vs_fpexe = abs(float(fpr) - float(fpexe)) if fpr is not None and fpexe is not None else math.nan
            abs_fppy_vs_fpexe = abs(float(fppy) - float(fpexe)) if fppy is not None and fpexe is not None else math.nan
            if math.isfinite(abs_fpr_vs_fppy):
                summary[variable]["max_abs_fpr_vs_fppy"] = max(summary[variable]["max_abs_fpr_vs_fppy"], float(abs_fpr_vs_fppy))
            if math.isfinite(abs_fpr_vs_fpexe):
                summary[variable]["max_abs_fpr_vs_fpexe"] = max(summary[variable]["max_abs_fpr_vs_fpexe"], float(abs_fpr_vs_fpexe))
            if math.isfinite(abs_fppy_vs_fpexe):
                summary[variable]["max_abs_fppy_vs_fpexe"] = max(summary[variable]["max_abs_fppy_vs_fpexe"], float(abs_fppy_vs_fpexe))

            rows.append(
                {
                    "period": period,
                    "variable": variable,
                    "fpexe": fpexe,
                    "fppy": fppy,
                    "fpr": fpr,
                    "fpr_minus_fppy": (float(fpr) - float(fppy)) if fpr is not None and fppy is not None else math.nan,
                    "fpr_minus_fpexe": (float(fpr) - float(fpexe)) if fpr is not None and fpexe is not None else math.nan,
                    "fppy_minus_fpexe": (float(fppy) - float(fpexe)) if fppy is not None and fpexe is not None else math.nan,
                    "abs_fpr_vs_fppy": abs_fpr_vs_fppy,
                    "abs_fpr_vs_fpexe": abs_fpr_vs_fpexe,
                    "abs_fppy_vs_fpexe": abs_fppy_vs_fpexe,
                    "rel_pct_fpr_vs_fppy": _safe_rel_pct(float(fpr), float(fppy), rel_scale_floor)
                    if fpr is not None and fppy is not None
                    else math.nan,
                    "rel_pct_fpr_vs_fpexe": _safe_rel_pct(float(fpr), float(fpexe), rel_scale_floor)
                    if fpr is not None and fpexe is not None
                    else math.nan,
                    "rel_pct_fppy_vs_fpexe": _safe_rel_pct(float(fppy), float(fpexe), rel_scale_floor)
                    if fppy is not None and fpexe is not None
                    else math.nan,
                }
            )

    return {
        "engine_paths": {engine: str(path) for engine, path in normalized_paths.items()},
        "variables": requested,
        "start": start,
        "end": end,
        "common_period_count": len(periods),
        "row_count": len(rows),
        "summary": summary,
        "rows": rows,
    }


def write_focused_series_compare_report(
    report: dict[str, Any],
    *,
    output_dir: Path | str,
) -> tuple[Path, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "focused_series_compare_report.json"
    csv_path = out_dir / "focused_series_compare_rows.csv"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    fieldnames = [
        "period",
        "variable",
        "fpexe",
        "fppy",
        "fpr",
        "fpr_minus_fppy",
        "fpr_minus_fpexe",
        "fppy_minus_fpexe",
        "abs_fpr_vs_fppy",
        "abs_fpr_vs_fpexe",
        "abs_fppy_vs_fpexe",
        "rel_pct_fpr_vs_fppy",
        "rel_pct_fpr_vs_fpexe",
        "rel_pct_fppy_vs_fpexe",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in report.get("rows", []):
            writer.writerow({name: row.get(name, "") for name in fieldnames})

    return json_path, csv_path
