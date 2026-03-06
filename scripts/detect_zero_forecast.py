"""Detect variables that are zero-filled in fppy over the active solve window."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from fppy.pabev_parity import PabevPeriod, parse_pabev, period_index

_SPOTLIGHT_VARIABLES = ("PCY", "PCPF", "PCWF", "PCPD", "Y1")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Report variables where fp-py values are all near-zero in the solve window "
            "while fp.exe has non-zero values."
        ),
    )
    parser.add_argument("--run-dir", required=True, help="Parity run directory")
    parser.add_argument(
        "--eps",
        type=float,
        default=1e-12,
        help="Absolute near-zero threshold (default: 1e-12)",
    )
    parser.add_argument(
        "--out-csv",
        default=None,
        help="Optional output CSV path (default: <run-dir>/zero_forecast_offenders.csv)",
    )
    return parser.parse_args(argv)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must be a JSON object")
    return payload


def _resolve_paths(run_dir: Path) -> tuple[Path, Path, Path | None]:
    left = run_dir / "work_fpexe" / "PABEV.TXT"
    right = run_dir / "work_fppy" / "PABEV.TXT"
    if not left.exists():
        raise FileNotFoundError(f"Missing fp.exe PABEV: {left}")
    if not right.exists():
        raise FileNotFoundError(f"Missing fp-py PABEV: {right}")
    report = run_dir / "work_fppy" / "fppy_report.json"
    if not report.exists():
        report = None
    return left, right, report


def _solve_window_from_report(report: dict[str, Any]) -> tuple[str | None, str | None]:
    summary = report.get("summary")
    if not isinstance(summary, dict):
        return None, None
    window = summary.get("solve_active_window")
    if not isinstance(window, dict):
        return None, None
    start = window.get("start")
    end = window.get("end")
    start_text = str(start).strip() if isinstance(start, str) else None
    end_text = str(end).strip() if isinstance(end, str) else None
    return start_text, end_text


def detect_zero_forecast(
    run_dir: Path,
    *,
    eps: float = 1e-12,
) -> tuple[list[dict[str, Any]], tuple[str, str]]:
    run_dir = Path(run_dir)
    left_path, right_path, report_path = _resolve_paths(run_dir)

    periods_left, series_left = parse_pabev(left_path)
    periods_right, series_right = parse_pabev(right_path)
    if periods_left != periods_right:
        raise ValueError("PABEV period mismatch between fp.exe and fp-py artifacts")
    periods = periods_left

    start_label: str | None = None
    end_label: str | None = None
    if report_path is not None:
        report = _load_json(report_path)
        start_label, end_label = _solve_window_from_report(report)

    if not start_label:
        start_label = str(periods[0])
    if not end_label:
        end_label = str(periods[-1])

    start_period = PabevPeriod.parse(start_label)
    end_period = PabevPeriod.parse(end_label)
    start_idx = period_index(periods, start_period)
    end_idx_exclusive = period_index(periods, end_period.next())
    if end_idx_exclusive <= start_idx:
        raise ValueError(
            f"Invalid solve window range after index resolution: {start_label}..{end_label}"
        )

    window_periods = periods[start_idx:end_idx_exclusive]
    offenders: list[dict[str, Any]] = []
    shared = sorted(set(series_left) & set(series_right))

    threshold = abs(float(eps))
    for variable in shared:
        left_values = series_left[variable][start_idx:end_idx_exclusive]
        right_values = series_right[variable][start_idx:end_idx_exclusive]
        if not left_values or not right_values:
            continue

        left_nonzero_count = sum(1 for value in left_values if abs(float(value)) > threshold)
        right_zero_count = sum(1 for value in right_values if abs(float(value)) <= threshold)
        right_min = min(float(value) for value in right_values)
        right_max = max(float(value) for value in right_values)
        left_min = min(float(value) for value in left_values)
        left_max = max(float(value) for value in left_values)
        right_span = abs(right_max - right_min)
        left_span = abs(left_max - left_min)

        pattern = ""
        if left_nonzero_count > 0 and right_zero_count == len(right_values):
            pattern = "zero_fill"
        elif right_span <= threshold and left_span > threshold:
            # Detect "carry-forward flatline" patterns where fp-py holds one
            # value across solve periods while fp.exe evolves.
            pattern = "flatline_fill"
        if not pattern:
            continue

        offenders.append({
            "variable": variable,
            "pattern": pattern,
            "window_start": str(window_periods[0]),
            "window_end": str(window_periods[-1]),
            "window_cells": len(right_values),
            "fpexe_nonzero_cells": int(left_nonzero_count),
            "fppy_zero_cells": int(right_zero_count),
            "fpexe_span": float(left_span),
            "fppy_span": float(right_span),
            "max_abs_fpexe": max(abs(float(value)) for value in left_values),
            "max_abs_fppy": max(abs(float(value)) for value in right_values),
        })

    offenders.sort(
        key=lambda row: (-float(row["max_abs_fpexe"]), str(row["variable"])),
    )
    return offenders, (start_label, end_label)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "variable",
                "pattern",
                "window_start",
                "window_end",
                "window_cells",
                "fpexe_nonzero_cells",
                "fppy_zero_cells",
                "fpexe_span",
                "fppy_span",
                "max_abs_fpexe",
                "max_abs_fppy",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    offenders, window = detect_zero_forecast(Path(args.run_dir), eps=float(args.eps))

    out_csv = (
        Path(args.out_csv) if args.out_csv else Path(args.run_dir) / "zero_forecast_offenders.csv"
    )
    _write_csv(out_csv, offenders)

    print(f"solve_window: {window[0]}..{window[1]}")
    print(f"offender_count: {len(offenders)}")
    for row in offenders[:20]:
        print(
            f"- {row['variable']} ({row['pattern']}): fppy_zero_cells={row['fppy_zero_cells']}/"
            f"{row['window_cells']} max_abs_fpexe={row['max_abs_fpexe']:.6g}"
        )
    spotlight_hits = [
        row for row in offenders if str(row.get("variable", "")).upper() in _SPOTLIGHT_VARIABLES
    ]
    if spotlight_hits:
        print("spotlight_hits:")
        for row in spotlight_hits:
            print(
                f"- {row['variable']} ({row['pattern']}): span={row['fpexe_span']:.6g} "
                f"fppy_value_span={row['fppy_span']:.6g}"
            )
    print(f"csv: {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
