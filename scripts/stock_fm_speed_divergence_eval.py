"""Evaluate stock-FM runtime and divergence KPIs against an fp.exe reference.

This script is artifact-level only: it reads existing output/report files and emits
one row of KPIs for a single run.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np

from fppy.pabev_parity import PabevPeriod, parse_pabev, period_index, period_stop_index


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute stock FM speed-vs-divergence KPIs for one fppy run versus "
            "an fp.exe reference output."
        )
    )
    parser.add_argument("--label", required=True, help="Run label for output row.")
    parser.add_argument(
        "--run-dir",
        default=None,
        help=(
            "Optional run directory to auto-resolve outputs/reports "
            "(parity run dir, work_fppy dir, or work dir)."
        ),
    )
    parser.add_argument(
        "--fpexe-output",
        default=None,
        help="Explicit fp.exe output path (PACEV.TXT/PABEV.TXT/LOADFORMAT.DAT).",
    )
    parser.add_argument(
        "--fppy-output",
        default=None,
        help="Explicit fp-py output path (PACEV.TXT/PABEV.TXT/LOADFORMAT.DAT).",
    )
    parser.add_argument(
        "--fppy-report",
        default=None,
        help="Explicit fppy_report.json path.",
    )
    parser.add_argument(
        "--window-start",
        default="2025.4",
        help="Comparison window start period (inclusive, YYYY.Q).",
    )
    parser.add_argument(
        "--window-end",
        default=None,
        help="Optional comparison window end period (inclusive, YYYY.Q).",
    )
    parser.add_argument(
        "--eps",
        type=float,
        default=1e-6,
        help="Denominator floor in %%diff: abs(fppy-fpexe)/max(abs(fpexe), eps)*100",
    )
    parser.add_argument("--top", type=int, default=10, help="Top offender count.")
    parser.add_argument(
        "--baseline-output",
        default=None,
        help="Optional baseline output path for byte-identity gating.",
    )
    parser.add_argument(
        "--fail-on-byte-mismatch",
        action="store_true",
        help="Exit non-zero if --baseline-output is provided and output hash differs.",
    )
    parser.add_argument(
        "--out-json",
        default=None,
        help="Optional JSON output path for the full row payload.",
    )
    parser.add_argument(
        "--out-csv",
        default=None,
        help="Optional CSV output path (appends one row, writes header if missing).",
    )
    return parser.parse_args(argv)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return None
    return None


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _resolve_output_path(root: Path) -> Path | None:
    for relative in (
        "PACEV.TXT",
        "PABEV.TXT",
        "LOADFORMAT.DAT",
    ):
        candidate = root / relative
        if candidate.exists():
            return candidate
    return None


def _resolve_fppy_report_path(root: Path) -> Path | None:
    for relative in (
        "fppy_report.json",
        "work_fppy/fppy_report.json",
        "work/fppy_report.json",
    ):
        candidate = root / relative
        if candidate.exists():
            return candidate
    return None


def _resolve_paths(
    *,
    run_dir: Path | None,
    fpexe_output: Path | None,
    fppy_output: Path | None,
    fppy_report: Path | None,
) -> tuple[Path, Path, Path | None]:
    if run_dir is not None:
        run_dir = run_dir.resolve()

    resolved_fpexe: Path | None = fpexe_output.resolve() if fpexe_output else None
    resolved_fppy: Path | None = fppy_output.resolve() if fppy_output else None
    resolved_report: Path | None = fppy_report.resolve() if fppy_report else None

    if run_dir is not None:
        if resolved_fpexe is None:
            for relative in ("work_fpexe",):
                candidate = _resolve_output_path(run_dir / relative)
                if candidate is not None:
                    resolved_fpexe = candidate
                    break
        if resolved_fppy is None:
            for relative in ("work_fppy", "work", "."):
                candidate = _resolve_output_path(run_dir / relative)
                if candidate is not None:
                    resolved_fppy = candidate
                    break
        if resolved_report is None:
            resolved_report = _resolve_fppy_report_path(run_dir)

    if resolved_fpexe is None:
        raise FileNotFoundError("Could not resolve fp.exe output path")
    if resolved_fppy is None:
        raise FileNotFoundError("Could not resolve fp-py output path")
    return resolved_fpexe, resolved_fppy, resolved_report


def _summary_fields(report_path: Path | None) -> dict[str, Any]:
    if report_path is None or not report_path.exists():
        return {}
    payload = _load_json(report_path)
    summary = payload.get("summary")
    return summary if isinstance(summary, dict) else {}


def _extract_runtime_seconds(
    *,
    report_summary: dict[str, Any],
    run_dir: Path | None,
) -> float | None:
    runtime = _coerce_float(report_summary.get("mini_run_runtime_total_seconds"))
    if runtime is not None:
        return runtime
    if run_dir is None:
        return None
    for relative in ("work_fppy/fppy.runtime.json", "work/fppy.runtime.json", "fppy.runtime.json"):
        runtime_path = run_dir / relative
        if not runtime_path.exists():
            continue
        payload = _load_json(runtime_path)
        value = _coerce_float(payload.get("elapsed_seconds"))
        if value is not None:
            return value
    return None


def _evaluate_percent_diff(
    *,
    fpexe_path: Path,
    fppy_path: Path,
    start: str,
    end: str | None,
    eps: float,
    top: int,
) -> dict[str, Any]:
    periods_ref, series_ref = parse_pabev(fpexe_path)
    periods_run, series_run = parse_pabev(fppy_path)
    if periods_ref != periods_run:
        raise ValueError("period ranges differ between fp.exe and fp-py outputs")

    start_period = PabevPeriod.parse(start)
    end_period = PabevPeriod.parse(end) if end else None
    start_idx = period_index(periods_ref, start_period)
    end_idx = period_stop_index(periods_ref, end_period)
    if end_idx < start_idx:
        end_idx = start_idx

    shared_vars = sorted(set(series_ref) & set(series_run))
    period_labels = [str(period) for period in periods_ref]
    all_pct: list[float] = []
    offenders: list[dict[str, Any]] = []

    for variable in shared_vars:
        ref_values = np.asarray(series_ref[variable][start_idx:end_idx], dtype=np.float64)
        run_values = np.asarray(series_run[variable][start_idx:end_idx], dtype=np.float64)
        if ref_values.size == 0:
            continue
        denom = np.maximum(np.abs(ref_values), float(eps))
        pct = np.abs(run_values - ref_values) / denom * 100.0
        finite_mask = np.isfinite(pct)
        if not np.any(finite_mask):
            continue
        pct_finite = pct[finite_mask]
        all_pct.extend(float(value) for value in pct_finite.tolist())
        local_max = float(np.max(pct_finite))
        local_p95 = float(np.percentile(pct_finite, 95))
        local_argmax = int(np.argmax(pct))
        period_pos = start_idx + local_argmax
        offenders.append({
            "variable": variable,
            "max_pct_diff": local_max,
            "p95_pct_diff": local_p95,
            "worst_period": period_labels[period_pos] if period_pos < len(period_labels) else "",
            "fpexe_value": float(ref_values[local_argmax]),
            "fppy_value": float(run_values[local_argmax]),
        })

    offenders.sort(
        key=lambda row: (-float(row["max_pct_diff"]), -float(row["p95_pct_diff"]), row["variable"])
    )
    comparable_cells = len(all_pct)
    max_pct = float(max(all_pct)) if all_pct else 0.0
    p95_pct = float(np.percentile(np.asarray(all_pct, dtype=np.float64), 95)) if all_pct else 0.0
    return {
        "window_start": str(start),
        "window_end": str(end) if end else None,
        "eps": float(eps),
        "shared_variable_count": len(shared_vars),
        "comparable_cells": comparable_cells,
        "max_pct_diff": max_pct,
        "p95_pct_diff": p95_pct,
        "top_offenders": offenders[: max(0, int(top))],
    }


def evaluate_stock_run(
    *,
    label: str,
    run_dir: Path | None,
    fpexe_output: Path | None,
    fppy_output: Path | None,
    fppy_report: Path | None,
    window_start: str,
    window_end: str | None,
    eps: float,
    top: int,
    baseline_output: Path | None = None,
) -> dict[str, Any]:
    left_path, right_path, report_path = _resolve_paths(
        run_dir=run_dir,
        fpexe_output=fpexe_output,
        fppy_output=fppy_output,
        fppy_report=fppy_report,
    )
    summary = _summary_fields(report_path)
    run_root = run_dir.resolve() if run_dir is not None else None
    runtime_seconds = _extract_runtime_seconds(report_summary=summary, run_dir=run_root)
    divergence = _evaluate_percent_diff(
        fpexe_path=left_path,
        fppy_path=right_path,
        start=window_start,
        end=window_end,
        eps=eps,
        top=top,
    )

    row: dict[str, Any] = {
        "label": str(label),
        "run_dir": str(run_root) if run_root is not None else "",
        "fpexe_output_path": str(left_path),
        "fppy_output_path": str(right_path),
        "fppy_report_path": str(report_path) if report_path is not None else "",
        "runtime_seconds": runtime_seconds,
        "window_start": divergence["window_start"],
        "window_end": divergence["window_end"],
        "eps": divergence["eps"],
        "shared_variable_count": divergence["shared_variable_count"],
        "comparable_cells": divergence["comparable_cells"],
        "max_pct_diff": divergence["max_pct_diff"],
        "p95_pct_diff": divergence["p95_pct_diff"],
        "stop_reason": summary.get("eq_backfill_stop_reason"),
        "convergence_classification": summary.get("eq_backfill_keyboard_stop_classification"),
        "eq_flags_preset": summary.get("eq_flags_preset"),
        "eq_structural_read_cache": summary.get("eq_structural_read_cache"),
        "eq_backfill_runtime_iteration_seconds": _coerce_float(
            summary.get("eq_backfill_runtime_iteration_seconds")
        ),
        "eq_backfill_runtime_iteration_main_apply_seconds": _coerce_float(
            summary.get("eq_backfill_runtime_iteration_main_apply_seconds")
        ),
        "eq_backfill_runtime_iteration_residual_checked_seconds": _coerce_float(
            summary.get("eq_backfill_runtime_iteration_residual_checked_seconds")
        ),
        "eq_backfill_runtime_iteration_residual_all_targets_seconds": _coerce_float(
            summary.get("eq_backfill_runtime_iteration_residual_all_targets_seconds")
        ),
        "eq_backfill_runtime_iteration_convergence_seconds": _coerce_float(
            summary.get("eq_backfill_runtime_iteration_convergence_seconds")
        ),
        "eq_backfill_runtime_iteration_deep_copy_seconds": _coerce_float(
            summary.get("eq_backfill_runtime_iteration_deep_copy_seconds")
        ),
        "eq_backfill_structural_scalar_reads_cached": _coerce_int(
            summary.get("eq_backfill_structural_scalar_reads_cached")
        ),
        "eq_backfill_structural_scalar_reads_frame": _coerce_int(
            summary.get("eq_backfill_structural_scalar_reads_frame")
        ),
        "top_offenders": divergence["top_offenders"],
        "fpexe_output_sha256": _sha256(left_path),
        "fppy_output_sha256": _sha256(right_path),
    }
    if baseline_output is not None:
        baseline_path = baseline_output.resolve()
        row["baseline_output_path"] = str(baseline_path)
        row["baseline_output_sha256"] = _sha256(baseline_path)
        row["byte_identical_to_baseline"] = bool(
            row["baseline_output_sha256"] == row["fppy_output_sha256"]
        )
    return row


def _write_csv_row(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    top_offenders = row.get("top_offenders", [])
    row_out = dict(row)
    row_out["top_offenders"] = json.dumps(top_offenders, separators=(",", ":"), sort_keys=True)
    fieldnames = list(row_out.keys())
    should_write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if should_write_header:
            writer.writeheader()
        writer.writerow(row_out)


def _print_summary(row: dict[str, Any]) -> None:
    runtime = row.get("runtime_seconds")
    runtime_text = f"{float(runtime):.6g}" if isinstance(runtime, (int, float)) else "n/a"
    print(
        f"{row['label']}: runtime={runtime_text}s "
        f"max_pct_diff={float(row['max_pct_diff']):.6g} "
        f"p95_pct_diff={float(row['p95_pct_diff']):.6g} "
        f"stop_reason={row.get('stop_reason')!s} "
        f"classification={row.get('convergence_classification')!s}"
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_dir = Path(args.run_dir).resolve() if args.run_dir else None
    fpexe_output = Path(args.fpexe_output).resolve() if args.fpexe_output else None
    fppy_output = Path(args.fppy_output).resolve() if args.fppy_output else None
    fppy_report = Path(args.fppy_report).resolve() if args.fppy_report else None
    baseline_output = Path(args.baseline_output).resolve() if args.baseline_output else None

    row = evaluate_stock_run(
        label=str(args.label),
        run_dir=run_dir,
        fpexe_output=fpexe_output,
        fppy_output=fppy_output,
        fppy_report=fppy_report,
        window_start=str(args.window_start),
        window_end=(str(args.window_end) if args.window_end is not None else None),
        eps=float(args.eps),
        top=int(args.top),
        baseline_output=baseline_output,
    )
    _print_summary(row)

    if args.out_json:
        out_json = Path(args.out_json).resolve()
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(row, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"json: {out_json}")
    if args.out_csv:
        out_csv = Path(args.out_csv).resolve()
        _write_csv_row(out_csv, row)
        print(f"csv: {out_csv}")

    if args.fail_on_byte_mismatch and baseline_output is not None:
        if not bool(row.get("byte_identical_to_baseline", False)):
            print("byte identity check failed")
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
