"""Compare OUTSIDE seeding diagnostics across one or more parity run dirs."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare seeded/inspected/candidate counters across parity runs.",
    )
    parser.add_argument(
        "run_dirs",
        nargs="+",
        help="One or more parity run dirs (or work_fppy dirs)",
    )
    parser.add_argument(
        "--out-csv",
        default=None,
        help="Optional output CSV path (default: no CSV)",
    )
    return parser.parse_args(argv)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must be a JSON object")
    return payload


def _resolve_report_path(run_dir: Path) -> Path | None:
    direct = run_dir / "fppy_report.json"
    nested = run_dir / "work_fppy" / "fppy_report.json"
    if direct.exists():
        return direct
    if nested.exists():
        return nested
    return None


def _resolve_parity_report_path(run_dir: Path) -> Path | None:
    path = run_dir / "parity_report.json"
    if path.exists():
        return path
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


def _classify_seed_state(*, seeded: int | None, candidate: int | None) -> str:
    if candidate is None:
        return "unknown"
    if candidate <= 0:
        return "no_candidates"
    if (seeded or 0) > 0:
        return "seeded"
    return "candidate_unseeded"


def compare_seed_diagnostics(run_dirs: list[Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for run_dir in run_dirs:
        resolved = Path(run_dir)
        report_path = _resolve_report_path(resolved)
        parity_report_path = _resolve_parity_report_path(resolved)
        if report_path is None:
            rows.append({
                "run_dir": str(resolved),
                "report_path": "",
                "parity_report_path": "",
                "solve_window_start": "",
                "solve_window_end": "",
                "solve_seeded": "",
                "solve_inspected": "",
                "solve_candidate": "",
                "post_seeded": "",
                "post_inspected": "",
                "post_candidate": "",
                "eq_flags_preset": "",
                "eq_use_setupsolve": "",
                "eq_backfill_iterations": "",
                "eq_backfill_min_iters": "",
                "eq_backfill_max_iters": "",
                "eq_backfill_stop_reason": "",
                "eq_backfill_converged": "",
                "setupsolve_miniters": "",
                "setupsolve_maxiters": "",
                "setupsolve_maxcheck": "",
                "setupsolve_nomiss": "",
                "setupsolve_tolall": "",
                "setupsolve_tolallabs": "",
                "setupsolve_dampall": "",
                "unsupported_examples_count": "",
                "eq_backfill_keyboard_targets_missing": "",
                "eq_backfill_keyboard_stop_classification": "",
                "seed_state": "missing_report",
            })
            continue

        report = _load_json(report_path)
        summary = report.get("summary")
        if not isinstance(summary, dict):
            summary = {}
        unsupported_examples = summary.get("unsupported_examples")
        if not isinstance(unsupported_examples, list):
            unsupported_examples = report.get("unsupported_examples")
        unsupported_examples_count = (
            len(unsupported_examples) if isinstance(unsupported_examples, list) else None
        )
        window = summary.get("solve_active_window")
        window_start = ""
        window_end = ""
        if isinstance(window, dict):
            start = window.get("start")
            end = window.get("end")
            window_start = str(start) if start is not None else ""
            window_end = str(end) if end is not None else ""

        solve_seeded = _coerce_int(summary.get("solve_outside_seeded_cells"))
        solve_inspected = _coerce_int(summary.get("solve_outside_seed_inspected_cells"))
        solve_candidate = _coerce_int(summary.get("solve_outside_seed_candidate_cells"))
        post_seeded = _coerce_int(summary.get("eq_backfill_outside_post_seed_cells"))
        post_inspected = _coerce_int(summary.get("eq_backfill_outside_post_seed_inspected_cells"))
        post_candidate = _coerce_int(summary.get("eq_backfill_outside_post_seed_candidate_cells"))
        eq_iters = _coerce_int(summary.get("eq_backfill_iterations"))
        eq_min_iters = _coerce_int(summary.get("eq_backfill_min_iters"))
        eq_max_iters = _coerce_int(summary.get("eq_backfill_max_iters"))
        eq_stop_reason = summary.get("eq_backfill_stop_reason")
        eq_converged = summary.get("eq_backfill_converged")
        keyboard_targets_missing = summary.get("eq_backfill_keyboard_targets_missing")
        keyboard_stop_classification = summary.get("eq_backfill_keyboard_stop_classification")
        eq_use_setupsolve = summary.get("eq_use_setupsolve")
        setupsolve = summary.get("setupsolve")
        if not isinstance(setupsolve, dict):
            setupsolve = {}

        eq_flags_preset = summary.get("eq_flags_preset")
        if not isinstance(eq_flags_preset, str) or not eq_flags_preset.strip():
            eq_flags_preset = ""
            if parity_report_path is not None:
                parity_report = _load_json(parity_report_path)
                engine_runs = parity_report.get("engine_runs")
                if isinstance(engine_runs, dict):
                    fppy = engine_runs.get("fppy")
                    if isinstance(fppy, dict):
                        details = fppy.get("details")
                        if isinstance(details, dict):
                            raw = details.get("eq_flags_preset")
                            if isinstance(raw, str) and raw.strip():
                                eq_flags_preset = raw.strip()

        rows.append({
            "run_dir": str(resolved),
            "report_path": str(report_path),
            "parity_report_path": str(parity_report_path) if parity_report_path else "",
            "solve_window_start": window_start,
            "solve_window_end": window_end,
            "solve_seeded": "" if solve_seeded is None else str(solve_seeded),
            "solve_inspected": "" if solve_inspected is None else str(solve_inspected),
            "solve_candidate": "" if solve_candidate is None else str(solve_candidate),
            "post_seeded": "" if post_seeded is None else str(post_seeded),
            "post_inspected": "" if post_inspected is None else str(post_inspected),
            "post_candidate": "" if post_candidate is None else str(post_candidate),
            "eq_flags_preset": eq_flags_preset,
            "eq_use_setupsolve": (
                "" if eq_use_setupsolve is None else str(bool(eq_use_setupsolve)).lower()
            ),
            "eq_backfill_iterations": "" if eq_iters is None else str(eq_iters),
            "eq_backfill_min_iters": "" if eq_min_iters is None else str(eq_min_iters),
            "eq_backfill_max_iters": "" if eq_max_iters is None else str(eq_max_iters),
            "eq_backfill_stop_reason": (
                str(eq_stop_reason).strip() if eq_stop_reason is not None else ""
            ),
            "eq_backfill_converged": (
                "" if eq_converged is None else str(bool(eq_converged)).lower()
            ),
            "setupsolve_miniters": (
                ""
                if _coerce_int(setupsolve.get("miniters")) is None
                else str(_coerce_int(setupsolve.get("miniters")))
            ),
            "setupsolve_maxiters": (
                ""
                if _coerce_int(setupsolve.get("maxiters")) is None
                else str(_coerce_int(setupsolve.get("maxiters")))
            ),
            "setupsolve_maxcheck": (
                ""
                if _coerce_int(setupsolve.get("maxcheck")) is None
                else str(_coerce_int(setupsolve.get("maxcheck")))
            ),
            "setupsolve_nomiss": (
                ""
                if setupsolve.get("nomiss") is None
                else str(bool(setupsolve.get("nomiss"))).lower()
            ),
            "setupsolve_tolall": (
                ""
                if _coerce_float(setupsolve.get("tolall")) is None
                else str(_coerce_float(setupsolve.get("tolall")))
            ),
            "setupsolve_tolallabs": (
                ""
                if setupsolve.get("tolallabs") is None
                else str(bool(setupsolve.get("tolallabs"))).lower()
            ),
            "setupsolve_dampall": (
                ""
                if _coerce_float(setupsolve.get("dampall")) is None
                else str(_coerce_float(setupsolve.get("dampall")))
            ),
            "unsupported_examples_count": (
                "" if unsupported_examples_count is None else str(int(unsupported_examples_count))
            ),
            "eq_backfill_keyboard_targets_missing": (
                ""
                if keyboard_targets_missing is None
                else str(bool(keyboard_targets_missing)).lower()
            ),
            "eq_backfill_keyboard_stop_classification": (
                str(keyboard_stop_classification).strip()
                if keyboard_stop_classification is not None
                else ""
            ),
            "seed_state": _classify_seed_state(seeded=solve_seeded, candidate=solve_candidate),
        })
    return rows


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "run_dir",
        "report_path",
        "parity_report_path",
        "solve_window_start",
        "solve_window_end",
        "solve_seeded",
        "solve_inspected",
        "solve_candidate",
        "post_seeded",
        "post_inspected",
        "post_candidate",
        "eq_flags_preset",
        "eq_use_setupsolve",
        "eq_backfill_iterations",
        "eq_backfill_min_iters",
        "eq_backfill_max_iters",
        "eq_backfill_stop_reason",
        "eq_backfill_converged",
        "setupsolve_miniters",
        "setupsolve_maxiters",
        "setupsolve_maxcheck",
        "setupsolve_nomiss",
        "setupsolve_tolall",
        "setupsolve_tolallabs",
        "setupsolve_dampall",
        "unsupported_examples_count",
        "eq_backfill_keyboard_targets_missing",
        "eq_backfill_keyboard_stop_classification",
        "seed_state",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_dirs = [Path(item) for item in args.run_dirs]
    rows = compare_seed_diagnostics(run_dirs)

    for row in rows:
        solve_triplet = (
            f"{row['solve_seeded'] or '?'}"
            f"/{row['solve_inspected'] or '?'}"
            f"/{row['solve_candidate'] or '?'}"
        )
        post_triplet = (
            f"{row['post_seeded'] or '?'}"
            f"/{row['post_inspected'] or '?'}"
            f"/{row['post_candidate'] or '?'}"
        )
        window_label = (
            f"{row['solve_window_start']}..{row['solve_window_end']}"
            if row["solve_window_start"] and row["solve_window_end"]
            else "<unknown>"
        )
        print(
            f"- run={row['run_dir']} window={window_label} "
            f"solve={solve_triplet} post={post_triplet} "
            f"preset={row['eq_flags_preset'] or '?'} "
            f"eq_iters={row['eq_backfill_iterations'] or '?'} "
            f"eq_minmax={row['eq_backfill_min_iters'] or '?'}/"
            f"{row['eq_backfill_max_iters'] or '?'} "
            f"setupsolve={row['eq_use_setupsolve'] or '?'} "
            f"stop={row['eq_backfill_stop_reason'] or '?'} "
            f"converged={row['eq_backfill_converged'] or '?'} "
            f"setupsolve_cfg="
            f"{row['setupsolve_miniters'] or '?'}/"
            f"{row['setupsolve_maxiters'] or '?'}/"
            f"{row['setupsolve_maxcheck'] or '?'} "
            f"unsupported_examples={row['unsupported_examples_count'] or '?'} "
            f"kbd_missing={row['eq_backfill_keyboard_targets_missing'] or '?'} "
            f"kbd_stop={row['eq_backfill_keyboard_stop_classification'] or '?'} "
            f"state={row['seed_state']}"
        )

    if args.out_csv:
        out_path = Path(args.out_csv)
        _write_csv(out_path, rows)
        print(f"csv: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
