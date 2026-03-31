"""Triage helpers for parity hard-fail cells.

This intentionally re-computes the hard-fail list from the PABEV artifacts so it
does not depend on `parity_report.json` only sampling a subset.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

from fppy.pabev_parity import toleranced_compare
from fp_wraptr.analysis.parity_regression import _pabev_paths, _parity_pair_from_report


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must be a JSON object")
    return payload


def _extract_seed_diagnostics(run_dir: Path) -> dict[str, Any]:
    report_path = run_dir / "work_fppy" / "fppy_report.json"
    if not report_path.exists():
        return {}
    payload = _read_json(report_path)
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return {}
    fields = (
        "solve_outside_seeded_cells",
        "solve_outside_seed_inspected_cells",
        "solve_outside_seed_candidate_cells",
        "eq_backfill_outside_post_seed_cells",
        "eq_backfill_outside_post_seed_inspected_cells",
        "eq_backfill_outside_post_seed_candidate_cells",
    )
    extracted = {name: summary.get(name) for name in fields if name in summary}
    return extracted


def triage_parity_hardfails(run_dir: Path) -> tuple[Path, Path]:
    run_dir = Path(run_dir)
    report_path = run_dir / "parity_report.json"
    if not report_path.exists():
        raise FileNotFoundError(f"Missing parity report: {report_path}")

    report = _read_json(report_path)
    detail = report.get("pabev_detail") if isinstance(report.get("pabev_detail"), dict) else {}
    left_engine, right_engine = _parity_pair_from_report(report)
    left_path, right_path = _pabev_paths(run_dir)

    compare_ok, recomputed = toleranced_compare(
        left_path,
        right_path,
        start=str(detail.get("start") or "2025.4"),
        atol=float(detail.get("atol") or 1e-3),
        rtol=float(detail.get("rtol") or 1e-6),
        top=1_000_000,
        hard_fail_top=None,
        missing_sentinels=frozenset(
            float(x) for x in (detail.get("missing_sentinels") or (-99.0,))
        ),
        discrete_eps=float(detail.get("discrete_eps") or 1e-12),
        signflip_eps=float(detail.get("signflip_eps") or 1e-3),
        collect_period_stats=False,
    )
    if not isinstance(recomputed, dict):
        raise ValueError("Unexpected toleranced_compare detail payload (expected dict)")

    hard_fails = recomputed.get("hard_fail_cells") or []
    if not isinstance(hard_fails, list):
        raise ValueError("Unexpected hard_fail_cells payload (expected list)")

    out_csv = run_dir / "triage_hardfails.csv"
    out_json = run_dir / "triage_hardfails_summary.json"

    reason_counts: Counter[str] = Counter()
    var_counts: Counter[str] = Counter()
    seed_diagnostics = _extract_seed_diagnostics(run_dir)

    fieldnames = [
        "variable",
        "period",
        "index",
        "reason",
        "left_value",
        "right_value",
        "abs_diff",
        "left_rounded",
        "right_rounded",
    ]

    dict_rows = [r for r in hard_fails if isinstance(r, dict)]
    with out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in dict_rows:
            var = str(row.get("variable", "")).strip()
            period = str(row.get("period", "")).strip()
            reason = str(row.get("reason", "")).strip()
            left_v = row.get("left_value", row.get("left"))
            right_v = row.get("right_value", row.get("right"))
            try:
                abs_diff = abs(float(left_v) - float(right_v))
            except (TypeError, ValueError):
                abs_diff = ""

            reason_counts[reason or ""] += 1
            var_counts[var or ""] += 1

            writer.writerow({
                "variable": var,
                "period": period,
                "index": row.get("index", ""),
                "reason": reason,
                "left_value": left_v,
                "right_value": right_v,
                "abs_diff": abs_diff,
                "left_rounded": row.get("left_rounded", ""),
                "right_rounded": row.get("right_rounded", ""),
            })

    out_json.write_text(
        json.dumps(
            {
                "run_dir": str(run_dir),
                "left_engine": left_engine,
                "right_engine": right_engine,
                "left_pabev": str(left_path),
                "right_pabev": str(right_path),
                "compare_ok": bool(compare_ok),
                "hard_fail_cell_count": len(dict_rows),
                "counts_by_reason": dict(reason_counts.most_common()),
                "seed_diagnostics": seed_diagnostics,
                "top_variables": [
                    {"variable": name, "count": int(count)}
                    for name, count in var_counts.most_common(50)
                    if name
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return out_csv, out_json
