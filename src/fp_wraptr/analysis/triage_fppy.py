"""Triage helpers for fppy_report.json outputs.

This is intentionally deterministic and side-effect free beyond writing the
triage artifacts to disk. It exists so both the CLI and scripts can share the
same implementation.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

BUCKET_ORDER = (
    "pandas_eval_np_module_attr",
    "missing_variable",
    "modeq_parse",
    "convergence",
    "other",
)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must be a JSON object")
    return payload


def resolve_fppy_report_path(run_dir: Path) -> Path:
    """Resolve a `fppy_report.json` path from either work dir or parity run dir."""

    run_dir = Path(run_dir)
    direct = run_dir / "fppy_report.json"
    if direct.exists():
        return direct
    nested = run_dir / "work_fppy" / "fppy_report.json"
    if nested.exists():
        return nested

    # backend=both layout: the parent run dir may contain parity_report.json while the fppy
    # work dir is nested elsewhere. Follow engine metadata when present.
    parity_report = run_dir / "parity_report.json"
    if parity_report.exists():
        try:
            payload = _load_json(parity_report)
        except Exception:  # pragma: no cover - best-effort hint
            payload = {}
        engine_runs = payload.get("engine_runs") if isinstance(payload, dict) else None
        if isinstance(engine_runs, dict):
            fppy_meta = (
                engine_runs.get("fppy") if isinstance(engine_runs.get("fppy"), dict) else {}
            )
            work_dir_value = fppy_meta.get("work_dir")
            if isinstance(work_dir_value, str) and work_dir_value.strip():
                raw = Path(work_dir_value)
                candidates = [
                    raw / "fppy_report.json",
                    Path.cwd() / raw / "fppy_report.json",
                    run_dir / raw / "fppy_report.json",
                ]
                for candidate in candidates:
                    if candidate.exists():
                        return candidate
    raise FileNotFoundError(
        f"Could not find fppy_report.json under {run_dir} "
        "(expected <run-dir>/fppy_report.json or <run-dir>/work_fppy/fppy_report.json)"
    )


def classify_fppy_issue(issue: dict[str, Any]) -> str:
    error = str(issue.get("error", "") or "")
    statement = str(issue.get("statement", "") or "")
    combined = f"{error}\n{statement}".lower()

    if "numpy._arrayfunctiondispatcher" in combined:
        return "pandas_eval_np_module_attr"

    if (
        "not present in input data" in combined
        or "missing variable" in combined
        or "missing column" in combined
        or "nameerror" in combined
        or "keyerror" in combined
        or "not defined" in combined
    ):
        return "missing_variable"

    if (
        "modeq" in statement.lower()
        or "modeq" in error.lower()
        or ("parse" in combined and "eq" in combined)
    ):
        return "modeq_parse"

    if "converg" in combined or "did not solve" in combined or "did not converge" in combined:
        return "convergence"

    return "other"


def triage_fppy_report_payload(
    report: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    issues = report.get("issues", [])
    if not isinstance(issues, list):
        raise ValueError("fppy_report.json field 'issues' must be a list")

    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for idx, issue in enumerate(issues):
        if not isinstance(issue, dict):
            continue
        bucket = classify_fppy_issue(issue)
        counts[bucket] += 1
        rows.append({
            "issue_index": idx,
            "bucket": bucket,
            "line": issue.get("line"),
            "statement": issue.get("statement", ""),
            "error": issue.get("error", ""),
        })

    ordered_counts = {bucket: int(counts.get(bucket, 0)) for bucket in BUCKET_ORDER}
    return rows, ordered_counts


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["issue_index", "bucket", "line", "statement", "error"],
        )
        writer.writeheader()
        writer.writerows(rows)


def triage_fppy_report(
    run_dir: Path,
    *,
    out_dir: Path | None = None,
) -> tuple[Path, Path]:
    """Read `fppy_report.json` and write `triage_summary.json` + `triage_issues.csv`."""

    report_path = resolve_fppy_report_path(Path(run_dir))
    report = _load_json(report_path)
    rows, bucket_counts = triage_fppy_report_payload(report)

    resolved_out_dir = Path(out_dir) if out_dir else report_path.parent
    resolved_out_dir.mkdir(parents=True, exist_ok=True)

    summary_payload = {
        "report_path": str(report_path),
        "issue_count": len(rows),
        "bucket_counts": bucket_counts,
        "top_buckets": [
            {"bucket": bucket, "count": count}
            for bucket, count in sorted(
                bucket_counts.items(),
                key=lambda item: (-int(item[1]), BUCKET_ORDER.index(item[0])),
            )
            if count > 0
        ],
    }
    summary_path = resolved_out_dir / "triage_summary.json"
    csv_path = resolved_out_dir / "triage_issues.csv"

    summary_path.write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_csv(csv_path, rows)
    return summary_path, csv_path
