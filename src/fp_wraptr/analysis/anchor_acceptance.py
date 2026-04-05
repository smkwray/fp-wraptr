"""Anchor-based acceptance reports for shared-semantic backend branches.

This report is intentionally narrower than full parity. The goal is to turn a
tracked branch, such as the PSE boundary seam, into a stable acceptance signal:

- shared-semantic anchors should be close or explicitly explained
- methodology bucket variables may still differ, but should be visible
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from fp_wraptr.analysis.backend_defensibility import build_backend_defensibility_report
from fp_wraptr.analysis.focused_series_compare import build_focused_series_compare_report

ANCHOR_ACCEPTANCE_PRESETS: dict[str, dict[str, Any]] = {
    "pse_rs_frontier": {
        "anchors": ["UR", "UR1", "RS", "RB", "RM"],
        "methodology": ["PCPD", "PCPF", "PIEF", "E", "SG", "XX", "SUBG"],
        "start": "2025.4",
    }
}
SHARED_SPLIT_TAIL_RATIO = 0.2


def _clean_names(items: list[str] | None) -> list[str]:
    seen: set[str] = set()
    cleaned: list[str] = []
    for item in items or []:
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return cleaned


def _leader_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        pair = str(row.get("max_abs_pair", "")).strip()
        if not pair:
            continue
        counts[pair] = counts.get(pair, 0) + 1
    return counts


def resolve_anchor_acceptance_preset(name: str | None) -> dict[str, Any] | None:
    key = str(name or "").strip().lower()
    if not key:
        return None
    preset = ANCHOR_ACCEPTANCE_PRESETS.get(key)
    if preset is None:
        known = ", ".join(sorted(ANCHOR_ACCEPTANCE_PRESETS))
        raise ValueError(f"Unknown anchor acceptance preset: {name!r}. Known presets: {known}")
    return {
        "name": key,
        "anchors": list(preset.get("anchors", []) or []),
        "methodology": list(preset.get("methodology", []) or []),
        "start": preset.get("start"),
        "end": preset.get("end"),
    }


def _review_scope(row: dict[str, Any]) -> str:
    if str(row.get("classification")) != "review":
        return str(row.get("classification", ""))
    pair = str(row.get("max_abs_pair", ""))
    if "fp-r" in pair:
        return "fp_r_leading"
    return "broad_split"


def _explanation_scope(row: dict[str, Any], focus_row: dict[str, Any]) -> str:
    review_scope = _review_scope(row)
    if review_scope != "fp_r_leading":
        return review_scope

    try:
        max_abs_fpr_vs_fppy = float(focus_row.get("max_abs_fpr_vs_fppy", 0.0) or 0.0)
        max_abs_fpr_vs_fpexe = float(focus_row.get("max_abs_fpr_vs_fpexe", 0.0) or 0.0)
        max_abs_fppy_vs_fpexe = float(focus_row.get("max_abs_fppy_vs_fpexe", 0.0) or 0.0)
    except (TypeError, ValueError):
        return review_scope

    shared_baseline = min(max_abs_fpr_vs_fpexe, max_abs_fppy_vs_fpexe)
    if shared_baseline > 0 and max_abs_fpr_vs_fppy <= SHARED_SPLIT_TAIL_RATIO * shared_baseline:
        return "fp_r_tail_on_shared_split"
    return review_scope


def build_anchor_acceptance_report(
    engine_paths: dict[str, Path | str],
    *,
    anchor_variables: list[str],
    methodology_variables: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    rel_scale_floor: float = 1.0,
    zero_band: float = 1e-6,
    close_rel_threshold: float = 1e-3,
    close_abs_threshold: float = 1.1e-3,
) -> dict[str, Any]:
    anchors = _clean_names(anchor_variables)
    methodology = _clean_names(methodology_variables)
    if not anchors:
        raise ValueError("At least one anchor variable is required")

    requested = anchors + [name for name in methodology if name not in set(anchors)]
    backend_report = build_backend_defensibility_report(
        engine_paths,
        start=start,
        end=end,
        variables=requested,
        focus_variables=requested,
        rel_scale_floor=rel_scale_floor,
        zero_band=zero_band,
        close_rel_threshold=close_rel_threshold,
        close_abs_threshold=close_abs_threshold,
    )
    focused_report = build_focused_series_compare_report(
        engine_paths,
        variables=requested,
        start=start,
        end=end,
        rel_scale_floor=rel_scale_floor,
    )

    summary_by_var = {
        str(row.get("variable")): row for row in backend_report.get("summary_rows", []) if row.get("variable")
    }
    focused_summary = {
        str(name): payload for name, payload in (focused_report.get("summary", {}) or {}).items()
    }

    anchor_rows = [summary_by_var[name] for name in anchors if name in summary_by_var]
    methodology_rows = [summary_by_var[name] for name in methodology if name in summary_by_var]

    for row in anchor_rows:
        row["review_scope"] = _review_scope(row)
        row["explanation_scope"] = _explanation_scope(row, focused_summary.get(str(row.get("variable", "")), {}))
    for row in methodology_rows:
        row["review_scope"] = _review_scope(row)
        row["explanation_scope"] = _explanation_scope(row, focused_summary.get(str(row.get("variable", "")), {}))

    anchor_review_rows = [row for row in anchor_rows if str(row.get("classification")) == "review"]
    methodology_review_rows = [row for row in methodology_rows if str(row.get("classification")) == "review"]
    anchor_fp_r_review_rows = [row for row in anchor_review_rows if row.get("review_scope") == "fp_r_leading"]
    methodology_fp_r_review_rows = [
        row for row in methodology_review_rows if row.get("review_scope") == "fp_r_leading"
    ]

    status = "ok" if not anchor_review_rows else "review"

    return {
        "engine_paths": backend_report.get("engine_paths", {}),
        "start": start,
        "end": end,
        "rel_scale_floor": float(rel_scale_floor),
        "zero_band": float(zero_band),
        "close_rel_threshold": float(close_rel_threshold),
        "close_abs_threshold": float(close_abs_threshold),
        "status": status,
        "anchor_variables": anchors,
        "methodology_variables": methodology,
        "counts": {
            "anchor_variable_count": len(anchors),
            "methodology_variable_count": len(methodology),
            "anchor_review_count": len(anchor_review_rows),
            "anchor_fp_r_review_count": len(anchor_fp_r_review_rows),
            "anchor_broad_review_count": len(anchor_review_rows) - len(anchor_fp_r_review_rows),
            "methodology_review_count": len(methodology_review_rows),
            "methodology_fp_r_review_count": len(methodology_fp_r_review_rows),
            "methodology_broad_review_count": len(methodology_review_rows) - len(methodology_fp_r_review_rows),
            "anchor_present_count": len(anchor_rows),
            "methodology_present_count": len(methodology_rows),
        },
        "anchor_rows": anchor_rows,
        "methodology_rows": methodology_rows,
        "anchor_focus_summary": {name: focused_summary.get(name, {}) for name in anchors},
        "methodology_focus_summary": {name: focused_summary.get(name, {}) for name in methodology},
        "anchor_leader_counts": _leader_counts(anchor_rows),
        "methodology_leader_counts": _leader_counts(methodology_rows),
    }


def write_anchor_acceptance_report(
    report: dict[str, Any],
    *,
    output_dir: Path | str,
) -> tuple[Path, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "anchor_acceptance_report.json"
    csv_path = out_dir / "anchor_acceptance_summary.csv"

    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    fieldnames = [
        "section",
        "variable",
        "classification",
        "review_scope",
        "explanation_scope",
        "first_diff_period",
        "max_abs_pair",
        "max_abs_period",
        "max_abs_diff",
        "max_rel_pct",
        "scale_at_max",
        "fpexe_at_max",
        "fppy_at_max",
        "fpr_at_max",
        "max_abs_fpr_vs_fppy",
        "max_abs_fpr_vs_fpexe",
        "max_abs_fppy_vs_fpexe",
        "first_period_present",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for section_name, rows_key, focus_key in (
            ("anchor", "anchor_rows", "anchor_focus_summary"),
            ("methodology", "methodology_rows", "methodology_focus_summary"),
        ):
            focus_summary = report.get(focus_key, {}) or {}
            for row in report.get(rows_key, []) or []:
                variable = str(row.get("variable", ""))
                focus_row = focus_summary.get(variable, {}) or {}
                writer.writerow({
                    "section": section_name,
                    "variable": variable,
                    "classification": row.get("classification", ""),
                    "review_scope": row.get("review_scope", ""),
                    "explanation_scope": row.get("explanation_scope", ""),
                    "first_diff_period": row.get("first_diff_period", ""),
                    "max_abs_pair": row.get("max_abs_pair", ""),
                    "max_abs_period": row.get("max_abs_period", ""),
                    "max_abs_diff": row.get("max_abs_diff", ""),
                    "max_rel_pct": row.get("max_rel_pct", ""),
                    "scale_at_max": row.get("scale_at_max", ""),
                    "fpexe_at_max": row.get("fpexe_at_max", ""),
                    "fppy_at_max": row.get("fppy_at_max", ""),
                    "fpr_at_max": row.get("fpr_at_max", ""),
                    "max_abs_fpr_vs_fppy": focus_row.get("max_abs_fpr_vs_fppy", ""),
                    "max_abs_fpr_vs_fpexe": focus_row.get("max_abs_fpr_vs_fpexe", ""),
                    "max_abs_fppy_vs_fpexe": focus_row.get("max_abs_fppy_vs_fpexe", ""),
                    "first_period_present": focus_row.get("first_period_present", ""),
                })

    return json_path, csv_path
