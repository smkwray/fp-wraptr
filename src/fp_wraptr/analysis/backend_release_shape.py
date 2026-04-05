"""Release-shape classification for backend acceptance artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _load_anchor_acceptance_report(path: Path | str) -> dict[str, Any]:
    report_path = Path(path).expanduser().resolve()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {report_path}")
    payload["_report_path"] = str(report_path)
    return payload


def _count_rows(rows: list[dict[str, Any]], *, review_scope: str | None = None, explanation_scope: str | None = None) -> int:
    count = 0
    for row in rows:
        if str(row.get("classification", "")) != "review":
            continue
        if review_scope is not None and str(row.get("review_scope", "")) != review_scope:
            continue
        if explanation_scope is not None and str(row.get("explanation_scope", "")) != explanation_scope:
            continue
        count += 1
    return count


def build_backend_release_shape_report(
    anchor_acceptance_report: dict[str, Any] | Path | str,
    *,
    stock_baseline_ok: bool,
    raw_input_public_ok: bool,
    modified_decks_run: bool,
    docs_honest: bool = True,
    corpus_green: bool = False,
) -> dict[str, Any]:
    report = (
        _load_anchor_acceptance_report(anchor_acceptance_report)
        if isinstance(anchor_acceptance_report, (str, Path))
        else dict(anchor_acceptance_report)
    )
    anchor_rows = list(report.get("anchor_rows", []) or [])
    methodology_rows = list(report.get("methodology_rows", []) or [])

    unexplained_anchor_fp_r = _count_rows(anchor_rows, review_scope="fp_r_leading", explanation_scope="fp_r_leading")
    explained_anchor_shared_tails = _count_rows(
        anchor_rows,
        review_scope="fp_r_leading",
        explanation_scope="fp_r_tail_on_shared_split",
    )
    broad_anchor_splits = _count_rows(anchor_rows, review_scope="broad_split")
    unexplained_methodology_fp_r = _count_rows(
        methodology_rows,
        review_scope="fp_r_leading",
        explanation_scope="fp_r_leading",
    )
    explained_methodology_shared_tails = _count_rows(
        methodology_rows,
        review_scope="fp_r_leading",
        explanation_scope="fp_r_tail_on_shared_split",
    )

    preview_ready = bool(stock_baseline_ok and raw_input_public_ok and modified_decks_run and docs_honest)
    peer_backend_ready = bool(preview_ready and corpus_green and unexplained_anchor_fp_r == 0)

    blockers: list[str] = []
    if not stock_baseline_ok:
        blockers.append("stock_baseline_not_green")
    if not raw_input_public_ok:
        blockers.append("raw_input_public_path_not_real")
    if not modified_decks_run:
        blockers.append("modified_decks_do_not_run")
    if not docs_honest:
        blockers.append("docs_not_honest")
    if unexplained_anchor_fp_r > 0:
        blockers.append("unexplained_fp_r_anchor_reviews")
    if not corpus_green:
        blockers.append("release_corpus_not_green")

    return {
        "anchor_acceptance_report_path": report.get("_report_path", ""),
        "anchor_acceptance_status": report.get("status", ""),
        "input_flags": {
            "stock_baseline_ok": bool(stock_baseline_ok),
            "raw_input_public_ok": bool(raw_input_public_ok),
            "modified_decks_run": bool(modified_decks_run),
            "docs_honest": bool(docs_honest),
            "corpus_green": bool(corpus_green),
        },
        "counts": {
            "anchor_review_count": int((report.get("counts", {}) or {}).get("anchor_review_count", 0) or 0),
            "methodology_review_count": int((report.get("counts", {}) or {}).get("methodology_review_count", 0) or 0),
            "unexplained_anchor_fp_r_review_count": unexplained_anchor_fp_r,
            "explained_anchor_shared_tail_count": explained_anchor_shared_tails,
            "broad_anchor_split_count": broad_anchor_splits,
            "unexplained_methodology_fp_r_review_count": unexplained_methodology_fp_r,
            "explained_methodology_shared_tail_count": explained_methodology_shared_tails,
        },
        "decision": {
            "preview_ready": preview_ready,
            "peer_backend_ready": peer_backend_ready,
            "recommended_label": (
                "peer_backend_ready"
                if peer_backend_ready
                else "preview_ready"
                if preview_ready
                else "not_preview_ready"
            ),
            "blockers": blockers,
        },
    }


def write_backend_release_shape_report(
    report: dict[str, Any],
    *,
    output_dir: Path | str,
) -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "backend_release_shape_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report_path
