from __future__ import annotations

import json
from pathlib import Path

from fp_wraptr.analysis.backend_release_shape import (
    build_backend_release_shape_report,
    write_backend_release_shape_report,
)


def _write_anchor_report(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_backend_release_shape_marks_preview_not_peer_for_explained_shared_tail(tmp_path: Path) -> None:
    anchor_report = tmp_path / "anchor_acceptance_report.json"
    _write_anchor_report(
        anchor_report,
        {
            "status": "review",
            "counts": {
                "anchor_review_count": 2,
                "methodology_review_count": 1,
            },
            "anchor_rows": [
                {
                    "variable": "RS",
                    "classification": "review",
                    "review_scope": "fp_r_leading",
                    "explanation_scope": "fp_r_tail_on_shared_split",
                },
                {
                    "variable": "UR",
                    "classification": "review",
                    "review_scope": "broad_split",
                    "explanation_scope": "broad_split",
                },
            ],
            "methodology_rows": [
                {
                    "variable": "PCPD",
                    "classification": "review",
                    "review_scope": "fp_r_leading",
                    "explanation_scope": "fp_r_tail_on_shared_split",
                }
            ],
        },
    )

    report = build_backend_release_shape_report(
        anchor_report,
        stock_baseline_ok=True,
        raw_input_public_ok=True,
        modified_decks_run=True,
        docs_honest=True,
        corpus_green=False,
    )

    assert report["decision"]["preview_ready"] is True
    assert report["decision"]["peer_backend_ready"] is False
    assert report["decision"]["recommended_label"] == "preview_ready"
    assert report["counts"]["unexplained_anchor_fp_r_review_count"] == 0
    assert report["counts"]["explained_anchor_shared_tail_count"] == 1
    assert "release_corpus_not_green" in report["decision"]["blockers"]


def test_backend_release_shape_marks_not_preview_ready_when_core_flags_fail(tmp_path: Path) -> None:
    anchor_report = tmp_path / "anchor_acceptance_report.json"
    _write_anchor_report(anchor_report, {"status": "review", "counts": {}, "anchor_rows": [], "methodology_rows": []})

    report = build_backend_release_shape_report(
        anchor_report,
        stock_baseline_ok=False,
        raw_input_public_ok=False,
        modified_decks_run=False,
        docs_honest=False,
        corpus_green=False,
    )

    assert report["decision"]["preview_ready"] is False
    assert report["decision"]["recommended_label"] == "not_preview_ready"
    assert "stock_baseline_not_green" in report["decision"]["blockers"]
    assert "raw_input_public_path_not_real" in report["decision"]["blockers"]


def test_backend_release_shape_writes_json(tmp_path: Path) -> None:
    report = {
        "decision": {"recommended_label": "preview_ready"},
        "counts": {},
    }
    path = write_backend_release_shape_report(report, output_dir=tmp_path / "out")
    assert path.exists()
    assert "preview_ready" in path.read_text(encoding="utf-8")
