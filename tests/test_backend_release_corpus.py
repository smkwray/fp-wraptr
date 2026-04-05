from __future__ import annotations

import json
from pathlib import Path

from fp_wraptr.analysis.backend_release_corpus import (
    build_backend_release_corpus_report,
    write_backend_release_corpus_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_backend_release_corpus_summarizes_ready_and_missing_entries(tmp_path: Path) -> None:
    manifest = tmp_path / "docs" / "backend-release-corpus.json"
    scenario_a = tmp_path / "examples" / "a.yaml"
    scenario_b = tmp_path / "examples" / "b.yaml"
    scenario_a.parent.mkdir(parents=True, exist_ok=True)
    scenario_a.write_text("name: a\n", encoding="utf-8")
    scenario_b.write_text("name: b\n", encoding="utf-8")

    release_shape = tmp_path / "artifacts" / "a" / "backend_release_shape_report.json"
    _write_json(
        release_shape,
        {
            "decision": {
                "recommended_label": "preview_ready",
                "preview_ready": True,
                "peer_backend_ready": False,
                "blockers": ["release_corpus_not_green"],
            }
        },
    )
    _write_json(
        manifest,
        {
            "name": "demo_corpus",
            "entries": [
                {
                    "name": "a",
                    "required": True,
                    "scenario": "examples/a.yaml",
                    "release_shape_report": "artifacts/a/backend_release_shape_report.json",
                },
                {
                    "name": "b",
                    "required": True,
                    "scenario": "examples/b.yaml",
                },
            ],
        },
    )

    report = build_backend_release_corpus_report(manifest)

    assert report["entry_count"] == 2
    assert report["required_entry_count"] == 2
    assert report["required_preview_ready_count"] == 1
    assert report["corpus_green_for_preview"] is False
    row_a = next(row for row in report["rows"] if row["name"] == "a")
    row_b = next(row for row in report["rows"] if row["name"] == "b")
    assert row_a["status"] == "preview_ready"
    assert row_b["status"] == "missing_release_shape"


def test_backend_release_corpus_writes_json_and_csv(tmp_path: Path) -> None:
    report = {
        "entry_count": 1,
        "required_entry_count": 1,
        "required_preview_ready_count": 0,
        "required_peer_ready_count": 0,
        "corpus_green_for_preview": False,
        "corpus_green_for_peer": False,
        "rows": [
            {
                "name": "demo",
                "category": "demo",
                "required": True,
                "status": "missing_release_shape",
                "recommended_label": "",
                "preview_ready": False,
                "peer_backend_ready": False,
                "scenario_exists": True,
                "anchor_report_exists": False,
                "release_shape_report_exists": False,
                "blockers": ["missing_release_shape_report"],
                "scenario_path": "/tmp/demo.yaml",
                "anchor_report_path": "",
                "release_shape_report_path": "",
                "notes": "",
            }
        ],
    }
    json_path, csv_path = write_backend_release_corpus_report(report, output_dir=tmp_path / "out")
    assert json_path.exists()
    assert csv_path.exists()
    assert "missing_release_shape" in csv_path.read_text(encoding="utf-8")
