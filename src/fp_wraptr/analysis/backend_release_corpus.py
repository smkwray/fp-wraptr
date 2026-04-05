"""Corpus-level readiness summary for backend release work."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def _resolve_path(base_dir: Path, raw: str | None) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    return str((base_dir / text).resolve())


def build_backend_release_corpus_report(
    manifest_path: Path | str,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = _load_json(manifest_file)
    entries = list(manifest.get("entries", []) or [])
    if not isinstance(entries, list):
        raise ValueError("Corpus manifest entries must be a list")

    base_dir = manifest_file.parent.parent
    rows: list[dict[str, Any]] = []
    required_total = 0
    required_preview_ready = 0
    required_peer_ready = 0

    for raw_entry in entries:
        if not isinstance(raw_entry, dict):
            raise ValueError("Corpus manifest entries must be objects")
        name = str(raw_entry.get("name", "") or "").strip()
        if not name:
            raise ValueError("Corpus manifest entry missing name")
        required = bool(raw_entry.get("required", True))
        if required:
            required_total += 1

        scenario_path = _resolve_path(base_dir, raw_entry.get("scenario"))
        anchor_report_path = _resolve_path(base_dir, raw_entry.get("anchor_report"))
        release_shape_path = _resolve_path(base_dir, raw_entry.get("release_shape_report"))

        scenario_exists = bool(scenario_path) and Path(scenario_path).exists()
        anchor_report_exists = bool(anchor_report_path) and Path(anchor_report_path).exists()
        release_shape_exists = bool(release_shape_path) and Path(release_shape_path).exists()

        recommended_label = ""
        preview_ready = False
        peer_backend_ready = False
        blockers: list[str] = []
        if release_shape_exists:
            release_shape = _load_json(Path(release_shape_path))
            decision = release_shape.get("decision", {}) or {}
            recommended_label = str(decision.get("recommended_label", "") or "")
            preview_ready = bool(decision.get("preview_ready", False))
            peer_backend_ready = bool(decision.get("peer_backend_ready", False))
            blockers = [str(item) for item in (decision.get("blockers", []) or [])]
        else:
            blockers = ["missing_release_shape_report"]

        if required and preview_ready:
            required_preview_ready += 1
        if required and peer_backend_ready:
            required_peer_ready += 1

        if not scenario_exists:
            status = "missing_scenario"
        elif not release_shape_exists:
            status = "missing_release_shape"
        elif peer_backend_ready:
            status = "peer_backend_ready"
        elif preview_ready:
            status = "preview_ready"
        else:
            status = "not_preview_ready"

        rows.append(
            {
                "name": name,
                "category": str(raw_entry.get("category", "") or ""),
                "required": required,
                "scenario_path": scenario_path,
                "scenario_exists": scenario_exists,
                "anchor_report_path": anchor_report_path,
                "anchor_report_exists": anchor_report_exists,
                "release_shape_report_path": release_shape_path,
                "release_shape_report_exists": release_shape_exists,
                "recommended_label": recommended_label,
                "preview_ready": preview_ready,
                "peer_backend_ready": peer_backend_ready,
                "status": status,
                "blockers": blockers,
                "notes": str(raw_entry.get("notes", "") or ""),
            }
        )

    return {
        "manifest_path": str(manifest_file),
        "name": str(manifest.get("name", "") or ""),
        "entry_count": len(rows),
        "required_entry_count": required_total,
        "required_preview_ready_count": required_preview_ready,
        "required_peer_ready_count": required_peer_ready,
        "corpus_green_for_preview": bool(required_total > 0 and required_preview_ready == required_total),
        "corpus_green_for_peer": bool(required_total > 0 and required_peer_ready == required_total),
        "rows": rows,
    }


def write_backend_release_corpus_report(
    report: dict[str, Any],
    *,
    output_dir: Path | str,
) -> tuple[Path, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "backend_release_corpus_report.json"
    csv_path = out_dir / "backend_release_corpus_summary.csv"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    fieldnames = [
        "name",
        "category",
        "required",
        "status",
        "recommended_label",
        "preview_ready",
        "peer_backend_ready",
        "scenario_exists",
        "anchor_report_exists",
        "release_shape_report_exists",
        "blockers",
        "scenario_path",
        "anchor_report_path",
        "release_shape_report_path",
        "notes",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in report.get("rows", []):
            encoded = dict(row)
            encoded["blockers"] = ",".join(str(item) for item in (row.get("blockers", []) or []))
            writer.writerow({name: encoded.get(name, "") for name in fieldnames})
    return json_path, csv_path
