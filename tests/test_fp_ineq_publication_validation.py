from __future__ import annotations

import json
from pathlib import Path

from fp_wraptr.analysis.fp_ineq_publication_validation import validate_fp_ineq_publication


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _matrix_payload() -> dict:
    return {
        "lane_rows": [
            {
                "variant_id": "baseline-observed",
                "classification": "modern_branch_ok",
                "evidence_mode": "baseline",
                "notes": "baseline ok"
            },
            {
                "variant_id": "federal-transfer-relief",
                "classification": "modern_branch_ok",
                "evidence_mode": "federal",
                "notes": "federal ok"
            },
            {
                "variant_id": "ui-relief",
                "classification": "modern_branch_ok_but_legacy_split",
                "evidence_mode": "ui",
                "notes": "ui split"
            }
        ]
    }


def test_fp_ineq_publication_validator_passes_safe_defaults(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    matrix = tmp_path / "matrix.json"
    contract = tmp_path / "contract.json"
    _write_json(
        manifest,
        {
            "runs": [
                {"run_id": "ineq-baseline-observed", "label": "Baseline"},
                {"run_id": "ineq-federal-transfer-relief", "label": "Federal"},
                {
                    "run_id": "ineq-ui-relief",
                    "label": "UI (legacy split)",
                    "summary": "Shared modern branch relative to legacy fp.exe."
                }
            ],
            "default_run_ids": [
                "ineq-baseline-observed",
                "ineq-federal-transfer-relief"
            ]
        },
    )
    _write_json(matrix, _matrix_payload())
    _write_json(
        contract,
        {
            "run_id_prefix": "ineq-",
            "required_run_ids": [
                "ineq-baseline-observed",
                "ineq-federal-transfer-relief",
                "ineq-ui-relief"
            ],
            "recommended_default_run_ids": [
                "ineq-baseline-observed",
                "ineq-federal-transfer-relief"
            ],
            "allow_published_legacy_split": True,
            "require_explicit_label_for_published_legacy_split": True,
            "legacy_split_label_fields": ["label", "summary"],
            "legacy_split_label_tokens": ["legacy split", "shared modern"],
            "require_default_runs_to_be_public_default_safe": True,
            "require_exact_default_run_ids": True
        },
    )

    report = validate_fp_ineq_publication(
        manifest_source=manifest,
        matrix_path=matrix,
        contract_path=contract,
    )

    assert report["ok"] is True
    assert report["summary"]["modern_branch_ok_count"] == 2
    assert report["summary"]["modern_branch_ok_but_legacy_split_count"] == 1
    assert report["summary"]["published_legacy_split_with_explicit_label_count"] == 1
    assert not report["failures"]
    assert report["warnings"]


def test_fp_ineq_publication_validator_flags_legacy_split_defaults(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    matrix = tmp_path / "matrix.json"
    contract = tmp_path / "contract.json"
    _write_json(
        manifest,
        {
            "runs": [
                {"run_id": "ineq-baseline-observed", "label": "Baseline"},
                {"run_id": "ineq-federal-transfer-relief", "label": "Federal"},
                {
                    "run_id": "ineq-ui-relief",
                    "label": "UI (legacy split)",
                    "summary": "Shared modern branch relative to legacy fp.exe."
                }
            ],
            "default_run_ids": [
                "ineq-baseline-observed",
                "ineq-ui-relief"
            ]
        },
    )
    _write_json(matrix, _matrix_payload())
    _write_json(
        contract,
        {
            "run_id_prefix": "ineq-",
            "required_run_ids": [
                "ineq-baseline-observed",
                "ineq-federal-transfer-relief",
                "ineq-ui-relief"
            ],
            "recommended_default_run_ids": [
                "ineq-baseline-observed",
                "ineq-federal-transfer-relief"
            ],
            "allow_published_legacy_split": True,
            "require_explicit_label_for_published_legacy_split": True,
            "legacy_split_label_fields": ["label", "summary"],
            "legacy_split_label_tokens": ["legacy split", "shared modern"],
            "require_default_runs_to_be_public_default_safe": True,
            "require_exact_default_run_ids": True
        },
    )

    report = validate_fp_ineq_publication(
        manifest_source=manifest,
        matrix_path=matrix,
        contract_path=contract,
    )

    assert report["ok"] is False
    reasons = {item["reason"] for item in report["failures"]}
    assert "default_run_not_public_default_safe" in reasons
    assert "default_run_ids_mismatch" in reasons


def test_fp_ineq_publication_validator_flags_missing_legacy_split_label(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    matrix = tmp_path / "matrix.json"
    contract = tmp_path / "contract.json"
    _write_json(
        manifest,
        {
            "runs": [
                {"run_id": "ineq-baseline-observed", "label": "Baseline"},
                {"run_id": "ineq-federal-transfer-relief", "label": "Federal"},
                {"run_id": "ineq-ui-relief", "label": "UI"}
            ],
            "default_run_ids": [
                "ineq-baseline-observed",
                "ineq-federal-transfer-relief"
            ]
        },
    )
    _write_json(matrix, _matrix_payload())
    _write_json(
        contract,
        {
            "run_id_prefix": "ineq-",
            "required_run_ids": [
                "ineq-baseline-observed",
                "ineq-federal-transfer-relief",
                "ineq-ui-relief"
            ],
            "recommended_default_run_ids": [
                "ineq-baseline-observed",
                "ineq-federal-transfer-relief"
            ],
            "allow_published_legacy_split": True,
            "require_explicit_label_for_published_legacy_split": True,
            "legacy_split_label_fields": ["label", "summary"],
            "legacy_split_label_tokens": ["legacy split", "shared modern"],
            "require_default_runs_to_be_public_default_safe": True,
            "require_exact_default_run_ids": True
        },
    )

    report = validate_fp_ineq_publication(
        manifest_source=manifest,
        matrix_path=matrix,
        contract_path=contract,
    )

    assert report["ok"] is False
    reasons = {item["reason"] for item in report["failures"]}
    assert "published_legacy_split_missing_explicit_label" in reasons
