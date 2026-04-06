"""Machine-checkable validation for fp-ineq publication against the fp-r gate."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import urlopen

DEFAULT_MANIFEST_SOURCE = "https://smkwray.github.io/fp-ineq/manifest.json"
DEFAULT_MATRIX_PATH = Path("artifacts/fp_ineq_fpr_matrix_20260405/fp_ineq_fpr_matrix.json")
DEFAULT_CONTRACT_PATH = Path("docs/fp-ineq-publication-guardrails.json")

PUBLIC_DEFAULT_SAFE = "public_default_safe"
REQUIRES_LABEL = "requires_explicit_legacy_split_label"
EXCLUDE = "exclude_from_public_fpr"


def _is_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _load_json_source(source: Path | str) -> dict[str, Any]:
    token = str(source).strip()
    if not token:
        raise ValueError("JSON source path/url is required")
    if _is_url(token):
        with urlopen(token) as response:
            payload = json.load(response)
    else:
        payload = json.loads(Path(token).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {token}")
    return payload


def _status_for_classification(classification: str) -> str:
    if classification == "modern_branch_ok":
        return PUBLIC_DEFAULT_SAFE
    if classification == "modern_branch_ok_but_legacy_split":
        return REQUIRES_LABEL
    return EXCLUDE


def _has_explicit_legacy_split_label(
    run_record: dict[str, Any],
    *,
    fields: list[str],
    tokens: list[str],
) -> bool:
    normalized_tokens = [str(token).strip().lower() for token in tokens if str(token).strip()]
    if not normalized_tokens:
        return True
    text = " ".join(str(run_record.get(field, "") or "") for field in fields).lower()
    return any(token in text for token in normalized_tokens)


def validate_fp_ineq_publication(
    *,
    manifest_source: Path | str = DEFAULT_MANIFEST_SOURCE,
    matrix_path: Path | str = DEFAULT_MATRIX_PATH,
    contract_path: Path | str = DEFAULT_CONTRACT_PATH,
) -> dict[str, Any]:
    manifest = _load_json_source(manifest_source)
    matrix = _load_json_source(matrix_path)
    contract = _load_json_source(contract_path)

    prefix = str(contract.get("run_id_prefix", "ineq-") or "ineq-")
    lane_rows = list(matrix.get("lane_rows", []) or [])
    lanes_by_variant = {
        str(item.get("variant_id", "") or "").strip(): item
        for item in lane_rows
        if isinstance(item, dict) and str(item.get("variant_id", "") or "").strip()
    }
    run_records = list(manifest.get("runs", []) or [])
    default_run_ids = [
        str(item).strip()
        for item in list(manifest.get("default_run_ids", []) or [])
        if str(item).strip()
    ]
    contract_required_run_ids = [
        str(item).strip()
        for item in list(contract.get("required_run_ids", []) or [])
        if str(item).strip()
    ]
    contract_recommended_default_run_ids = [
        str(item).strip()
        for item in list(contract.get("recommended_default_run_ids", []) or [])
        if str(item).strip()
    ]
    allow_published_legacy_split = bool(contract.get("allow_published_legacy_split", True))
    require_explicit_legacy_split_label = bool(
        contract.get("require_explicit_label_for_published_legacy_split", False)
    )
    legacy_split_label_fields = [
        str(item).strip()
        for item in list(contract.get("legacy_split_label_fields", []) or [])
        if str(item).strip()
    ]
    if not legacy_split_label_fields:
        legacy_split_label_fields = ["label", "summary"]
    legacy_split_label_tokens = [
        str(item).strip()
        for item in list(contract.get("legacy_split_label_tokens", []) or [])
        if str(item).strip()
    ]
    require_default_safe = bool(contract.get("require_default_runs_to_be_public_default_safe", True))
    require_exact_default_set = bool(contract.get("require_exact_default_run_ids", False))

    run_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for raw_run in run_records:
        if not isinstance(raw_run, dict):
            continue
        run_id = str(raw_run.get("run_id", "") or "").strip()
        if not run_id:
            failures.append({"reason": "missing_run_id"})
            continue
        if not run_id.startswith(prefix):
            failures.append(
                {
                    "reason": "bad_run_id_prefix",
                    "run_id": run_id,
                    "expected_prefix": prefix,
                }
            )
            continue
        variant_id = run_id[len(prefix) :]
        lane = lanes_by_variant.get(variant_id)
        if lane is None:
            failures.append(
                {
                    "reason": "missing_matrix_lane",
                    "run_id": run_id,
                    "variant_id": variant_id,
                }
            )
            continue
        classification = str(lane.get("classification", "") or "").strip()
        evidence_mode = str(lane.get("evidence_mode", "") or "").strip()
        public_status = str(lane.get("public_fp_r_status", "") or "").strip() or _status_for_classification(
            classification
        )
        default_selected = run_id in default_run_ids
        explicit_legacy_split_label_present = _has_explicit_legacy_split_label(
            raw_run,
            fields=legacy_split_label_fields,
            tokens=legacy_split_label_tokens,
        )
        row = {
            "run_id": run_id,
            "variant_id": variant_id,
            "label": str(raw_run.get("label", "") or "").strip(),
            "group": str(raw_run.get("group", "") or "").strip(),
            "default_selected": default_selected,
            "classification": classification,
            "evidence_mode": evidence_mode,
            "public_fp_r_status": public_status,
            "explicit_legacy_split_label_present": explicit_legacy_split_label_present,
            "notes": str(lane.get("notes", "") or "").strip(),
        }
        run_rows.append(row)
        if public_status == EXCLUDE:
            failures.append(
                {
                    "reason": "published_run_not_public_ready",
                    "run_id": run_id,
                    "classification": classification,
                }
            )
        elif public_status == REQUIRES_LABEL and not allow_published_legacy_split:
            failures.append(
                {
                    "reason": "published_legacy_split_disallowed",
                    "run_id": run_id,
                    "classification": classification,
                }
            )
        elif public_status == REQUIRES_LABEL:
            warnings.append(
                {
                    "reason": "published_legacy_split_requires_label",
                    "run_id": run_id,
                    "classification": classification,
                }
            )
            if require_explicit_legacy_split_label and not explicit_legacy_split_label_present:
                failures.append(
                    {
                        "reason": "published_legacy_split_missing_explicit_label",
                        "run_id": run_id,
                        "classification": classification,
                        "label_fields": legacy_split_label_fields,
                        "required_tokens": legacy_split_label_tokens,
                    }
                )
        if default_selected and public_status != PUBLIC_DEFAULT_SAFE and require_default_safe:
            failures.append(
                {
                    "reason": "default_run_not_public_default_safe",
                    "run_id": run_id,
                    "classification": classification,
                }
            )

    published_run_ids = [row["run_id"] for row in run_rows]
    missing_required_run_ids = sorted(set(contract_required_run_ids) - set(published_run_ids))
    unexpected_run_ids = (
        sorted(set(published_run_ids) - set(contract_required_run_ids))
        if contract_required_run_ids
        else []
    )
    if missing_required_run_ids:
        failures.append({"reason": "missing_required_run_ids", "run_ids": missing_required_run_ids})
    if contract_required_run_ids and unexpected_run_ids:
        failures.append({"reason": "unexpected_run_ids", "run_ids": unexpected_run_ids})

    if require_exact_default_set and contract_recommended_default_run_ids:
        if default_run_ids != contract_recommended_default_run_ids:
            failures.append(
                {
                    "reason": "default_run_ids_mismatch",
                    "expected_default_run_ids": contract_recommended_default_run_ids,
                    "actual_default_run_ids": default_run_ids,
                }
            )

    summary = {
        "published_run_count": len(run_rows),
        "modern_branch_ok_count": sum(
            row["classification"] == "modern_branch_ok" for row in run_rows
        ),
        "modern_branch_ok_but_legacy_split_count": sum(
            row["classification"] == "modern_branch_ok_but_legacy_split" for row in run_rows
        ),
        "published_legacy_split_with_explicit_label_count": sum(
            row["classification"] == "modern_branch_ok_but_legacy_split"
            and row["explicit_legacy_split_label_present"]
            for row in run_rows
        ),
        "not_public_ready_count": sum(
            row["classification"] == "not_public_ready" for row in run_rows
        ),
        "default_run_count": len(default_run_ids),
        "default_runs_public_default_safe_count": sum(
            row["default_selected"] and row["public_fp_r_status"] == PUBLIC_DEFAULT_SAFE
            for row in run_rows
        ),
        "default_runs_requiring_legacy_split_label_count": sum(
            row["default_selected"] and row["public_fp_r_status"] == REQUIRES_LABEL
            for row in run_rows
        ),
        "recommended_default_run_ids": contract_recommended_default_run_ids,
        "missing_required_run_ids": missing_required_run_ids,
    }

    return {
        "ok": not failures,
        "manifest_source": str(manifest_source),
        "matrix_path": str(matrix_path),
        "contract_path": str(contract_path),
        "summary": summary,
        "failures": failures,
        "warnings": warnings,
        "run_rows": run_rows,
    }


def write_fp_ineq_publication_report(
    report: dict[str, Any],
    *,
    output_dir: Path | str,
) -> tuple[Path, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "fp_ineq_publication_validation_report.json"
    csv_path = out_dir / "fp_ineq_publication_validation_rows.csv"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    fieldnames = [
        "run_id",
        "variant_id",
        "label",
        "group",
        "default_selected",
        "classification",
        "evidence_mode",
        "public_fp_r_status",
        "explicit_legacy_split_label_present",
        "notes",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in report.get("run_rows", []) or []:
            writer.writerow({name: row.get(name, "") for name in fieldnames})
    return json_path, csv_path


def _format_report(report: dict[str, Any]) -> str:
    lines = []
    status = "PASS" if report.get("ok") else "FAIL"
    lines.append(f"{status}: fp-ineq publication validation")
    lines.append(f"manifest: {report.get('manifest_source')}")
    lines.append(f"matrix: {report.get('matrix_path')}")
    lines.append(f"contract: {report.get('contract_path')}")
    summary = report.get("summary", {}) or {}
    lines.append(
        "published_runs="
        f"{summary.get('published_run_count', 0)} "
        f"modern_ok={summary.get('modern_branch_ok_count', 0)} "
        f"legacy_split={summary.get('modern_branch_ok_but_legacy_split_count', 0)} "
        f"not_public_ready={summary.get('not_public_ready_count', 0)}"
    )
    lines.append(
        "default_runs="
        f"{summary.get('default_run_count', 0)} "
        f"default_safe={summary.get('default_runs_public_default_safe_count', 0)} "
        f"default_needing_label={summary.get('default_runs_requiring_legacy_split_label_count', 0)}"
    )
    failures = list(report.get("failures", []) or [])
    warnings = list(report.get("warnings", []) or [])
    if not failures:
        lines.append("All configured fp-ineq publication gates passed.")
    else:
        lines.append(f"Failures: {len(failures)}")
        for item in failures:
            lines.append(f"- {item.get('reason')}: {json.dumps(item, sort_keys=True)}")
    if warnings:
        lines.append(f"Warnings: {len(warnings)}")
        for item in warnings[:20]:
            lines.append(f"- {item.get('reason')}: {json.dumps(item, sort_keys=True)}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate an fp-ineq publication bundle against the fp-r defensibility gate."
    )
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST_SOURCE, help="Manifest path or URL")
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX_PATH)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT_PATH)
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args(argv)

    report = validate_fp_ineq_publication(
        manifest_source=args.manifest,
        matrix_path=args.matrix,
        contract_path=args.contract,
    )
    if args.json_out is not None:
        args.json_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(_format_report(report))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
