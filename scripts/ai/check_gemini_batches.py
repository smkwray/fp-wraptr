#!/usr/bin/env python3
"""Check completeness/validity of Gemini batch JSON outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

CORE_BATCHES = {
    "gemini_batch_01_fiscal_finance",
    "gemini_batch_02_labor_hours_wages",
    "gemini_batch_03_capital_investment",
    "gemini_batch_04_prices_output_external",
    "gemini_batch_05_dividends_distribution",
    "gemini_batch_06_internal_dummy_triage",
}

FOLLOWUP_BATCHES = {
    "gemini_batch_07_unmapped_exogenous_priority",
    "gemini_batch_08_jm_armed_forces_jobs",
}

REQUIRED_KEYS = {
    "batch_id",
    "high_confidence_candidates",
    "medium_confidence_candidates",
    "unresolved_blockers",
}


def _expected_batches(profile: str) -> set[str]:
    if profile == "core":
        return set(CORE_BATCHES)
    if profile == "followup":
        return set(FOLLOWUP_BATCHES)
    if profile == "all":
        return set(CORE_BATCHES | FOLLOWUP_BATCHES)
    raise ValueError(f"Unknown expected profile: {profile}")


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Top-level JSON must be an object.")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dir",
        default="do",
        help="Directory containing gemini batch JSON files (default: do).",
    )
    parser.add_argument(
        "--expected",
        choices=("core", "followup", "all"),
        default="core",
        help="Expected batch set to validate (default: core).",
    )
    parser.add_argument(
        "--allow-unexpected",
        action="store_true",
        help="Do not fail when extra batch IDs are present outside expected set.",
    )
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Do not fail when expected batch IDs are missing.",
    )
    args = parser.parse_args()
    expected_batches = _expected_batches(args.expected)

    root = Path(args.dir)
    files = sorted(root.glob("gemini_batch_*.json")) + sorted(
        root.glob("gemini_batch_*_mapped.json")
    )
    files = sorted(set(files))

    if not files:
        print("No batch JSON files found.")
        return 1

    seen_by_batch: dict[str, list[Path]] = {}
    invalid_files: list[str] = []
    key_issues: list[str] = []

    for path in files:
        try:
            payload = _load_json(path)
        except Exception as exc:
            invalid_files.append(f"{path}: {exc}")
            continue

        missing = sorted(REQUIRED_KEYS - set(payload.keys()))
        if missing:
            key_issues.append(f"{path}: missing keys [{', '.join(missing)}]")
            continue

        batch_id = str(payload.get("batch_id", "")).strip()
        if not batch_id:
            key_issues.append(f"{path}: empty batch_id")
            continue

        for key in (
            "high_confidence_candidates",
            "medium_confidence_candidates",
            "unresolved_blockers",
        ):
            if not isinstance(payload.get(key), list):
                key_issues.append(f"{path}: key '{key}' must be a JSON array")
                break
        else:
            seen_by_batch.setdefault(batch_id, []).append(path)

    seen_batches = set(seen_by_batch.keys())
    missing_batches = sorted(expected_batches - seen_batches)
    unexpected_batches = sorted(seen_batches - expected_batches)
    duplicates = {k: v for k, v in seen_by_batch.items() if len(v) > 1}

    print(f"Found files: {len(files)}")
    print(f"Expected profile: {args.expected} ({len(expected_batches)} batches)")
    print(f"Valid batch payloads: {len(seen_batches)}")
    for batch_id in sorted(seen_batches):
        paths = ", ".join(str(p) for p in seen_by_batch[batch_id])
        print(f"- {batch_id}: {paths}")

    if missing_batches:
        print("\nMissing batches:")
        for batch in missing_batches:
            print(f"- {batch}")

    if unexpected_batches:
        print("\nUnexpected batch IDs:")
        for batch in unexpected_batches:
            print(f"- {batch}")

    if duplicates:
        print("\nDuplicate batch IDs:")
        for batch, paths in sorted(duplicates.items()):
            print(f"- {batch}: {', '.join(str(p) for p in paths)}")

    if invalid_files:
        print("\nInvalid JSON files:")
        for item in invalid_files:
            print(f"- {item}")

    if key_issues:
        print("\nSchema/key issues:")
        for item in key_issues:
            print(f"- {item}")

    has_issues = bool(
        ((not args.allow_missing) and missing_batches)
        or ((not args.allow_unexpected) and unexpected_batches)
        or duplicates
        or invalid_files
        or key_issues
    )
    return 1 if has_issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
