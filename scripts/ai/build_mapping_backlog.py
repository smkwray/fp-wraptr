#!/usr/bin/env python3
"""Build a deterministic backlog report for unmapped exogenous variables."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

from fp_wraptr.data import load_source_map
from fp_wraptr.data.dictionary import ModelDictionary, VariableRecord

_DUMMY_RE = re.compile(r"^D\d+$")


def _normalize(text: str) -> str:
    return " ".join((text or "").strip().split())


def _is_dummy(var_name: str, description: str) -> bool:
    if _DUMMY_RE.fullmatch(var_name):
        return True
    desc = description.lower()
    return desc.startswith("1 in ") and "0 otherwise" in desc


def _is_deterministic_trend(var_name: str, description: str, construction: str) -> bool:
    if var_name in {"T", "TBL2"}:
        return True
    text = f"{description} {construction}".lower()
    return "time varying" in text or "time trend" in text


def _looks_derived_identity(construction: str) -> bool:
    c = construction.strip().lower()
    if not c:
        return False
    markers = (
        "sum of",
        "def.",
        "computed",
        "peak to peak",
        "interpolation",
        "ratio of",
        "base period",
    )
    if any(m in c for m in markers):
        return True
    return any(op in construction for op in ("+", "-", "*", "/", "(", ")"))


def _classify(var_name: str, record: VariableRecord) -> str:
    description = _normalize(record.description)
    construction = _normalize(record.construction)
    desc_lower = description.lower()
    cons_upper = construction.upper()

    # Known split variables generated from annual social-insurance split formulas
    # (R200-R205 path in the Fair workbook) rather than direct quarterly observables.
    if var_name in {"SIGG", "SIHI", "SISS"}:
        return "model_identity_or_derived"

    if _is_dummy(var_name, description):
        return "deterministic_dummy"
    if _is_deterministic_trend(var_name, description, construction):
        return "deterministic_trend"
    if cons_upper.endswith("IGDPD"):
        return "model_identity_or_derived"
    if desc_lower.startswith("ratio of"):
        return "model_identity_or_derived"
    if "potential value" in desc_lower:
        return "model_identity_or_derived"
    if _looks_derived_identity(construction):
        return "model_identity_or_derived"
    if construction == var_name and description:
        return "external_candidate_with_description"
    if construction == var_name and not description:
        return "external_candidate_sparse_definition"
    if not construction and description:
        return "external_candidate_with_description"
    if not construction and not description:
        return "sparse_definition_needs_context"
    return "manual_review"


def _priority_score(bucket: str) -> int:
    order = {
        "external_candidate_with_description": 0,
        "external_candidate_sparse_definition": 1,
        "manual_review": 2,
        "sparse_definition_needs_context": 3,
        "manual_deferred": 4,
        "model_identity_or_derived": 5,
        "deterministic_trend": 6,
        "deterministic_dummy": 7,
    }
    return order.get(bucket, 99)


def _to_row(var_name: str, record: VariableRecord, bucket: str) -> dict[str, Any]:
    return {
        "variable": var_name,
        "bucket": bucket,
        "description": _normalize(record.description),
        "construction": _normalize(record.construction),
        "category": record.category,
    }


def _load_deferred_vars(path: Path | None) -> set[str]:
    if path is None or not path.exists():
        return set()
    deferred: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip().upper()
        if not line:
            continue
        deferred.add(line)
    return deferred


def build_report(
    model_dictionary: ModelDictionary,
    source_map_path: Path | None = None,
    deferred_vars: set[str] | None = None,
) -> dict[str, Any]:
    source_map = load_source_map(source_map_path)
    deferred_vars = deferred_vars or set()

    exogenous = sorted(
        (name, var)
        for name, var in model_dictionary.variables.items()
        if var.category == "exogenous"
    )

    rows: list[dict[str, Any]] = []
    for name, record in exogenous:
        if name in source_map:
            continue
        bucket = "manual_deferred" if name in deferred_vars else _classify(name, record)
        rows.append(_to_row(name, record, bucket))

    rows.sort(key=lambda row: (_priority_score(str(row["bucket"])), str(row["variable"])))
    bucket_counts = Counter(str(row["bucket"]) for row in rows)

    return {
        "summary": {
            "exogenous_total": len(exogenous),
            "unmapped_exogenous": len(rows),
            "bucket_counts": dict(sorted(bucket_counts.items())),
        },
        "priority_order": [
            "external_candidate_with_description",
            "external_candidate_sparse_definition",
            "manual_review",
            "sparse_definition_needs_context",
            "manual_deferred",
            "model_identity_or_derived",
            "deterministic_trend",
            "deterministic_dummy",
        ],
        "rows": rows,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# Mapping Backlog (Unmapped Exogenous Variables)",
        "",
        f"- exogenous total: `{summary['exogenous_total']}`",
        f"- unmapped exogenous: `{summary['unmapped_exogenous']}`",
        "",
        "## Bucket Counts",
    ]
    for bucket, count in summary["bucket_counts"].items():
        lines.append(f"- {bucket}: `{count}`")

    lines.append("")
    lines.append("## Priority Candidates")
    priority_count = 0
    for row in payload["rows"]:
        bucket = row["bucket"]
        if bucket not in {
            "external_candidate_with_description",
            "external_candidate_sparse_definition",
            "manual_review",
        }:
            continue
        priority_count += 1
        desc = row["description"] or "<missing>"
        cons = row["construction"] or "<missing>"
        lines.append(f"- {row['variable']} [{bucket}] :: desc={desc} :: construction={cons}")

    if priority_count == 0:
        lines.append("- <none>")

    deferred_rows = [row for row in payload["rows"] if row["bucket"] == "manual_deferred"]
    if deferred_rows:
        lines.append("")
        lines.append("## Deferred Candidates")
        for row in deferred_rows:
            desc = row["description"] or "<missing>"
            cons = row["construction"] or "<missing>"
            lines.append(
                f"- {row['variable']} [manual_deferred] :: desc={desc} :: construction={cons}"
            )

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-map",
        default="",
        help="Optional path to source_map YAML (defaults to bundled map).",
    )
    parser.add_argument(
        "--out-json",
        default="do/mapping_backlog_exogenous.json",
        help="Output JSON path.",
    )
    parser.add_argument(
        "--out-md",
        default="do/mapping_backlog_exogenous.md",
        help="Output Markdown summary path.",
    )
    parser.add_argument(
        "--deferred-vars-file",
        default="do/mapping_backlog_deferred_vars.txt",
        help=(
            "Optional newline-delimited variable list to defer from priority buckets. "
            "Default: do/mapping_backlog_deferred_vars.txt"
        ),
    )
    args = parser.parse_args()

    source_map_path = Path(args.source_map) if args.source_map else None
    deferred_vars_path = Path(args.deferred_vars_file) if args.deferred_vars_file else None
    deferred_vars = _load_deferred_vars(deferred_vars_path)
    dictionary = ModelDictionary.load()
    payload = build_report(
        dictionary,
        source_map_path=source_map_path,
        deferred_vars=deferred_vars,
    )

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")

    out_md = Path(args.out_md)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(_render_markdown(payload), encoding="utf-8")

    print(f"Wrote JSON: {out_json}")
    print(f"Wrote Markdown: {out_md}")
    print(f"Unmapped exogenous: {payload['summary']['unmapped_exogenous']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
