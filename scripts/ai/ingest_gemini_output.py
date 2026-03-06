#!/usr/bin/env python3
"""Ingest one Gemini batch JSON payload from stdin and save by batch_id."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REQUIRED_KEYS = {
    "batch_id",
    "high_confidence_candidates",
    "medium_confidence_candidates",
    "unresolved_blockers",
}


def _extract_json_object(text: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            payload, _end = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and "batch_id" in payload:
            return payload
    raise ValueError("Could not find a valid JSON object containing batch_id.")


def _ensure_list(payload: dict[str, Any], key: str) -> None:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ValueError(f"Key '{key}' must be a JSON array.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out-dir",
        default="do",
        help="Directory where batch JSON files are written (default: do).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting an existing output file.",
    )
    args = parser.parse_args()

    raw = sys.stdin.read()
    if not raw.strip():
        raise SystemExit("No input detected on stdin.")

    payload = _extract_json_object(raw)
    missing_keys = sorted(REQUIRED_KEYS - set(payload.keys()))
    if missing_keys:
        raise SystemExit(f"Missing required keys: {', '.join(missing_keys)}")
    _ensure_list(payload, "high_confidence_candidates")
    _ensure_list(payload, "medium_confidence_candidates")
    _ensure_list(payload, "unresolved_blockers")

    batch_id = str(payload["batch_id"]).strip()
    if not batch_id:
        raise SystemExit("batch_id is empty.")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{batch_id}.json"
    if out_path.exists() and not args.overwrite:
        raise SystemExit(f"Refusing to overwrite existing file: {out_path} (use --overwrite)")

    out_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")

    high = len(payload["high_confidence_candidates"])
    medium = len(payload["medium_confidence_candidates"])
    unresolved = len(payload["unresolved_blockers"])
    print(f"Saved: {out_path}")
    print(f"Counts: high={high} medium={medium} unresolved={unresolved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
