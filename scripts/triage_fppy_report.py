"""Deterministically bucket fppy parity issues for quick triage.

CLI wrapper around `fp_wraptr.analysis.triage_fppy`.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from fp_wraptr.analysis.triage_fppy import triage_fppy_report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bucket fppy_report.json issues into stable triage categories.",
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to work_fppy directory (or parity run root that contains work_fppy).",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Optional output directory for triage_summary.json and triage_issues.csv.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    summary_path, csv_path = triage_fppy_report(
        Path(args.run_dir),
        out_dir=(Path(args.out_dir) if args.out_dir else None),
    )
    print(f"summary: {summary_path}")
    print(f"csv: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
