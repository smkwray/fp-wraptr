"""Emit a deterministic triage artifact for parity hard-fail cells.

CLI wrapper around `fp_wraptr.analysis.triage_parity_hardfails`.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from fp_wraptr.analysis.triage_parity_hardfails import triage_parity_hardfails


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True, help="Parity run directory")
    args = parser.parse_args(argv)

    out_csv, out_json = triage_parity_hardfails(Path(args.run_dir))
    print(f"Wrote {out_csv}")
    print(f"Wrote {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
