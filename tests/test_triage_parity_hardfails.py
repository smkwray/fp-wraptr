from __future__ import annotations

import json
from pathlib import Path

from scripts.triage_parity_hardfails import triage_parity_hardfails


def _write_pabev(path: Path, *, start: str, end: str, series: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"SMPL {start} {end};"]
    for name, values in series.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(str(v) for v in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_triage_parity_hardfails_emits_full_list(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000000"
    run_dir.mkdir(parents=True)
    _write_pabev(
        run_dir / "work_fpexe" / "PABEV.TXT",
        start="2025.4",
        end="2025.4",
        series={"X": [1.0], "D": [0.0]},
    )
    _write_pabev(
        run_dir / "work_fppy" / "PABEV.TXT",
        start="2025.4",
        end="2025.4",
        series={"X": [-2.0], "D": [1.0]},
    )
    (run_dir / "parity_report.json").write_text(
        json.dumps(
            {
                "engine_runs": {
                    "fpexe": {"pabev_path": str(run_dir / "work_fpexe" / "PABEV.TXT")},
                    "fppy": {"pabev_path": str(run_dir / "work_fppy" / "PABEV.TXT")},
                },
                "pabev_detail": {
                    "start": "2025.4",
                    "atol": 1e-3,
                    "rtol": 1e-6,
                    "missing_sentinels": [-99.0],
                    "discrete_eps": 1e-12,
                    "signflip_eps": 1e-3,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "work_fppy" / "fppy_report.json").write_text(
        json.dumps(
            {
                "summary": {
                    "solve_outside_seeded_cells": 0,
                    "solve_outside_seed_inspected_cells": 5,
                    "solve_outside_seed_candidate_cells": 0,
                }
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    out_csv, out_json = triage_parity_hardfails(run_dir)
    assert out_csv.exists()
    assert out_json.exists()

    summary = json.loads(out_json.read_text(encoding="utf-8"))
    # Single-period series are not treated as discrete; only the sign flip remains.
    assert summary["hard_fail_cell_count"] == 1
    assert summary["counts_by_reason"]["sign_flip"] == 1
    assert summary["seed_diagnostics"]["solve_outside_seed_inspected_cells"] == 5
