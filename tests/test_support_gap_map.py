from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.support_gap_map import build_support_gap_map


def _write_pabev(path: Path, *, start: str, end: str, series: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"SMPL {start} {end};"]
    for name, values in series.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(str(value) for value in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_support_gap_map_emits_ranked_rows_from_existing_hardfail_triage(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000000"
    work_fppy = run_dir / "work_fppy"
    work_fppy.mkdir(parents=True)
    (work_fppy / "fppy_report.json").write_text(
        json.dumps(
            {
                "summary": {
                    "unsupported": 3,
                    "unsupported_counts": {"EQ": 2, "MODEQ": 1},
                    "solve_outside_seeded_cells": 0,
                    "solve_outside_seed_inspected_cells": 10,
                    "solve_outside_seed_candidate_cells": 0,
                },
                "unsupported_examples": [
                    {"line": 10, "command": "EQ", "statement": "EQ 1 LFOO C AG1;"},
                    {"line": 20, "command": "MODEQ", "statement": "MODEQ 1 LBAR C AG2;"},
                    {"line": 30, "command": "EQ", "statement": "EQ 2 LNONE C AG3;"},
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    lines = [""] * 40
    lines[9] = "EQ 1 LFOO C AG1;"
    lines[10] = "LHS PCPD=EXP(LFOO);"
    lines[19] = "MODEQ 1 LBAR C AG2;"
    lines[20] = "LHS HFF=EXP(LBAR);"
    lines[29] = "EQ 2 LNONE C AG3;"
    (work_fppy / "fminput.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    (run_dir / "triage_hardfails.csv").write_text(
        "variable,period,index,reason,left_value,right_value,abs_diff,left_rounded,right_rounded\n"
        "PCPD,2025.4,1,discrete_mismatch,3.0,0.0,3.0,3,0\n"
        "PCPD,2026.1,2,discrete_mismatch,3.1,0.0,3.1,3,0\n"
        "HFF,2025.4,3,sign_flip,0.5,-0.6,1.1,,\n",
        encoding="utf-8",
    )

    out_csv, out_md = build_support_gap_map(run_dir)

    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 3
    assert rows[0]["unsupported_line"] == "10"
    assert rows[0]["lhs_variable_guess"] == "PCPD"
    assert rows[0]["hard_fail_var_hit"] == "true"
    assert rows[0]["hard_fail_cell_count"] == "2"
    assert rows[0]["hard_fail_reason_counts"] == "discrete_mismatch=2"
    assert rows[0]["estimated_hard_fail_cell_count"] == "2"
    assert rows[0]["estimated_hard_fail_vars"] == "PCPD"

    assert rows[1]["unsupported_line"] == "20"
    assert rows[1]["lhs_variable_guess"] == "HFF"
    assert rows[1]["hard_fail_var_hit"] == "true"
    assert rows[1]["hard_fail_cell_count"] == "1"
    assert int(rows[1]["estimated_hard_fail_cell_count"]) >= 1
    assert "HFF" in rows[1]["estimated_hard_fail_vars"].split(",")

    assert rows[2]["unsupported_line"] == "30"
    assert rows[2]["hard_fail_var_hit"] == "false"
    assert rows[2]["hard_fail_cell_count"] == "0"

    top_md = out_md.read_text(encoding="utf-8")
    assert "OUTSIDE Seed Diagnostics" in top_md
    assert "solve_outside_seeded/inspected/candidate: 0/10/0" in top_md
    assert "EQ line 10 | lhs=PCPD" in top_md
    assert "MODEQ line 20 | lhs=HFF" in top_md


def test_support_gap_map_generates_hardfail_triage_when_missing(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000001"
    work_fpexe = run_dir / "work_fpexe"
    work_fppy = run_dir / "work_fppy"
    work_fpexe.mkdir(parents=True)
    work_fppy.mkdir(parents=True)

    _write_pabev(
        work_fpexe / "PABEV.TXT",
        start="2025.4",
        end="2025.4",
        series={"X": [1.0]},
    )
    _write_pabev(
        work_fppy / "PABEV.TXT",
        start="2025.4",
        end="2025.4",
        series={"X": [-2.0]},
    )

    (run_dir / "parity_report.json").write_text(
        json.dumps(
            {
                "engine_runs": {
                    "fpexe": {"pabev_path": str(work_fpexe / "PABEV.TXT")},
                    "fppy": {"pabev_path": str(work_fppy / "PABEV.TXT")},
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
    (work_fppy / "fppy_report.json").write_text(
        json.dumps(
            {
                "summary": {"unsupported": 1, "unsupported_counts": {"EQ": 1}},
                "unsupported_examples": [{"line": 5, "command": "EQ", "statement": "EQ 1 X C;"}],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (work_fppy / "fminput.txt").write_text(
        "\n" * 4 + "EQ 1 X C;\n",
        encoding="utf-8",
    )

    out_csv, _out_md = build_support_gap_map(run_dir)

    assert (run_dir / "triage_hardfails.csv").exists()
    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["lhs_variable_guess"] == "X"
    assert rows[0]["hard_fail_var_hit"] == "true"
    assert rows[0]["hard_fail_cell_count"] == "1"
    assert rows[0]["estimated_hard_fail_cell_count"] == "1"
    assert rows[0]["estimated_hard_fail_vars"] == "X"


def test_support_gap_map_estimates_downstream_hardfail_impact(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000002"
    work_fppy = run_dir / "work_fppy"
    work_fppy.mkdir(parents=True)

    (work_fppy / "fppy_report.json").write_text(
        json.dumps(
            {
                "summary": {"unsupported": 1, "unsupported_counts": {"EQ": 1}},
                "unsupported_examples": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (work_fppy / "fminput.txt").write_text(
        "EQ 11 LY C X;\nLHS Y=EXP(LY);\nGENR PCY=100*((Y/Y(-1))**4-1);\n",
        encoding="utf-8",
    )
    (run_dir / "triage_hardfails.csv").write_text(
        "variable,period,index,reason,left_value,right_value,abs_diff,left_rounded,right_rounded\n"
        "PCY,2025.4,1,discrete_mismatch,3.0,0.0,3.0,3,0\n"
        "PCY,2026.1,2,discrete_mismatch,3.1,0.0,3.1,3,0\n",
        encoding="utf-8",
    )

    out_csv, out_md = build_support_gap_map(run_dir)

    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["unsupported_line"] == "1"
    assert rows[0]["lhs_variable_guess"] == "Y"
    assert rows[0]["hard_fail_var_hit"] == "false"
    assert rows[0]["hard_fail_cell_count"] == "0"
    assert rows[0]["estimated_hard_fail_cell_count"] == "2"
    assert rows[0]["estimated_hard_fail_vars"] == "PCY"
    assert rows[0]["estimated_hard_fail_reason_counts"] == "discrete_mismatch=2"

    top_md_text = out_md.read_text(encoding="utf-8")
    assert "touched=PCY" in top_md_text
