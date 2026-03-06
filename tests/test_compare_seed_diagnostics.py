from __future__ import annotations

import json
from pathlib import Path

from scripts.compare_seed_diagnostics import compare_seed_diagnostics


def test_compare_seed_diagnostics_reads_nested_report(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000000"
    report_path = run_dir / "work_fppy" / "fppy_report.json"
    report_path.parent.mkdir(parents=True)
    report_path.write_text(
        json.dumps({
            "summary": {
                "solve_active_window": {"start": "2025.4", "end": "2029.4"},
                "solve_outside_seeded_cells": 0,
                "solve_outside_seed_inspected_cells": 85,
                "solve_outside_seed_candidate_cells": 0,
                "eq_backfill_outside_post_seed_cells": 0,
                "eq_backfill_outside_post_seed_inspected_cells": 0,
                "eq_backfill_outside_post_seed_candidate_cells": 0,
            }
        })
        + "\n",
        encoding="utf-8",
    )

    rows = compare_seed_diagnostics([run_dir])
    assert len(rows) == 1
    row = rows[0]
    assert row["solve_window_start"] == "2025.4"
    assert row["solve_window_end"] == "2029.4"
    assert row["solve_seeded"] == "0"
    assert row["solve_inspected"] == "85"
    assert row["solve_candidate"] == "0"
    assert row["eq_backfill_iterations"] == ""
    assert row["seed_state"] == "no_candidates"


def test_compare_seed_diagnostics_marks_candidate_unseeded(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000001"
    report_path = run_dir / "fppy_report.json"
    run_dir.mkdir(parents=True)
    report_path.write_text(
        json.dumps({
            "summary": {
                "solve_outside_seeded_cells": 0,
                "solve_outside_seed_candidate_cells": 2,
            }
        })
        + "\n",
        encoding="utf-8",
    )

    rows = compare_seed_diagnostics([run_dir])
    assert rows[0]["seed_state"] == "candidate_unseeded"


def test_compare_seed_diagnostics_reads_runtime_fields_from_summary(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000010"
    report_path = run_dir / "work_fppy" / "fppy_report.json"
    report_path.parent.mkdir(parents=True)
    report_path.write_text(
        json.dumps({
            "summary": {
                "eq_flags_preset": "parity",
                "eq_use_setupsolve": True,
                "eq_backfill_iterations": 680,
                "eq_backfill_min_iters": 40,
                "eq_backfill_max_iters": 100,
                "eq_backfill_stop_reason": "converged",
                "eq_backfill_converged": True,
                "setupsolve": {
                    "miniters": 40,
                    "maxiters": 100,
                    "maxcheck": 3,
                    "nomiss": True,
                    "tolall": 0.001,
                    "tolallabs": True,
                    "dampall": 1.0,
                },
                "unsupported_examples": [{"line": 9, "command": "EQ"}],
                "eq_backfill_keyboard_targets_missing": False,
                "eq_backfill_keyboard_stop_classification": "converged",
                "solve_outside_seeded_cells": 0,
                "solve_outside_seed_candidate_cells": 0,
            }
        })
        + "\n",
        encoding="utf-8",
    )

    rows = compare_seed_diagnostics([run_dir])
    row = rows[0]
    assert row["eq_flags_preset"] == "parity"
    assert row["eq_use_setupsolve"] == "true"
    assert row["eq_backfill_iterations"] == "680"
    assert row["eq_backfill_min_iters"] == "40"
    assert row["eq_backfill_max_iters"] == "100"
    assert row["eq_backfill_stop_reason"] == "converged"
    assert row["eq_backfill_converged"] == "true"
    assert row["setupsolve_miniters"] == "40"
    assert row["setupsolve_maxiters"] == "100"
    assert row["setupsolve_maxcheck"] == "3"
    assert row["setupsolve_nomiss"] == "true"
    assert row["setupsolve_tolall"] == "0.001"
    assert row["setupsolve_tolallabs"] == "true"
    assert row["setupsolve_dampall"] == "1.0"
    assert row["unsupported_examples_count"] == "1"
    assert row["eq_backfill_keyboard_targets_missing"] == "false"
    assert row["eq_backfill_keyboard_stop_classification"] == "converged"


def test_compare_seed_diagnostics_falls_back_to_parity_report_for_preset(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000011"
    report_path = run_dir / "work_fppy" / "fppy_report.json"
    report_path.parent.mkdir(parents=True)
    report_path.write_text(
        json.dumps({
            "summary": {"solve_outside_seeded_cells": 0, "solve_outside_seed_candidate_cells": 0}
        })
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "parity_report.json").write_text(
        json.dumps({
            "engine_runs": {
                "fppy": {
                    "details": {
                        "eq_flags_preset": "parity_minimal",
                    }
                }
            }
        })
        + "\n",
        encoding="utf-8",
    )

    rows = compare_seed_diagnostics([run_dir])
    row = rows[0]
    assert row["eq_flags_preset"] == "parity_minimal"
    assert row["parity_report_path"].endswith("parity_report.json")


def test_compare_seed_diagnostics_handles_missing_report(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000002"
    run_dir.mkdir(parents=True)

    rows = compare_seed_diagnostics([run_dir])
    assert rows[0]["seed_state"] == "missing_report"
    assert rows[0]["solve_seeded"] == ""
    assert rows[0]["eq_flags_preset"] == ""
    assert rows[0]["eq_backfill_stop_reason"] == ""
    assert rows[0]["unsupported_examples_count"] == ""
