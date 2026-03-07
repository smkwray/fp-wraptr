from __future__ import annotations

import json
from pathlib import Path

import pytest
from scripts.stock_fm_speed_divergence_eval import evaluate_stock_run


def _write_minimal_pacev(
    path: Path, *, a_vals: tuple[float, float], b_vals: tuple[float, float]
) -> None:
    path.write_text(
        "\n".join([
            "SMPL 2025.4 2026.1;",
            "LOAD A;",
            f"{a_vals[0]} {a_vals[1]}",
            "'END'",
            "LOAD B;",
            f"{b_vals[0]} {b_vals[1]}",
            "'END'",
            "",
        ]),
        encoding="utf-8",
    )


def test_evaluate_stock_run_parity_style_dir(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    work_fpexe = run_dir / "work_fpexe"
    work_fppy = run_dir / "work_fppy"
    work_fpexe.mkdir(parents=True)
    work_fppy.mkdir(parents=True)

    _write_minimal_pacev(
        work_fpexe / "PACEV.TXT",
        a_vals=(100.0, 200.0),
        b_vals=(50.0, 100.0),
    )
    _write_minimal_pacev(
        work_fppy / "PACEV.TXT",
        a_vals=(102.0, 198.0),
        b_vals=(52.5, 101.0),
    )
    (work_fppy / "fppy_report.json").write_text(
        json.dumps({
            "summary": {
                "mini_run_runtime_total_seconds": 12.34,
                "eq_backfill_stop_reason": "converged",
                "eq_backfill_keyboard_stop_classification": "converged",
                "eq_structural_read_cache": "numpy_columns",
                "eq_backfill_runtime_iteration_residual_checked_seconds": 1.5,
                "eq_backfill_runtime_iteration_residual_all_targets_seconds": 0.7,
                "eq_backfill_runtime_iteration_convergence_seconds": 2.0,
                "eq_backfill_runtime_iteration_deep_copy_seconds": 0.5,
                "eq_backfill_runtime_iteration_main_apply_seconds": 3.0,
                "eq_backfill_structural_scalar_reads_cached": 11,
                "eq_backfill_structural_scalar_reads_frame": 22,
            }
        })
        + "\n",
        encoding="utf-8",
    )

    row = evaluate_stock_run(
        label="case_parity",
        run_dir=run_dir,
        fpexe_output=None,
        fppy_output=None,
        fppy_report=None,
        window_start="2025.4",
        window_end=None,
        eps=1e-6,
        top=3,
    )

    assert row["label"] == "case_parity"
    assert row["runtime_seconds"] == pytest.approx(12.34)
    assert row["max_pct_diff"] == pytest.approx(5.0)
    assert row["p95_pct_diff"] > 0.0
    assert row["stop_reason"] == "converged"
    assert row["convergence_classification"] == "converged"
    assert row["eq_structural_read_cache"] == "numpy_columns"
    assert row["eq_backfill_structural_scalar_reads_cached"] == 11
    assert row["eq_backfill_structural_scalar_reads_frame"] == 22
    assert row["top_offenders"][0]["variable"] == "B"
    assert row["top_offenders"][0]["max_pct_diff"] == pytest.approx(5.0)


def test_evaluate_stock_run_sets_byte_identity_flag(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    work_fpexe = run_dir / "work_fpexe"
    work_fppy = run_dir / "work_fppy"
    work_fpexe.mkdir(parents=True)
    work_fppy.mkdir(parents=True)

    reference = work_fpexe / "PACEV.TXT"
    _write_minimal_pacev(reference, a_vals=(1.0, 2.0), b_vals=(3.0, 4.0))
    mirror = work_fppy / "PACEV.TXT"
    mirror.write_bytes(reference.read_bytes())

    row = evaluate_stock_run(
        label="case_identity",
        run_dir=run_dir,
        fpexe_output=None,
        fppy_output=None,
        fppy_report=None,
        window_start="2025.4",
        window_end=None,
        eps=1e-6,
        top=1,
        baseline_output=mirror,
    )

    assert row["max_pct_diff"] == pytest.approx(0.0)
    assert row["byte_identical_to_baseline"] is True
