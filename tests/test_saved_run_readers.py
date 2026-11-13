"""Saved fppy/fp-r runs (LOADFORMAT output only) must be readable by every saved-run consumer."""

from __future__ import annotations

from pathlib import Path

import yaml

from fp_wraptr.analysis.report import build_run_report
from fp_wraptr.scenarios.batch import compare_to_golden, save_golden
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import ScenarioResult

PABEV = "SMPL 2025.3 2026.2;\nLOAD UR;\n0.05\n0.04\n0.045\n0.05\n'END'\n"


def _fppy_run(root: Path, name: str = "fppy_run_20260101_000000") -> Path:
    run_dir = root / name
    run_dir.mkdir(parents=True)
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump({
            "name": "fppy_run",
            "backend": "fppy",
            "forecast_start": "2026.1",
            "forecast_end": "2026.2",
        }),
        encoding="utf-8",
    )
    (run_dir / "LOADFORMAT.DAT").write_text(PABEV, encoding="utf-8")
    return run_dir


def test_report_reads_loadformat_only_run(tmp_path: Path) -> None:
    run_dir = _fppy_run(tmp_path)
    report = build_run_report(run_dir, baseline_dir=run_dir)
    assert "No level series found" not in report
    assert "UR" in report
    assert "Baseline output missing" not in report


def test_golden_round_trip_for_loadformat_run(tmp_path: Path) -> None:
    run_dir = _fppy_run(tmp_path / "artifacts")
    result = ScenarioResult(
        config=ScenarioConfig.from_yaml(run_dir / "scenario.yaml"), output_dir=run_dir
    )
    golden = tmp_path / "golden"

    saved = save_golden(result, golden)

    assert saved == golden / "LOADFORMAT.DAT"
    assert (golden / "scenario.yaml").exists()
    comparison = compare_to_golden(result, golden)
    assert comparison["matches"] is True
    assert comparison["variable_diffs"]["UR"]["abs_delta"] == 0.0
