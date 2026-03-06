from __future__ import annotations

from pathlib import Path

import pytest

from fp_wraptr.dashboard.artifacts import RunArtifact


def _write_loadformat(path: Path) -> None:
    # Minimal PABEV/LOADFORMAT-style file with 8 quarterly periods:
    # 2025.1 .. 2026.4 (inclusive).
    path.write_text(
        "\n".join([
            "SMPL    2025.1   2026.4 ;",
            "LOAD WF       ;",
            "  1 2 3 4",
            "  5 6 7 8",
            "'END'",
            "LOAD PF       ;",
            "  10 10 10 10",
            "  10 10 10 10",
            "'END'",
            "",
        ]),
        encoding="utf-8",
    )


def test_load_output_falls_back_to_loadformat_and_slices_periods(tmp_path: Path) -> None:
    _write_loadformat(tmp_path / "LOADFORMAT.DAT")
    (tmp_path / "scenario.yaml").write_text(
        "\n".join([
            "name: test_scenario",
            "forecast_start: '2025.3'",
            "forecast_end: '2026.2'",
            "backend: fppy",
            "",
        ]),
        encoding="utf-8",
    )

    artifact = RunArtifact(
        run_dir=tmp_path,
        scenario_name="test_scenario",
        timestamp="",
        has_output=True,
        has_chart=False,
        config=None,
    )
    output = artifact.load_series_output()
    assert output is not None
    assert output.periods == ["2025.3", "2025.4", "2026.1", "2026.2"]
    assert "WF" in output.variables
    assert "PF" in output.variables
    assert "WR" in output.variables  # derived series (WF / PF)
    assert output.variables["WF"].levels == [3.0, 4.0, 5.0, 6.0]
    assert output.variables["WR"].levels == [0.3, 0.4, 0.5, 0.6]
    # `changes` and `pct_changes` should be populated for LOADFORMAT-derived output.
    assert output.variables["WF"].changes == [1.0, 1.0, 1.0, 1.0]
    assert output.variables["WF"].pct_changes == pytest.approx([
        50.0,
        33.33333333333333,
        25.0,
        20.0,
    ])
    wr_pct = output.variables["WR"].pct_changes
    assert wr_pct == pytest.approx([50.0, 33.33333333333333, 25.0, 20.0])
