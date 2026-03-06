from __future__ import annotations

import json
from pathlib import Path

from scripts.detect_zero_forecast import detect_zero_forecast


def _write_pabev(path: Path, *, start: str, end: str, series: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"SMPL {start} {end};"]
    for name, values in series.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(str(value) for value in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_detect_zero_forecast_flags_zero_filled_window(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000000"
    _write_pabev(
        run_dir / "work_fpexe" / "PABEV.TXT",
        start="2025.3",
        end="2026.1",
        series={
            "PCY": [10.0, 11.0, 12.0],
            "UR": [4.0, 4.1, 4.2],
            "ZERO": [0.0, 0.0, 0.0],
        },
    )
    _write_pabev(
        run_dir / "work_fppy" / "PABEV.TXT",
        start="2025.3",
        end="2026.1",
        series={
            "PCY": [10.0, 0.0, 0.0],
            "UR": [4.0, 4.0, 4.0],
            "ZERO": [0.0, 0.0, 0.0],
        },
    )
    (run_dir / "work_fppy" / "fppy_report.json").write_text(
        json.dumps({"summary": {"solve_active_window": {"start": "2025.4", "end": "2026.1"}}}),
        encoding="utf-8",
    )

    offenders, window = detect_zero_forecast(run_dir)

    assert window == ("2025.4", "2026.1")
    by_var = {row["variable"]: row for row in offenders}
    assert "PCY" in by_var
    assert by_var["PCY"]["pattern"] == "zero_fill"
    assert by_var["PCY"]["fpexe_nonzero_cells"] == 2
    assert by_var["PCY"]["fppy_zero_cells"] == 2


def test_detect_zero_forecast_uses_full_periods_without_report(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000001"
    _write_pabev(
        run_dir / "work_fpexe" / "PABEV.TXT",
        start="2025.4",
        end="2025.4",
        series={"X": [1.0]},
    )
    _write_pabev(
        run_dir / "work_fppy" / "PABEV.TXT",
        start="2025.4",
        end="2025.4",
        series={"X": [0.0]},
    )

    offenders, window = detect_zero_forecast(run_dir)

    assert window == ("2025.4", "2025.4")
    assert len(offenders) == 1
    assert offenders[0]["variable"] == "X"
    assert offenders[0]["pattern"] == "zero_fill"


def test_detect_zero_forecast_flags_flatline_fill_when_fpexe_varies(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000002"
    _write_pabev(
        run_dir / "work_fpexe" / "PABEV.TXT",
        start="2025.4",
        end="2026.2",
        series={"PCY": [10.0, 12.0, 15.0]},
    )
    _write_pabev(
        run_dir / "work_fppy" / "PABEV.TXT",
        start="2025.4",
        end="2026.2",
        series={"PCY": [5.0, 5.0, 5.0]},
    )
    (run_dir / "work_fppy" / "fppy_report.json").write_text(
        json.dumps({"summary": {"solve_active_window": {"start": "2025.4", "end": "2026.2"}}}),
        encoding="utf-8",
    )

    offenders, _window = detect_zero_forecast(run_dir)

    assert len(offenders) == 1
    assert offenders[0]["variable"] == "PCY"
    assert offenders[0]["pattern"] == "flatline_fill"
