"""fppy and fp-r runs must report their own solution, not the staged fp.exe stock output."""

from __future__ import annotations

from pathlib import Path

import pytest

from fp_wraptr.runtime.backend import RunResult
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import parsed_output_from_loadformat, run_scenario

# fppy writes PABEV.TXT (LOADFORMAT). UR moves 0.05 -> 0.04 at the base period 2025.4.
PABEV = "SMPL 2025.3 2026.2;\nLOAD UR;\n0.05\n0.04\n0.045\n0.05\n'END'\n"


def _fp_home(tmp_path: Path) -> Path:
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text("TITLE T;\nSOLVE;\n", encoding="utf-8")
    for name in ("fmdata.txt", "fmage.txt", "fmexog.txt"):
        (fp_home / name).write_text("SMPL 2025.3 2026.2;\n'END'\n", encoding="utf-8")
    # The stock fp.exe output. fppy stages it as its coefficient input only.
    (fp_home / "fmout.txt").write_text("STOCK FP.EXE OUTPUT\n", encoding="utf-8")
    return fp_home


def test_fppy_run_reports_its_own_loadformat_solution(tmp_path, monkeypatch) -> None:
    def fake_run(self, input_file, work_dir, extra_env=None):
        (work_dir / "PABEV.TXT").write_text(PABEV, encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=input_file,
            output_file=work_dir / "PABEV.TXT",
            duration_seconds=0.0,
        )

    monkeypatch.setattr(
        "fp_wraptr.runtime.fairpy.FairPyBackend.check_available", lambda _self: True
    )
    monkeypatch.setattr("fp_wraptr.runtime.fairpy.FairPyBackend.run", fake_run)

    config = ScenarioConfig(
        name="fppy_results",
        fp_home=_fp_home(tmp_path),
        forecast_start="2026.1",
        forecast_end="2026.2",
        backend="fppy",
    )
    result = run_scenario(config, output_dir=tmp_path / "artifacts")

    # The stale stock output must not be presented as this run's output.
    assert not (result.output_dir / "fmout.txt").exists()
    parsed = result.parsed_output
    assert parsed is not None
    assert parsed.base_period == "2025.4"
    assert parsed.periods == ["2025.4", "2026.1", "2026.2"]
    ur = parsed.variables["UR"]
    assert ur.levels == pytest.approx([0.04, 0.045, 0.05])
    assert ur.changes[0] == pytest.approx(-0.01)
    # fp.exe prints annualized quarterly percent changes.
    assert ur.pct_changes[1] == pytest.approx(100.0 * ((0.045 / 0.04) ** 4 - 1.0))


@pytest.mark.parametrize(
    ("previous", "current", "expected"),
    [
        # fp.for GET0 (dynamic quarterly table): ((Y/Y(-1))**4 - 1) * 100 for any
        # nonzero previous level, with no positive-ratio restriction.
        (2.0, 3.0, 100.0 * (1.5**4 - 1.0)),
        (1.0, 0.0, -100.0),
        (1.0, -2.0, 1500.0),
        # A zero previous level prints the 99999 sentinel, not a percent change.
        (0.0, 1.0, 99999.0),
    ],
)
def test_loadformat_pct_change_follows_fp_exe_dynamic_table(
    tmp_path, previous: float, current: float, expected: float
) -> None:
    path = tmp_path / "PABEV.TXT"
    path.write_text(
        f"SMPL 2025.4 2026.1;\nLOAD A;\n{previous}\n{current}\n'END'\n", encoding="utf-8"
    )
    parsed = parsed_output_from_loadformat(path, forecast_start="2026.1", forecast_end="2026.1")
    assert parsed.periods == ["2025.4", "2026.1"]
    assert parsed.variables["A"].pct_changes[1] == pytest.approx(expected)
