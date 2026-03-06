from __future__ import annotations

from pathlib import Path

from fp_wraptr.runtime.backend import RunResult
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import run_scenario


def _write_minimal_fp_home(fp_home: Path) -> None:
    fp_home.mkdir(parents=True, exist_ok=True)
    for fname in ("fmdata.txt", "fmage.txt", "fmexog.txt", "fminput.txt"):
        (fp_home / fname).write_text(f"{fname}\n", encoding="utf-8")


def _write_minimal_pabev(path: Path) -> None:
    path.write_text(
        "SMPL 2025.4 2025.4;\nLOAD A;\n1.0\n'END'\n",
        encoding="utf-8",
    )


def test_run_scenario_fppy_defaults_eq_flags_preset_to_parity(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "FM"
    _write_minimal_fp_home(fp_home)
    config = ScenarioConfig(name="preset_default", fp_home=fp_home, backend="fppy")

    def fake_check_available(self) -> bool:
        return True

    def fake_run(self, input_file=None, work_dir=None, extra_env=None):
        assert self.eq_flags_preset == "parity"
        assert work_dir is not None
        work_dir = Path(work_dir)
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "PABEV.TXT",
            duration_seconds=0.0,
        )

    monkeypatch.setattr(
        "fp_wraptr.scenarios.runner.FairPyBackend.check_available", fake_check_available
    )
    monkeypatch.setattr("fp_wraptr.scenarios.runner.FairPyBackend.run", fake_run)

    result = run_scenario(config, output_dir=tmp_path / "artifacts")
    assert result.run_result is not None
    assert result.run_result.return_code == 0


def test_run_scenario_fppy_respects_opt_out_eq_flags_preset_default(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "FM"
    _write_minimal_fp_home(fp_home)
    config = ScenarioConfig(
        name="preset_opt_out",
        fp_home=fp_home,
        backend="fppy",
        fppy={"eq_flags_preset": "default"},
    )

    def fake_check_available(self) -> bool:
        return True

    def fake_run(self, input_file=None, work_dir=None, extra_env=None):
        assert self.eq_flags_preset == "default"
        assert work_dir is not None
        work_dir = Path(work_dir)
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "PABEV.TXT",
            duration_seconds=0.0,
        )

    monkeypatch.setattr(
        "fp_wraptr.scenarios.runner.FairPyBackend.check_available", fake_check_available
    )
    monkeypatch.setattr("fp_wraptr.scenarios.runner.FairPyBackend.run", fake_run)

    result = run_scenario(config, output_dir=tmp_path / "artifacts")
    assert result.run_result is not None
    assert result.run_result.return_code == 0


def test_run_scenario_fppy_plumbs_num_threads(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "FM"
    _write_minimal_fp_home(fp_home)
    config = ScenarioConfig(
        name="threads_plumbed",
        fp_home=fp_home,
        backend="fppy",
        fppy={"num_threads": 7},
    )

    def fake_check_available(self) -> bool:
        return True

    def fake_run(self, input_file=None, work_dir=None, extra_env=None):
        assert self.num_threads == 7
        assert work_dir is not None
        work_dir = Path(work_dir)
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "PABEV.TXT",
            duration_seconds=0.0,
        )

    monkeypatch.setattr(
        "fp_wraptr.scenarios.runner.FairPyBackend.check_available", fake_check_available
    )
    monkeypatch.setattr("fp_wraptr.scenarios.runner.FairPyBackend.run", fake_run)

    result = run_scenario(config, output_dir=tmp_path / "artifacts")
    assert result.run_result is not None
    assert result.run_result.return_code == 0
