"""Unit tests for parity helper logic."""

from __future__ import annotations

from pathlib import Path

from fp_wraptr.analysis.parity import (
    DriftConfig,
    GateConfig,
    _drift_check_from_period_stats,
    _fingerprint_matches,
    _scan_fpexe_solution_errors,
    run_parity,
)
from fp_wraptr.runtime.backend import RunResult
from fp_wraptr.runtime.fp_exe import FPExecutableError
from fp_wraptr.scenarios.config import ScenarioConfig


def test_fingerprint_matches_ok() -> None:
    lock = {
        "algo": "sha256",
        "files": {"fminput.txt": "aaa", "fmdata.txt": "bbb"},
    }
    observed = {
        "algo": "sha256",
        "files": {"fminput.txt": "aaa", "fmdata.txt": "bbb"},
    }
    ok, message = _fingerprint_matches(lock, observed)
    assert ok is True
    assert message == "ok"


def test_fingerprint_matches_mismatch() -> None:
    lock = {"algo": "sha256", "files": {"fminput.txt": "aaa"}}
    observed = {"algo": "sha256", "files": {"fminput.txt": "ccc"}}
    ok, message = _fingerprint_matches(lock, observed)
    assert ok is False
    assert "fingerprint mismatch" in message


def test_drift_check_flags_growth_failure() -> None:
    stats = [
        {"period": "2025.4", "max_abs_diff": 1e-4, "p99_abs_diff": 1e-4},
        # Large later-period p99 triggers growth failure even after applying the
        # reference floor used to avoid false positives when early diffs are ~0.
        {"period": "2026.4", "max_abs_diff": 2e-4, "p99_abs_diff": 0.6},
    ]
    drift = DriftConfig(
        enabled=True, max_abs=1.0, growth_factor=10.0, quantile=0.99, ref_periods=1
    )
    out = _drift_check_from_period_stats(stats, drift=drift)
    assert out["status"] == "failed"
    assert "quantile_growth_exceeds_factor" in out["fail_reasons"]
    assert out["quantile_name"] == "p99_abs_diff"


def _write_minimal_pabev(path: Path) -> None:
    path.write_text(
        "SMPL 2025.4 2025.4;\nLOAD A;\n1.0\n'END'\n",
        encoding="utf-8",
    )


def _write_two_period_pabev(path: Path, *, first: float, second: float) -> None:
    path.write_text(
        f"SMPL 2025.4 2026.1;\nLOAD A;\n{first} {second}\n'END'\n",
        encoding="utf-8",
    )


def test_scan_fpexe_solution_errors_accepts_short_format(tmp_path) -> None:
    work_dir = tmp_path / "work_fpexe"
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "fmout.txt").write_text("header\nSolution error in SOL1.\nfooter\n", encoding="utf-8")

    rows = _scan_fpexe_solution_errors(work_dir)

    assert rows
    assert rows[0].get("solve") == "SOL1"
    assert rows[0].get("iters") is None
    assert rows[0].get("period") is None


def test_run_parity_does_not_mutate_input_config_fp_home(tmp_path, monkeypatch) -> None:
    original_home = tmp_path / "orig_home"
    override_home = tmp_path / "override_home"
    original_home.mkdir()
    override_home.mkdir()
    config = ScenarioConfig(name="no_mutate", fp_home=original_home, forecast_end="2025.4")

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fp-exe.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fppy.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fppy.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_compare(*args, **kwargs):
        return True, {
            "status": "ok",
            "hard_fail_cell_count": 0,
            "diff_variable_count": 0,
            "top_first_diffs": [],
            "missing_left": [],
            "missing_right": [],
            "hard_fail_cells": [],
        }

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)
    monkeypatch.setattr("fppy.pabev_parity.toleranced_compare", fake_compare)

    result = run_parity(config, output_dir=tmp_path / "artifacts", fp_home_override=override_home)

    assert result.status == "ok"
    assert config.fp_home == original_home


def test_run_parity_records_fppy_num_threads(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    config = ScenarioConfig(
        name="parity_threads",
        fp_home=fp_home,
        forecast_end="2025.4",
        fppy={"num_threads": 9},
    )

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fp-exe.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        assert self.num_threads == 9
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fppy.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fppy.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_compare(*args, **kwargs):
        return True, {
            "status": "ok",
            "hard_fail_cell_count": 0,
            "diff_variable_count": 0,
            "top_first_diffs": [],
            "missing_left": [],
            "missing_right": [],
            "hard_fail_cells": [],
        }

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)
    monkeypatch.setattr("fppy.pabev_parity.toleranced_compare", fake_compare)

    result = run_parity(config, output_dir=tmp_path / "artifacts")

    assert result.engine_runs["fppy"].details["num_threads"] == 9
    payload = result.to_dict()
    assert payload.get("schema_version") == 1
    assert "producer_version" in payload

    report_path = Path(result.run_dir) / "parity_report.json"
    report = __import__("json").loads(report_path.read_text(encoding="utf-8"))
    assert report.get("schema_version") == 1
    assert "producer_version" in report


def test_run_parity_retries_fpexe_once_on_missing_pabev_nonzero_return(
    tmp_path, monkeypatch
) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    config = ScenarioConfig(name="retry_case", fp_home=fp_home, forecast_end="2025.4")
    call_counter = {"fpexe": 0}

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        call_counter["fpexe"] += 1
        (work_dir / "fp-exe.stdout.txt").write_text("stdout\n", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("stderr\n", encoding="utf-8")
        if call_counter["fpexe"] >= 2:
            _write_minimal_pabev(work_dir / "PABEV.TXT")
            code = 0
        else:
            code = 1
        return RunResult(
            return_code=code,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fppy.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fppy.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_compare(*args, **kwargs):
        return True, {
            "status": "ok",
            "hard_fail_cell_count": 0,
            "diff_variable_count": 0,
            "top_first_diffs": [],
            "missing_left": [],
            "missing_right": [],
            "hard_fail_cells": [],
        }

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)
    monkeypatch.setattr("fppy.pabev_parity.toleranced_compare", fake_compare)

    result = run_parity(config, output_dir=tmp_path / "artifacts")

    assert result.status == "ok"
    assert call_counter["fpexe"] == 2
    assert result.pabev_detail["fpexe_retry"]["attempted"] is True
    assert result.pabev_detail["fpexe_retry"]["first_return_code"] == 1
    assert result.pabev_detail["fpexe_retry"]["second_return_code"] == 0


def test_run_parity_missing_output_reports_diagnostics(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    config = ScenarioConfig(name="missing_output_case", fp_home=fp_home, forecast_end="2025.4")

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        (work_dir / "fp-exe.stdout.txt").write_text("stdout\n", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("stderr\n", encoding="utf-8")
        return RunResult(
            return_code=1,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fppy.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fppy.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)

    result = run_parity(config, output_dir=tmp_path / "artifacts")

    assert result.status == "missing_output"
    assert result.pabev_detail["fpexe_retry"]["attempted"] is True
    assert result.pabev_detail["fpexe_stdout_path"].endswith("/work_fpexe/fp-exe.stdout.txt")
    assert result.pabev_detail["fpexe_stderr_path"].endswith("/work_fpexe/fp-exe.stderr.txt")
    assert result.engine_runs["fpexe"].stdout_path.endswith("/work_fpexe/fp-exe.stdout.txt")
    assert result.engine_runs["fpexe"].stderr_path.endswith("/work_fpexe/fp-exe.stderr.txt")


def test_run_parity_quick_applies_input_patches_to_includes(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text("INPUT FILE=common.txt;\nRETURN;\n", encoding="utf-8")
    (fp_home / "common.txt").write_text(
        "SMPL 2025.4 2029.4;\nSMPL 1952.1 2029.4;\nRETURN;\n",
        encoding="utf-8",
    )

    config = ScenarioConfig(
        name="quick_patches_includes",
        fp_home=fp_home,
        forecast_start="2025.4",
        forecast_end="2029.4",
    )

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        include = (work_dir / "common.txt").read_text(encoding="utf-8")
        assert "SMPL 2025.4 2025.4;" in include
        assert "SMPL 1952.1 2025.4;" in include
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fp-exe.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        include = (work_dir / "common.txt").read_text(encoding="utf-8")
        assert "SMPL 2025.4 2025.4;" in include
        assert "SMPL 1952.1 2025.4;" in include
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fppy.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fppy.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_compare(*args, **kwargs):
        return True, {
            "status": "ok",
            "hard_fail_cell_count": 0,
            "diff_variable_count": 0,
            "top_first_diffs": [],
            "missing_left": [],
            "missing_right": [],
            "hard_fail_cells": [],
        }

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)
    monkeypatch.setattr("fppy.pabev_parity.toleranced_compare", fake_compare)

    result = run_parity(
        config,
        output_dir=tmp_path / "artifacts",
        gate=GateConfig(pabev_start="2025.4", pabev_end="2025.4"),
    )

    assert result.status == "ok"


def test_run_parity_engine_failure_persists_fppy_eq_flags_preset(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    config = ScenarioConfig(name="fppy_failure_case", fp_home=fp_home, forecast_end="2025.4")

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fp-exe.stdout.txt").write_text("stdout\n", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("stderr\n", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        raise RuntimeError("boom")

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)

    result = run_parity(config, output_dir=tmp_path / "artifacts")

    assert result.status == "engine_failure"
    assert result.engine_runs["fppy"].details["eq_flags_preset"] == "parity"


def test_run_parity_plumbs_fppy_eq_iter_trace_and_links_trace_artifact(
    tmp_path, monkeypatch
) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    config = ScenarioConfig(
        name="fppy_trace_case",
        fp_home=fp_home,
        forecast_end="2025.4",
        fppy={
            "eq_iter_trace": True,
            "eq_iter_trace_period": "2025.4",
            "eq_iter_trace_targets": "LL2Z,L2,E",
            "eq_iter_trace_max_events": 123,
        },
    )

    captured_backend_kwargs: dict[str, object] = {}

    class FakeFairPyBackend:
        def __init__(self, **kwargs):
            captured_backend_kwargs.update(kwargs)

        def run(self, input_file=None, work_dir=None, extra_env=None):
            assert work_dir is not None
            _write_minimal_pabev(work_dir / "PABEV.TXT")
            (work_dir / "eq_iter_trace.json").write_text(
                '{"period":"2025.4","events":[]}\n', encoding="utf-8"
            )
            (work_dir / "fppy.stdout.txt").write_text("", encoding="utf-8")
            (work_dir / "fppy.stderr.txt").write_text("", encoding="utf-8")
            return RunResult(
                return_code=0,
                stdout="",
                stderr="",
                working_dir=work_dir,
                input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
                output_file=work_dir / "fmout.txt",
                duration_seconds=0.0,
            )

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fp-exe.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_compare(*args, **kwargs):
        return True, {
            "status": "ok",
            "hard_fail_cell_count": 0,
            "diff_variable_count": 0,
            "top_first_diffs": [],
            "missing_left": [],
            "missing_right": [],
            "hard_fail_cells": [],
        }

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend", FakeFairPyBackend)
    monkeypatch.setattr("fppy.pabev_parity.toleranced_compare", fake_compare)

    result = run_parity(config, output_dir=tmp_path / "artifacts")

    assert result.status == "ok"
    assert captured_backend_kwargs["eq_iter_trace"] is True
    assert captured_backend_kwargs["eq_iter_trace_period"] == "2025.4"
    assert captured_backend_kwargs["eq_iter_trace_targets"] == "LL2Z,L2,E"
    assert captured_backend_kwargs["eq_iter_trace_max_events"] == 123
    fppy_details = result.engine_runs["fppy"].details
    assert fppy_details["eq_iter_trace"] is True
    assert fppy_details["eq_iter_trace_period"] == "2025.4"
    assert fppy_details["eq_iter_trace_targets"] == "LL2Z,L2,E"
    assert fppy_details["eq_iter_trace_max_events"] == 123
    assert fppy_details["eq_iter_trace_path"].endswith("/work_fppy/eq_iter_trace.json")


def test_run_parity_engine_failure_persists_fppy_eq_iter_trace_settings(
    tmp_path, monkeypatch
) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    config = ScenarioConfig(
        name="fppy_trace_failure_case",
        fp_home=fp_home,
        forecast_end="2025.4",
        fppy={
            "eq_iter_trace": True,
            "eq_iter_trace_period": "2025.4",
            "eq_iter_trace_targets": "LL2Z,L2,E",
            "eq_iter_trace_max_events": 123,
        },
    )

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fp-exe.stdout.txt").write_text("stdout\n", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("stderr\n", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        raise RuntimeError("boom")

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)

    result = run_parity(config, output_dir=tmp_path / "artifacts")

    assert result.status == "engine_failure"
    fppy_details = result.engine_runs["fppy"].details
    assert fppy_details["eq_iter_trace"] is True
    assert fppy_details["eq_iter_trace_period"] == "2025.4"
    assert fppy_details["eq_iter_trace_targets"] == "LL2Z,L2,E"
    assert fppy_details["eq_iter_trace_max_events"] == 123
    assert result.pabev_detail["eq_iter_trace"] is True
    assert result.pabev_detail["eq_iter_trace_period"] == "2025.4"
    assert result.pabev_detail["eq_iter_trace_targets"] == "LL2Z,L2,E"
    assert result.pabev_detail["eq_iter_trace_max_events"] == 123


def test_run_parity_engine_failure_includes_fpexe_timeout_details(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    config = ScenarioConfig(name="fpexe_timeout_case", fp_home=fp_home, forecast_end="2025.4")

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        raise FPExecutableError(
            "fp.exe timed out after 600s",
            details={
                "timeout_seconds": 600,
                "termination_reason": "timeout_expired",
                "stdout_path": str((work_dir or fp_home) / "fp-exe.stdout.txt"),
                "stderr_path": str((work_dir or fp_home) / "fp-exe.stderr.txt"),
            },
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)

    result = run_parity(config, output_dir=tmp_path / "artifacts")

    assert result.status == "engine_failure"
    fpexe_details = result.engine_runs["fpexe"].details
    assert fpexe_details["timeout_seconds"] == 600
    assert fpexe_details["termination_reason"] == "timeout_expired"
    assert result.pabev_detail["fpexe_details"]["timeout_seconds"] == 600


def test_run_parity_engine_failure_includes_fpexe_preflight_report(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text("SMPL 2025.4 2025.4;\n", encoding="utf-8")
    (fp_home / "fmdata.txt").write_text("", encoding="utf-8")
    (fp_home / "fmage.txt").write_text("", encoding="utf-8")
    (fp_home / "fmexog.txt").write_text("", encoding="utf-8")
    config = ScenarioConfig(name="fpexe_preflight_case", fp_home=fp_home, forecast_end="2025.4")

    def fake_preflight_report(self, input_file=None, work_dir=None):
        _ = self, input_file, work_dir
        return {
            "available": False,
            "exe_exists": True,
            "wine_required": True,
            "wine_available": False,
        }

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        raise RuntimeError("wine missing")

    monkeypatch.setattr(
        "fp_wraptr.analysis.parity.FPExecutable.preflight_report",
        fake_preflight_report,
    )
    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)

    result = run_parity(config, output_dir=tmp_path / "artifacts")

    assert result.status == "engine_failure"
    preflight = result.engine_runs["fpexe"].details.get("preflight_report")
    assert isinstance(preflight, dict)
    assert preflight["wine_required"] is True
    assert preflight["wine_available"] is False
    assert result.pabev_detail["fpexe_details"]["preflight_report"]["wine_available"] is False


def test_run_parity_pabev_end_excludes_later_period_mismatch(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    config = ScenarioConfig(name="windowed_gate_case", fp_home=fp_home, forecast_end="2026.1")

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_two_period_pabev(work_dir / "PABEV.TXT", first=1.0, second=1.0)
        (work_dir / "fp-exe.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_two_period_pabev(work_dir / "PABEV.TXT", first=1.0, second=-1.0)
        (work_dir / "fppy.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fppy.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)

    bounded = run_parity(
        config,
        output_dir=tmp_path / "artifacts_bounded",
        gate=GateConfig(pabev_start="2025.4", pabev_end="2025.4"),
    )
    assert bounded.status == "ok"
    assert bounded.exit_code == 0
    assert bounded.pabev_detail["hard_fail_cell_count"] == 0
    assert bounded.pabev_detail["end"] == "2025.4"

    unbounded = run_parity(
        config,
        output_dir=tmp_path / "artifacts_unbounded",
        gate=GateConfig(pabev_start="2025.4"),
    )
    assert unbounded.status == "hard_fail"
    assert unbounded.pabev_detail["hard_fail_cell_count"] == 1
    assert unbounded.pabev_detail["hard_fail_cells"][0]["period"] == "2026.1"


def test_run_parity_surfaces_fpexe_solution_errors_short_format(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    config = ScenarioConfig(name="solution_error_case", fp_home=fp_home, forecast_end="2025.4")

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fmout.txt").write_text("Solution error in SOL1.\n", encoding="utf-8")
        (work_dir / "fp-exe.stdout.txt").write_text("stdout\n", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("stderr\n", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fppy.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fppy.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_compare(*args, **kwargs):
        return True, {
            "status": "ok",
            "hard_fail_cell_count": 0,
            "diff_variable_count": 0,
            "top_first_diffs": [],
            "missing_left": [],
            "missing_right": [],
            "hard_fail_cells": [],
        }

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)
    monkeypatch.setattr("fppy.pabev_parity.toleranced_compare", fake_compare)

    result = run_parity(config, output_dir=tmp_path / "artifacts")

    solution_errors = result.engine_runs["fpexe"].details.get("solution_errors")
    assert isinstance(solution_errors, list)
    assert solution_errors
    assert solution_errors[0]["solve"] == "SOL1"
    assert result.pabev_detail.get("fpexe_solution_errors_present") is True
    warnings = result.pabev_detail.get("warnings", [])
    assert isinstance(warnings, list)
    assert any("unreliable" in str(item).lower() for item in warnings)
    report = __import__("json").loads(
        (Path(result.run_dir) / "parity_report.json").read_text(encoding="utf-8")
    )
    fpexe_details = report.get("engine_runs", {}).get("fpexe", {}).get("details", {})
    report_solution_errors = fpexe_details.get("solution_errors")
    assert isinstance(report_solution_errors, list)
    assert report_solution_errors
    assert report_solution_errors[0]["solve"] == "SOL1"
    assert report.get("pabev_detail", {}).get("fpexe_solution_errors_present") is True


def test_run_parity_accepts_pacev_output_name(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    config = ScenarioConfig(name="pacev_output_case", fp_home=fp_home, forecast_end="2025.4")

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PACEV.TXT")
        (work_dir / "fp-exe.stdout.txt").write_text("stdout\n", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("stderr\n", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PACEV.TXT")
        (work_dir / "fppy.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fppy.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_compare(left, right, *args, **kwargs):
        assert Path(left).name == "PACEV.TXT"
        assert Path(right).name == "PACEV.TXT"
        return True, {
            "status": "ok",
            "hard_fail_cell_count": 0,
            "diff_variable_count": 0,
            "top_first_diffs": [],
            "missing_left": [],
            "missing_right": [],
            "hard_fail_cells": [],
        }

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)
    monkeypatch.setattr("fppy.pabev_parity.toleranced_compare", fake_compare)

    result = run_parity(config, output_dir=tmp_path / "artifacts")

    assert result.status == "ok"
    assert result.engine_runs["fpexe"].pabev_path.endswith("/PACEV.TXT")
    assert result.engine_runs["fppy"].pabev_path.endswith("/PACEV.TXT")


def test_run_parity_writes_absolute_fp_home_in_artifact_scenario(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    config = ScenarioConfig(name="abs_fp_home_case", fp_home=Path("fp_home"), forecast_end="2025.4")

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fp-exe.stdout.txt").write_text("stdout\n", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("stderr\n", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fppy.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fppy.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_compare(*args, **kwargs):
        return True, {
            "status": "ok",
            "hard_fail_cell_count": 0,
            "diff_variable_count": 0,
            "top_first_diffs": [],
            "missing_left": [],
            "missing_right": [],
            "hard_fail_cells": [],
        }

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)
    monkeypatch.setattr("fppy.pabev_parity.toleranced_compare", fake_compare)

    result = run_parity(config, output_dir=tmp_path / "artifacts")
    scenario_text = (Path(result.run_dir) / "scenario.yaml").read_text(encoding="utf-8")
    assert f"fp_home: {fp_home}" in scenario_text


def test_run_parity_truncated_pabev_reports_missing_output(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "fp_home"
    fp_home.mkdir()
    config = ScenarioConfig(name="truncated_pabev_case", fp_home=fp_home, forecast_end="2026.1")

    def fake_fpexe_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fp-exe.stdout.txt").write_text("stdout\n", encoding="utf-8")
        (work_dir / "fp-exe.stderr.txt").write_text("stderr\n", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    def fake_fppy_run(self, input_file=None, work_dir=None, extra_env=None):
        assert work_dir is not None
        _write_minimal_pabev(work_dir / "PABEV.TXT")
        (work_dir / "fppy.stdout.txt").write_text("", encoding="utf-8")
        (work_dir / "fppy.stderr.txt").write_text("", encoding="utf-8")
        return RunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=Path(input_file) if input_file is not None else work_dir / "fminput.txt",
            output_file=work_dir / "fmout.txt",
            duration_seconds=0.0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.FPExecutable.run", fake_fpexe_run)
    monkeypatch.setattr("fp_wraptr.analysis.parity.FairPyBackend.run", fake_fppy_run)

    result = run_parity(config, output_dir=tmp_path / "artifacts")

    assert result.status == "missing_output"
    assert result.exit_code == 4
    assert "expected compare end 2026.1" in str(result.pabev_detail.get("error", ""))
    truncated = result.pabev_detail.get("truncated_outputs", [])
    assert isinstance(truncated, list)
    assert len(truncated) >= 1
