"""fp.exe solve-error detection: non-converged periods must be visible, not silent."""

from __future__ import annotations

from pathlib import Path

from fp_wraptr.runtime.solve_errors import scan_run_solution_errors, scan_solution_errors


def test_iteration_limit_messages_are_all_detected(tmp_path: Path) -> None:
    # fp.for prints this when a period hits MAXITERS, then carries the unconverged
    # values forward; every such period must be counted (no cap).
    lines = [" ITERS=    40  2026.1"] + [
        f" Solution error. Iteration limit of   100 reached for observation number {n}    4.3"
        for n in range(300, 316)
    ]
    (tmp_path / "fmout.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    matches = scan_solution_errors(tmp_path)

    assert len(matches) == 16
    assert {m.solve for m in matches} == {"iteration_limit"}
    assert {m.iters for m in matches} == {100}


def test_estimation_rho_iteration_limit_is_not_a_solve_error(tmp_path: Path) -> None:
    (tmp_path / "fmout.txt").write_text(
        "Iteration limit for RHO reached.  Limit =   50\n", encoding="utf-8"
    )
    assert scan_solution_errors(tmp_path) == []


def test_run_dir_scan_falls_back_to_work_dir(tmp_path: Path) -> None:
    (tmp_path / "work").mkdir()
    (tmp_path / "work" / "fmout.txt").write_text(
        " Solution error. Iteration limit of   100 reached for observation number 301 4.3\n",
        encoding="utf-8",
    )
    assert len(scan_run_solution_errors(tmp_path)) == 1


def test_fp_exe_run_records_non_converged_periods(tmp_path: Path, monkeypatch) -> None:
    from fp_wraptr.runtime.fp_exe import FPRunResult
    from fp_wraptr.scenarios.config import ScenarioConfig
    from fp_wraptr.scenarios.runner import run_scenario

    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text("TITLE T;\nSOLVE;\n", encoding="utf-8")
    for name in ("fmdata.txt", "fmage.txt", "fmexog.txt"):
        (fp_home / name).write_text("SMPL 2025.4 2026.2;\n'END'\n", encoding="utf-8")

    def fake_run(self, input_file, work_dir, extra_env=None):
        out = work_dir / "fmout.txt"
        out.write_text(
            " Solution error. Iteration limit of   100 reached for observation number 301 4.3\n",
            encoding="utf-8",
        )
        return FPRunResult(
            return_code=0,
            stdout="",
            stderr="",
            working_dir=work_dir,
            input_file=input_file,
            output_file=out,
            duration_seconds=0.0,
        )

    monkeypatch.setattr(
        "fp_wraptr.scenarios.runner.FPExecutable.check_available", lambda _self: True
    )
    monkeypatch.setattr("fp_wraptr.scenarios.runner.FPExecutable.run", fake_run)

    config = ScenarioConfig(
        name="fpexe_nonconverged", fp_home=fp_home, forecast_start="2026.1", forecast_end="2026.2"
    )
    result = run_scenario(config, output_dir=tmp_path / "artifacts")

    errors = result.backend_diagnostics["solve_errors"]
    assert len(errors) == 1
    assert errors[0]["solve"] == "iteration_limit"


def test_run_dir_scan_skips_staged_input_of_loadformat_backends(tmp_path: Path) -> None:
    # For fppy/fp-r, work/fmout.txt is the staged fp.exe stock output, not their result.
    (tmp_path / "work").mkdir()
    (tmp_path / "work" / "fmout.txt").write_text(
        " Solution error. Iteration limit of   100 reached for observation number 301 4.3\n",
        encoding="utf-8",
    )
    for backend in ("fppy", "fp-r"):
        (tmp_path / "scenario.yaml").write_text(f"name: s\nbackend: {backend}\n", encoding="utf-8")
        assert scan_run_solution_errors(tmp_path) == []
    (tmp_path / "scenario.yaml").write_text("name: s\nbackend: fpexe\n", encoding="utf-8")
    assert len(scan_run_solution_errors(tmp_path)) == 1
