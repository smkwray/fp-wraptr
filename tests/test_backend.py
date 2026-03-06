"""Backend interface and scenario integration tests."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from fp_wraptr.runtime.backend import BackendInfo, ModelBackend, RunResult
from fp_wraptr.runtime.fairpy import FairPyBackend, FairPyBackendError
from fp_wraptr.runtime.fp_exe import FPExecutable, FPExecutableError, FPRunResult
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import run_scenario


def test_run_result_success(tmp_path):
    result = RunResult(
        return_code=0,
        stdout="ok",
        stderr="",
        working_dir=tmp_path,
        input_file=tmp_path / "fminput.txt",
        output_file=tmp_path / "fmout.txt",
        duration_seconds=0.1,
    )
    assert result.success is True


def test_run_result_failure(tmp_path):
    result = RunResult(
        return_code=1,
        stdout="",
        stderr="error",
        working_dir=tmp_path,
        input_file=tmp_path / "fminput.txt",
        output_file=tmp_path / "fmout.txt",
        duration_seconds=0.1,
    )
    assert result.success is False


def test_fp_run_result_is_run_result(tmp_path):
    result = FPRunResult(
        return_code=0,
        stdout="ok",
        stderr="",
        working_dir=tmp_path,
        input_file=tmp_path / "fminput.txt",
        output_file=tmp_path / "fmout.txt",
        duration_seconds=0.1,
    )
    assert isinstance(result, RunResult)


def test_fp_executable_is_model_backend(tmp_path):
    assert isinstance(FPExecutable(fp_home=tmp_path), ModelBackend)


def test_fairpy_backend_check_available():
    backend = FairPyBackend()
    assert backend.check_available() is True


def test_fairpy_backend_run_raises():
    backend = FairPyBackend(fp_home=Path("FM"))
    with pytest.raises(FairPyBackendError):
        backend.run(input_file=Path("missing.txt"), work_dir=Path("missing"))


def test_fairpy_backend_info():
    backend = FairPyBackend()
    info = backend.info()
    assert info.name == "fair-py"
    assert info.available is True


def test_fairpy_backend_default_preset_has_no_extra_eq_flags(tmp_path):
    backend = FairPyBackend(eq_flags_preset="default")
    assert backend._eq_args(fmout_coefs=tmp_path / "fmout.txt") == []


def test_fairpy_backend_parity_preset_enables_setupsolve(tmp_path):
    backend = FairPyBackend(eq_flags_preset="parity")
    args = backend._eq_args(fmout_coefs=tmp_path / "fmout.txt")
    assert "--enable-eq" in args
    assert "--eq-use-setupsolve" in args
    assert "--eq-flags-preset-label" in args
    assert "parity" in args
    assert "--eq-period-sequential" in args
    assert args[-2:] == ["--eq-coefs-fmout", str(tmp_path / "fmout.txt")]


def test_fairpy_backend_trace_flags_disabled_by_default() -> None:
    backend = FairPyBackend()
    assert backend._eq_iter_trace_args() == []


def test_fairpy_backend_trace_flags_emit_cli_args() -> None:
    backend = FairPyBackend(
        eq_iter_trace=True,
        eq_iter_trace_period="2025.4",
        eq_iter_trace_targets="LL2Z,L2,E",
        eq_iter_trace_max_events=123,
    )
    assert backend._eq_iter_trace_args() == [
        "--eq-iter-trace",
        "--eq-iter-trace-period",
        "2025.4",
        "--eq-iter-trace-targets",
        "LL2Z,L2,E",
        "--eq-iter-trace-max-events",
        "123",
    ]


def test_fairpy_backend_thread_env_overrides_disabled_by_default() -> None:
    backend = FairPyBackend()
    assert backend._thread_env_overrides() == {}


def test_fairpy_backend_thread_env_overrides_emit_expected_vars() -> None:
    backend = FairPyBackend(num_threads=6)
    assert backend._thread_env_overrides() == {
        "OMP_NUM_THREADS": "6",
        "OPENBLAS_NUM_THREADS": "6",
        "MKL_NUM_THREADS": "6",
        "NUMEXPR_NUM_THREADS": "6",
        "VECLIB_MAXIMUM_THREADS": "6",
    }


def test_fairpy_backend_runtime_records_requested_threads(tmp_path, monkeypatch) -> None:
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    for fname in ("fminput.txt", "fmdata.txt", "fmage.txt", "fmexog.txt"):
        (work_dir / fname).write_text(f"{fname}\n", encoding="utf-8")
    (fp_home / "fmout.txt").write_text("fmout\n", encoding="utf-8")

    backend = FairPyBackend(fp_home=fp_home, num_threads=5)
    captured_env: dict[str, str] = {}

    def _fake_write_eq_overlay(self, *, fmout_path, out_path):
        out_path.write_text("EQ 1;\n", encoding="utf-8")
        return out_path

    def _fake_write_model_config(self, work_dir, *, fminput_path, fmexog_path, fmout_path):
        config_path = work_dir / "model-config.toml"
        config_path.write_text("[model]\n", encoding="utf-8")
        return config_path

    monkeypatch.setattr(FairPyBackend, "check_available", lambda self: True)
    monkeypatch.setattr(FairPyBackend, "_write_eq_overlay_from_fmout", _fake_write_eq_overlay)
    monkeypatch.setattr(
        FairPyBackend,
        "_write_fppy_wrapper_input",
        lambda self, *, work_dir, base_input, eq_overlay, identity_overlay=None: base_input,
    )
    monkeypatch.setattr(FairPyBackend, "_write_model_config", _fake_write_model_config)

    class FakeProc:
        def __init__(self, *args, **kwargs):
            nonlocal captured_env
            captured_env = dict(kwargs.get("env") or {})
            Path(kwargs["cwd"]).joinpath("PABEV.TXT").write_text(
                "SMPL 2025.4 2025.4;\nLOAD A;\n1.0\n'END'\n",
                encoding="utf-8",
            )
            self.pid = 12345
            self.returncode = 0

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            return self.returncode

        def kill(self):
            self.returncode = -9

        def terminate(self):
            return None

    monkeypatch.setattr(subprocess, "Popen", FakeProc)

    result = backend.run(input_file=work_dir / "fminput.txt", work_dir=work_dir)

    assert result.return_code == 0
    assert captured_env["OMP_NUM_THREADS"] == "5"
    runtime_payload = json.loads((work_dir / "fppy.runtime.json").read_text(encoding="utf-8"))
    assert runtime_payload["num_threads_requested"] == 5
    assert runtime_payload["thread_env_overrides"]["OMP_NUM_THREADS"] == "5"


def test_fairpy_identity_overlay_keeps_multiline_statements(tmp_path):
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text(
        "\n".join([
            "@ base deck",
            "SMPL 2025.1 2025.4;",
            "IDENT PIEF = LOG(",
            "    PCX",
            "    / PCD",
            ") ;",
            "",
            "GENR KEEP_ME = 1;",
            "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;",
            "IDENT AFTER_SOLVE = 0;",
            "",
        ]),
        encoding="utf-8",
    )
    backend = FairPyBackend(fp_home=fp_home, eq_flags_preset="parity")
    out_path = tmp_path / "overlay.txt"
    backend._write_identity_overlay_from_base_deck(fp_home=fp_home, out_path=out_path)

    overlay = out_path.read_text(encoding="utf-8")
    assert "IDENT PIEF = LOG(" in overlay
    assert "    PCX" in overlay
    assert "    / PCD" in overlay
    assert ") ;" in overlay
    assert "GENR KEEP_ME = 1;" in overlay
    assert "AFTER_SOLVE" not in overlay


def test_fairpy_identity_overlay_keeps_multiline_ident_and_genr_until_semicolon(tmp_path):
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text(
        "\n".join([
            "@ base deck",
            "SMPL 2025.1 2025.4;",
            "IDENT PIEF = LOG(",
            "    PCX",
            "    / PCD",
            ");",
            "GENR RATIO = (",
            "    PIEF",
            "    + PCX",
            ");",
            "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;",
            "GENR AFTER_SOLVE = 0;",
            "",
        ]),
        encoding="utf-8",
    )

    backend = FairPyBackend(fp_home=fp_home, eq_flags_preset="parity")
    out_path = tmp_path / "overlay.txt"
    backend._write_identity_overlay_from_base_deck(fp_home=fp_home, out_path=out_path)

    overlay = out_path.read_text(encoding="utf-8")
    assert "IDENT PIEF = LOG(" in overlay
    assert "    / PCD" in overlay
    assert ");" in overlay
    assert "GENR RATIO = (" in overlay
    assert "    + PCX" in overlay
    assert "AFTER_SOLVE" not in overlay


def test_fairpy_identity_overlay_allows_semicolon_terminator_on_own_line(tmp_path):
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text(
        "\n".join([
            "@ base deck",
            "SMPL 2025.1 2025.4;",
            "IDENT PIEF=XX+PIV*IVF+SUBS+SUBG+USOTHER",
            "-WF*JF*(HN+1.5*HO)-RNT-INTZ-INTF",
            ";",
            "GENR KEEP_ME = 1;",
            "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;",
            "",
        ]),
        encoding="utf-8",
    )
    backend = FairPyBackend(fp_home=fp_home, eq_flags_preset="parity")
    out_path = tmp_path / "overlay.txt"
    backend._write_identity_overlay_from_base_deck(fp_home=fp_home, out_path=out_path)

    overlay = out_path.read_text(encoding="utf-8")
    assert "IDENT PIEF=XX+PIV*IVF+SUBS+SUBG+USOTHER" in overlay
    assert "-WF*JF*(HN+1.5*HO)-RNT-INTZ-INTF" in overlay
    assert "\n;\n" in overlay
    assert "GENR KEEP_ME = 1;" in overlay


def test_fairpy_identity_overlay_keeps_prerequisites_not_just_targets(tmp_path):
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text(
        "\n".join([
            "CREATE LYDZ;",
            "GENR LYDZ = EXP(PCX);",
            "IDENT PIEF = LYDZ + PCD;",
            "LHS JGJ = JGJ + 1;",
            "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;",
            "GENR AFTER_SOLVE = 0;",
            "",
        ]),
        encoding="utf-8",
    )
    backend = FairPyBackend(fp_home=fp_home, eq_flags_preset="parity")
    # Simulate a stale caller-provided target subset; extraction should not trim.
    backend._identity_overlay_targets = ("PIEF",)  # type: ignore[attr-defined]
    out_path = tmp_path / "overlay.txt"
    backend._write_identity_overlay_from_base_deck(fp_home=fp_home, out_path=out_path)

    overlay = out_path.read_text(encoding="utf-8")
    assert "CREATE LYDZ;" in overlay
    assert "GENR LYDZ = EXP(PCX);" in overlay
    assert "IDENT PIEF = LYDZ + PCD;" in overlay
    assert "LHS JGJ = JGJ + 1;" in overlay
    assert "AFTER_SOLVE" not in overlay


def test_fairpy_identity_overlay_preserves_smpl_context(tmp_path):
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text(
        "\n".join([
            "@ base deck",
            "SMPL 1952.1 2025.3;",
            "CREATE D2=0;",
            "SMPL 1972.1 1989.4;",
            "CREATE D2=1;",
            "SMPL 1952.1 2025.3;",
            "CREATE CNST2L2=D2+1;",
            "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;",
            "",
        ]),
        encoding="utf-8",
    )
    backend = FairPyBackend(fp_home=fp_home, eq_flags_preset="parity")
    out_path = tmp_path / "overlay.txt"
    backend._write_identity_overlay_from_base_deck(fp_home=fp_home, out_path=out_path)

    overlay = out_path.read_text(encoding="utf-8")
    assert "SMPL 1952.1 2025.3;" in overlay
    assert "SMPL 1972.1 1989.4;" in overlay
    assert "CREATE D2=1;" in overlay
    assert "CREATE CNST2L2=D2+1;" in overlay


def test_fairpy_identity_overlay_skips_symbols_defined_in_scenario_tree(tmp_path):
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text(
        "\n".join([
            "SMPL 1952.1 2025.3;",
            "IDENT GDP=1;",
            "IDENT GDPR=2;",
            "GENR KEEP_ME=3;",
            "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;",
            "",
        ]),
        encoding="utf-8",
    )
    backend = FairPyBackend(fp_home=fp_home, eq_flags_preset="parity")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    entry_input = work_dir / "scenario.txt"
    entry_input.write_text(
        "\n".join([
            "INPUT FILE=child.txt;",
            "IDENT GDP=9;",
            "IDENT GDPR=9;",
            "",
        ]),
        encoding="utf-8",
    )
    (work_dir / "child.txt").write_text("GENR CHILD=1;\n", encoding="utf-8")

    defined_lhs = backend._collect_assignment_lhs_from_input_tree(
        work_dir=work_dir,
        entry_input=entry_input,
    )
    assert "GDP" in defined_lhs
    assert "GDPR" in defined_lhs
    assert "CHILD" in defined_lhs

    out_path = tmp_path / "overlay.txt"
    backend._write_identity_overlay_from_base_deck(
        fp_home=fp_home,
        out_path=out_path,
        exclude_lhs=defined_lhs,
    )
    overlay = out_path.read_text(encoding="utf-8")
    assert "IDENT GDP=1;" not in overlay
    assert "IDENT GDPR=2;" not in overlay
    assert "GENR KEEP_ME=3;" in overlay


def test_fairpy_wrapper_restores_smpl_after_identity_overlay(tmp_path):
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    base_input = tmp_path / "base_input.txt"
    eq_overlay = tmp_path / "eq_overlay.txt"
    identity_overlay = tmp_path / "identity_overlay.txt"

    base_input.write_text(
        "\n".join([
            "@ base",
            "SMPL 2025.4 2025.4;",
            "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;",
            "QUIT;",
            "",
        ]),
        encoding="utf-8",
    )
    eq_overlay.write_text("EQ 1 ;\n", encoding="utf-8")
    identity_overlay.write_text(
        "\n".join([
            "@ overlay mutates SMPL",
            "SMPL 1952.1 2025.3;",
            "CREATE D2=1;",
            "",
        ]),
        encoding="utf-8",
    )

    backend = FairPyBackend(eq_flags_preset="parity")
    wrapper_path = backend._write_fppy_wrapper_input(
        work_dir=work_dir,
        base_input=base_input,
        eq_overlay=eq_overlay,
        identity_overlay=identity_overlay,
    )

    text = wrapper_path.read_text(encoding="utf-8")
    # Identity overlay inserted before SOLVE.
    assert "CREATE D2=1;" in text
    # SMPL should be restored to the base deck's SMPL before SOLVE executes.
    assert "SMPL 2025.4 2025.4;" in text
    assert text.index("CREATE D2=1;") < text.index(
        "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;"
    )
    # The last SMPL before SOLVE should be the base SMPL, not the overlay SMPL.
    last_smpl_before_solve = text.rsplit("SMPL", 1)[-1]
    assert "2025.4 2025.4" in last_smpl_before_solve


def test_fairpy_backend_run_missing_prereq_file_raises_actionable_error(tmp_path):
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fmout.txt").write_text("SOLVE DYNAMIC;\n", encoding="utf-8")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    (work_dir / "fminput.txt").write_text("SMPL 2025.1 2025.1;\n", encoding="utf-8")
    (work_dir / "fmdata.txt").write_text("", encoding="utf-8")
    # Intentionally omit fmage.txt so run fails before subprocess execution.
    (work_dir / "fmexog.txt").write_text("", encoding="utf-8")

    backend = FairPyBackend(fp_home=fp_home, eq_flags_preset="parity")
    with pytest.raises(FairPyBackendError) as excinfo:
        backend.run(input_file=work_dir / "fminput.txt", work_dir=work_dir)

    message = str(excinfo.value)
    assert "Missing required fmage.txt" in message
    assert str(work_dir / "fmage.txt") in message


def test_backend_info_fields():
    info = BackendInfo(name="test", version="1.0", available=True)
    assert info.name == "test"
    assert info.version == "1.0"
    assert info.available is True
    assert isinstance(info.details, dict)


def test_custom_backend_satisfies_protocol():
    class CustomBackend:
        def check_available(self) -> bool:
            return True

        def run(
            self,
            input_file: Path | None = None,
            work_dir: Path | None = None,
            extra_env: dict[str, str] | None = None,
        ) -> RunResult:
            return RunResult(
                return_code=0,
                stdout="ok",
                stderr="",
                working_dir=work_dir or Path("."),
                input_file=input_file or Path("fminput.txt"),
                output_file=None,
                duration_seconds=0.0,
            )

    backend = CustomBackend()
    assert isinstance(backend, ModelBackend)


FMOUT_CONTENT = """\
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
Variable   Periods forecast are  2025.4  TO   2025.4

                   2025.4      2025.5
                               2026.1

   1 PCY      P lv   1.0      2.0
             P ch   0.1      0.1
             P %ch  10.0     10.0
"""


def test_run_scenario_with_custom_backend(tmp_path):
    class MockBackend:
        def check_available(self) -> bool:
            return True

        def run(
            self,
            input_file: Path | None = None,
            work_dir: Path | None = None,
            extra_env: dict[str, str] | None = None,
        ) -> RunResult:
            assert input_file is not None
            assert work_dir is not None
            output_path = work_dir / "fmout.txt"
            output_path.write_text(FMOUT_CONTENT, encoding="utf-8")
            return RunResult(
                return_code=0,
                stdout="ok",
                stderr="",
                working_dir=work_dir,
                input_file=input_file,
                output_file=output_path,
                duration_seconds=0.01,
            )

    config = ScenarioConfig(name="custom_backend", fp_home=tmp_path)
    (tmp_path / "fminput.txt").write_text("SMPL 2025.4 2025.4;\n", encoding="utf-8")
    (tmp_path / "fmdata.txt").write_text("", encoding="utf-8")

    result = run_scenario(config, output_dir=tmp_path / "out", backend=MockBackend())

    assert result.run_result is not None
    assert result.success is True
    assert result.parsed_output is not None


def test_run_scenario_unavailable_backend(tmp_path):
    class UnavailableBackend:
        def check_available(self) -> bool:
            return False

        def run(
            self,
            input_file: Path | None = None,
            work_dir: Path | None = None,
            extra_env: dict[str, str] | None = None,
        ) -> RunResult:
            raise AssertionError("run should not be called when backend unavailable")

    config = ScenarioConfig(name="unavailable_backend", fp_home=tmp_path)
    (tmp_path / "fminput.txt").write_text("SMPL 2025.4 2025.4;\n", encoding="utf-8")
    (tmp_path / "fmdata.txt").write_text("", encoding="utf-8")

    result = run_scenario(config, output_dir=tmp_path / "out", backend=UnavailableBackend())

    assert result.run_result is None
    assert result.parsed_output is None


def test_run_scenario_default_backend(tmp_path):
    config = ScenarioConfig(name="default_backend", fp_home=tmp_path)
    (tmp_path / "fminput.txt").write_text("SMPL 2025.4 2025.4;\n", encoding="utf-8")
    result = run_scenario(config, output_dir=tmp_path / "out")

    assert result.run_result is None
    assert result.backend_diagnostics is not None
    preflight_path = result.output_dir / "backend_preflight.json"
    assert preflight_path.exists()
    assert "missing_data_files" in preflight_path.read_text(encoding="utf-8")


def test_run_scenario_backend_both_uses_parity_path(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    parity_dir = tmp_path / "parity_run"
    parity_dir.mkdir()
    (parity_dir / "parity_report.json").write_text("{}", encoding="utf-8")
    (parity_dir / "work_fppy").mkdir()
    (parity_dir / "work_fppy" / "PABEV.TXT").write_text("A,2025.4,1.0\n", encoding="utf-8")

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)

    config = ScenarioConfig(name="both_mode", fp_home=tmp_path, backend="both")
    result = run_scenario(config, output_dir=tmp_path / "out")

    assert result.run_result is not None
    assert result.run_result.return_code == 0
    assert result.backend_diagnostics is not None
    assert result.backend_diagnostics["mode"] == "both"
    assert (result.output_dir / "parity_report.json").exists()
    assert (result.output_dir / "PABEV.TXT").exists()


def test_fp_executable_preflight_report_includes_missing_files(tmp_path):
    backend = FPExecutable(fp_home=tmp_path, use_wine=False)
    report = backend.preflight_report()

    assert report["available"] is False
    assert report["exe_exists"] is False
    assert report["input_file_exists"] is False
    assert "fmdata.txt" in report["missing_data_files"]


def test_fp_executable_run_missing_input_raises_clear_error(tmp_path):
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fp.exe").write_text("", encoding="utf-8")
    (fp_home / "fmdata.txt").write_text("", encoding="utf-8")
    (fp_home / "fmage.txt").write_text("", encoding="utf-8")
    (fp_home / "fmexog.txt").write_text("", encoding="utf-8")

    backend = FPExecutable(fp_home=fp_home, use_wine=False)

    with pytest.raises(FPExecutableError) as excinfo:
        backend.run(input_file=fp_home / "missing.txt", work_dir=tmp_path / "work")

    assert "Input file for fp.exe is missing in work directory" in str(excinfo.value)


def test_fp_executable_run_missing_wine_includes_preflight_report(tmp_path, monkeypatch):
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fp.exe").write_text("", encoding="utf-8")
    (fp_home / "fminput.txt").write_text("SMPL 2025.4 2025.4;\n", encoding="utf-8")
    (fp_home / "fmdata.txt").write_text("", encoding="utf-8")
    (fp_home / "fmage.txt").write_text("", encoding="utf-8")
    (fp_home / "fmexog.txt").write_text("", encoding="utf-8")
    monkeypatch.setattr("fp_wraptr.runtime.fp_exe.shutil.which", lambda _: None)

    backend = FPExecutable(fp_home=fp_home, use_wine=True)
    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    for name in ("fminput.txt", "fmdata.txt", "fmage.txt", "fmexog.txt"):
        (work_dir / name).write_text("", encoding="utf-8")

    with pytest.raises(FPExecutableError) as excinfo:
        backend.run(input_file=work_dir / "fminput.txt", work_dir=work_dir)

    details = excinfo.value.details
    assert isinstance(details.get("preflight_report"), dict)
    preflight = details["preflight_report"]
    assert preflight["wine_required"] is True
    assert preflight["wine_available"] is False


def test_fp_executable_run_missing_data_files_raises_clear_error(tmp_path):
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fp.exe").write_text("", encoding="utf-8")
    (fp_home / "fminput.txt").write_text("SMPL 2025.4 2025.4;\n", encoding="utf-8")
    (fp_home / "fmdata.txt").write_text("", encoding="utf-8")
    # intentionally omit fmage.txt and fmexog.txt

    backend = FPExecutable(fp_home=fp_home, use_wine=False)

    with pytest.raises(FPExecutableError) as excinfo:
        backend.run(input_file=fp_home / "fminput.txt", work_dir=tmp_path / "work")

    message = str(excinfo.value)
    assert "Missing required FP data files in work directory" in message
    assert "fmage.txt" in message
    assert "fmexog.txt" in message


def test_fp_executable_run_persists_stdout_stderr_artifacts(tmp_path, monkeypatch):
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fp.exe").write_text("", encoding="utf-8")
    (fp_home / "fminput.txt").write_text("SMPL 2025.4 2025.4;\n", encoding="utf-8")
    (fp_home / "fmdata.txt").write_text("", encoding="utf-8")
    (fp_home / "fmage.txt").write_text("", encoding="utf-8")
    (fp_home / "fmexog.txt").write_text("", encoding="utf-8")

    backend = FPExecutable(fp_home=fp_home, use_wine=False)

    def fake_subprocess_run(*args, **kwargs):
        return subprocess.CompletedProcess(
            args=["fp.exe"],
            returncode=1,
            stdout="SAMPLE STDOUT\n",
            stderr="SAMPLE STDERR\n",
        )

    monkeypatch.setattr("fp_wraptr.runtime.fp_exe.subprocess.run", fake_subprocess_run)

    result = backend.run(input_file=fp_home / "fminput.txt", work_dir=tmp_path / "work")

    assert result.return_code == 1
    assert (tmp_path / "work" / "fp-exe.stdout.txt").read_text(
        encoding="utf-8"
    ) == "SAMPLE STDOUT\n"
    assert (tmp_path / "work" / "fp-exe.stderr.txt").read_text(
        encoding="utf-8"
    ) == "SAMPLE STDERR\n"
