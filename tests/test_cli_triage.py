import json
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from fp_wraptr.cli import app

runner = CliRunner()


def _write_pabev(path: Path, *, values_by_var: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["SMPL 2025.4 2026.1;"]
    for name, values in values_by_var.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(str(float(v)) for v in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_fp_triage_parity_hardfails_writes_outputs(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000000"
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_pabev(run_dir / "work_fpexe" / "PABEV.TXT", values_by_var={"HF": [0.5, 0.4]})
    _write_pabev(run_dir / "work_fppy" / "PABEV.TXT", values_by_var={"HF": [0.5, -0.4]})

    (run_dir / "parity_report.json").write_text(
        json.dumps({
            "pabev_detail": {
                "start": "2025.4",
                "atol": 1e-3,
                "rtol": 1e-6,
                "missing_sentinels": [-99.0],
                "discrete_eps": 1e-12,
                "signflip_eps": 1e-3,
            },
            "engine_runs": {},
        })
        + "\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["triage", "parity-hardfails", str(run_dir)])
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr

    assert (run_dir / "triage_hardfails.csv").exists()
    assert (run_dir / "triage_hardfails_summary.json").exists()


def test_fp_triage_fppy_report_writes_outputs(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000001"
    (run_dir / "work_fppy").mkdir(parents=True, exist_ok=True)
    (run_dir / "work_fppy" / "fppy_report.json").write_text(
        json.dumps({
            "issues": [
                {
                    "line": 10,
                    "statement": "GENR X=LOG(Y);",
                    "error": "KeyError: column 'Y' not present in input data",
                }
            ]
        })
        + "\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["triage", "fppy-report", str(run_dir)])
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr

    assert (run_dir / "work_fppy" / "triage_summary.json").exists()
    assert (run_dir / "work_fppy" / "triage_issues.csv").exists()


def test_fp_triage_loop_runs_parity_and_triage(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"))

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **_kwargs):
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="ok",
            exit_code=0,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)

    result = runner.invoke(
        app, ["triage", "loop", str(scenario_path), "--output-dir", str(tmp_path)]
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert (run_dir / "triage_hardfails.csv").exists()
    assert (run_dir / "triage_hardfails_summary.json").exists()
    assert (run_dir / "work_fppy" / "triage_summary.json").exists()
    assert (run_dir / "work_fppy" / "triage_issues.csv").exists()


def test_fp_triage_loop_passes_gate_pabev_end(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run_gate_end"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")
    observed = {"gate_end": None}

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"))

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **kwargs):
        gate = kwargs.get("gate")
        observed["gate_end"] = getattr(gate, "pabev_end", None)
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="ok",
            exit_code=0,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)

    result = runner.invoke(
        app,
        [
            "triage",
            "loop",
            str(scenario_path),
            "--output-dir",
            str(tmp_path),
            "--gate-pabev-end",
            "2025.4",
        ],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert observed["gate_end"] == "2025.4"


def test_fp_triage_loop_quick_sets_gate_end_to_forecast_start(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run_quick"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")
    observed = {"gate_end": None}

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"), forecast_start="2032.3")

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **kwargs):
        gate = kwargs.get("gate")
        observed["gate_end"] = getattr(gate, "pabev_end", None)
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="ok",
            exit_code=0,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)

    result = runner.invoke(
        app,
        [
            "triage",
            "loop",
            str(scenario_path),
            "--output-dir",
            str(tmp_path),
            "--quick",
        ],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert observed["gate_end"] == "2032.3"


def test_fp_triage_loop_strict_failure_prints_run_dir(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run_strict_fail"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"))

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **_kwargs):
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="gate_failed",
            exit_code=2,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.5},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)

    result = runner.invoke(
        app,
        ["triage", "loop", str(scenario_path), "--output-dir", str(tmp_path), "--strict"],
    )
    assert result.exit_code == 2, result.stdout + "\n" + result.stderr
    output = result.stdout + result.stderr
    assert "run dir →" in output


def test_fp_triage_loop_warns_on_fpexe_solution_errors(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run_solution_warning"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")

    class _EngineSummary:
        def __init__(self):
            self.details = {"solution_errors": [{"solve": "SOL1"}]}

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"))

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **_kwargs):
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="ok",
            exit_code=0,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
            engine_runs={"fpexe": _EngineSummary()},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)

    result = runner.invoke(
        app,
        ["triage", "loop", str(scenario_path), "--output-dir", str(tmp_path)],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    output = result.stdout + result.stderr
    assert "treat diffs as unreliable" in output


def test_fp_triage_loop_regression_failure_sets_exit_code(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run_regress"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")
    regression_dir = tmp_path / "golden"
    regression_dir.mkdir(parents=True, exist_ok=True)

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"))

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **_kwargs):
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="ok",
            exit_code=0,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    def _compare(_run_dir: Path, _golden_dir: Path):
        return {
            "status": "new_findings",
            "counts": {
                "new_missing_left": 0,
                "new_missing_right": 0,
                "new_hard_fail_cells": 1,
                "new_diff_variables": 0,
            },
        }

    def _write(payload: dict, _run_dir: Path):
        path = run_dir / "parity_regression.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)
    monkeypatch.setattr("fp_wraptr.analysis.parity_regression.compare_parity_to_golden", _compare)
    monkeypatch.setattr("fp_wraptr.analysis.parity_regression.write_regression_report", _write)

    result = runner.invoke(
        app,
        [
            "triage",
            "loop",
            str(scenario_path),
            "--output-dir",
            str(tmp_path),
            "--regression",
            str(regression_dir),
        ],
    )
    assert result.exit_code == 6, result.stdout + "\n" + result.stderr
    assert (run_dir / "parity_regression.json").exists()
