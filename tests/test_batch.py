"""Tests for batch scenario runner helpers."""

from __future__ import annotations

import pytest

from fp_wraptr.scenarios.batch import compare_to_golden, run_batch, save_golden
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import ScenarioResult


def test_run_batch_executes_multiple(tmp_path, monkeypatch):
    from fp_wraptr.runtime.fp_exe import FPRunResult
    from fp_wraptr.scenarios.runner import FPExecutable

    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text("PRINTVAR FILEOUT=OUT.DAT GDP;\n", encoding="utf-8")

    def fake_run(self, input_file, work_dir, extra_env=None):
        output_path = work_dir / "fmout.txt"
        output_path.write_text(
            "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;\n",
            encoding="utf-8",
        )
        return FPRunResult(
            return_code=0,
            stdout="ok",
            stderr="",
            working_dir=work_dir,
            input_file=input_file,
            output_file=output_path,
            duration_seconds=0.01,
        )

    monkeypatch.setattr(FPExecutable, "check_available", lambda _self: True)
    monkeypatch.setattr(FPExecutable, "run", fake_run)

    configs = [
        ScenarioConfig(name="batch_a", fp_home=fp_home),
        ScenarioConfig(name="batch_b", fp_home=fp_home),
    ]
    results = run_batch(configs, output_dir=tmp_path / "batch_runs")

    assert len(results) == 2
    assert results[0].output_dir.exists()
    assert results[1].output_dir.exists()


def test_compare_to_golden_and_save_golden(tmp_path):
    from fp_wraptr.scenarios.runner import ScenarioResult

    config = ScenarioConfig(name="compare")
    output_dir = tmp_path / "run"
    output_dir.mkdir()
    (output_dir / "fmout.txt").write_text(
        """\
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
Variable   Periods forecast are  2025.4  TO   2025.4

                   2025.4      2025.5
                               2026.1

   1 GDP      P lv   1.0      2.0
             P ch   0.1      0.1
             P %ch  10.0     10.0
""",
        encoding="utf-8",
    )
    (tmp_path / "golden").mkdir()
    (tmp_path / "golden" / "fmout.txt").write_text(
        """\
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
Variable   Periods forecast are  2025.4  TO   2025.4

                   2025.4      2025.5
                               2026.1

   1 GDP      P lv   1.0      1.95
             P ch   0.1      0.1
             P %ch  10.0     10.0
""",
        encoding="utf-8",
    )

    result = ScenarioResult(
        config=config,
        output_dir=output_dir,
    )

    comparison = compare_to_golden(result, tmp_path / "golden", tolerance=0.2)
    assert comparison["matches"] is True
    assert "GDP" in comparison["variable_diffs"]

    saved = save_golden(result, tmp_path / "golden_out")
    assert saved.exists()
    assert (tmp_path / "golden_out" / "fmout.txt").read_text(encoding="utf-8").strip() == (
        output_dir / "fmout.txt"
    ).read_text(encoding="utf-8").strip()


def test_run_batch_with_baseline_comparison(tmp_path, monkeypatch):
    def build_output(value: float) -> str:
        return (
            "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;\n"
            "Variable   Periods forecast are  2025.4  TO   2025.4\n"
            "\n"
            "                   2025.4      2025.5\n"
            "                               2026.1\n"
            "\n"
            f"   1 GDP      P lv   1.0      {value}\n"
            "             P ch   0.1      0.1\n"
            "             P %ch  10.0     10.0\n"
        )

    scenario_outputs = {
        "baseline": build_output(2.0),
        "scenario": build_output(2.0),
    }

    def fake_run_scenario(config, output_dir):
        run_dir = output_dir / config.name
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "fmout.txt").write_text(scenario_outputs[config.name], encoding="utf-8")
        return ScenarioResult(
            config=config,
            output_dir=run_dir,
            parsed_output=None,
            run_result=None,
        )

    monkeypatch.setattr("fp_wraptr.scenarios.runner.run_scenario", fake_run_scenario)

    baseline_dir = tmp_path / "golden"
    configs = [ScenarioConfig(name="baseline"), ScenarioConfig(name="scenario")]
    for config in configs:
        synthetic_dir = tmp_path / "synthetic_baseline" / config.name
        synthetic_dir.mkdir(parents=True, exist_ok=True)
        synthetic_output = synthetic_dir / "fmout.txt"
        synthetic_output.write_text(scenario_outputs[config.name], encoding="utf-8")
        save_golden(
            ScenarioResult(config=config, output_dir=synthetic_dir), baseline_dir / config.name
        )

    results = run_batch(configs, output_dir=tmp_path / "batch_runs", baseline_dir=baseline_dir)
    assert len(results) == 2
    assert results[0].golden_comparison is not None
    assert results[1].golden_comparison is not None
    assert results[0].golden_comparison["matches"] is True
    assert results[1].golden_comparison["matches"] is True


def test_run_batch_empty_configs(tmp_path):
    results = run_batch([], output_dir=tmp_path / "empty_batch")
    assert results == []


def test_compare_to_golden_missing_golden_dir(tmp_path):
    config = ScenarioConfig(name="missing")
    output_dir = tmp_path / "run"
    output_dir.mkdir()
    (output_dir / "fmout.txt").write_text(
        """\
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
Variable   Periods forecast are  2025.4  TO   2025.4

                   2025.4      2025.5
                               2026.1

   1 GDP      P lv   1.0      2.0
             P ch   0.1      0.1
             P %ch  10.0     10.0
""",
        encoding="utf-8",
    )
    result = ScenarioResult(config=config, output_dir=output_dir)
    comparison = compare_to_golden(result, tmp_path / "does_not_exist")
    assert comparison["matches"] is False


def test_save_golden_missing_fmout(tmp_path):
    result = ScenarioResult(config=ScenarioConfig(name="missing"), output_dir=tmp_path / "run")
    with pytest.raises(FileNotFoundError):
        save_golden(result, tmp_path / "golden")
