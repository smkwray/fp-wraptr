"""Tests for sensitivity analysis helpers."""

from __future__ import annotations

from pathlib import Path

from fp_wraptr.analysis.sensitivity import SensitivityResult, _make_sweep_config, run_sensitivity
from fp_wraptr.io.parser import ForecastVariable, FPOutputData
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import ScenarioResult


def _result_from_levels(
    config: ScenarioConfig,
    period_levels: dict[str, list[float]],
    periods: list[str],
    output_dir: Path,
) -> ScenarioResult:
    variables = {
        name: ForecastVariable(
            var_id=idx + 1,
            name=name,
            levels=levels,
            changes=[],
            pct_changes=[],
        )
        for idx, (name, levels) in enumerate(period_levels.items())
    }

    return ScenarioResult(
        config=config,
        output_dir=output_dir,
        parsed_output=FPOutputData(
            forecast_start=periods[0],
            forecast_end=periods[-1],
            periods=periods,
            variables=variables,
        ),
    )


def test_sensitivity_result_properties():
    result = SensitivityResult(
        sweep_variable="YS",
        sweep_values=[0.01, 0.02, 0.03],
        results=[],
        response_table={"GDPR": [100.0, 101.0, 102.0], "UR": [4.0, 3.9, 3.8]},
    )

    assert result.n_runs == 0
    assert result.to_dict()["n_runs"] == 0
    assert set(result.to_dict()["response_table"].keys()) == {"GDPR", "UR"}


def test_run_sensitivity_basic(tmp_path, monkeypatch):
    base = ScenarioConfig(name="base", track_variables=["GDPR", "UR"])
    periods = ["2025.4", "2025.5", "2025.6"]
    runs_payload = [
        {"GDPR": [100.0, 100.5, 101.0], "UR": [4.0, 3.8, 3.6]},
        {"GDPR": [100.0, 100.7, 101.4], "UR": [4.0, 3.7, 3.5]},
        {"GDPR": [100.0, 100.9, 101.8], "UR": [4.0, 3.6, 3.4]},
    ]

    def fake_run_scenario(config, output_dir=None, backend=None):
        idx = len(run_calls)
        run_calls.append(config)
        return _result_from_levels(
            config,
            period_levels=runs_payload[idx],
            periods=periods,
            output_dir=tmp_path / f"sweep_{idx}",
        )

    run_calls: list[ScenarioConfig] = []
    monkeypatch.setattr("fp_wraptr.analysis.sensitivity.run_scenario", fake_run_scenario)

    result = run_sensitivity(
        base,
        "YS",
        [0.01, 0.02, 0.03],
        track_variables=["GDPR", "UR"],
    )

    assert result.n_runs == 3
    assert set(result.response_table.keys()) == {"GDPR", "UR"}
    assert len(result.response_table["GDPR"]) == 3
    assert len(result.response_table["UR"]) == 3


def test_run_sensitivity_missing_variable(tmp_path, monkeypatch):
    base = ScenarioConfig(name="base", track_variables=["GDPR", "UR"])
    calls: list[int] = []

    def fake_run_scenario(config, output_dir=None, backend=None):
        calls.append(len(calls))
        return _result_from_levels(
            config,
            period_levels={"GDPR": [100.0, 100.5, 101.0]},
            periods=["2025.4", "2025.5", "2025.6"],
            output_dir=tmp_path / f"missing_{len(calls)}",
        )

    monkeypatch.setattr("fp_wraptr.analysis.sensitivity.run_scenario", fake_run_scenario)

    result = run_sensitivity(
        base,
        "YS",
        [0.01, 0.02, 0.03],
        track_variables=["GDPR", "UR"],
    )

    assert result.response_table["GDPR"] == [101.0, 101.0, 101.0]
    assert result.response_table["UR"] == [None, None, None]


def test_make_sweep_config():
    base = ScenarioConfig(name="base")

    swept = _make_sweep_config(base, "YS", 0.05, "CHGSAMEPCT")

    assert "YS" in swept.overrides
    assert swept.overrides["YS"].method == "CHGSAMEPCT"
    assert swept.overrides["YS"].value == 0.05


def test_run_sensitivity_custom_method(tmp_path, monkeypatch):
    base = ScenarioConfig(name="base", track_variables=["GDPR"])
    methods: list[str] = []

    def fake_run_scenario(config, output_dir=None, backend=None):
        methods.append(config.overrides["YS"].method)
        return _result_from_levels(
            config,
            period_levels={"GDPR": [100.0, 100.5, 101.0]},
            periods=["2025.4", "2025.5", "2025.6"],
            output_dir=tmp_path / f"custom_{len(methods)}",
        )

    monkeypatch.setattr("fp_wraptr.analysis.sensitivity.run_scenario", fake_run_scenario)

    result = run_sensitivity(
        base,
        "YS",
        [0.01, 0.02, 0.03],
        method="SAMEVALUE",
        track_variables=["GDPR"],
    )

    assert result.n_runs == 3
    assert methods == ["SAMEVALUE", "SAMEVALUE", "SAMEVALUE"]
