"""Tests for multiplier calculations."""

from __future__ import annotations

import json

from fp_wraptr.analysis.multipliers import MultiplierResult, MultiplierRow, compute_multipliers
from fp_wraptr.io.parser import ForecastVariable, FPOutputData
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import ScenarioResult


def _build_output(levels_by_variable: dict[str, list[float]]) -> FPOutputData:
    periods = ["2025.4", "2025.5", "2025.6"]
    variables = {}
    for idx, (name, levels) in enumerate(levels_by_variable.items(), start=1):
        variables[name] = ForecastVariable(
            var_id=idx,
            name=name,
            levels=levels,
            changes=[],
            pct_changes=[],
        )
    return FPOutputData(periods=periods, variables=variables)


def test_multiplier_result_properties():
    rows = [
        MultiplierRow(
            period="2025.4",
            response_var="GDPR",
            baseline_level=100.0,
            shocked_level=101.0,
            delta=1.0,
            multiplier=1.0,
        ),
        MultiplierRow(
            period="2025.4",
            response_var="UR",
            baseline_level=4.0,
            shocked_level=3.9,
            delta=-0.1,
            multiplier=-0.5,
        ),
        MultiplierRow(
            period="2025.5",
            response_var="GDPR",
            baseline_level=100.0,
            shocked_level=101.5,
            delta=1.5,
            multiplier=1.5,
        ),
    ]
    result = MultiplierResult(
        shock_variable="YS",
        shock_size=0.01,
        response_variables=["GDPR", "UR"],
        rows=rows,
    )

    assert result.n_periods == 2
    table = result.table()
    assert table["GDPR"]["2025.4"] == 1.0
    assert table["UR"]["2025.4"] == -0.5

    payload = result.to_dict()
    assert json.dumps(payload)


def test_compute_multipliers_basic():
    baseline = _build_output({"GDPR": [100.0, 101.0, 102.0]})
    shocked = _build_output({"GDPR": [100.0, 102.0, 104.0]})

    result = compute_multipliers(baseline, shocked, "TBGQ", 0.01)

    assert result.response_variables == ["GDPR"]
    assert result.table()["GDPR"]["2025.5"] == 100.0


def test_compute_multipliers_with_scenario_results(tmp_path):
    baseline_output = _build_output({"GDPR": [100.0, 101.0, 102.0]})
    shocked_output = _build_output({"GDPR": [100.0, 102.0, 104.0]})

    baseline = ScenarioResult(
        config=ScenarioConfig(name="baseline"),
        output_dir=tmp_path / "baseline",
        parsed_output=baseline_output,
    )
    shocked = ScenarioResult(
        config=ScenarioConfig(name="shocked"),
        output_dir=tmp_path / "shocked",
        parsed_output=shocked_output,
    )

    result = compute_multipliers(baseline, shocked, "TBGQ", 0.01)

    assert result.response_variables == ["GDPR"]
    assert result.table()["GDPR"]["2025.5"] == 100.0


def test_compute_multipliers_selected_variables():
    baseline = _build_output({"GDPR": [100.0, 101.0, 102.0], "UR": [4.0, 3.9, 3.8]})
    shocked = _build_output({"GDPR": [100.0, 102.0, 104.0], "UR": [4.0, 3.7, 3.5]})

    result = compute_multipliers(
        baseline,
        shocked,
        "TBGQ",
        0.01,
        response_variables=["GDPR"],
    )

    assert result.response_variables == ["GDPR"]
    assert set(result.table().keys()) == {"GDPR"}


def test_compute_multipliers_zero_shock():
    baseline = _build_output({"GDPR": [100.0, 101.0, 102.0]})
    shocked = _build_output({"GDPR": [100.0, 102.0, 104.0]})

    result = compute_multipliers(baseline, shocked, "TBGQ", 0.0)

    assert all(row.multiplier is None for row in result.rows)


def test_compute_multipliers_no_output(tmp_path):
    baseline = ScenarioResult(
        config=ScenarioConfig(name="baseline"),
        output_dir=tmp_path / "baseline",
        parsed_output=None,
    )
    shocked = ScenarioResult(
        config=ScenarioConfig(name="shocked"),
        output_dir=tmp_path / "shocked",
        parsed_output=None,
    )

    result = compute_multipliers(baseline, shocked, "TBGQ", 0.01)

    assert result.rows == []
    assert result.n_periods == 0
