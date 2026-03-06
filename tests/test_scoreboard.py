"""Tests for scoreboard computations."""

from __future__ import annotations

from fp_wraptr.analysis.scoreboard import compute_jg_metrics, compute_scoreboard
from fp_wraptr.io.parser import ForecastVariable, FPOutputData


def _build_output(
    *,
    gdp: list[float],
    ur: list[float],
    pcy: list[float],
) -> FPOutputData:
    periods = ["2025.3", "2025.4", "2026.1"]
    return FPOutputData(
        periods=periods,
        variables={
            "GDPR": ForecastVariable(var_id=1, name="GDPR", levels=gdp),
            "UR": ForecastVariable(var_id=2, name="UR", levels=ur),
            "PCY": ForecastVariable(var_id=3, name="PCY", levels=pcy),
        },
    )


def test_compute_scoreboard_macro_core() -> None:
    scenario = _build_output(gdp=[100.0, 101.0, 103.0], ur=[4.0, 3.8, 3.6], pcy=[2.0, 2.1, 2.2])
    baseline = _build_output(gdp=[100.0, 100.5, 102.0], ur=[4.0, 4.0, 4.0], pcy=[2.0, 2.0, 2.0])

    board = compute_scoreboard(
        scenario, baseline, scenario_name="policy", baseline_name="baseline"
    )

    assert board.scenario_name == "policy"
    assert board.baseline_name == "baseline"
    assert board.has_baseline is True
    assert len(board.metrics) == 3 * 3
    ur_metric = next(
        item for item in board.metrics if item.variable == "UR" and item.period == "2026.1"
    )
    assert ur_metric.deviation == -0.4
    assert ur_metric.pct_deviation == -10.0


def test_compute_scoreboard_no_baseline() -> None:
    scenario = _build_output(gdp=[100.0, 101.0, 102.0], ur=[4.0, 3.9, 3.8], pcy=[2.0, 2.1, 2.2])
    board = compute_scoreboard(scenario, scenario_name="solo")

    assert board.has_baseline is False
    sample = board.metrics[0]
    assert sample.baseline_level is None
    assert sample.deviation is None


def test_scoreboard_to_dict() -> None:
    scenario = _build_output(gdp=[100.0, 101.0, 102.0], ur=[4.0, 3.9, 3.8], pcy=[2.0, 2.1, 2.2])
    board = compute_scoreboard(scenario, scenario_name="solo")
    payload = board.to_dict()

    assert payload["scenario_name"] == "solo"
    assert payload["has_baseline"] is False
    assert "summary" in payload
    assert isinstance(payload["metrics"], list)


def test_jg_metrics_pack() -> None:
    scenario = FPOutputData(
        periods=["2025.3", "2025.4", "2026.1"],
        variables={
            "GDPR": ForecastVariable(var_id=1, name="GDPR", levels=[100.0, 100.5, 101.0]),
            "UR": ForecastVariable(var_id=2, name="UR", levels=[4.0, 3.9, 3.7]),
            "SGP": ForecastVariable(var_id=3, name="SGP", levels=[-10.0, -9.5, -9.0]),
        },
    )
    baseline = FPOutputData(
        periods=["2025.3", "2025.4", "2026.1"],
        variables={
            "GDPR": ForecastVariable(var_id=1, name="GDPR", levels=[100.0, 100.0, 100.0]),
            "UR": ForecastVariable(var_id=2, name="UR", levels=[4.0, 4.0, 4.0]),
            "SGP": ForecastVariable(var_id=3, name="SGP", levels=[-10.0, -10.0, -10.0]),
        },
    )

    payload = compute_jg_metrics(
        scenario=scenario,
        baseline=baseline,
        gross_cost_bn=1.0,
        scenario_name="jg",
        baseline_name="baseline",
    )

    assert payload["scenario_name"] == "jg"
    assert payload["baseline_name"] == "baseline"
    assert payload["gross_cost_bn"] == 1.0
    assert payload["net_fiscal_impact_bn"] == 1.0
