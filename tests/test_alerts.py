"""Tests for forecast alert threshold evaluation."""

from __future__ import annotations

from pathlib import Path

from fp_wraptr.analysis.alerts import check_alerts
from fp_wraptr.io.parser import ForecastVariable, FPOutputData
from fp_wraptr.scenarios.config import ScenarioConfig


def _build_output() -> FPOutputData:
    return FPOutputData(
        periods=["2025.3", "2025.4", "2026.1"],
        variables={
            "UR": ForecastVariable(
                var_id=1,
                name="UR",
                levels=[4.0, 5.5, 7.0],
            ),
            "PCY": ForecastVariable(
                var_id=2,
                name="PCY",
                levels=[2.0, 1.5, -1.0],
            ),
        },
    )


def test_alert_breached_max():
    output = _build_output()
    alerts = {"UR": {"max": 6.0}}

    breaches = check_alerts(alerts, output)

    assert len(breaches) == 1
    assert breaches[0].variable == "UR"
    assert breaches[0].threshold_type == "max"
    assert breaches[0].threshold_value == 6.0
    assert breaches[0].actual_value == 7.0
    assert breaches[0].period == "2026.1"


def test_alert_breached_min():
    output = _build_output()
    alerts = {"PCY": {"min": -0.5}}

    breaches = check_alerts(alerts, output)

    assert len(breaches) == 1
    assert breaches[0].variable == "PCY"
    assert breaches[0].threshold_type == "min"
    assert breaches[0].threshold_value == -0.5
    assert breaches[0].actual_value == -1.0
    assert breaches[0].period == "2026.1"


def test_alert_no_breach():
    output = _build_output()
    alerts = {
        "UR": {"max": 8.0},
        "PCY": {"min": -5.0},
    }

    breaches = check_alerts(alerts, output)

    assert len(breaches) == 0


def test_alert_missing_variable():
    output = _build_output()
    alerts = {"MISSING": {"max": 2.0}}

    breaches = check_alerts(alerts, output)

    assert breaches == []


def test_config_with_alerts(tmp_path: Path):
    config = ScenarioConfig(
        name="alerts-test",
        alerts={"UR": {"max": 6.0}, "PCY": {"min": -2.0}},
    )
    path = tmp_path / "scenario.yaml"

    config.to_yaml(path)
    loaded = ScenarioConfig.from_yaml(path)

    assert loaded.alerts == {"UR": {"max": 6.0}, "PCY": {"min": -2.0}}
