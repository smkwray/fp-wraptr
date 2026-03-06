"""Tests for forecast plotting helpers."""

import matplotlib

matplotlib.use("Agg")

import pytest

matplotlib_available = pytest.importorskip("matplotlib")

from fp_wraptr.io.parser import ForecastVariable, FPOutputData  # noqa: E402
from fp_wraptr.viz.plots import plot_comparison, plot_forecast  # noqa: E402


def _make_forecast_output() -> FPOutputData:
    return FPOutputData(
        forecast_start="2025.4",
        forecast_end="2026.2",
        periods=["2025.4", "2025.5", "2026.1", "2026.2"],
        variables={
            "PCY": ForecastVariable(
                var_id=1,
                name="PCY",
                levels=[1.0, 2.0, 2.5, 3.0],
                changes=[0.5, 0.5, 0.25, 0.2],
                pct_changes=[50.0, 25.0, 20.0, 15.0],
            ),
            "UR": ForecastVariable(
                var_id=2,
                name="UR",
                levels=[4.0, 3.8, 3.6, 3.4],
                changes=[-0.2, -0.2, -0.2, -0.2],
                pct_changes=[-5.0, -4.9, -4.8, -4.7],
            ),
        },
    )


def test_plot_forecast_basic(tmp_path):
    data = _make_forecast_output()
    output = tmp_path / "forecast.png"

    output_path = plot_forecast(data, output_path=output)

    assert output_path == output
    assert output.exists()
    assert output.stat().st_size > 0


def test_plot_forecast_selected_variables(tmp_path):
    data = _make_forecast_output()
    output = tmp_path / "forecast_selected.png"

    output_path = plot_forecast(data, variables=["PCY"], output_path=output)

    assert output_path == output
    assert output.exists()
    assert output.stat().st_size > 0


def test_plot_forecast_no_matching_variables(tmp_path):
    data = _make_forecast_output()
    output = tmp_path / "forecast_missing.png"

    with pytest.raises(ValueError):
        plot_forecast(data, variables=["NONEXISTENT"], output_path=output)

    assert not output.exists()


def test_plot_forecast_custom_title(tmp_path):
    data = _make_forecast_output()
    output = tmp_path / "forecast_title.png"

    output_path = plot_forecast(data, title="Custom Title", output_path=output)

    assert output_path == output
    assert output.exists()
    assert output.stat().st_size > 0


def test_plot_comparison_basic(tmp_path):
    baseline = _make_forecast_output()
    scenario = FPOutputData(
        forecast_start="2025.4",
        forecast_end="2026.2",
        periods=["2025.4", "2025.5", "2026.1", "2026.2"],
        variables={
            "PCY": ForecastVariable(
                var_id=1,
                name="PCY",
                levels=[1.0, 2.0, 2.7, 3.1],
                changes=[0.5, 0.5, 0.35, 0.2],
                pct_changes=[50.0, 25.0, 20.0, 15.0],
            ),
            "UR": ForecastVariable(
                var_id=2,
                name="UR",
                levels=[4.0, 3.9, 3.7, 3.6],
                changes=[-0.2, -0.2, -0.3, -0.2],
                pct_changes=[-5.0, -4.9, -4.8, -4.7],
            ),
        },
    )
    output = tmp_path / "comparison.png"

    output_path = plot_comparison(baseline=baseline, scenario=scenario, output_path=output)

    assert output_path == output
    assert output.exists()
    assert output.stat().st_size > 0


def test_plot_comparison_selected_variables(tmp_path):
    baseline = _make_forecast_output()
    scenario = _make_forecast_output()
    output = tmp_path / "comparison_selected.png"

    output_path = plot_comparison(
        baseline=baseline,
        scenario=scenario,
        variables=["PCY"],
        output_path=output,
    )

    assert output_path == output
    assert output.exists()
    assert output.stat().st_size > 0


def test_plot_comparison_no_common_variables(tmp_path):
    baseline = _make_forecast_output()
    scenario = _make_forecast_output()
    scenario.variables = {
        "OTHER": ForecastVariable(
            var_id=3,
            name="OTHER",
            levels=[1.0, 1.0, 1.0, 1.0],
            changes=[0.0, 0.0, 0.0, 0.0],
            pct_changes=[0.0, 0.0, 0.0, 0.0],
        )
    }
    output = tmp_path / "comparison_missing.png"

    with pytest.raises(ValueError):
        plot_comparison(
            baseline=baseline,
            scenario=scenario,
            variables=["NONEXISTENT"],
            output_path=output,
        )

    assert not output.exists()


def test_plot_comparison_auto_variables(tmp_path):
    baseline = _make_forecast_output()
    scenario = FPOutputData(
        forecast_start="2025.4",
        forecast_end="2026.2",
        periods=["2025.4", "2025.5", "2026.1", "2026.2"],
        variables={
            "PCY": ForecastVariable(
                var_id=1,
                name="PCY",
                levels=[1.0, 2.0, 2.8, 3.2],
                changes=[0.5, 0.5, 0.35, 0.2],
                pct_changes=[50.0, 25.0, 20.0, 15.0],
            ),
            "NONMATCH": ForecastVariable(
                var_id=3,
                name="NONMATCH",
                levels=[0.0, 0.0, 0.0, 0.0],
                changes=[0.0, 0.0, 0.0, 0.0],
                pct_changes=[0.0, 0.0, 0.0, 0.0],
            ),
        },
    )
    output = tmp_path / "comparison_auto.png"

    output_path = plot_comparison(
        baseline=baseline,
        scenario=scenario,
        variables=None,
        output_path=output,
    )

    assert output_path == output
    assert output.exists()
    assert output.stat().st_size > 0
