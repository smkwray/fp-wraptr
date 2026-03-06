"""Tests for dashboard modules."""

from __future__ import annotations

import importlib.util
import os
import textwrap
from pathlib import Path
from types import ModuleType

import pytest
import yaml

plotly = pytest.importorskip("plotly")
go = plotly.graph_objects

from fp_wraptr.dashboard.artifacts import (  # noqa: E402
    RunArtifact,
    existing_overlay_paths,
    overlay_paths_for_runs,
    scan_artifacts,
)
from fp_wraptr.dashboard.charts import (  # noqa: E402
    comparison_figure,
    delta_bar_chart,
    forecast_figure,
)
from fp_wraptr.io.parser import ForecastVariable, FPOutputData  # noqa: E402
from fp_wraptr.scenarios.config import ScenarioConfig  # noqa: E402


def _run_dir(
    artifacts_root: Path, scenario_name: str, timestamp: str, has_output: bool = True
) -> Path:
    run_dir = artifacts_root / "group" / f"{scenario_name}_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump({
            "name": scenario_name,
            "description": "test run",
            "track_variables": ["PCY", "UR"],
        }),
        encoding="utf-8",
    )
    if has_output:
        (run_dir / "fmout.txt").write_text(_SYNTHETIC_FMOUT, encoding="utf-8")
    if timestamp.endswith("01"):
        (run_dir / "forecast.png").write_bytes(b"PNG")
    return run_dir


def _build_output() -> FPOutputData:
    return FPOutputData(
        periods=["2025.3", "2025.4", "2026.1", "2026.2"],
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


_SYNTHETIC_FMOUT = textwrap.dedent(
    """
    SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
    Variable   Periods forecast are  2025.4  TO   2025.6

                       2025.4      2025.5      2025.6
                                   2026.1

     1 PCY      P lv   1.0         1.2         1.3
                                  1.4
                P ch   0.1         0.1         0.1
                                  0.1
                P %ch  10.0        10.0        10.0
                                  10.0

     2 UR       P lv   4.0         3.8         3.6
                                  3.4
                P ch  -0.2        -0.2        -0.2
                                  -0.2
                P %ch -5.0       -4.9        -4.8
                                  -4.7
    """
).strip()


def test_scan_artifacts_discovers_runs(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    newest = _run_dir(root, "scenario_b", "20260223_120001")
    _run_dir(root, "scenario_a", "20260223_120000")
    runs = scan_artifacts(root)

    assert len(runs) == 2
    assert runs[0].run_dir == newest
    assert runs[0].scenario_name == "scenario_b"
    assert runs[0].timestamp == "20260223_120001"
    assert runs[0].has_output is True
    assert runs[0].config is not None
    assert runs[0].display_name == "scenario_b (20260223_120001)"


def test_scan_artifacts_with_corrupted_scenario_yaml(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    run_dir = root / "bad_20260223_120000"
    run_dir.mkdir(parents=True)
    run_dir.joinpath("scenario.yaml").write_text(
        yaml.safe_dump({
            "name": "bad_run",
            "track_variables": {"not": "a list"},
        }),
        encoding="utf-8",
    )
    run_dir.joinpath("fmout.txt").write_text("SOLVE DYNAMIC;\n", encoding="utf-8")

    runs = scan_artifacts(root)

    assert len(runs) == 1
    assert runs[0].config is None


def test_scan_artifacts_ignores_parity_subtree(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    regular_run = _run_dir(root, "baseline", "20260223_120000")
    _run_dir(root / "parity", "baseline", "20260223_120001")

    runs = scan_artifacts(root)

    assert len(runs) == 1
    assert runs[0].run_dir == regular_run


def test_scan_artifacts_ignores_sensitivity_and_hidden_subtrees(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    regular_run = _run_dir(root, "baseline", "20260223_120000")
    _run_dir(root / "sensitivity", "baseline", "20260223_120001")
    _run_dir(root / ".dashboard" / "sensitivity", "baseline", "20260223_120002")

    runs = scan_artifacts(root)

    assert len(runs) == 1
    assert runs[0].run_dir == regular_run


def test_scan_empty_dir(tmp_path: Path) -> None:
    assert scan_artifacts(tmp_path / "empty") == []


def test_existing_overlay_paths_applies_shared_extensions_before_scenario_overlay(tmp_path: Path) -> None:
    project_root = tmp_path
    overlay_dir = project_root / "projects_local" / "pse2025"
    overlay_ext = overlay_dir / "dictionary_extensions"
    overlay_ext.mkdir(parents=True, exist_ok=True)
    (overlay_ext / "00-shared.json").write_text("{}", encoding="utf-8")
    (overlay_ext / "10-sector.json").write_text("{}", encoding="utf-8")

    global_ext = project_root / "projects_local" / "dictionary_extensions"
    global_ext.mkdir(parents=True, exist_ok=True)
    (global_ext / "20-global.json").write_text("{}", encoding="utf-8")

    run_dir = project_root / "artifacts" / "baseline_20260223_120000"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "dictionary_overlay.json").write_text("{}", encoding="utf-8")
    scenario_overlay = overlay_dir / "dictionary_overlays" / "baseline.json"
    scenario_overlay.parent.mkdir(parents=True, exist_ok=True)
    scenario_overlay.write_text("{}", encoding="utf-8")

    config = ScenarioConfig(
        name="baseline",
        fp_home=Path("FM"),
        input_file="fminput.txt",
        input_overlay_dir=overlay_dir,
        forecast_start="2025.4",
        forecast_end="2029.4",
    )
    run = RunArtifact(
        run_dir=run_dir,
        scenario_name="baseline",
        timestamp="20260223_120000",
        has_output=True,
        has_chart=False,
        config=config,
    )

    previous_cwd = Path.cwd()
    try:
        os.chdir(project_root)
        paths = existing_overlay_paths(run)
    finally:
        os.chdir(previous_cwd)

    names = [path.name for path in paths]
    assert names[:3] == ["00-shared.json", "10-sector.json", "20-global.json"]
    assert "dictionary_overlay.json" in names
    assert "baseline.json" in names


def test_existing_overlay_paths_maps_stock_fm_baseline_to_baseline_overlay(tmp_path: Path) -> None:
    project_root = tmp_path
    shared_root = project_root / "projects_local" / "dictionary_overlays"
    shared_root.mkdir(parents=True, exist_ok=True)
    (shared_root / "baseline.json").write_text("{}", encoding="utf-8")

    run_dir = project_root / "artifacts" / "stock_fm_baseline_20260223_120000"
    run_dir.mkdir(parents=True, exist_ok=True)

    config = ScenarioConfig(
        name="stock_fm_baseline",
        fp_home=Path("FM"),
        input_file="fminput.txt",
        forecast_start="2025.4",
        forecast_end="2029.4",
    )
    run = RunArtifact(
        run_dir=run_dir,
        scenario_name="stock_fm_baseline",
        timestamp="20260223_120000",
        has_output=True,
        has_chart=False,
        config=config,
    )

    previous_cwd = Path.cwd()
    try:
        os.chdir(project_root)
        paths = existing_overlay_paths(run)
    finally:
        os.chdir(previous_cwd)

    assert any(path.name == "baseline.json" for path in paths)


def test_existing_overlay_paths_applies_global_baseline_before_scenario_overlay(tmp_path: Path) -> None:
    project_root = tmp_path
    shared_root = project_root / "projects_local" / "dictionary_overlays"
    shared_root.mkdir(parents=True, exist_ok=True)
    (shared_root / "baseline.json").write_text("{}", encoding="utf-8")

    overlay_dir = project_root / "projects_local" / "pse2025"
    scenario_overlay = overlay_dir / "dictionary_overlays" / "pse2025_base.json"
    scenario_overlay.parent.mkdir(parents=True, exist_ok=True)
    scenario_overlay.write_text("{}", encoding="utf-8")

    run_dir = project_root / "artifacts" / "pse2025_base_20260223_120000"
    run_dir.mkdir(parents=True, exist_ok=True)

    config = ScenarioConfig(
        name="pse2025_base",
        fp_home=Path("FM"),
        input_file="fminput.txt",
        input_overlay_dir=overlay_dir,
        forecast_start="2025.4",
        forecast_end="2029.4",
    )
    run = RunArtifact(
        run_dir=run_dir,
        scenario_name="pse2025_base",
        timestamp="20260223_120000",
        has_output=True,
        has_chart=False,
        config=config,
    )

    previous_cwd = Path.cwd()
    try:
        os.chdir(project_root)
        paths = existing_overlay_paths(run)
    finally:
        os.chdir(previous_cwd)

    names = [path.name for path in paths]
    assert "baseline.json" in names
    assert "pse2025_base.json" in names
    assert names.index("baseline.json") < names.index("pse2025_base.json")


def test_overlay_paths_for_runs_dedupes_shared_extension_files(tmp_path: Path) -> None:
    project_root = tmp_path
    overlay_dir = project_root / "projects_local" / "pse2025"
    shared_ext = overlay_dir / "dictionary_extensions" / "00-shared.json"
    shared_ext.parent.mkdir(parents=True, exist_ok=True)
    shared_ext.write_text("{}", encoding="utf-8")

    scenario_overlay_dir = overlay_dir / "dictionary_overlays"
    scenario_overlay_dir.mkdir(parents=True, exist_ok=True)
    (scenario_overlay_dir / "baseline.json").write_text("{}", encoding="utf-8")
    (scenario_overlay_dir / "alt.json").write_text("{}", encoding="utf-8")

    config = ScenarioConfig(
        name="baseline",
        fp_home=Path("FM"),
        input_file="fminput.txt",
        input_overlay_dir=overlay_dir,
        forecast_start="2025.4",
        forecast_end="2029.4",
    )
    run_a = RunArtifact(
        run_dir=project_root / "artifacts" / "baseline_20260223_120000",
        scenario_name="baseline",
        timestamp="20260223_120000",
        has_output=True,
        has_chart=False,
        config=config,
    )
    run_b = RunArtifact(
        run_dir=project_root / "artifacts" / "alt_20260223_120001",
        scenario_name="alt",
        timestamp="20260223_120001",
        has_output=True,
        has_chart=False,
        config=ScenarioConfig(
            name="alt",
            fp_home=Path("FM"),
            input_file="fminput.txt",
            input_overlay_dir=overlay_dir,
            forecast_start="2025.4",
            forecast_end="2029.4",
        ),
    )

    run_a.run_dir.mkdir(parents=True, exist_ok=True)
    run_b.run_dir.mkdir(parents=True, exist_ok=True)

    previous_cwd = Path.cwd()
    try:
        os.chdir(project_root)
        paths = overlay_paths_for_runs([run_a, run_b])
    finally:
        os.chdir(previous_cwd)

    names = [path.name for path in paths]
    assert names.count("00-shared.json") == 1
    assert "baseline.json" in names
    assert "alt.json" in names


def test_run_artifact_display_name() -> None:
    artifact = RunArtifact(
        run_dir=Path("/tmp"),
        scenario_name="baseline",
        timestamp="20260223_120000",
        has_output=False,
        has_chart=False,
        config=None,
    )
    assert artifact.display_name == "baseline (20260223_120000)"


def test_forecast_figure() -> None:
    figure = forecast_figure(_build_output(), variables=["PCY", "UR"], title="Forecast")
    assert isinstance(figure, go.Figure)
    assert len(figure.data) == 2
    assert figure.layout.paper_bgcolor == "white"


def test_forecast_figure_modes() -> None:
    output = _build_output()
    for mode in ("levels", "changes", "pct_changes"):
        figure = forecast_figure(output, variables=["PCY", "UR"], mode=mode, title="Forecast")
        assert isinstance(figure, go.Figure)
        assert len(figure.data) == 2


def test_comparison_figure() -> None:
    baseline = _build_output()
    scenario = _build_output()
    scenario.variables["PCY"].levels[-1] = 3.2

    figure = comparison_figure(baseline, scenario, variables=["PCY", "UR"], title="Compare")
    assert isinstance(figure, go.Figure)
    assert len(figure.data) == 4
    assert figure.layout.paper_bgcolor == "white"


def test_delta_bar_chart() -> None:
    figure = delta_bar_chart(
        {
            "deltas": {
                "PCY": {"baseline": 1.0, "scenario": 1.5, "abs_delta": 0.5, "pct_delta": 50.0},
                "UR": {"baseline": 2.0, "scenario": 1.0, "abs_delta": -1.0, "pct_delta": -50.0},
                "INF": {"baseline": 0.5, "scenario": 0.8, "abs_delta": 0.3, "pct_delta": 60.0},
            }
        },
        top_n=2,
    )
    assert isinstance(figure, go.Figure)
    assert figure.data[0].orientation == "h"
    assert len(figure.data[0].x) == 2
    assert figure.layout.paper_bgcolor == "white"


def test_forecast_figure_no_variables() -> None:
    figure = forecast_figure(_build_output(), variables=["MISSING"])

    assert isinstance(figure, go.Figure)
    assert len(figure.data) == 0


def test_comparison_figure_no_common_variables() -> None:
    figure = comparison_figure(_build_output(), _build_output(), variables=["MISSING"])

    assert isinstance(figure, go.Figure)
    assert len(figure.data) == 0


def test_forecast_figure_with_empty_periods() -> None:
    figure = forecast_figure(
        FPOutputData(
            periods=[],
            variables={
                "PCY": ForecastVariable(
                    var_id=1,
                    name="PCY",
                    levels=[1.0, 2.0],
                    changes=[1.0, 1.0],
                    pct_changes=[10.0, 10.0],
                )
            },
        ),
        variables=["PCY"],
    )

    assert isinstance(figure, go.Figure)
    assert len(figure.data) == 1
    assert list(figure.data[0].x) == [0, 1]


def test_comparison_figure_with_mismatched_periods() -> None:
    baseline = _build_output()
    scenario = _build_output()
    baseline.periods = ["2025.3", "2025.4", "2026.1", "2026.2"]
    scenario.periods = ["2025.3", "2025.4"]

    figure = comparison_figure(
        baseline,
        scenario,
        variables=["PCY"],
        title="Compare",
    )

    assert isinstance(figure, go.Figure)
    assert len(figure.data) == 2
    assert len(figure.data[0].x) == 2
    assert len(figure.data[1].x) == 2


def test_delta_bar_chart_top_n_allows_empty_input() -> None:
    figure = delta_bar_chart({"deltas": {}}, top_n=10)

    assert isinstance(figure, go.Figure)
    assert figure.layout.height == 300


def test_scan_artifacts_with_no_output_file(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    run_dir = _run_dir(root, "scenario_nofmout", "20260223_120000", has_output=False)

    runs = scan_artifacts(root)
    assert len(runs) == 1
    assert runs[0].run_dir == run_dir
    assert runs[0].has_output is False


def test_delta_bar_chart_handles_none_pct_delta() -> None:
    figure = delta_bar_chart(
        {
            "deltas": {
                "PCY": {
                    "baseline": 1.0,
                    "scenario": 2.0,
                    "abs_delta": 1.0,
                    "pct_delta": None,
                },
                "UR": {"baseline": 1.0, "scenario": 1.0, "abs_delta": 0.0, "pct_delta": 0.0},
            }
        },
        top_n=2,
        sort_by="pct_delta",
    )

    assert isinstance(figure, go.Figure)
    assert len(figure.data[0].x) == 2
    assert "N/A" in list(figure.data[0].text)


def test_historical_fit_page_import() -> None:
    page_path = (
        Path(__file__).resolve().parents[1]
        / "apps"
        / "dashboard"
        / "pages"
        / "8_Historical_Fit.py"
    )
    spec = importlib.util.spec_from_file_location("historical_fit_page", page_path)
    module = importlib.util.module_from_spec(spec) if spec and spec.loader else None
    assert isinstance(module, ModuleType)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    assert hasattr(module, "main")
