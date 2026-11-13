"""Tests for run-artifact discovery used by `fp history`, MCP and the Pages export."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import yaml

from fp_wraptr.run_artifacts import (
    RunArtifact,
    existing_overlay_paths,
    latest_runs,
    overlay_paths_for_runs,
    scan_artifacts,
)
from fp_wraptr.scenarios.config import ScenarioConfig


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


def test_existing_overlay_paths_applies_shared_extensions_before_scenario_overlay(
    tmp_path: Path,
) -> None:
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


def test_existing_overlay_paths_applies_global_baseline_before_scenario_overlay(
    tmp_path: Path,
) -> None:
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


def test_scan_artifacts_with_no_output_file(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    run_dir = _run_dir(root, "scenario_nofmout", "20260223_120000", has_output=False)

    runs = scan_artifacts(root)
    assert len(runs) == 1
    assert runs[0].run_dir == run_dir
    assert runs[0].has_output is False


def test_latest_runs_orders_by_timestamp_and_filters_on_output(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    _run_dir(root, "oldest", "20260223_110000")
    newest_without_output = _run_dir(root, "newest", "20260223_130000", has_output=False)
    middle = _run_dir(root, "middle", "20260223_120000")

    runs = scan_artifacts(root)

    assert [run.run_dir for run in latest_runs(runs, limit=1)] == [newest_without_output]
    with_output = latest_runs(runs, limit=2, has_output=True)
    assert [run.run_dir for run in with_output] == [
        middle,
        root / "group" / "oldest_20260223_110000",
    ]
    assert latest_runs(runs, limit=0) == []
