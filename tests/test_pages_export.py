"""Tests for the portable GitHub Pages run-export workflow."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from fp_wraptr.cli import app
from fp_wraptr.model_runs_semantics import (
    COMPARE_DIFF_VS_RUN,
    COMPARE_PCT_DIFF_VS_RUN,
    TRANSFORM_LVL_CHANGE,
    TRANSFORM_PCT_CHANGE,
    TRANSFORM_PCT_OF,
    apply_run_comparison,
    transform_series,
)
from fp_wraptr.pages_export import PagesExportError, export_pages_bundle

runner = CliRunner()


def _write_loadformat(path: Path, *, wf: list[float], pf: list[float], gdp: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join([
            "SMPL 2025.1 2025.4;",
            "LOAD WF;",
            " " + " ".join(str(value) for value in wf),
            "'END'",
            "LOAD PF;",
            " " + " ".join(str(value) for value in pf),
            "'END'",
            "LOAD GDP;",
            " " + " ".join(str(value) for value in gdp),
            "'END'",
            "LOAD GDPR;",
            " " + " ".join(str(value) for value in gdp),
            "'END'",
            "",
        ]),
        encoding="utf-8",
    )


def _write_run(
    root: Path,
    *,
    scenario_name: str,
    timestamp: str,
    forecast_start: str = "2025.1",
    forecast_end: str = "2025.4",
    input_overlay_dir: Path | None = None,
    wf: list[float] | None = None,
    pf: list[float] | None = None,
    gdp: list[float] | None = None,
) -> Path:
    run_dir = root / "artifacts" / f"{scenario_name}_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_payload: dict[str, object] = {
        "name": scenario_name,
        "forecast_start": forecast_start,
        "forecast_end": forecast_end,
        "backend": "fpexe",
    }
    if input_overlay_dir is not None:
        scenario_payload["input_overlay_dir"] = str(input_overlay_dir)
    (run_dir / "scenario.yaml").write_text(yaml.safe_dump(scenario_payload), encoding="utf-8")
    _write_loadformat(
        run_dir / "LOADFORMAT.DAT",
        wf=wf or [1.0, 2.0, 3.0, 4.0],
        pf=pf or [10.0, 10.0, 10.0, 10.0],
        gdp=gdp or [100.0, 101.0, 102.0, 103.0],
    )
    return run_dir


def _write_spec(
    path: Path, *, title: str = "Fixture Explorer", scenario_name: str = "scenario"
) -> None:
    payload = {
        "version": 1,
        "title": title,
        "site_subpath": "model-runs",
        "runs": [
            {
                "run_id": "fixture-run",
                "label": "Fixture Run",
                "scenario_name": scenario_name,
                "summary": "Fixture summary",
                "details": ["Fixture detail A", "Fixture detail B"],
            }
        ],
        "default_run_ids": ["fixture-run"],
        "presets": [
            {
                "id": "fixture-preset",
                "label": "Fixture Preset",
                "variables": ["GDP", "WF", "WR"],
            }
        ],
        "default_preset_ids": ["fixture-preset"],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def test_export_pages_bundle_resolves_latest_artifact_and_preserves_run_id(tmp_path: Path) -> None:
    _write_run(tmp_path, scenario_name="scenario", timestamp="20260306_120000")
    _write_run(
        tmp_path,
        scenario_name="scenario",
        timestamp="20260306_130000",
        gdp=[200.0, 201.0, 202.0, 203.0],
    )
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    _write_spec(spec_path)

    result = export_pages_bundle(
        spec_path=spec_path,
        artifacts_dir=tmp_path / "artifacts",
        out_dir=tmp_path / "public" / "model-runs",
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["runs"] == [
        {
            "data_path": "runs/fixture-run.json",
            "details": ["Fixture detail A", "Fixture detail B"],
            "forecast_end": "2025.4",
            "forecast_start": "2025.1",
            "label": "Fixture Run",
            "run_id": "fixture-run",
            "scenario_name": "scenario",
            "summary": "Fixture summary",
            "timestamp": "20260306_130000",
        }
    ]
    run_payload = json.loads(
        (result.out_dir / "runs" / "fixture-run.json").read_text(encoding="utf-8")
    )
    assert run_payload["run_id"] == "fixture-run"
    assert run_payload["scenario_name"] == "scenario"
    assert run_payload["series"]["GDP"] == [200.0, 201.0, 202.0, 203.0]


def test_export_pages_bundle_uses_loadformat_adds_wr_and_serializes_nonfinite_to_null(
    tmp_path: Path,
) -> None:
    _write_run(
        tmp_path,
        scenario_name="scenario",
        timestamp="20260306_130000",
        wf=[1.0, 2.0, 3.0, 4.0],
        pf=[10.0, 0.0, 5.0, 5.0],
    )
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    _write_spec(spec_path)

    export_pages_bundle(
        spec_path=spec_path,
        artifacts_dir=tmp_path / "artifacts",
        out_dir=tmp_path / "public" / "model-runs",
    )

    run_payload = json.loads(
        (tmp_path / "public" / "model-runs" / "runs" / "fixture-run.json").read_text(
            encoding="utf-8"
        )
    )
    assert run_payload["series"]["WF"] == [1.0, 2.0, 3.0, 4.0]
    assert run_payload["series"]["WR"] == [0.1, None, 0.6, 0.8]


def test_export_pages_bundle_applies_dictionary_overlay_precedence(tmp_path: Path) -> None:
    overlay_dir = tmp_path / "projects_local" / "scenario_overlay"
    (tmp_path / "projects_local" / "dictionary_extensions").mkdir(parents=True, exist_ok=True)
    (tmp_path / "projects_local" / "dictionary_overlays").mkdir(parents=True, exist_ok=True)
    (overlay_dir / "dictionary_overlays").mkdir(parents=True, exist_ok=True)

    (tmp_path / "projects_local" / "dictionary_extensions" / "00-shared.json").write_text(
        json.dumps({"variables": {"GDP": {"short_name": "Shared GDP"}}}, indent=2),
        encoding="utf-8",
    )
    (tmp_path / "projects_local" / "dictionary_overlays" / "baseline.json").write_text(
        json.dumps({"variables": {"GDP": {"description": "Baseline GDP description"}}}, indent=2),
        encoding="utf-8",
    )
    (overlay_dir / "dictionary_overlays" / "scenario.json").write_text(
        json.dumps(
            {"variables": {"GDP": {"description": "Scenario GDP description", "units": "Index"}}},
            indent=2,
        ),
        encoding="utf-8",
    )

    _write_run(
        tmp_path,
        scenario_name="scenario",
        timestamp="20260306_130000",
        input_overlay_dir=overlay_dir,
    )
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    _write_spec(spec_path)

    previous_cwd = Path.cwd()
    try:
        os.chdir(tmp_path)
        export_pages_bundle(
            spec_path=spec_path,
            artifacts_dir=tmp_path / "artifacts",
            out_dir=tmp_path / "public" / "model-runs",
        )
    finally:
        os.chdir(previous_cwd)

    dictionary_payload = json.loads(
        (tmp_path / "public" / "model-runs" / "dictionary.json").read_text(encoding="utf-8")
    )
    assert dictionary_payload["variables"]["GDP"] == {
        "code": "GDP",
        "description": "Scenario GDP description",
        "short_name": "Shared GDP",
        "units": "Index",
    }


def test_export_pages_bundle_rejects_absolute_path_strings(tmp_path: Path) -> None:
    _write_run(tmp_path, scenario_name="scenario", timestamp="20260306_130000")
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    _write_spec(spec_path, title="/tmp/not-public")

    with pytest.raises(PagesExportError, match="absolute path"):
        export_pages_bundle(
            spec_path=spec_path,
            artifacts_dir=tmp_path / "artifacts",
            out_dir=tmp_path / "public" / "model-runs",
        )


def test_export_pages_bundle_uses_relative_static_paths(tmp_path: Path) -> None:
    _write_run(tmp_path, scenario_name="scenario", timestamp="20260306_130000")
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    _write_spec(spec_path)

    export_pages_bundle(
        spec_path=spec_path,
        artifacts_dir=tmp_path / "artifacts",
        out_dir=tmp_path / "public" / "model-runs",
    )

    index_html = (tmp_path / "public" / "model-runs" / "index.html").read_text(encoding="utf-8")
    manifest = json.loads(
        (tmp_path / "public" / "model-runs" / "manifest.json").read_text(encoding="utf-8")
    )

    assert 'href="./styles.css"' in index_html
    assert 'src="./app.js"' in index_html
    assert 'id="variableSearch"' in index_html
    assert 'id="runInfo"' in index_html
    assert manifest["dictionary_path"] == "dictionary.json"
    assert manifest["presets_path"] == "presets.json"
    assert all(not item["data_path"].startswith("/") for item in manifest["runs"])


def test_export_pages_bundle_preserves_run_labels_with_symbols(tmp_path: Path) -> None:
    _write_run(tmp_path, scenario_name="scenario", timestamp="20260306_130000")
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    spec_payload = {
        "version": 1,
        "title": "Fixture Explorer",
        "site_subpath": "model-runs",
        "runs": [
            {
                "run_id": "fixture-run",
                "label": "PSE2025 High $25/h",
                "scenario_name": "scenario",
            }
        ],
        "default_run_ids": ["fixture-run"],
        "presets": [
            {
                "id": "fixture-preset",
                "label": "Fixture Preset",
                "variables": ["GDP"],
            }
        ],
        "default_preset_ids": ["fixture-preset"],
    }
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(yaml.safe_dump(spec_payload, sort_keys=False), encoding="utf-8")

    result = export_pages_bundle(
        spec_path=spec_path,
        artifacts_dir=tmp_path / "artifacts",
        out_dir=tmp_path / "public" / "model-runs",
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["runs"][0]["label"] == "PSE2025 High $25/h"


def test_export_pages_cli_writes_bundle(tmp_path: Path) -> None:
    _write_run(tmp_path, scenario_name="scenario", timestamp="20260306_130000")
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    _write_spec(spec_path)

    result = runner.invoke(
        app,
        [
            "export",
            "pages",
            "--spec",
            str(spec_path),
            "--artifacts-dir",
            str(tmp_path / "artifacts"),
            "--out-dir",
            str(tmp_path / "public" / "model-runs"),
        ],
    )

    assert result.exit_code == 0
    assert (tmp_path / "public" / "model-runs" / "manifest.json").exists()


def test_model_runs_transform_semantics_match_run_panels_contract() -> None:
    pct_of_values = transform_series(
        mode=TRANSFORM_PCT_OF,
        level_values=[10.0, 20.0, 30.0],
        denominator_values=[100.0, 200.0, 0.0],
    )
    lvl_change_values = transform_series(
        mode=TRANSFORM_LVL_CHANGE,
        level_values=[100.0, 101.5, 103.0],
    )
    pct_change_values = transform_series(
        mode=TRANSFORM_PCT_CHANGE,
        level_values=[100.0, 110.0, 121.0],
    )

    assert pct_of_values[:2] == [10.0, 10.0]
    assert math.isnan(pct_of_values[2])
    assert math.isnan(lvl_change_values[0])
    assert lvl_change_values[1:] == [1.5, 1.5]
    assert math.isnan(pct_change_values[0])
    assert pct_change_values[1:] == pytest.approx([10.0, 10.0])


def test_model_runs_run_comparison_semantics_match_run_panels_contract() -> None:
    diff_values = apply_run_comparison(
        mode=COMPARE_DIFF_VS_RUN,
        values=[102.0, 105.0, 110.0],
        reference_values=[100.0, 100.0, 100.0],
    )
    pct_diff_values = apply_run_comparison(
        mode=COMPARE_PCT_DIFF_VS_RUN,
        values=[102.0, 105.0, 110.0],
        reference_values=[100.0, 100.0, 100.0],
    )

    assert diff_values == [2.0, 5.0, 10.0]
    assert pct_diff_values == pytest.approx([2.0, 5.0, 10.0])
