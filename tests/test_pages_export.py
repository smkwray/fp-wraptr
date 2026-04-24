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


def _write_loadformat(
    path: Path,
    *,
    wf: list[float],
    pf: list[float],
    gdp: list[float],
    extra_series: dict[str, list[float]] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
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
    ]
    for name, values in (extra_series or {}).items():
        lines.extend([
            f"LOAD {name};",
            " " + " ".join(str(value) for value in values),
            "'END'",
        ])
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


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
    fminput_text: str | None = None,
    extra_series: dict[str, list[float]] | None = None,
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
        extra_series=extra_series,
    )
    if fminput_text is not None:
        work_dir = run_dir / "work"
        work_dir.mkdir(parents=True, exist_ok=True)
        (work_dir / "fminput.txt").write_text(fminput_text, encoding="utf-8")
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


def test_export_pages_bundle_copies_run_group_and_public_metadata(tmp_path: Path) -> None:
    _write_run(tmp_path, scenario_name="scenario", timestamp="20260306_130000")
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    spec_payload = {
        "version": 1,
        "title": "Fixture Explorer",
        "site_subpath": "model-runs",
        "runs": [
            {
                "run_id": "fixture-run",
                "label": "Fixture Run",
                "scenario_name": "scenario",
                "group": "Childcare Regimes",
                "public_metadata": {
                    "scenario_kind": "care_infrastructure_package",
                    "financing_rule": "mixed",
                    "regime_inputs": {"alpha_t": 0.14},
                },
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
    run_record = manifest["runs"][0]
    assert run_record["group"] == "Childcare Regimes"
    assert run_record["scenario_kind"] == "care_infrastructure_package"
    assert run_record["financing_rule"] == "mixed"
    assert run_record["regime_inputs"] == {"alpha_t": 0.14}

    run_payload = json.loads(
        (result.out_dir / "runs" / "fixture-run.json").read_text(encoding="utf-8")
    )
    assert run_payload["group"] == "Childcare Regimes"
    assert run_payload["scenario_kind"] == "care_infrastructure_package"
    assert run_payload["financing_rule"] == "mixed"
    assert run_payload["regime_inputs"] == {"alpha_t": 0.14}


def test_export_pages_bundle_copies_top_level_family_and_horizon_metadata(tmp_path: Path) -> None:
    _write_run(tmp_path, scenario_name="scenario", timestamp="20260306_130000")
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    spec_payload = {
        "version": 1,
        "title": "Fixture Explorer",
        "site_subpath": "model-runs",
        "runs": [
            {
                "run_id": "fixture-run",
                "label": "Fixture Run",
                "scenario_name": "scenario",
                "group": "Standard 2026",
                "family_id": "fixture_family",
                "horizon_id": "10y",
                "horizon_label": "10Y",
                "horizon_years": 10,
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
    run_record = manifest["runs"][0]
    assert run_record["group"] == "Standard 2026"
    assert run_record["family_id"] == "fixture_family"
    assert run_record["horizon_id"] == "10y"
    assert run_record["horizon_label"] == "10Y"
    assert run_record["horizon_years"] == 10

    run_payload = json.loads(
        (result.out_dir / "runs" / "fixture-run.json").read_text(encoding="utf-8")
    )
    assert run_payload["group"] == "Standard 2026"
    assert run_payload["family_id"] == "fixture_family"
    assert run_payload["horizon_id"] == "10y"
    assert run_payload["horizon_label"] == "10Y"
    assert run_payload["horizon_years"] == 10


def test_export_pages_bundle_rejects_duplicate_top_level_and_public_metadata_horizon_keys(
    tmp_path: Path,
) -> None:
    _write_run(tmp_path, scenario_name="scenario", timestamp="20260306_130000")
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    spec_payload = {
        "version": 1,
        "title": "Fixture Explorer",
        "site_subpath": "model-runs",
        "runs": [
            {
                "run_id": "fixture-run",
                "label": "Fixture Run",
                "scenario_name": "scenario",
                "family_id": "fixture_family",
                "public_metadata": {
                    "family_id": "other_family",
                },
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

    with pytest.raises(PagesExportError) as excinfo:
        export_pages_bundle(
            spec_path=spec_path,
            artifacts_dir=tmp_path / "artifacts",
            out_dir=tmp_path / "public" / "model-runs",
        )

    assert "duplicates top-level horizon/family metadata" in str(excinfo.value)


def test_export_pages_bundle_applies_childcare_regime_compiler(tmp_path: Path) -> None:
    _write_run(
        tmp_path,
        scenario_name="childcare_base",
        timestamp="20260306_120000",
        extra_series={
            "GCCOST": [10.0, 10.0, 10.0, 10.0],
            "GCSUB": [0.0, 0.0, 0.0, 0.0],
            "GCCHH": [0.5, 0.5, 0.5, 0.5],
            "L2C": [1.0, 1.0, 1.0, 1.0],
            "GU6SHR": [0.4, 0.4, 0.4, 0.4],
            "GTAXWD": [0.3, 0.3, 0.3, 0.3],
        },
        gdp=[100.0, 100.0, 100.0, 100.0],
    )
    _write_run(
        tmp_path,
        scenario_name="childcare_relief",
        timestamp="20260306_130000",
        extra_series={
            "GCCOST": [12.0, 12.0, 12.0, 12.0],
            "GCSUB": [2.0, 2.0, 2.0, 2.0],
            "L2C": [2.0, 3.0, 4.0, 5.0],
            "GU6SHR": [0.4, 0.4, 0.4, 0.4],
            "GTAXWD": [0.3, 0.3, 0.3, 0.3],
        },
        gdp=[110.0, 111.0, 112.0, 113.0],
    )
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    spec_payload = {
        "version": 1,
        "title": "Fixture Explorer",
        "site_subpath": "model-runs",
        "runs": [
            {
                "run_id": "childcare-base",
                "label": "Childcare Base",
                "group": "Childcare Regimes",
                "scenario_name": "childcare_base",
                "public_metadata": {
                    "base_run_id": "childcare-base",
                    "case_role": "observed_anchor",
                    "financing_rule": "not_applicable",
                    "method_tag": "childcare_fixed_point_v1",
                    "public_interpretation": "Observed childcare base.",
                    "quantity_basis": "unpriced_active_care_bridge",
                    "regime_inputs": {
                        "alpha_t": 0.0,
                        "kappa_c_t": 0.0,
                        "kappa_q_t": 0.0,
                        "public_cost_share": 0.0,
                        "sub_t": 0.0,
                    },
                    "scenario_kind": "baseline",
                },
            },
            {
                "run_id": "childcare-relief",
                "label": "Childcare Relief",
                "group": "Childcare Regimes",
                "scenario_name": "childcare_relief",
                "public_metadata": {
                    "base_run_id": "childcare-base",
                    "case_role": "combined_package",
                    "financing_rule": "mixed",
                    "method_tag": "childcare_fixed_point_v1",
                    "public_interpretation": "Combined childcare package.",
                    "quantity_basis": "unpriced_active_care_bridge",
                    "regime_inputs": {
                        "alpha_t": 0.1,
                        "kappa_c_t": -0.05,
                        "kappa_q_t": 0.2,
                        "public_cost_share": 0.8,
                        "sub_t": 0.2,
                    },
                    "scenario_kind": "care_infrastructure_package",
                },
            },
        ],
        "default_run_ids": ["childcare-base", "childcare-relief"],
        "presets": [
            {
                "id": "fixture-preset",
                "label": "Fixture Preset",
                "variables": [
                    "GDP",
                    "GCCPRICE",
                    "GCCREAL",
                    "GDPR_NET",
                    "ALPHA_CC",
                    "QCAP",
                    "GTAXSEC",
                ],
            }
        ],
        "default_preset_ids": ["fixture-preset"],
    }
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(yaml.safe_dump(spec_payload, sort_keys=False), encoding="utf-8")

    compiler_config_path = tmp_path / "compiler.json"
    compiler_config_path.write_text(
        json.dumps(
            {
                "version": 1,
                "method_tag": "childcare_fixed_point_v1",
                "quantity_basis": "unpriced_active_care_bridge",
                "quantity_proxy_order": ["L2C"],
                "solver": {
                    "max_iterations": 24,
                    "tolerance": 1e-4,
                    "relaxation": 0.35,
                },
                "assumptions": {
                    "added_paid_care_hours_per_worker": 1200.0,
                    "childcare_worker_hours_per_year": 1800.0,
                    "childcare_cost_per_hour_2017usd": 14.0,
                    "household_burden_floor": 0.0,
                    "unpaid_care_pool_multiplier": 0.5,
                    "quantity_per_worker_delta": 1.0,
                    "alpha_feedback_from_l2c": 1.0,
                    "alpha_subsidy_feedback": 0.25,
                    "alpha_price_penalty": 0.5,
                    "slot_pressure_price_scale": 0.75,
                    "kappa_c_price_scale": 1.0,
                    "qcap_floor_multiplier": 0.05,
                    "qcap_alpha_accommodation": 0.15,
                    "tax_exposure_floor": 0.2,
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    input_contract_path = tmp_path / "input-contract.json"
    input_contract_path.write_text(
        json.dumps(
            {
                "required_metadata": {
                    "quantity_basis": {"required_value": "unpriced_active_care_bridge"}
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    output_contract_path = tmp_path / "output-contract.json"
    output_contract_path.write_text(
        json.dumps(
            {
                "required_metadata": {
                    "method_tag": {"required_value": "childcare_fixed_point_v1"},
                    "quantity_basis": {"required_value": "unpriced_active_care_bridge"},
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    result = export_pages_bundle(
        spec_path=spec_path,
        artifacts_dir=tmp_path / "artifacts",
        out_dir=tmp_path / "public" / "model-runs",
        childcare_regime_compiler_config_path=compiler_config_path,
        childcare_regime_input_contract_path=input_contract_path,
        childcare_regime_output_contract_path=output_contract_path,
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert "GCCREAL" in manifest["available_variables"]
    assert "GDPR_NET" in manifest["available_variables"]
    assert "ALPHA_CC" in manifest["available_variables"]
    assert "QCAP" in manifest["available_variables"]
    assert "GTAXSEC" in manifest["available_variables"]

    run_payload = json.loads(
        (result.out_dir / "runs" / "childcare-relief.json").read_text(encoding="utf-8")
    )
    assert run_payload["group"] == "Childcare Regimes"
    assert run_payload["scenario_kind"] == "care_infrastructure_package"
    assert run_payload["financing_rule"] == "mixed"
    assert run_payload["regime_meta"]["case_role"] == "combined_package"
    assert run_payload["regime_meta"]["regime_inputs"]["public_cost_share"] == 0.8
    assert run_payload["regime_meta"]["method_tag"] == "childcare_fixed_point_v1"
    assert run_payload["regime_meta"]["solver_mode"] == "conditional_fixed_point"
    assert run_payload["regime_meta"]["solver_converged"] is True
    assert run_payload["regime_meta"]["solver_iterations"] >= 1
    assert run_payload["outer_loop_meta"]["handoff_mode"] == "childcare_to_fair_outer_loop_seed"
    assert run_payload["outer_loop_meta"]["handoff_ready"] is True
    assert run_payload["outer_loop_meta"]["requires_fair_rerun"] is True
    assert run_payload["outer_loop_meta"]["max_positive_dl2c"] > 0.0
    assert run_payload["outer_loop_meta"]["max_dqpaid_loop"] > 0.0
    assert run_payload["outer_loop_meta"]["max_alpha_next"] > 0.0
    assert run_payload["outer_loop_meta"]["max_u0_cc"] > 0.0
    assert run_payload["series"]["ALPHA_CC"][0] > 0.1
    assert run_payload["series"]["QCAP"][0] > 0.0
    assert run_payload["series"]["GTAXSEC"] == pytest.approx([0.12, 0.12, 0.12, 0.12])
    assert run_payload["series"]["DL2C_LOOP"][0] > 0.0
    assert run_payload["series"]["DQPAID_LOOP"][0] > 0.0
    assert run_payload["series"]["ALPHA_NEXT"][0] > 0.0
    assert run_payload["series"]["U0_CC"][0] > 0.0
    assert run_payload["series"]["GCCPRICE"][0] < 12.0
    assert run_payload["series"]["GCBUR"][0] < run_payload["series"]["GCCPRICE"][0]
    assert 0.0 < run_payload["series"]["GCCHH"][0] < 0.5
    assert run_payload["series"]["GCCJOBS"][0] > 0.0
    assert run_payload["series"]["GCCREAL"][0] > 0.0
    assert run_payload["series"]["GCGOV"][0] > 0.0
    assert run_payload["series"]["GDPR_NET"][0] < 110.0


def test_export_pages_bundle_uses_external_unpriced_handoff_when_present(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    overlay_dir = tmp_path / "overlay"
    overlay_dir.mkdir(parents=True, exist_ok=True)
    (overlay_dir / "GCBURWT.DAT").write_text("SMPL 2025.1 2025.4;\nLOAD GCBURWT;\n 0 0 0 0\n'END'\n", encoding="utf-8")

    for scenario_name, timestamp, l2c_values in (
        ("childcare_base", "20260306_120000", [0.50, 0.50, 0.50, 0.50]),
        ("childcare_relief", "20260306_120500", [0.62, 0.64, 0.67, 0.70]),
    ):
        _write_run(
            tmp_path,
            scenario_name=scenario_name,
            timestamp=timestamp,
            input_overlay_dir=overlay_dir,
            wf=[1.0, 2.0, 3.0, 4.0],
            pf=[10.0, 10.0, 10.0, 10.0],
            gdp=[100.0, 105.0, 108.0, 110.0],
            extra_series={
                "L2C": l2c_values,
                "L2O": [1.20, 1.22, 1.24, 1.25],
                "GU6SHR": [0.30, 0.30, 0.30, 0.30],
                "GU6WEDGE": [-0.10, -0.10, -0.10, -0.10],
                "GCCOST": [12.0, 12.0, 12.0, 12.0],
                "GCSUB": [2.0 if scenario_name == "childcare_relief" else 0.0] * 4,
                "GCCHH": [0.12, 0.12, 0.12, 0.12],
                "GTAXWD": [0.40, 0.40, 0.40, 0.40],
                "UR": [5.0, 5.0, 5.0, 5.0],
                "PCY": [1.0, 1.0, 1.0, 1.0],
                "PIEF": [1.0, 1.0, 1.0, 1.0],
            },
        )

    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    spec_payload = {
        "version": 1,
        "title": "Fixture Explorer",
        "site_subpath": "model-runs",
        "runs": [
            {
                "run_id": "childcare-base",
                "label": "Observed baseline",
                "scenario_name": "childcare_base",
                "group": "Childcare Regimes",
                "public_metadata": {
                    "method_tag": "childcare_fixed_point_v1",
                    "quantity_basis": "unpriced_active_care_bridge",
                    "base_run_id": "childcare-base",
                    "case_role": "observed_base",
                    "financing_rule": "not_applicable",
                    "public_interpretation": "Observed childcare baseline",
                    "scenario_kind": "baseline",
                },
            },
            {
                "run_id": "childcare-relief",
                "label": "Package case",
                "scenario_name": "childcare_relief",
                "group": "Childcare Regimes",
                "public_metadata": {
                    "method_tag": "childcare_fixed_point_v1",
                    "quantity_basis": "unpriced_active_care_bridge",
                    "base_run_id": "childcare-base",
                    "case_role": "combined_package",
                    "financing_rule": "mixed",
                    "public_interpretation": "Package scenario",
                    "regime_inputs": {
                        "alpha_t": 0.35,
                        "sub_t": 0.3,
                        "public_cost_share": 0.8,
                        "kappa_q_t": 0.25,
                        "kappa_c_t": 0.1,
                    },
                    "scenario_kind": "care_infrastructure_package",
                },
            },
        ],
        "default_run_ids": ["childcare-base", "childcare-relief"],
        "presets": [
            {
                "id": "fixture-preset",
                "label": "Fixture Preset",
                "variables": ["GDP", "GCBUR", "GTAXSEC"],
            }
        ],
        "default_preset_ids": ["fixture-preset"],
    }
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(yaml.safe_dump(spec_payload, sort_keys=False), encoding="utf-8")

    compiler_config_path = tmp_path / "compiler.json"
    compiler_config_path.write_text(
        json.dumps(
            {
                "version": 1,
                "method_tag": "childcare_fixed_point_v1",
                "quantity_basis": "unpriced_active_care_bridge",
                "quantity_proxy_order": ["L2C"],
                "unpriced_external_handoff": {
                    "enabled": True,
                    "source_env_var": "UNPRICED_EXTERNAL_HANDOFF_NORMALIZED_PATH",
                    "required_method_tag": "unpriced_external_v1",
                    "required_quantity_basis": "unpriced_active_care_bridge",
                    "series_names": {
                        "price": "P0_t",
                        "paid_quantity": "Q0_t",
                        "unpaid_quantity": "U0_t",
                    },
                },
                "solver": {"max_iterations": 24, "tolerance": 1e-4, "relaxation": 0.35},
                "assumptions": {
                    "added_paid_care_hours_per_worker": 1200.0,
                    "childcare_worker_hours_per_year": 1800.0,
                    "childcare_cost_per_hour_2017usd": 14.0,
                    "household_burden_floor": 0.0,
                    "unpaid_care_pool_multiplier": 0.5,
                    "quantity_per_worker_delta": 1.0,
                    "alpha_feedback_from_l2c": 1.0,
                    "alpha_subsidy_feedback": 0.25,
                    "alpha_price_penalty": 0.5,
                    "slot_pressure_price_scale": 0.75,
                    "kappa_c_price_scale": 1.0,
                    "qcap_floor_multiplier": 0.05,
                    "qcap_alpha_accommodation": 0.15,
                    "tax_exposure_floor": 0.2,
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    input_contract_path = tmp_path / "input-contract.json"
    input_contract_path.write_text(
        json.dumps({"required_metadata": {"quantity_basis": {"required_value": "unpriced_active_care_bridge"}}}, indent=2),
        encoding="utf-8",
    )
    output_contract_path = tmp_path / "output-contract.json"
    output_contract_path.write_text(
        json.dumps(
            {
                "required_metadata": {
                    "method_tag": {"required_value": "childcare_fixed_point_v1"},
                    "quantity_basis": {"required_value": "unpriced_active_care_bridge"},
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    unpriced_handoff_path = tmp_path / "unpriced-external.json"
    unpriced_handoff_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "method_tag": "unpriced_external_v1",
                "quantity_basis": "unpriced_active_care_bridge",
                "source_repo": "unpriced",
                "generated_at": "2026-04-12T15:45:00Z",
                "handoff_mode": "local_pinned_package",
                "run_inputs": {
                    "childcare-relief": {
                        "series": {
                            "P0_t": [85.0, 86.0, 87.0, 88.0],
                            "Q0_t": [9.0, 9.0, 9.0, 9.0],
                            "U0_t": [4.0, 4.1, 4.2, 4.3],
                        }
                    }
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("UNPRICED_EXTERNAL_HANDOFF_NORMALIZED_PATH", str(unpriced_handoff_path))

    result = export_pages_bundle(
        spec_path=spec_path,
        artifacts_dir=tmp_path / "artifacts",
        out_dir=tmp_path / "public" / "model-runs",
        childcare_regime_compiler_config_path=compiler_config_path,
        childcare_regime_input_contract_path=input_contract_path,
        childcare_regime_output_contract_path=output_contract_path,
    )

    run_payload = json.loads((result.out_dir / "runs" / "childcare-relief.json").read_text(encoding="utf-8"))
    assert run_payload["regime_meta"]["unpriced_method"] == "unpriced_external_v1"
    assert run_payload["regime_meta"]["unpriced_source"] == "local_pinned_package"
    assert run_payload["regime_meta"]["unpriced_external"] is False
    assert run_payload["outer_loop_meta"]["unpriced_method"] == "unpriced_external_v1"
    assert run_payload["outer_loop_meta"]["unpriced_source"] == "local_pinned_package"
    assert run_payload["outer_loop_meta"]["unpriced_external"] is False
    assert run_payload["series"]["P0_CC"] == pytest.approx([85.0, 86.0, 87.0, 88.0])
    assert run_payload["series"]["Q0_CC"] == pytest.approx([9.0, 9.0, 9.0, 9.0])
    assert run_payload["series"]["U0_CC_BASE"] == pytest.approx([4.0, 4.1, 4.2, 4.3])


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


def test_export_pages_bundle_serializes_missing_sentinel_to_null(tmp_path: Path) -> None:
    _write_run(
        tmp_path,
        scenario_name="scenario",
        timestamp="20260306_130000",
        extra_series={"JGJ": [-99.0, 9.5, 13.7, 17.8]},
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
    assert run_payload["series"]["JGJ"] == [None, 9.5, 13.7, 17.8]


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
        "defined_by_equation": 82,
        "short_name": "Shared GDP",
        "used_in_equations": [84, 129],
        "units": "Index",
    }
    assert dictionary_payload["equations"]["82"]["lhs_expr"] == "GDP"


def test_export_pages_bundle_includes_overlay_equations(tmp_path: Path) -> None:
    overlay_dir = tmp_path / "projects_local" / "scenario_overlay"
    (overlay_dir / "dictionary_overlays").mkdir(parents=True, exist_ok=True)
    (overlay_dir / "dictionary_overlays" / "scenario.json").write_text(
        json.dumps(
            {
                "equations": {
                    "999": {
                        "label": "Scenario helper equation",
                        "lhs_expr": "JGJ",
                        "rhs_variables": ["JF", "JGJ"],
                        "formula": "JF + JGJ",
                    }
                }
            },
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
    assert dictionary_payload["equations"]["999"] == {
        "formula": "JF + JGJ",
        "id": 999,
        "label": "Scenario helper equation",
        "lhs_expr": "JGJ",
        "rhs_variables": ["JF", "JGJ"],
        "sector_block": "",
        "display_id": "Eq 999",
        "source_runs": [],
        "type": "definitional",
    }


def test_export_pages_bundle_includes_run_input_equations_for_scenario_variables(
    tmp_path: Path,
) -> None:
    _write_run(
        tmp_path,
        scenario_name="scenario",
        timestamp="20260306_130000",
        fminput_text=(
            "CREATE JGCOLA=0;\n"
            "IDENT JGW=JGW(-1)*(1+JGCOLA);\n"
            "GENR JGJ=JGPART+JGNOTLF+JGU;\n"
            "EQ 16 LWFQZ LWFQZPF(-1) LPFZ C;\n"
            "LHS WF=EXP(LWFQZ+DELTA1*LPF(-1))*LAM*(1+JGWFADJ);\n"
        ),
        extra_series={
            "JGCOLA": [0.0, 0.0, 0.0, 0.0],
            "JGW": [1.0, 1.0, 1.0, 1.0],
            "JGJ": [0.0, 0.0, 0.0, 0.0],
        },
    )
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    spec_payload = {
        "version": 1,
        "title": "Fixture Explorer",
        "site_subpath": "model-runs",
        "runs": [
            {
                "run_id": "fixture-run",
                "label": "Fixture Run",
                "scenario_name": "scenario",
            }
        ],
        "default_run_ids": ["fixture-run"],
        "presets": [
            {
                "id": "fixture-preset",
                "label": "Fixture Preset",
                "variables": ["GDP", "WF", "WR", "JGCOLA", "JGW", "JGJ"],
            }
        ],
        "default_preset_ids": ["fixture-preset"],
    }
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(yaml.safe_dump(spec_payload, sort_keys=False), encoding="utf-8")

    export_pages_bundle(
        spec_path=spec_path,
        artifacts_dir=tmp_path / "artifacts",
        out_dir=tmp_path / "public" / "model-runs",
    )

    dictionary_payload = json.loads(
        (tmp_path / "public" / "model-runs" / "dictionary.json").read_text(encoding="utf-8")
    )
    equation_records = list(dictionary_payload["equations"].values())
    create_record = next(
        item for item in equation_records if item["display_id"] == "CREATE JGCOLA"
    )
    ident_record = next(item for item in equation_records if item["display_id"] == "IDENT JGW")
    genr_record = next(item for item in equation_records if item["display_id"] == "GENR JGJ")

    assert create_record["lhs_expr"] == "JGCOLA"
    assert create_record["formula"] == "0"
    assert create_record["source_runs"] == ["scenario"]
    assert ident_record["rhs_variables"] == ["JGW", "JGCOLA"]
    assert ident_record["source_runs"] == ["scenario"]
    assert genr_record["rhs_variables"] == ["JGPART", "JGNOTLF", "JGU"]


def test_export_pages_bundle_allows_authored_controls_that_are_only_used_for_equation_metadata(
    tmp_path: Path,
) -> None:
    overlay_dir = tmp_path / "projects_local" / "scenario_overlay"
    overlay_dir.mkdir(parents=True, exist_ok=True)
    (overlay_dir / "fmexog.txt").write_text(
        "\n".join(
            [
                "SMPL 2025.1 2025.1;",
                "CHANGEVAR;",
                "JGCOLA ADDDIFABS",
                "0.0",
                ";",
                "SMPL 2025.2 2025.2;",
                "CHANGEVAR;",
                "JGCOLA ADDDIFABS",
                "0.007417071777732875",
                ";",
                "SMPL 2025.3 2025.3;",
                "CHANGEVAR;",
                "JGCOLA ADDDIFABS",
                "0.007417071777732875",
                ";",
                "SMPL 2025.4 2025.4;",
                "CHANGEVAR;",
                "JGCOLA ADDDIFABS",
                "0.007417071777732875",
                ";",
                "RETURN;",
                "",
            ]
        ),
        encoding="utf-8",
    )

    _write_run(
        tmp_path,
        scenario_name="scenario",
        timestamp="20260306_130000",
        input_overlay_dir=overlay_dir,
        extra_series={
            "JGCOLA": [0.0, 0.064788335218, 0.064788335218, 0.15575850733],
        },
    )
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    spec_payload = {
        "version": 1,
        "title": "Fixture Explorer",
        "site_subpath": "model-runs",
        "runs": [
            {
                "run_id": "fixture-run",
                "label": "Fixture Run",
                "scenario_name": "scenario",
            }
        ],
        "default_run_ids": ["fixture-run"],
        "presets": [
            {
                "id": "fixture-preset",
                "label": "Fixture Preset",
                "variables": ["GDP", "JGCOLA"],
            }
        ],
        "default_preset_ids": ["fixture-preset"],
    }
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(yaml.safe_dump(spec_payload, sort_keys=False), encoding="utf-8")

    export_pages_bundle(
        spec_path=spec_path,
        artifacts_dir=tmp_path / "artifacts",
        out_dir=tmp_path / "public" / "model-runs",
    )

    assert (tmp_path / "public" / "model-runs" / "dictionary.json").exists()


def test_export_pages_bundle_ignores_fmexog_pct_change_cards_when_validating_overlay_series(
    tmp_path: Path,
) -> None:
    overlay_dir = tmp_path / "projects_local" / "scenario_overlay"
    overlay_dir.mkdir(parents=True, exist_ok=True)
    (overlay_dir / "fmexog.txt").write_text(
        "\n".join(
            [
                "SMPL 2025.1 2025.4;",
                "CHANGEVAR;",
                "CCGQ CHGSAMEPCT",
                "0.007417072",
                ";",
                "RETURN;",
                "",
            ]
        ),
        encoding="utf-8",
    )

    _write_run(
        tmp_path,
        scenario_name="scenario",
        timestamp="20260306_130000",
        input_overlay_dir=overlay_dir,
        extra_series={
            "CCGQ": [81.0, 82.0, 83.0, 84.0],
        },
    )
    spec_path = tmp_path / "public" / "model-runs.spec.yaml"
    spec_payload = {
        "version": 1,
        "title": "Fixture Explorer",
        "site_subpath": "model-runs",
        "runs": [
            {
                "run_id": "fixture-run",
                "label": "Fixture Run",
                "scenario_name": "scenario",
            }
        ],
        "default_run_ids": ["fixture-run"],
        "presets": [
            {
                "id": "fixture-preset",
                "label": "Fixture Preset",
                "variables": ["GDP", "CCGQ"],
            }
        ],
        "default_preset_ids": ["fixture-preset"],
    }
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(yaml.safe_dump(spec_payload, sort_keys=False), encoding="utf-8")

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
    assert run_payload["series"]["CCGQ"] == [81.0, 82.0, 83.0, 84.0]


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

    assert 'href="./styles.css' in index_html
    assert 'src="./app.js' in index_html
    assert 'id="variableSearch"' in index_html
    assert 'id="runInfoSelect"' in index_html
    assert 'id="runInfo"' in index_html
    assert 'id="equationSearch"' in index_html
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
