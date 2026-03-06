"""CLI integration tests."""

import csv
import importlib
import json
import shutil
from pathlib import Path

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from fp_wraptr import __version__
from fp_wraptr.cli import app
from tests.test_authoring import _make_repo

runner = CliRunner()


def _write_dictionary_json(path):
    payload = {
        "model_version": "2025-12-23",
        "source": {"pabapa_md": "fixture", "extraction_timestamp": "2026-02-23T00:00:00Z"},
        "variables": {
            "GDP": {
                "name": "GDP",
                "description": "Gross domestic product.",
                "units": "B$",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 82,
                "used_in_equations": [84],
                "raw_data_sources": ["R11"],
                "construction": "Def., Eq. 82",
            },
            "UR": {
                "name": "UR",
                "description": "",
                "units": "",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 87,
                "used_in_equations": [1, 2],
                "raw_data_sources": [],
                "construction": "Def., Eq. 87",
            },
            "HG": {
                "name": "HG",
                "description": "Hours worked in government.",
                "units": "",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 10,
                "used_in_equations": [82],
                "raw_data_sources": [],
                "construction": "",
            },
        },
        "equations": {
            "82": {
                "id": 82,
                "type": "definitional",
                "sector_block": "nominal",
                "label": "Nominal GDP",
                "lhs_expr": "GDP",
                "rhs_variables": ["HG", "UR"],
                "formula": "HG + UR",
            },
            "87": {
                "id": 87,
                "type": "definitional",
                "sector_block": "labor",
                "label": "Unemployment rate",
                "lhs_expr": "UR",
                "rhs_variables": ["U", "LF"],
                "formula": "U/LF",
            },
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_dictionary_json_with_raw_data(path):
    payload = {
        "model_version": "2025-12-23",
        "source": {"pabapa_md": "fixture", "extraction_timestamp": "2026-02-23T00:00:00Z"},
        "variables": {
            "GDP": {
                "name": "GDP",
                "description": "Gross domestic product.",
                "units": "B$",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 82,
                "used_in_equations": [84],
                "raw_data_sources": ["R11"],
                "construction": "Def., Eq. 82",
            },
            "UR": {
                "name": "UR",
                "description": "",
                "units": "",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 87,
                "used_in_equations": [1, 2],
                "raw_data_sources": [],
                "construction": "Def., Eq. 87",
            },
        },
        "equations": {
            "82": {
                "id": 82,
                "type": "definitional",
                "sector_block": "nominal",
                "label": "Nominal GDP",
                "lhs_expr": "GDP",
                "rhs_variables": ["UR"],
                "formula": "UR",
            },
        },
        "raw_data": {
            "R11": {
                "r_number": "R11",
                "variable": "GDP",
                "source_type": "NIPA",
                "table": "1.1.5",
                "line": "1",
                "description": "Gross domestic product",
            },
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_source_map_yaml(path):
    path.write_text(
        """\
GDP:
  description: Gross domestic product
  source: fred
  series_id: GDP
  frequency: Q
  units: Billions
  transform: level
""",
        encoding="utf-8",
    )


def _write_source_map_yaml_monthly_annual_rate(path):
    path.write_text(
        """\
UR:
  description: Unemployment rate annual-rate fixture
  source: fred
  series_id: UNRATE
  frequency: M
  units: Percent, annual rate
  transform: level
  annual_rate: true
""",
        encoding="utf-8",
    )


def _write_source_map_yaml_with_quality_issue(path):
    path.write_text(
        """\
GDP:
  description: Gross domestic product
  source: fred
  frequency: Q
  units: Billions
  transform: level
""",
        encoding="utf-8",
    )


def _write_source_map_yaml_with_window(path):
    path.write_text(
        """\
CTGB:
  description: Financial stabilization payments
  source: fred
  series_id: BOGZ1FA315410093Q
  frequency: Q
  units: Millions of dollars, SAAR
  transform: level
  annual_rate: true
  window_start: 2008Q4
  window_end: 2012Q2
  outside_window_value: 0.0
""",
        encoding="utf-8",
    )


def _write_local_pack_manifest(repo_root: Path) -> None:
    target = repo_root / "projects_local" / "packs" / "pse2025" / "pack.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "\n".join(
            [
                "pack_id: pse2025",
                "label: PSE2025 Working Pack",
                "family: pse2025",
                "visibility: local",
                "cards_family: pse2025",
                "catalog_entry_ids: [pse-base, pse-bundle]",
                "recipes:",
                "  - recipe_id: change-coefficients",
                "    label: Change coefficients",
                "visualizations:",
                "  - view_id: pse-main",
                "    label: Main tracks",
                "    chart_type: forecast_overlay",
                "    variables: [INTGADJ, JGW]",
                "",
            ]
        ),
        encoding="utf-8",
    )
    catalog_path = repo_root / "projects_local" / "scenario_catalog.yaml"
    if catalog_path.exists():
        catalog_path.write_text(
            catalog_path.read_text(encoding="utf-8").replace("public: true", "public: false"),
            encoding="utf-8",
        )


def _expected_query_snapshot() -> dict:
    equation = {
        "id": 82,
        "type": "definitional",
        "sector_block": "nominal",
        "label": "Nominal GDP",
        "lhs_expr": "GDP",
        "rhs_variables": ["HG", "UR"],
        "formula": "HG + UR",
    }
    ur_payload = {
        "name": "UR",
        "description": "Construction note: Def., Eq. 87",
        "units": "",
        "sector": "",
        "category": "endogenous",
        "defined_by_equation": 87,
        "used_in_equations": [1, 2],
        "raw_data_sources": [],
        "construction": "Def., Eq. 87",
        "description_source": "construction",
        "links": {"defining_equation": 87, "used_in_equations": [1, 2]},
    }
    return {
        "query": "UR in equation 82",
        "intent": {
            "kind": "variable_in_equation",
            "raw": "UR in equation 82",
            "variable": "UR",
            "equation_id": 82,
        },
        "focus": {
            "variable_in_equation": {
                "variable": "UR",
                "equation_id": 82,
                "present_in_equation": True,
            }
        },
        "equation_matches": [
            {
                "score": 100,
                "reason": "equation_id_exact",
                "equation": equation,
                "links": {
                    "lhs_variables": ["GDP"],
                    "rhs_variables": ["HG", "UR"],
                    "related_variables": ["GDP", "HG", "UR"],
                },
            }
        ],
        "variable_matches": [
            {
                "score": 95,
                "reason": "variable_code_exact",
                "variable": ur_payload,
            }
        ],
    }


def _expected_explain_snapshot() -> dict:
    equation = {
        "id": 82,
        "type": "definitional",
        "sector_block": "nominal",
        "label": "Nominal GDP",
        "lhs_expr": "GDP",
        "rhs_variables": ["HG", "UR"],
        "formula": "HG + UR",
    }
    return {
        "equation": equation,
        "variables": [
            {
                "name": "GDP",
                "description": "Gross domestic product.",
                "units": "B$",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 82,
                "used_in_equations": [84],
                "raw_data_sources": ["R11"],
                "construction": "Def., Eq. 82",
                "description_source": "dictionary",
                "links": {"defining_equation": 82, "used_in_equations": [84]},
                "role": "lhs",
                "found_in_dictionary": True,
            },
            {
                "name": "HG",
                "description": "Hours worked in government.",
                "units": "",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 10,
                "used_in_equations": [82],
                "raw_data_sources": [],
                "construction": "",
                "description_source": "dictionary",
                "links": {"defining_equation": 10, "used_in_equations": [82]},
                "role": "rhs",
                "found_in_dictionary": True,
            },
            {
                "name": "UR",
                "description": "Construction note: Def., Eq. 87",
                "units": "",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 87,
                "used_in_equations": [1, 2],
                "raw_data_sources": [],
                "construction": "Def., Eq. 87",
                "description_source": "construction",
                "links": {"defining_equation": 87, "used_in_equations": [1, 2]},
                "role": "rhs",
                "found_in_dictionary": True,
            },
        ],
        "cross_links": {
            "equation_id": 82,
            "lhs_variables": ["GDP"],
            "rhs_variables": ["HG", "UR"],
            "variable_to_equations": {
                "GDP": {
                    "role": "lhs",
                    "defined_by_equation": 82,
                    "used_in_equations": [84],
                },
                "HG": {
                    "role": "rhs",
                    "defined_by_equation": 10,
                    "used_in_equations": [82],
                },
                "UR": {
                    "role": "rhs",
                    "defined_by_equation": 87,
                    "used_in_equations": [1, 2],
                },
            },
        },
    }


def test_io_parse_output_json(tmp_path):
    fmout = tmp_path / "fmout.txt"
    fmout.write_text(
        """\
Some header text
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;

    Variable   Periods forecast are  2025.4  TO   2025.5

                   2025.4      2025.5
                               2026.1      2026.2
                               2026.3

   1 GDP      P lv   1.0      2.0      3.0      4.0
             P ch   0.1      0.1      0.1      0.1
             P %ch  10.0     10.0     10.0     10.0
""",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["io", "parse-output", str(fmout)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["forecast_start"] == "2025.4"
    assert payload["forecast_end"] == "2025.5"
    assert "variables" in payload
    assert "GDP" in payload["variables"]


def test_io_parse_input(tmp_path):
    fminput = tmp_path / "fminput.txt"
    fminput.write_text(
        """\
US MODEL TEST
SPACE MAXVAR=100;
SMPL 2025.4 2029.4;
GENR LY=LOG(Y);
""",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["io", "parse-input", str(fminput)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["title"] == "US MODEL TEST"
    assert payload["space"]["maxvar"] == "100"


def test_io_parse_output_corrupt_file_fails(tmp_path):
    fmout = tmp_path / "corrupt_fmout.txt"
    fmout.write_text("this is not a valid fp output", encoding="utf-8")

    result = runner.invoke(app, ["io", "parse-output", str(fmout)])

    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "no forecast variables found" in output


def test_run_with_missing_yaml_fails(tmp_path):
    missing = tmp_path / "missing.yaml"

    result = runner.invoke(app, ["run", str(missing)])

    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "not found" in output


def test_run_with_corrupt_yaml(tmp_path):
    corrupt = tmp_path / "corrupt.yaml"
    corrupt.write_text("{{{{", encoding="utf-8")

    result = runner.invoke(app, ["run", str(corrupt)])

    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "validation failed" in output or "scenario file not found" in output


def test_run_with_missing_fp_home_fails(tmp_path):
    scenario = tmp_path / "missing_fp_home.yaml"
    missing_home = tmp_path / "does_not_exist_fm"
    scenario.write_text(
        f"name: missing_fp_home\nfp_home: {missing_home}\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["run", str(scenario)])

    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "fp_home path not found" in output or isinstance(result.exception, FileNotFoundError)


def test_run_backend_both_emits_parity_report(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        parity_dir = Path(output_dir) / f"{config.name}_20990101_000000"
        (parity_dir / "work_fpexe").mkdir(parents=True)
        (parity_dir / "work_fppy").mkdir(parents=True)
        (parity_dir / "parity_report.json").write_text("{}", encoding="utf-8")
        (parity_dir / "work_fppy" / "PABEV.TXT").write_text("A,2025.4,1.0\n", encoding="utf-8")
        (parity_dir / "work_fpexe" / "PABEV.TXT").write_text("A,2025.4,1.0\n", encoding="utf-8")
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: both_cli\nfp_home: {tmp_path}\n", encoding="utf-8")

    output_root = tmp_path / "artifacts"
    result = runner.invoke(
        app,
        ["run", str(scenario), "--backend", "both", "--output-dir", str(output_root)],
    )

    assert result.exit_code == 0
    report_paths = list(output_root.glob("both_cli_*/parity_report.json"))
    assert len(report_paths) == 1


def test_run_backend_both_prints_seed_diagnostics_when_available(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        parity_dir = Path(output_dir) / f"{config.name}_20990101_000000"
        (parity_dir / "work_fpexe").mkdir(parents=True)
        (parity_dir / "work_fppy").mkdir(parents=True)
        (parity_dir / "work_fppy" / "fppy_report.json").write_text(
            json.dumps({
                "summary": {
                    "solve_outside_seeded_cells": 0,
                    "solve_outside_seed_inspected_cells": 85,
                    "solve_outside_seed_candidate_cells": 0,
                    "eq_backfill_outside_post_seed_cells": 0,
                    "eq_backfill_outside_post_seed_inspected_cells": 0,
                    "eq_backfill_outside_post_seed_candidate_cells": 0,
                }
            })
            + "\n",
            encoding="utf-8",
        )
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: both_cli\nfp_home: {tmp_path}\n", encoding="utf-8")
    output_root = tmp_path / "artifacts"
    result = runner.invoke(
        app,
        ["run", str(scenario), "--backend", "both", "--output-dir", str(output_root)],
    )

    assert result.exit_code == 0
    output = result.stdout + result.stderr
    assert "solve_seeded/inspected/candidate=0/85/0" in output
    assert "post_seeded/inspected/candidate=0/0/0" in output


def test_run_backend_both_prints_runtime_profile_when_available(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        parity_dir = Path(output_dir) / f"{config.name}_20990101_000000"
        (parity_dir / "work_fpexe").mkdir(parents=True)
        (parity_dir / "work_fppy").mkdir(parents=True)
        (parity_dir / "parity_report.json").write_text(
            json.dumps({
                "engine_runs": {
                    "fppy": {
                        "details": {
                            "eq_flags_preset": "parity_minimal",
                        }
                    }
                }
            })
            + "\n",
            encoding="utf-8",
        )
        (parity_dir / "work_fppy" / "fppy_report.json").write_text(
            json.dumps({
                "summary": {
                    "eq_backfill_iterations": 2,
                    "eq_backfill_min_iters": 2,
                    "eq_backfill_max_iters": 3,
                }
            })
            + "\n",
            encoding="utf-8",
        )
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: both_cli\nfp_home: {tmp_path}\n", encoding="utf-8")
    output_root = tmp_path / "artifacts"

    result = runner.invoke(
        app,
        ["run", str(scenario), "--backend", "both", "--output-dir", str(output_root)],
    )
    assert result.exit_code == 0
    output = result.stdout + result.stderr
    assert "preset=parity_minimal" in output
    assert "eq_iters=2" in output
    assert "eq_minmax=2/3" in output


def test_run_backend_both_rejects_baseline_option(tmp_path):
    scenario = tmp_path / "scenario.yaml"
    baseline = tmp_path / "baseline.yaml"
    scenario.write_text(f"name: both_cli\nfp_home: {tmp_path}\n", encoding="utf-8")
    baseline.write_text(f"name: base_cli\nfp_home: {tmp_path}\n", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "run",
            str(scenario),
            "--backend",
            "both",
            "--baseline",
            str(baseline),
        ],
    )

    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "--baseline is not supported with --backend both" in output


def test_run_backend_both_passes_gate_pabev_end(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    observed = {"gate_end": None}

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        _ = fp_home_override, fingerprint_lock
        observed["gate_end"] = getattr(gate, "pabev_end", None)
        parity_dir = Path(output_dir) / f"{config.name}_20990101_000000"
        parity_dir.mkdir(parents=True, exist_ok=True)
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: both_cli_gate\nfp_home: {tmp_path}\n", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "run",
            str(scenario),
            "--backend",
            "both",
            "--gate-pabev-end",
            "2025.4",
            "--output-dir",
            str(tmp_path / "artifacts"),
        ],
    )

    assert result.exit_code == 0
    assert observed["gate_end"] == "2025.4"


def test_run_backend_both_parity_quick_sets_gate_end_to_forecast_start(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    observed = {"gate_end": None}

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        _ = fp_home_override, fingerprint_lock
        observed["gate_end"] = getattr(gate, "pabev_end", None)
        parity_dir = Path(output_dir) / f"{config.name}_20990101_000000"
        parity_dir.mkdir(parents=True, exist_ok=True)
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(
        f"name: both_cli_quick\nfp_home: {tmp_path}\nforecast_start: '2030.1'\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "run",
            str(scenario),
            "--backend",
            "both",
            "--parity-quick",
            "--output-dir",
            str(tmp_path / "artifacts"),
        ],
    )

    assert result.exit_code == 0
    assert observed["gate_end"] == "2030.1"


def test_run_backend_both_strict_failure_prints_run_dir(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        _ = fp_home_override, gate, fingerprint_lock
        parity_dir = Path(output_dir) / f"{config.name}_20990101_000000"
        parity_dir.mkdir(parents=True, exist_ok=True)
        return ParityResult(
            status="gate_failed",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.5},
            exit_code=2,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: both_cli_gate_fail\nfp_home: {tmp_path}\n", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "run",
            str(scenario),
            "--backend",
            "both",
            "--parity-strict",
            "--output-dir",
            str(tmp_path / "artifacts"),
        ],
    )

    assert result.exit_code == 2
    output = result.stdout + result.stderr
    assert "run dir →" in output


def test_run_unknown_backend_reports_expected_choices(tmp_path):
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(
        f"name: unknown_backend\nfp_home: {tmp_path}\nbackend: mystery\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["run", str(scenario)])

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)
    assert "expected fpexe|fppy|both" in str(result.exception)


def test_parity_command_save_golden(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    parity_dir = tmp_path / "parity_run"
    (parity_dir / "work_fpexe").mkdir(parents=True)
    (parity_dir / "work_fppy").mkdir(parents=True)
    (parity_dir / "parity_report.json").write_text(
        json.dumps({
            "scenario_name": "baseline",
            "status": "ok",
            "exit_code": 0,
            "pabev_detail": {
                "start": "2025.4",
                "atol": 1e-3,
                "rtol": 1e-6,
                "missing_sentinels": [-99.0],
                "discrete_eps": 1e-12,
                "signflip_eps": 1e-3,
            },
        }),
        encoding="utf-8",
    )
    (parity_dir / "work_fpexe" / "PABEV.TXT").write_text(
        "SMPL 2025.4 2025.4;\nLOAD A;\n1.0\n'END'\n",
        encoding="utf-8",
    )
    (parity_dir / "work_fppy" / "PABEV.TXT").write_text(
        "SMPL 2025.4 2025.4;\nLOAD A;\n1.0\n'END'\n",
        encoding="utf-8",
    )

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: baseline\nfp_home: {tmp_path}\n", encoding="utf-8")

    golden_root = tmp_path / "golden"
    result = runner.invoke(
        app,
        ["parity", str(scenario), "--save-golden", str(golden_root)],
    )

    assert result.exit_code == 0
    assert (golden_root / "baseline" / "parity_report.json").exists()
    assert (golden_root / "baseline" / "work_fpexe" / "PABEV.TXT").exists()
    assert (golden_root / "baseline" / "work_fppy" / "PABEV.TXT").exists()
    assert (golden_root / "baseline" / "gate.json").exists()


def test_parity_command_gate_pabev_end_can_suppress_late_window_failure(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: gate_window\nfp_home: {tmp_path}\n", encoding="utf-8")

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        _ = fp_home_override, fingerprint_lock
        parity_dir = Path(output_dir) / f"{config.name}_20990101_000000"
        parity_dir.mkdir(parents=True, exist_ok=True)
        if getattr(gate, "pabev_end", None) == "2025.4":
            return ParityResult(
                status="ok",
                run_dir=str(parity_dir),
                scenario_name=config.name,
                input_fingerprint={"algo": "sha256", "files": {}},
                pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
                exit_code=0,
            )
        # Simulate a late-window mismatch when no gate end is set.
        return ParityResult(
            status="failed",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            pabev_detail={"hard_fail_cell_count": 1, "max_abs_diff": 1.0},
            exit_code=2,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)

    without_gate = runner.invoke(app, ["parity", str(scenario), "--strict"])
    assert without_gate.exit_code == 2

    with_gate = runner.invoke(
        app,
        ["parity", str(scenario), "--strict", "--gate-pabev-end", "2025.4"],
    )
    assert with_gate.exit_code == 0


def test_parity_command_quick_sets_gate_end_to_forecast_start(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    observed = {"gate_end": None}

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        _ = fp_home_override, fingerprint_lock, output_dir
        observed["gate_end"] = getattr(gate, "pabev_end", None)
        parity_dir = Path(output_dir) / f"{config.name}_20990101_000000"
        parity_dir.mkdir(parents=True, exist_ok=True)
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(
        f"name: quick_gate\nfp_home: {tmp_path}\nforecast_start: '2031.2'\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["parity", str(scenario), "--quick"])

    assert result.exit_code == 0
    assert observed["gate_end"] == "2031.2"


def test_parity_command_strict_failure_prints_run_dir(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: strict_fail\nfp_home: {tmp_path}\n", encoding="utf-8")

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        _ = fp_home_override, gate, fingerprint_lock
        parity_dir = Path(output_dir) / f"{config.name}_20990101_000000"
        parity_dir.mkdir(parents=True, exist_ok=True)
        return ParityResult(
            status="gate_failed",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.5},
            exit_code=2,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)

    result = runner.invoke(app, ["parity", str(scenario), "--strict"])
    assert result.exit_code == 2
    output = result.stdout + result.stderr
    assert "run dir →" in output


def test_parity_command_prints_seed_diagnostics_when_available(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    parity_dir = tmp_path / "parity_run"
    (parity_dir / "work_fpexe").mkdir(parents=True)
    (parity_dir / "work_fppy").mkdir(parents=True)
    (parity_dir / "work_fppy" / "fppy_report.json").write_text(
        json.dumps({
            "summary": {
                "solve_outside_seeded_cells": 1,
                "solve_outside_seed_inspected_cells": 85,
                "solve_outside_seed_candidate_cells": 2,
                "eq_backfill_outside_post_seed_cells": 1,
                "eq_backfill_outside_post_seed_inspected_cells": 10,
                "eq_backfill_outside_post_seed_candidate_cells": 1,
            }
        })
        + "\n",
        encoding="utf-8",
    )

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: baseline\nfp_home: {tmp_path}\n", encoding="utf-8")

    result = runner.invoke(app, ["parity", str(scenario)])
    assert result.exit_code == 0
    output = result.stdout + result.stderr
    assert "solve_seeded/inspected/candidate=1/85/2" in output
    assert "post_seeded/inspected/candidate=1/10/1" in output


def test_parity_command_prints_runtime_profile_when_available(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    parity_dir = tmp_path / "parity_run"
    (parity_dir / "work_fpexe").mkdir(parents=True)
    (parity_dir / "work_fppy").mkdir(parents=True)
    (parity_dir / "parity_report.json").write_text(
        json.dumps({
            "engine_runs": {
                "fppy": {
                    "details": {
                        "eq_flags_preset": "balanced",
                    }
                }
            }
        })
        + "\n",
        encoding="utf-8",
    )
    (parity_dir / "work_fppy" / "fppy_report.json").write_text(
        json.dumps({
            "summary": {
                "eq_backfill_iterations": 4,
                "eq_backfill_min_iters": 3,
                "eq_backfill_max_iters": 5,
            }
        })
        + "\n",
        encoding="utf-8",
    )

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: baseline\nfp_home: {tmp_path}\n", encoding="utf-8")

    result = runner.invoke(app, ["parity", str(scenario)])
    assert result.exit_code == 0
    output = result.stdout + result.stderr
    assert "preset=balanced" in output
    assert "eq_iters=4" in output
    assert "eq_minmax=3/5" in output


def test_parity_command_warns_on_fpexe_solution_errors(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import EngineRunSummary, ParityResult

    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: warn_case\nfp_home: {tmp_path}\n", encoding="utf-8")

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        _ = fp_home_override, gate, fingerprint_lock
        parity_dir = Path(output_dir) / f"{config.name}_20990101_000000"
        parity_dir.mkdir(parents=True, exist_ok=True)
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            engine_runs={
                "fpexe": EngineRunSummary(
                    name="fpexe",
                    ok=True,
                    details={"solution_errors": [{"solve": "SOL1", "iters": None, "period": None}]},
                )
            },
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)

    result = runner.invoke(app, ["parity", str(scenario)])
    assert result.exit_code == 0
    output = result.stdout + result.stderr
    assert "treat diffs as unreliable" in output


def test_parity_command_warns_when_wine_missing(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import EngineRunSummary, ParityResult

    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: wine_missing\nfp_home: {tmp_path}\n", encoding="utf-8")

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        _ = fp_home_override, gate, fingerprint_lock
        parity_dir = Path(output_dir) / f"{config.name}_20990101_000000"
        parity_dir.mkdir(parents=True, exist_ok=True)
        return ParityResult(
            status="engine_failure",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            engine_runs={
                "fpexe": EngineRunSummary(
                    name="fpexe",
                    ok=False,
                    details={
                        "preflight_report": {
                            "wine_required": True,
                            "wine_available": False,
                        }
                    },
                )
            },
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
            exit_code=4,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)

    result = runner.invoke(app, ["parity", str(scenario)])
    assert result.exit_code == 4
    output = result.stdout + result.stderr
    assert "Wine not found; install with" in output
    assert "brew install --cask wine-stable" in output
    assert "run dir →" in output


def test_parity_command_rejects_save_golden_with_regression(tmp_path):
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: baseline\nfp_home: {tmp_path}\n", encoding="utf-8")
    golden_root = tmp_path / "golden"

    result = runner.invoke(
        app,
        [
            "parity",
            str(scenario),
            "--save-golden",
            str(golden_root),
            "--regression",
            str(golden_root),
        ],
    )

    assert result.exit_code == 1
    output = (result.stdout + result.stderr).lower()
    assert "--save-golden and --regression cannot be used together" in output


def test_parity_command_regression_flag_fails_on_new_findings(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult
    from fp_wraptr.analysis.parity_regression import save_parity_golden

    fixture_dir = Path(__file__).parent / "fixtures" / "pabev"
    golden_run = tmp_path / "golden_run"
    (golden_run / "work_fpexe").mkdir(parents=True)
    (golden_run / "work_fppy").mkdir(parents=True)
    shutil.copy2(fixture_dir / "golden_fpexe.pabev", golden_run / "work_fpexe" / "PABEV.TXT")
    shutil.copy2(fixture_dir / "golden_fppy.pabev", golden_run / "work_fppy" / "PABEV.TXT")
    (golden_run / "parity_report.json").write_text(
        json.dumps({
            "scenario_name": "baseline",
            "status": "ok",
            "exit_code": 0,
            "pabev_detail": {
                "start": "2025.4",
                "atol": 1e-3,
                "rtol": 1e-6,
                "missing_sentinels": [-99.0],
                "discrete_eps": 1e-12,
                "signflip_eps": 1e-3,
            },
        })
        + "\n",
        encoding="utf-8",
    )
    golden_root = tmp_path / "golden"
    save_parity_golden(golden_run, golden_root)

    current_run = tmp_path / "current_run"
    (current_run / "work_fpexe").mkdir(parents=True)
    (current_run / "work_fppy").mkdir(parents=True)
    shutil.copy2(fixture_dir / "golden_fpexe.pabev", current_run / "work_fpexe" / "PABEV.TXT")
    shutil.copy2(
        fixture_dir / "current_new_diff_fppy.pabev",
        current_run / "work_fppy" / "PABEV.TXT",
    )
    (current_run / "parity_report.json").write_text(
        json.dumps({
            "scenario_name": "baseline",
            "status": "ok",
            "exit_code": 0,
            "pabev_detail": {
                "start": "2025.4",
                "atol": 1e-3,
                "rtol": 1e-6,
                "missing_sentinels": [-99.0],
                "discrete_eps": 1e-12,
                "signflip_eps": 1e-3,
            },
        })
        + "\n",
        encoding="utf-8",
    )

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        return ParityResult(
            status="ok",
            run_dir=str(current_run),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: baseline\nfp_home: {tmp_path}\n", encoding="utf-8")

    result = runner.invoke(
        app,
        ["parity", str(scenario), "--regression", str(golden_root)],
    )

    assert result.exit_code == 6
    output = result.stdout + result.stderr
    assert "regression_status=failed" in output


def test_parity_command_regression_missing_golden_dir_fails(tmp_path, monkeypatch):
    from fp_wraptr.analysis.parity import ParityResult

    parity_dir = tmp_path / "parity_run"
    (parity_dir / "work_fpexe").mkdir(parents=True)
    (parity_dir / "work_fppy").mkdir(parents=True)
    (parity_dir / "parity_report.json").write_text(
        json.dumps({
            "scenario_name": "baseline",
            "status": "ok",
            "exit_code": 0,
            "pabev_detail": {
                "start": "2025.4",
                "atol": 1e-3,
                "rtol": 1e-6,
                "missing_sentinels": [-99.0],
                "discrete_eps": 1e-12,
                "signflip_eps": 1e-3,
            },
        })
        + "\n",
        encoding="utf-8",
    )
    (parity_dir / "work_fpexe" / "PABEV.TXT").write_text(
        "SMPL 2025.4 2025.4;\nLOAD A;\n1.0\n'END'\n",
        encoding="utf-8",
    )
    (parity_dir / "work_fppy" / "PABEV.TXT").write_text(
        "SMPL 2025.4 2025.4;\nLOAD A;\n1.0\n'END'\n",
        encoding="utf-8",
    )

    def fake_run_parity(
        config, output_dir, fp_home_override=None, gate=None, fingerprint_lock=None
    ):
        return ParityResult(
            status="ok",
            run_dir=str(parity_dir),
            scenario_name=config.name,
            input_fingerprint={"algo": "sha256", "files": {}},
            exit_code=0,
        )

    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", fake_run_parity)
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(f"name: baseline\nfp_home: {tmp_path}\n", encoding="utf-8")

    missing_golden = tmp_path / "does_not_exist_golden"
    result = runner.invoke(
        app,
        ["parity", str(scenario), "--regression", str(missing_golden)],
    )

    assert result.exit_code == 1
    output = (result.stdout + result.stderr).lower()
    assert "parity regression compare failed" in output
    assert "missing golden parity report" in output


def test_validate_command_with_valid_yaml(tmp_path):
    scenario = tmp_path / "valid.yaml"
    scenario.write_text("name: test\n", encoding="utf-8")

    result = runner.invoke(app, ["validate", str(scenario)])

    assert result.exit_code == 0
    output = (result.stdout + result.stderr).lower()
    assert "scenario summary" in output
    assert "name" in output
    assert "test" in output


def test_validate_command_with_invalid_yaml(tmp_path):
    scenario = tmp_path / "invalid.yaml"
    scenario.write_text("track_variables: not-a-list\n", encoding="utf-8")

    result = runner.invoke(app, ["validate", str(scenario)])

    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "validation failed" in output


def test_validate_with_missing_required_field(tmp_path):
    scenario = tmp_path / "missing.yaml"
    scenario.write_text("description: missing name\n", encoding="utf-8")

    result = runner.invoke(app, ["validate", str(scenario)])

    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "validation failed" in output


def test_diff_with_missing_dirs_fails(tmp_path):
    first = tmp_path / "run_a"
    second = tmp_path / "run_b"

    result = runner.invoke(app, ["diff", str(first), str(second)])

    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "missing output files" in output or "diff failed" in output


def test_diff_with_empty_dirs(tmp_path):
    first = tmp_path / "empty_a"
    second = tmp_path / "empty_b"

    result = runner.invoke(app, ["diff", str(first), str(second)])

    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "missing output files" in output or "diff failed" in output


def test_diff_export_csv(tmp_path):
    run_a = tmp_path / "run_a"
    run_b = tmp_path / "run_b"
    run_a.mkdir()
    run_b.mkdir()

    fmout_a = run_a / "fmout.txt"
    fmout_b = run_b / "fmout.txt"
    run_a_data = """\
Some header text
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;

    Variable   Periods forecast are  2025.4  TO   2025.4

                   2025.4      2025.5
                               2026.1

   1 PCY      P lv   1.0      2.0
             P ch   0.1      0.1
             P %ch  10.0     10.0
"""
    fmout_a.write_text(
        run_a_data,
        encoding="utf-8",
    )
    fmout_b.write_text(
        run_a_data.replace("1.0      2.0", "1.0      2.5"),
        encoding="utf-8",
    )

    export_path = tmp_path / "diff.csv"
    result = runner.invoke(
        app,
        ["diff", str(run_a), str(run_b), "--export", "csv", "--output", str(export_path)],
    )

    assert result.exit_code == 0
    assert export_path.exists()
    text = export_path.read_text(encoding="utf-8")
    assert "variable,baseline,scenario,abs_delta,pct_delta" in text


def test_diff_export_csv_with_zero_baseline(tmp_path):
    run_a = tmp_path / "run_a"
    run_b = tmp_path / "run_b"
    run_a.mkdir()
    run_b.mkdir()

    run_a_data = """\
Some header text
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;

    Variable   Periods forecast are  2025.4  TO   2025.4

                   2025.4      2025.5
                               2026.1

   1 ZERO     P lv   1.0      0.0
             P ch   0.1      0.1
             P %ch  10.0     10.0
"""
    run_b_data = run_a_data.replace("1.0      0.0", "1.0      2.0")
    (run_a / "fmout.txt").write_text(run_a_data, encoding="utf-8")
    (run_b / "fmout.txt").write_text(run_b_data, encoding="utf-8")

    export_path = tmp_path / "diff_zero_baseline.csv"
    result = runner.invoke(
        app,
        [
            "diff",
            str(run_a),
            str(run_b),
            "--export",
            "csv",
            "--output",
            str(export_path),
        ],
    )

    assert result.exit_code == 0
    assert export_path.exists()
    rows = list(csv.DictReader(export_path.read_text(encoding="utf-8").splitlines()))
    assert rows[0]["variable"] == "ZERO"
    assert rows[0]["pct_delta"] in {"", "N/A"}


def test_diff_export_excel_with_zero_baseline(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")

    run_a = tmp_path / "run_a"
    run_b = tmp_path / "run_b"
    run_a.mkdir()
    run_b.mkdir()

    run_a_data = """\
Some header text
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;

    Variable   Periods forecast are  2025.4  TO   2025.4

                   2025.4      2025.5
                               2026.1

   1 ZERO     P lv   1.0      0.0
             P ch   0.1      0.1
             P %ch  10.0     10.0
"""
    run_b_data = run_a_data.replace("1.0      0.0", "1.0      2.0")
    (run_a / "fmout.txt").write_text(run_a_data, encoding="utf-8")
    (run_b / "fmout.txt").write_text(run_b_data, encoding="utf-8")

    export_path = tmp_path / "diff_zero_baseline.xlsx"
    result = runner.invoke(
        app,
        [
            "diff",
            str(run_a),
            str(run_b),
            "--export",
            "excel",
            "--output",
            str(export_path),
        ],
    )

    assert result.exit_code == 0
    assert export_path.exists()
    workbook = openpyxl.load_workbook(export_path)
    sheet = workbook.active
    headers = [cell.value for cell in sheet[1]]
    pct_delta_index = headers.index("pct_delta")
    assert sheet.cell(row=2, column=pct_delta_index + 1).value in (
        None,
        "",
    )
    assert sheet.cell(row=2, column=1).value == "ZERO"


def test_diff_export_unsupported_format(tmp_path):
    run_a = tmp_path / "run_a"
    run_b = tmp_path / "run_b"
    run_a.mkdir()
    run_b.mkdir()

    (run_a / "fmout.txt").write_text("SOLVE DYNAMIC;\n", encoding="utf-8")
    (run_b / "fmout.txt").write_text("SOLVE DYNAMIC;\n", encoding="utf-8")

    result = runner.invoke(
        app,
        ["diff", str(run_a), str(run_b), "--export", "json", "--output", "x.txt"],
    )
    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "export format must be 'csv' or 'excel'" in output


def test_history_lists_runs(tmp_path):
    run_a = tmp_path / "artifacts" / "baseline_20260223_120000"
    run_b = tmp_path / "artifacts" / "policy_20260223_120001"
    run_a.mkdir(parents=True)
    run_b.mkdir(parents=True)

    run_a.joinpath("scenario.yaml").write_text(
        yaml.safe_dump({
            "name": "baseline",
            "description": "baseline run",
            "track_variables": ["PCY", "UR"],
        }),
        encoding="utf-8",
    )
    run_b.joinpath("scenario.yaml").write_text(
        yaml.safe_dump({
            "name": "policy",
            "description": "policy run",
            "track_variables": ["PCY", "UR"],
        }),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["history", "--artifacts-dir", str(tmp_path / "artifacts")])

    assert result.exit_code == 0
    output = result.stdout
    assert "policy" in output
    assert "baseline" in output
    assert "Run History" in output


def test_history_latest_shows_single_newest_run(tmp_path):
    run_a = tmp_path / "artifacts" / "baseline_20260223_120000"
    run_b = tmp_path / "artifacts" / "policy_20260223_120001"
    run_a.mkdir(parents=True)
    run_b.mkdir(parents=True)

    run_a.joinpath("scenario.yaml").write_text("name: baseline\n", encoding="utf-8")
    run_b.joinpath("scenario.yaml").write_text("name: policy\n", encoding="utf-8")

    result = runner.invoke(
        app,
        ["history", "--artifacts-dir", str(tmp_path / "artifacts"), "--latest"],
    )

    assert result.exit_code == 0
    output = result.stdout
    assert "policy" in output
    assert "baseline" not in output
    assert "run dir →" in output


def test_history_empty_directory(tmp_path):
    result = runner.invoke(app, ["history", "--artifacts-dir", str(tmp_path / "missing")])
    assert result.exit_code == 0
    output = result.stdout
    assert "No runs found" in output


def test_history_empty_artifacts_directory(tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    result = runner.invoke(app, ["history", "--artifacts-dir", str(artifacts_dir)])

    assert result.exit_code == 0
    output = result.stdout
    assert "No runs found" in output


def test_history_with_corrupted_scenario_yaml(tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    run_dir = artifacts_dir / "bad_20260223_000000"
    run_dir.mkdir(parents=True)
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "bad_run",
                "track_variables": {"not": "a list"},
            },
        ),
        encoding="utf-8",
    )
    (run_dir / "fmout.txt").write_text("SOLVE DYNAMIC;\n", encoding="utf-8")

    result = runner.invoke(app, ["history", "--artifacts-dir", str(artifacts_dir)])

    assert result.exit_code == 0
    output = result.stdout
    assert "Run History" in output
    assert "bad" in output
    assert "bad_run" not in output


def test_graph_command_outputs_summary(tmp_path):
    pytest.importorskip("networkx")

    fminput = tmp_path / "fminput.txt"
    fminput.write_text(
        "SMPL 2025.4 2025.6;\nEQ 1 AA BB CC;\nIDENT XX = AA + BB;\nGENR YY=ZZ;\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["graph", str(fminput), "--variable", "AA"])
    assert result.exit_code == 0
    output = (result.stdout + result.stderr).lower()
    assert "dependency graph summary" in output
    assert "upstream" in output


def test_graph_command_without_networkx(tmp_path, monkeypatch):
    fminput = tmp_path / "fminput.txt"
    fminput.write_text("SMPL 2025.4 2025.6;\n", encoding="utf-8")

    monkeypatch.setattr(importlib.util, "find_spec", lambda *_args, **_kwargs: None)
    result = runner.invoke(app, ["graph", str(fminput)])

    assert result.exit_code == 1
    output = (result.stdout + result.stderr).lower()
    assert "networkx is required" in output


def test_graph_command_exports_dot(tmp_path):
    pytest.importorskip("networkx")

    fminput = tmp_path / "fminput.txt"
    fminput.write_text(
        "SMPL 2025.4 2025.6;\nEQ 1 AA BB CC;\nIDENT XX = AA + BB;\nGENR YY=ZZ;\n",
        encoding="utf-8",
    )

    output_path = tmp_path / "deps.dot"
    result = runner.invoke(
        app,
        ["graph", str(fminput), "--export", "dot", "--output", str(output_path)],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    payload = output_path.read_text(encoding="utf-8")
    assert "digraph fp_dependencies" in payload
    assert "AA" in payload and "BB" in payload and "CC" in payload


def test_viz_plot_missing_output_file():
    result = runner.invoke(app, ["viz", "plot", "does-not-exist.txt"])

    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "not found" in output


def test_batch_command_runs_multiple_scenarios(tmp_path, monkeypatch):
    from fp_wraptr.runtime.fp_exe import FPRunResult

    run_a = tmp_path / "scenario_a.yaml"
    run_b = tmp_path / "scenario_b.yaml"
    run_a.write_text("name: scenario_a\n", encoding="utf-8")
    run_b.write_text("name: scenario_b\n", encoding="utf-8")

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

    monkeypatch.setattr(
        "fp_wraptr.scenarios.runner.FPExecutable.check_available", lambda _self: True
    )
    monkeypatch.setattr("fp_wraptr.scenarios.runner.FPExecutable.run", fake_run)

    result = runner.invoke(
        app,
        [
            "batch",
            str(run_a),
            str(run_b),
            "--output-dir",
            str(tmp_path / "batch_artifacts"),
        ],
    )

    assert result.exit_code == 0
    output = (result.stdout + result.stderr).lower()
    assert "batch run results" in output
    assert "scenario_a" in output
    assert "scenario_b" in output


def test_batch_command_with_nonexistent_yaml(tmp_path):
    result = runner.invoke(app, ["batch", str(tmp_path / "nonexistent.yaml")])

    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "unable to read scenario" in output or "not found" in output


def test_dashboard_command_without_streamlit(monkeypatch):
    import fp_wraptr.cli as cli_module

    monkeypatch.setattr(
        cli_module.importlib.util,
        "find_spec",
        lambda *args, **kwargs: None,
    )
    result = runner.invoke(app, ["dashboard"])

    assert result.exit_code == 1
    output = (result.stdout + result.stderr).lower()
    assert "streamlit is not installed" in output


def test_version_command():
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    output = (result.stdout + result.stderr).lower()
    assert "fp-wraptr" in output
    assert __version__.lower() in output


def test_version_flag():
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    output = (result.stdout + result.stderr).lower()
    assert "fp-wraptr" in output


def test_info_command():
    result = runner.invoke(app, ["info"])

    assert result.exit_code == 0
    output = result.stdout + result.stderr
    assert "fp-wraptr" in output
    assert "Python" in output


def test_info_command_prints_wineprefix_hint_when_uninitialized(monkeypatch):
    def _fake_preflight(_self):
        return {
            "available": False,
            "exe_path": "/tmp/fp.exe",
            "exe_exists": False,
            "missing_data_files": [],
            "input_file_exists": True,
            "wine_required": True,
            "wineprefix": "/tmp/wineprefix",
            "wineprefix_exists": True,
            "wineprefix_initialized": False,
        }

    monkeypatch.setattr("fp_wraptr.runtime.fp_exe.FPExecutable.preflight_report", _fake_preflight)
    result = runner.invoke(app, ["info"])

    assert result.exit_code == 0
    output = result.stdout + result.stderr
    assert "WINEPREFIX" in output
    assert "Wine hint" in output
    assert "wineboot" in output


def test_data_fetch_fair_bundle_command(monkeypatch, tmp_path):
    out_dir = tmp_path / "official_bundle"
    local_zip = tmp_path / "artifacts" / "FMFP.ZIP"
    local_zip.parent.mkdir(parents=True, exist_ok=True)
    local_zip.write_bytes(b"fake")
    observed: dict[str, str | None] = {"zip_path": None}

    def _fake_fetch_and_unpack_fair_bundle(*, out_dir, url, timeout_seconds=60, zip_path=None):
        _ = url, timeout_seconds
        observed["zip_path"] = str(zip_path) if zip_path is not None else None
        model_dir = Path(out_dir) / "FM"
        model_dir.mkdir(parents=True, exist_ok=True)
        manifest = Path(out_dir) / "fair_bundle_manifest.json"
        manifest.write_text("{}", encoding="utf-8")
        return {
            "output_dir": str(out_dir),
            "model_dir": str(model_dir),
            "manifest_path": str(manifest),
        }

    monkeypatch.setattr(
        "fp_wraptr.data.fair_bundle.fetch_and_unpack_fair_bundle",
        _fake_fetch_and_unpack_fair_bundle,
    )

    result = runner.invoke(
        app,
        [
            "data",
            "fetch-fair-bundle",
            "--out-dir",
            str(out_dir),
            "--zip-path",
            str(local_zip),
        ],
    )
    assert result.exit_code == 0
    assert observed["zip_path"] == str(local_zip)
    output = result.stdout + result.stderr
    assert "Bundle output:" in output
    assert "Manifest:" in output
    assert "does not include `fp.exe`" in output
    assert "run dir →" in output


def test_describe_command_json(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        ["describe", "GDP", "--dictionary", str(dictionary_path), "--format", "json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["name"] == "GDP"
    assert payload["defined_by_equation"] == 82


def test_dictionary_search_json(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "search",
            "eq 82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["equation_matches"][0]["equation"]["id"] == 82


def test_dictionary_search_compact_intent_json(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "search",
            "UR in equation 82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["intent"]["kind"] == "variable_in_equation"
    assert payload["focus"]["variable_in_equation"]["present_in_equation"] is True


def test_dictionary_search_intent_diagnostics_json(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "search",
            "UR in equation 82",
            "--dictionary",
            str(dictionary_path),
            "--intent-diagnostics",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["query"] == "UR in equation 82"
    assert payload["intent"]["kind"] == "variable_in_equation"
    assert payload["focus"]["variable_in_equation"]["equation_id"] == 82
    assert "equation_matches" not in payload


def test_dictionary_search_variables_in_equation_json(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "search",
            "variables in equation 82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["intent"]["kind"] == "equation_variables_lookup"
    assert payload["focus"]["equation_variables_lookup"]["equation_id"] == 82
    assert payload["equation_matches"][0]["equation"]["id"] == 82
    assert any(match["reason"] == "equation_variable" for match in payload["variable_matches"])


def test_dictionary_search_meaning_in_equation_intent_diagnostics(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "search",
            "meaning of UR in equation 82",
            "--dictionary",
            str(dictionary_path),
            "--intent-diagnostics",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["intent"]["kind"] == "variable_meaning_in_equation"
    assert payload["focus"]["variable_meaning_in_equation"]["equation_id"] == 82


def test_dictionary_search_json_snapshot(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "search",
            "UR in equation 82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload == _expected_query_snapshot()


def test_dictionary_search_csv_stdout(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "search",
            "UR in equation 82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "csv",
        ],
    )
    assert result.exit_code == 0
    rows = list(csv.DictReader(result.stdout.splitlines()))
    assert rows
    sections = {row["section"] for row in rows}
    assert "equation" in sections
    assert "variable" in sections
    assert any(row["intent_kind"] == "variable_in_equation" for row in rows)


def test_dictionary_search_json_output_file(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    output_path = tmp_path / "search.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "search",
            "eq 82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["equation_matches"][0]["equation"]["id"] == 82


def test_dictionary_search_table_with_output_fails(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    output_path = tmp_path / "search.txt"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "search",
            "eq 82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "table",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "--output requires --format json or --format csv" in output


def test_dictionary_equation_json(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "equation",
            "82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["equation"]["id"] == 82
    names = {item["name"] for item in payload["variables"]}
    assert {"GDP", "HG", "UR"} <= names
    ur = next(item for item in payload["variables"] if item["name"] == "UR")
    assert ur["description_source"] == "construction"


def test_dictionary_equation_csv_stdout(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "equation",
            "82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "csv",
        ],
    )
    assert result.exit_code == 0
    rows = list(csv.DictReader(result.stdout.splitlines()))
    assert rows
    names = {row["variable_name"] for row in rows}
    assert {"GDP", "HG", "UR"} <= names
    assert all(row["equation_id"] == "82" for row in rows)


def test_dictionary_equation_json_output_file(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    output_path = tmp_path / "equation.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "equation",
            "82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["equation"]["id"] == 82


def test_dictionary_equation_table_with_output_fails(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    output_path = tmp_path / "equation.txt"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "equation",
            "82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "table",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code != 0
    output = (result.stdout + result.stderr).lower()
    assert "--output requires --format json or --format csv" in output


def test_dictionary_equation_json_snapshot(tmp_path):
    dictionary_path = tmp_path / "dictionary.json"
    _write_dictionary_json(dictionary_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "equation",
            "82",
            "--dictionary",
            str(dictionary_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload == _expected_explain_snapshot()


def test_dictionary_sources_json(tmp_path):
    dictionary_path = tmp_path / "dictionary_raw.json"
    source_map_path = tmp_path / "source_map.yaml"
    _write_dictionary_json_with_raw_data(dictionary_path)
    _write_source_map_yaml(source_map_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "sources",
            "GDP",
            "--dictionary",
            str(dictionary_path),
            "--source-map",
            str(source_map_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["variable"] == "GDP"
    assert payload["mapping_status"] == "mapped"
    assert payload["source_map_entry"]["source"] == "fred"
    assert payload["source_map_entry"]["annual_rate"] is False
    assert payload["normalization"] is None
    assert payload["dictionary_raw_data_sources"] == ["R11"]
    assert payload["dictionary_raw_data_details"][0]["r_number"] == "R11"


def test_dictionary_sources_json_monthly_annual_rate_has_divisor(tmp_path):
    dictionary_path = tmp_path / "dictionary_raw.json"
    source_map_path = tmp_path / "source_map.yaml"
    _write_dictionary_json_with_raw_data(dictionary_path)
    _write_source_map_yaml_monthly_annual_rate(source_map_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "sources",
            "UR",
            "--dictionary",
            str(dictionary_path),
            "--source-map",
            str(source_map_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["variable"] == "UR"
    assert payload["source_map_entry"]["annual_rate"] is True
    assert payload["normalization"]["annual_rate_divisor"] == 12
    assert payload["normalization"]["per_period_formula"] == "value / 12"


def test_dictionary_source_coverage_json(tmp_path):
    dictionary_path = tmp_path / "dictionary_raw.json"
    source_map_path = tmp_path / "source_map.yaml"
    _write_dictionary_json_with_raw_data(dictionary_path)
    _write_source_map_yaml(source_map_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "source-coverage",
            "--dictionary",
            str(dictionary_path),
            "--source-map",
            str(source_map_path),
            "--only-with-raw-data",
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["scope"] == "variables_with_raw_data"
    assert payload["population_count"] == 1
    assert payload["mapped_count"] == 1
    assert payload["missing_count"] == 0


def test_dictionary_source_quality_json(tmp_path):
    dictionary_path = tmp_path / "dictionary_raw.json"
    source_map_path = tmp_path / "source_map_bad.yaml"
    _write_dictionary_json_with_raw_data(dictionary_path)
    _write_source_map_yaml_with_quality_issue(source_map_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "source-quality",
            "--dictionary",
            str(dictionary_path),
            "--source-map",
            str(source_map_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["scope"] == "all_dictionary_variables"
    assert payload["issue_count"] >= 1
    assert payload["issue_breakdown"]["missing_series_id"] >= 1


def test_dictionary_source_report_json_output_file(tmp_path):
    dictionary_path = tmp_path / "dictionary_raw.json"
    source_map_path = tmp_path / "source_map.yaml"
    output_path = tmp_path / "source_report.json"
    _write_dictionary_json_with_raw_data(dictionary_path)
    _write_source_map_yaml(source_map_path)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "source-report",
            "--dictionary",
            str(dictionary_path),
            "--source-map",
            str(source_map_path),
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["dictionary_variable_count"] == 2
    assert payload["source_map_variable_count"] == 1
    assert "coverage_with_raw_data" in payload
    assert "quality_with_raw_data" in payload


def test_dictionary_source_window_check_json(tmp_path, monkeypatch):
    source_map_path = tmp_path / "source_map_window.yaml"
    _write_source_map_yaml_with_window(source_map_path)
    monkeypatch.setattr(
        "fp_wraptr.cli.importlib.util.find_spec",
        lambda *_args, **_kwargs: object(),
    )

    def _fake_fetch_series(series_ids, start=None, end=None, cache_dir=None):
        assert series_ids == ["BOGZ1FA315410093Q"]
        return pd.DataFrame(
            {"BOGZ1FA315410093Q": [0.0, 10.0, 0.0, 2.0]},
            index=pd.to_datetime(["2008-07-01", "2008-10-01", "2012-04-01", "2012-07-01"]),
        )

    monkeypatch.setattr("fp_wraptr.fred.ingest.fetch_series", _fake_fetch_series)

    result = runner.invoke(
        app,
        [
            "dictionary",
            "source-window-check",
            "--source-map",
            str(source_map_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["series_checked"] == 1
    assert payload["violation_count"] == 1
    assert payload["checks"][0]["variable"] == "CTGB"
    assert payload["checks"][0]["status"] == "violation"


def test_report_command_generates_markdown(tmp_path):
    scenario_dir = tmp_path / "run_20260223_120000"
    scenario_dir.mkdir()
    (scenario_dir / "scenario.yaml").write_text(
        """\
name: test_run
description: test report run
""",
        encoding="utf-8",
    )
    (scenario_dir / "fmout.txt").write_text(
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

    result = runner.invoke(app, ["report", str(scenario_dir)])

    assert result.exit_code == 0
    output = (result.stdout + result.stderr).lower()
    assert "fp-wraptr run report" in output
    assert "test_run" in output
    assert "top 10 variable levels" in output


def test_report_command_with_baseline(tmp_path):
    baseline_dir = tmp_path / "baseline_20260223_120000"
    scenario_dir = tmp_path / "run_20260223_120000"
    baseline_dir.mkdir()
    scenario_dir.mkdir()

    baseline_dir.joinpath("scenario.yaml").write_text(
        """\
name: baseline_run
description: baseline report run
""",
        encoding="utf-8",
    )
    baseline_dir.joinpath("fmout.txt").write_text(
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

    scenario_dir.joinpath("scenario.yaml").write_text(
        """\
name: test_run
description: test report run
""",
        encoding="utf-8",
    )
    scenario_dir.joinpath("fmout.txt").write_text(
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

    result = runner.invoke(app, ["report", str(scenario_dir), "--baseline", str(baseline_dir)])

    assert result.exit_code == 0
    output = (result.stdout + result.stderr).lower()
    assert "baseline comparison" in output
    assert "compared variables" in output


def test_cli_packs_and_workspace_flow(tmp_path, monkeypatch):
    repo = _make_repo(tmp_path / "repo")
    _write_local_pack_manifest(repo)
    monkeypatch.chdir(repo)

    result = runner.invoke(app, ["packs", "list"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["packs"][0]["pack_id"] == "pse2025"

    result = runner.invoke(app, ["workspace", "create-catalog", "pse-base"])
    assert result.exit_code == 0
    workspace = json.loads(result.stdout)
    workspace_id = workspace["workspace_id"]
    assert workspace_id == "pse2025--pse2025-base"

    result = runner.invoke(app, ["workspace", "cards", workspace_id])
    assert result.exit_code == 0
    cards = json.loads(result.stdout)
    assert {item["card_id"] for item in cards["cards"]} == {
        "pse2025.intgadj",
        "pse2025.jg_constants",
    }

    result = runner.invoke(
        app,
        [
            "workspace",
            "apply-card",
            workspace_id,
            "pse2025.jg_constants",
            "--constants",
            '{"JPTUR": 0.55}',
        ],
    )
    assert result.exit_code == 0
    updated = json.loads(result.stdout)
    assert updated["recipe_history"][-1]["operation"] == "apply_workspace_card"

    result = runner.invoke(
        app,
        [
            "workspace",
            "import-series",
            workspace_id,
            "pse2025.intgadj",
            "--series-json",
            '{"2025.4": 1.5, "2026.1": 2.5}',
        ],
    )
    assert result.exit_code == 0
    updated = json.loads(result.stdout)
    assert updated["recipe_history"][-1]["operation"] == "import_workspace_series"

    result = runner.invoke(app, ["workspace", "compile", workspace_id])
    assert result.exit_code == 0
    compiled = json.loads(result.stdout)
    assert compiled["ok"] is True


def test_cli_workspace_bundle_variant_and_public_catalog_filter(tmp_path, monkeypatch):
    repo = _make_repo(tmp_path / "repo")
    _write_local_pack_manifest(repo)
    monkeypatch.chdir(repo)

    result = runner.invoke(app, ["workspace", "create-catalog", "pse-bundle"])
    assert result.exit_code == 0
    workspace = json.loads(result.stdout)
    workspace_id = workspace["workspace_id"]

    result = runner.invoke(
        app,
        [
            "workspace",
            "add-variant",
            workspace_id,
            "custom",
            "--scenario-name",
            "custom",
            "--clone-from",
            "base",
            "--input-file",
            "psebase.txt",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["variant_count"] == 4
    assert payload["variants"][-1]["scenario_name"] == "custom"

    result = runner.invoke(app, ["workspace", "cards", workspace_id, "--variant-id", "custom"])
    assert result.exit_code == 0
    scoped_cards = json.loads(result.stdout)
    assert scoped_cards["scope"] == "variant"
    assert scoped_cards["variant_id"] == "custom"

    result = runner.invoke(
        app,
        [
            "workspace",
            "update-variant",
            workspace_id,
            "custom",
            "--label",
            "Custom Variant",
            "--scenario-name",
            "custom_exact",
            "--input-file",
            "pselow.txt",
            "--disabled",
        ],
    )
    assert result.exit_code == 0
    updated_variant_payload = json.loads(result.stdout)
    updated_variant = next(item for item in updated_variant_payload["variants"] if item["variant_id"] == "custom")
    assert updated_variant["label"] == "Custom Variant"
    assert updated_variant["scenario_name"] == "custom_exact"
    assert updated_variant["input_file"] == "pselow.txt"
    assert updated_variant["enabled"] is False

    result = runner.invoke(
        app,
        [
            "workspace",
            "clone-variant",
            workspace_id,
            "recipe_variant",
            "--clone-from",
            "base",
            "--label",
            "Recipe Variant",
            "--scenario-name",
            "recipe_variant_exact",
            "--input-file",
            "psebase.txt",
            "--card-id",
            "pse2025.jg_constants",
            "--constants",
            '{"JGW": 0.015, "JGCOLA": 0}',
        ],
    )
    assert result.exit_code == 0
    cloned_variant_payload = json.loads(result.stdout)
    cloned_variant = next(item for item in cloned_variant_payload["variants"] if item["variant_id"] == "recipe_variant")
    assert cloned_variant["scenario_name"] == "recipe_variant_exact"

    result = runner.invoke(app, ["workspace", "visualizations", workspace_id])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert any(item["view_id"] == "pse-main" for item in payload["visualizations"])

    catalog = importlib.import_module("fp_wraptr.scenarios.catalog").load_scenario_catalog(repo_root=repo)
    assert [entry.entry_id for entry in catalog.filtered(surface="home", public_only=True)] == []
