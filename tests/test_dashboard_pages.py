"""Smoke tests for Streamlit dashboard pages with synthetic artifacts."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path

import pandas as pd
import pytest
import yaml

streamlit = pytest.importorskip("streamlit")
apptest = pytest.importorskip("streamlit.testing.v1")

from fp_wraptr.dashboard.artifacts import scan_artifacts  # noqa: E402
from fp_wraptr.scenarios.config import ScenarioConfig, VariableOverride  # noqa: E402

AppTest = apptest.AppTest


_FMOUT_TEXT = """\
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
Variable   Periods forecast are  2025.4  TO   2025.4

                   2025.4      2025.5
                               2026.1

   1 PCY      P lv   1.0      2.0
             P ch   0.1      0.1
             P %ch  10.0     10.0

   2 UR       P lv   4.0      3.8
             P ch  -0.2      -0.2
             P %ch -5.0      -4.9
"""


def _write_run(root: Path, name: str, timestamp: str, growth: float) -> Path:
    run_dir = root / f"{name}_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump({
            "name": name,
            "description": f"synthetic {name}",
            "track_variables": ["PCY", "UR"],
        }),
        encoding="utf-8",
    )
    (run_dir / "fmout.txt").write_text(
        _FMOUT_TEXT.replace("1.0      2.0", f"1.0      {growth:.1f}"),
        encoding="utf-8",
    )
    return run_dir


def _dashboard_root() -> Path:
    return Path(__file__).resolve().parents[1] / "apps" / "dashboard"


def _write_pabev(path: Path, *, vars_to_values: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["SMPL 2025.4 2026.1;"]
    for name, values in vars_to_values.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(f"{value:.6f}" for value in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_parity_run(
    root: Path,
    name: str,
    timestamp: str,
    *,
    status: str = "hard_fail",
    hard_fail_count: int = 1,
    write_triage_hardfails_csv: bool = False,
    write_support_gap_top_md: bool = False,
    write_support_gap_map_csv: bool = False,
    eq_flags_preset_in_report: str | None = "parity",
    write_fppy_stdout: bool = False,
    stdout_eq_flags_preset: str | None = None,
    write_parity_regression_json: bool = False,
    fpexe_solution_errors: list[dict[str, object]] | None = None,
    fpexe_preflight_report: dict[str, object] | None = None,
    eq_backfill_failed: int | None = None,
    eq_backfill_first_failure_error: str | None = None,
    outside_seed_inspected_cells: int | None = None,
    outside_seed_candidate_cells: int | None = None,
) -> Path:
    run_dir = root / f"{name}_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump({"name": name, "description": f"synthetic parity {name}"}),
        encoding="utf-8",
    )
    _write_pabev(
        run_dir / "work_fpexe" / "PABEV.TXT",
        vars_to_values={
            "AA": [100.0, 101.0],
            "IVF": [5.0, -99.0],
        },
    )
    _write_pabev(
        run_dir / "work_fppy" / "PABEV.TXT",
        vars_to_values={
            "AA": [99.0, 103.0],
            "IVF": [-5.0, 110.0],
        },
    )
    summary_payload = {
        "unsupported": 3,
        "unsupported_counts": {"EQ": 1, "MODEQ": 1, "FSR": 1},
        "solve_active_window": {"start": "2025.4", "end": "2026.1"},
        "eq_backfill_converged": False,
        "eq_backfill_iterations": 1,
        "eq_backfill_min_iters": 1,
        "eq_backfill_max_iters": 1,
        "eq_backfill_stop_reason": "max_iters_reached",
        "eq_backfill_period_sequential": False,
        "eq_use_setupsolve": True,
    }
    if outside_seed_inspected_cells is not None:
        summary_payload["solve_outside_seed_inspected_cells"] = int(outside_seed_inspected_cells)
    if outside_seed_candidate_cells is not None:
        summary_payload["solve_outside_seed_candidate_cells"] = int(outside_seed_candidate_cells)
    if eq_backfill_failed is not None:
        summary_payload["eq_backfill_failed"] = int(eq_backfill_failed)
    if eq_backfill_first_failure_error is not None:
        summary_payload["eq_backfill_first_failure_blame"] = {
            "error": str(eq_backfill_first_failure_error)
        }

    (run_dir / "work_fppy" / "fppy_report.json").write_text(
        json.dumps(
            {
                "summary": summary_payload,
                "unsupported_examples": [
                    {
                        "command": "EQ",
                        "line": 101,
                        "statement": "EQ XX = YY;",
                    },
                    {
                        "command": "MODEQ",
                        "line": 202,
                        "statement": "MODEQ ZZ = WW;",
                    },
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    hard_fail_rows = [
        {
            "variable": "MISSING_IN_SHARED",
            "period": "2025.4",
            "reason": "sign_flip",
        }
        for _ in range(max(1, hard_fail_count))
    ]
    (run_dir / "parity_report.json").write_text(
        json.dumps(
            {
                "status": status,
                "exit_code": 3,
                "engine_runs": {
                    "fpexe": {
                        "pabev_path": None,
                        "details": ({
                            **(
                                {"solution_errors": fpexe_solution_errors}
                                if fpexe_solution_errors is not None
                                else {}
                            ),
                            **(
                                {"preflight_report": fpexe_preflight_report}
                                if fpexe_preflight_report is not None
                                else {}
                            ),
                        }),
                    },
                    "fppy": {
                        "pabev_path": None,
                        "details": (
                            {"eq_flags_preset": eq_flags_preset_in_report}
                            if eq_flags_preset_in_report is not None
                            else {}
                        ),
                    },
                },
                "pabev_detail": {
                    "missing_sentinels": [-99.0],
                    "hard_fail_cell_count": max(1, hard_fail_count),
                    "hard_fail_cells": hard_fail_rows,
                    "missing_left": [],
                    "missing_right": [],
                    "max_abs_diff": 2.0,
                    "median_abs_diff": 1.0,
                    "p90_abs_diff": 2.0,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    if write_triage_hardfails_csv:
        triage_csv = run_dir / "work_fppy" / "triage_hardfails.csv"
        triage_csv.parent.mkdir(parents=True, exist_ok=True)
        triage_csv.write_text(
            "variable,period,reason,abs_diff\n"
            "AA,2025.4,sign_flip,1.0\n"
            "IVF,2026.1,missing_sentinel_mismatch,2.0\n",
            encoding="utf-8",
        )
    if write_support_gap_top_md:
        support_gap_md = run_dir / "work_fppy" / "support_gap_top.md"
        support_gap_md.parent.mkdir(parents=True, exist_ok=True)
        support_gap_md.write_text(
            "# Support Gap Top\n\n"
            "- EQ: 1 unsupported statement\n"
            "- MODEQ: 1 unsupported statement\n",
            encoding="utf-8",
        )
    if write_support_gap_map_csv:
        support_gap_map = run_dir / "work_fppy" / "support_gap_map.csv"
        support_gap_map.parent.mkdir(parents=True, exist_ok=True)
        support_gap_map.write_text(
            "command,line,statement\nEQ,101,EQ XX = YY;\nMODEQ,202,MODEQ ZZ = WW;\n",
            encoding="utf-8",
        )
    if write_fppy_stdout:
        stdout_path = run_dir / "work_fppy" / "fppy.stdout.txt"
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        preset_text = stdout_eq_flags_preset or "parity"
        stdout_path.write_text(
            f"run config: eq_flags_preset={preset_text}\n",
            encoding="utf-8",
        )
    if write_parity_regression_json:
        (run_dir / "parity_regression.json").write_text(
            json.dumps(
                {
                    "status": "failed",
                    "reason": "new_findings",
                    "counts": {
                        "new_missing_left": 1,
                        "new_missing_right": 0,
                        "new_hard_fail_cells": 2,
                        "new_diff_variables": 1,
                        "resolved_missing_left": 0,
                        "resolved_missing_right": 1,
                        "resolved_hard_fail_cells": 0,
                        "resolved_diff_variables": 1,
                    },
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    return run_dir


def test_dashboard_home_page_smoke(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    _write_run(artifacts_dir, "baseline", "20260223_120000", 2.0)
    _write_run(artifacts_dir, "policy", "20260223_120001", 2.4)

    app_path = _dashboard_root() / "app.py"
    at = AppTest.from_file(str(app_path))
    at.query_params["artifacts-dir"] = str(artifacts_dir)
    at.run()

    assert not at.exception


def test_dashboard_home_active_run_checkbox_can_be_rechecked(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    _write_run(artifacts_dir, "baseline", "20260223_120000", 2.0)
    _write_run(artifacts_dir, "policy", "20260223_120001", 2.4)
    _write_run(artifacts_dir, "alt", "20260223_120002", 2.8)

    app_path = _dashboard_root() / "app.py"
    at = AppTest.from_file(str(app_path))
    at.query_params["artifacts-dir"] = str(artifacts_dir)
    at.run(timeout=10)

    initial_keys = list(at.session_state["dashboard_active_run_dirs"])
    assert len(initial_keys) == 3
    first_checkbox_key = (
        f"home_run_pick_{hashlib.sha1(initial_keys[0].encode('utf-8')).hexdigest()[:12]}"
    )

    at.session_state[first_checkbox_key] = False
    at.run(timeout=10)

    unchecked_keys = list(at.session_state["dashboard_active_run_dirs"])
    assert unchecked_keys == initial_keys[1:]

    at.session_state[first_checkbox_key] = True
    at.run(timeout=10)

    assert set(at.session_state["dashboard_active_run_dirs"]) == set(initial_keys)
    assert initial_keys[0] in at.session_state["dashboard_active_run_dirs"]
    assert not at.exception


def test_dashboard_compare_page_smoke(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    _write_run(artifacts_dir, "baseline", "20260223_120000", 2.0)
    _write_run(artifacts_dir, "policy", "20260223_120001", 2.6)

    page_path = _dashboard_root() / "pages" / "2_Compare_Runs.py"
    at = AppTest.from_file(str(page_path))
    at.session_state["runs"] = scan_artifacts(artifacts_dir)
    at.run()

    assert not at.exception


def test_dashboard_mini_page_smoke(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    _write_run(artifacts_dir, "baseline", "20260223_120000", 2.0)
    _write_run(artifacts_dir, "policy", "20260223_120001", 2.6)

    page_path = _dashboard_root() / "pages" / "0_Run_Panels.py"
    at = AppTest.from_file(str(page_path))
    at.query_params["artifacts-dir"] = str(artifacts_dir)
    at.run(timeout=10)

    assert not at.exception


def test_dashboard_mini_run_picker_checkbox_can_be_rechecked(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    _write_run(artifacts_dir, "baseline", "20260223_120000", 2.0)
    _write_run(artifacts_dir, "policy", "20260223_120001", 2.6)
    _write_run(artifacts_dir, "alt", "20260223_120002", 2.8)

    page_path = _dashboard_root() / "pages" / "0_Run_Panels.py"
    at = AppTest.from_file(str(page_path))
    at.query_params["artifacts-dir"] = str(artifacts_dir)
    at.run(timeout=10)

    initial_keys = list(at.session_state["mini_dash_run_keys"])
    assert len(initial_keys) == 3
    first_checkbox_key = (
        f"mini_dash_run_pick_{hashlib.sha1(initial_keys[0].encode('utf-8')).hexdigest()[:12]}"
    )

    at.session_state[first_checkbox_key] = False
    at.run(timeout=10)

    unchecked_keys = list(at.session_state["mini_dash_run_keys"])
    assert unchecked_keys == initial_keys[1:]

    at.session_state[first_checkbox_key] = True
    at.run(timeout=10)

    assert set(at.session_state["mini_dash_run_keys"]) == set(initial_keys)
    assert initial_keys[0] in at.session_state["mini_dash_run_keys"]
    assert not at.exception


def test_dashboard_new_run_page_smoke(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    page_path = _dashboard_root() / "pages" / "3_New_Run.py"
    at = AppTest.from_file(str(page_path))
    at.query_params["artifacts-dir"] = str(artifacts_dir)
    at.run()

    assert not at.exception


def test_dashboard_sensitivity_page_smoke(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    _write_run(artifacts_dir, "baseline", "20260223_120000", 2.0)
    _write_run(artifacts_dir, "policy", "20260223_120001", 2.6)

    page_path = _dashboard_root() / "pages" / "7_Sensitivity.py"
    at = AppTest.from_file(str(page_path))
    at.session_state["runs"] = scan_artifacts(artifacts_dir)
    at.run()

    assert not at.exception


def test_sensitivity_sweep_candidates_include_scenario_overrides(tmp_path: Path) -> None:
    page_path = _dashboard_root() / "pages" / "7_Sensitivity.py"
    spec = importlib.util.spec_from_file_location("sensitivity_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    fm_home = tmp_path / "FM"
    fm_home.mkdir(parents=True, exist_ok=True)
    (fm_home / "fmexog.txt").write_text(
        "\n".join([
            "SMPL 2025.4 2029.4;",
            "CHANGEVAR;",
            "YS CHGSAMEPCT",
            "0.02",
            ";",
            "RETURN;",
            "",
        ]),
        encoding="utf-8",
    )
    config = ScenarioConfig(
        name="baseline",
        fp_home=fm_home,
        overrides={"pse2025_high": VariableOverride(method="CHGSAMEABS", value=1.0)},
    )

    candidates = module._sweep_variable_candidates(config)

    assert "YS" in candidates
    assert "PSE2025_HIGH" in candidates


def test_sensitivity_sweep_candidates_include_input_tree_changevar(tmp_path: Path) -> None:
    page_path = _dashboard_root() / "pages" / "7_Sensitivity.py"
    spec = importlib.util.spec_from_file_location("sensitivity_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    fm_home = tmp_path / "FM"
    fm_home.mkdir(parents=True, exist_ok=True)
    (fm_home / "fmexog.txt").write_text(
        "\n".join([
            "SMPL 2025.4 2029.4;",
            "CHANGEVAR;",
            "YS CHGSAMEPCT",
            "0.02",
            ";",
            "RETURN;",
            "",
        ]),
        encoding="utf-8",
    )
    (fm_home / "fminput.txt").write_text(
        "\n".join([
            "INPUT FILE=custom_exog.txt;",
            "RETURN;",
            "",
        ]),
        encoding="utf-8",
    )

    overlay_dir = tmp_path / "overlay"
    overlay_dir.mkdir(parents=True, exist_ok=True)
    (overlay_dir / "custom_exog.txt").write_text(
        "\n".join([
            "SMPL 2025.4 2029.4;",
            "CHANGEVAR;",
            "PSE2025_LOW CHGSAMEABS",
            "-1.0",
            ";",
            "RETURN;",
            "",
        ]),
        encoding="utf-8",
    )

    config = ScenarioConfig(
        name="baseline",
        fp_home=fm_home,
        input_overlay_dir=overlay_dir,
        input_file="fminput.txt",
    )
    candidates = module._sweep_variable_candidates(config)

    assert "YS" in candidates
    assert "PSE2025_LOW" in candidates


def test_dashboard_data_update_page_smoke(tmp_path: Path) -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    at = AppTest.from_file(str(page_path))
    at.run()

    assert not at.exception


def test_dashboard_dictionary_page_smoke(tmp_path: Path) -> None:
    page_path = _dashboard_root() / "pages" / "9_Dictionary.py"
    at = AppTest.from_file(str(page_path))
    at.run()

    assert not at.exception


def test_dictionary_page_scenario_overlay_candidate_filter() -> None:
    page_path = _dashboard_root() / "pages" / "9_Dictionary.py"
    spec = importlib.util.spec_from_file_location("dictionary_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    paths = [
        Path("projects_local/dictionary_extensions/global.json"),
        Path("projects_local/dictionary_overlays/baseline.json"),
        Path("artifacts/baseline_20260223_120000/dictionary_overlay.json"),
    ]
    filtered = module._scenario_overlay_candidates(paths)
    names = [path.name for path in filtered]
    assert names == ["baseline.json", "dictionary_overlay.json"]


def test_dashboard_parity_page_smoke(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    _write_parity_run(artifacts_dir, "baseline", "20260223_120010")

    page_path = _dashboard_root() / "pages" / "11_Parity.py"
    at = AppTest.from_file(str(page_path))
    at.query_params["artifacts-dir"] = str(artifacts_dir)
    at.run(timeout=10)

    assert not at.exception


def test_dashboard_parity_page_smoke_many_hardfails(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    _write_parity_run(
        artifacts_dir,
        "baseline",
        "20260223_120012",
        hard_fail_count=150,
        write_triage_hardfails_csv=True,
        write_support_gap_top_md=True,
        write_support_gap_map_csv=True,
        write_parity_regression_json=True,
        outside_seed_inspected_cells=42,
        outside_seed_candidate_cells=7,
    )

    page_path = _dashboard_root() / "pages" / "11_Parity.py"
    at = AppTest.from_file(str(page_path))
    at.query_params["artifacts-dir"] = str(artifacts_dir)
    at.run(timeout=10)

    assert not at.exception


def test_parity_page_logic_masks_sentinel_and_handles_missing_default_var(tmp_path: Path) -> None:
    page_path = _dashboard_root() / "pages" / "11_Parity.py"
    spec = importlib.util.spec_from_file_location("parity_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Hard-fail var not present in shared vars should not crash default selection.
    picked = module._pick_default_series_variable(["AA", "IVF"], ["NOT_SHARED", "ALSO_MISSING"])
    assert picked == "AA"

    # None path should not be coerced into \"None\" candidate and fallback should resolve.
    run_dir = _write_parity_run(tmp_path / "artifacts", "baseline", "20260223_120011")
    run = module.ParityRunArtifact(
        run_dir=run_dir,
        scenario_name="baseline",
        timestamp="20260223_120011",
        parity_report_path=run_dir / "parity_report.json",
        status="hard_fail",
        exit_code=3,
    )
    resolved = module._resolve_report_path(run, None, "work_fpexe/PABEV.TXT")
    assert resolved == run_dir / "work_fpexe" / "PABEV.TXT"

    left = module._load_pabev_frame(str(run_dir / "work_fpexe" / "PABEV.TXT"), "left")
    right = module._load_pabev_frame(str(run_dir / "work_fppy" / "PABEV.TXT"), "right")
    _, _, diff = module._compute_abs_diff(
        left,
        right,
        start_period="2025.4",
        end_period=None,
        missing_sentinels=frozenset((-99.0,)),
    )
    # IVF second period contains sentinel in left side and must be masked.
    assert diff.loc["IVF", "2026.1"] != diff.loc["IVF", "2026.1"]


def test_parity_page_eq_flags_preset_fallback_from_stdout(tmp_path: Path) -> None:
    page_path = _dashboard_root() / "pages" / "11_Parity.py"
    spec = importlib.util.spec_from_file_location("parity_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    run_dir = _write_parity_run(
        tmp_path / "artifacts",
        "baseline",
        "20260223_120013",
        eq_flags_preset_in_report=None,
        write_fppy_stdout=True,
        stdout_eq_flags_preset="parity",
    )
    run = module.ParityRunArtifact(
        run_dir=run_dir,
        scenario_name="baseline",
        timestamp="20260223_120013",
        parity_report_path=run_dir / "parity_report.json",
        status="hard_fail",
        exit_code=3,
    )

    parsed = module._extract_eq_flags_preset_from_stdout("cfg eq_flags_preset=legacy\n")
    assert parsed == "legacy"

    preset, source = module._resolve_eq_flags_preset(run, {"pabev_path": None, "details": {}})
    assert preset == "parity"
    assert source == "work_fppy/fppy.stdout.txt"


def test_parity_page_extract_solution_error_records() -> None:
    page_path = _dashboard_root() / "pages" / "11_Parity.py"
    spec = importlib.util.spec_from_file_location("parity_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    records = module._extract_solution_error_records({
        "solution_errors": [
            {
                "solve_name": "SOLX",
                "iters": 3,
                "period": "2025.4",
                "match": "Solution error in SOLX at 2025.4",
            },
            {
                "name": "SOLY",
                "iterations": 2,
                "quarter": "2025.5",
                "error": "Solution error in SOLY",
            },
            {
                "solve": "SOLZ",
                "n_iter": 4,
                "solve_period": "2025.6",
                "raw": "line noise",
            },
            {"solve_name": "SOL_SKIP"},
        ],
    })
    assert records[0]["solve"] == "SOLX"
    assert records[0]["iters"] == "3"
    assert records[0]["period"] == "2025.4"
    assert records[0]["raw_match"].startswith("Solution error in SOLX")
    assert records[1]["solve"] == "SOLY"
    assert records[1]["iters"] == "2"
    assert records[1]["period"] == "2025.5"
    assert records[2]["solve"] == "SOLZ"
    assert records[2]["iters"] == "4"
    assert len(records) == 3


def test_parity_page_sort_parity_runs_latest_first(tmp_path: Path) -> None:
    page_path = _dashboard_root() / "pages" / "11_Parity.py"
    spec = importlib.util.spec_from_file_location("parity_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    runs = [
        module.ParityRunArtifact(
            run_dir=tmp_path / "scenario_20260223_120011",
            scenario_name="scenario",
            timestamp="20260223_120011",
            parity_report_path=tmp_path / "scenario_20260223_120011" / "parity_report.json",
            status="hard_fail",
            exit_code=3,
        ),
        module.ParityRunArtifact(
            run_dir=tmp_path / "scenario_20260223_120013",
            scenario_name="scenario",
            timestamp="20260223_120013",
            parity_report_path=tmp_path / "scenario_20260223_120013" / "parity_report.json",
            status="hard_fail",
            exit_code=3,
        ),
    ]

    sorted_runs = module._sort_parity_runs_latest_first(runs)
    assert sorted_runs[0].timestamp == "20260223_120013"


def test_parity_page_safe_percent_diff() -> None:
    page_path = _dashboard_root() / "pages" / "11_Parity.py"
    spec = importlib.util.spec_from_file_location("parity_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    left = pd.Series([1.0, 0.0, 5.0])
    right = pd.Series([2.0, 0.0, 2.0])
    pct = module._safe_percent_diff(left, right)
    assert pct.tolist() == [0.5, 0.0, 0.6]


def test_dashboard_parity_page_warns_for_fpexe_solution_errors(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    _write_parity_run(
        artifacts_dir,
        "baseline",
        "20260223_120014",
        fpexe_solution_errors=[
            {
                "solve_name": "SOLX",
                "iters": 3,
                "period": "2025.4",
                "match": "Solution error in SOLX at 2025.4",
            }
        ],
    )

    page_path = _dashboard_root() / "pages" / "11_Parity.py"
    at = AppTest.from_file(str(page_path))
    at.query_params["artifacts-dir"] = str(artifacts_dir)
    at.run(timeout=10)

    assert not at.exception


def test_dashboard_parity_page_smoke_fppy_prereq_and_preflight(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    _write_parity_run(
        artifacts_dir,
        "baseline",
        "20260223_120015",
        status="engine_failure",
        eq_backfill_failed=5600,
        eq_backfill_first_failure_error="missing variable 'LYDZ' for equation 'LCSZ'",
        fpexe_preflight_report={
            "wine_required": True,
            "wine_available": False,
            "wineprefix": "/tmp/fake-wineprefix",
        },
    )

    page_path = _dashboard_root() / "pages" / "11_Parity.py"
    at = AppTest.from_file(str(page_path))
    at.query_params["artifacts-dir"] = str(artifacts_dir)
    at.run(timeout=10)

    assert not at.exception


def test_parity_page_extract_fpexe_preflight_report_fallbacks() -> None:
    page_path = _dashboard_root() / "pages" / "11_Parity.py"
    spec = importlib.util.spec_from_file_location("parity_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    from_engine = module._extract_fpexe_preflight_report(
        {"fpexe_details": {"preflight_report": {"wine_available": False}}},
        {"details": {"preflight_report": {"wine_available": True}}},
    )
    assert from_engine == {"wine_available": True}

    from_detail = module._extract_fpexe_preflight_report(
        {"fpexe_details": {"preflight_report": {"wine_available": False}}},
        {"details": {}},
    )
    assert from_detail == {"wine_available": False}

    missing = module._extract_fpexe_preflight_report({}, {"details": {}})
    assert missing is None


def test_parity_page_missing_variable_failure_detector() -> None:
    page_path = _dashboard_root() / "pages" / "11_Parity.py"
    spec = importlib.util.spec_from_file_location("parity_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module._is_missing_variable_failure("missing variable 'LYDZ' for equation 'LCSZ'")
    assert module._is_missing_variable_failure("undefined variable in expression")
    assert not module._is_missing_variable_failure("timeout while running parity")


def test_data_update_page_failure_hints() -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    spec = importlib.util.spec_from_file_location("data_update_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    hints = module._failure_hints(
        "",
        "\n".join([
            "FRED_API_KEY environment variable not set",
            "BEA_API_KEY environment variable not set",
            "BLS_API_KEY environment variable not set",
            "No variables with FRED series mappings were eligible for update",
            "Missing values in extended sample (missing_cells=12)",
            "fredapi is required for this command.",
        ]),
    )
    assert any("FRED_API_KEY" in item for item in hints)
    assert any("BEA_API_KEY" in item for item in hints)
    assert any("BLS_API_KEY" in item for item in hints)
    assert any("source-map" in item.lower() for item in hints)
    assert any("carry_forward" in item for item in hints)
    assert any("fredapi" in item for item in hints)


def test_data_update_page_build_command_multisource() -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    spec = importlib.util.spec_from_file_location("data_update_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    cmd = module._build_update_command(
        model_dir="FM",
        out_dir="artifacts/model_updates/smoke",
        end_period="2025.4",
        cache_dir="/tmp/fp-wraptr-cache",
        sources=["fred", "bea", "bls"],
        selected_vars=["GDPR", "UR", "JS"],
        replace_history=False,
        extend_sample=False,
        allow_carry_forward=False,
    )

    rendered = " ".join(cmd)
    assert "--sources fred" in rendered
    assert "--sources bea" in rendered
    assert "--sources bls" in rendered
    assert "--variables GDPR" in rendered
    assert "--variables UR" in rendered
    assert "--variables JS" in rendered


def test_data_update_page_build_command_official_bundle_options() -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    spec = importlib.util.spec_from_file_location("data_update_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    cmd = module._build_update_command(
        model_dir="FM",
        out_dir="artifacts/model_updates/smoke",
        end_period="2025.4",
        cache_dir="/tmp/fp-wraptr-cache",
        sources=["fred"],
        selected_vars=["GDPR", "UR", "JS"],
        replace_history=False,
        extend_sample=False,
        allow_carry_forward=False,
        use_official_bundle=True,
        official_bundle_url="https://example.com/FMFP.ZIP",
        base_dir="/tmp/fair_bundle_base",
    )

    rendered = " ".join(cmd)
    assert "--from-official-bundle" in rendered
    assert "--official-bundle-url https://example.com/FMFP.ZIP" in rendered
    assert "--base-dir /tmp/fair_bundle_base" in rendered


def test_data_update_page_build_command_official_bundle_base_dir_optional() -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    spec = importlib.util.spec_from_file_location("data_update_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    cmd = module._build_update_command(
        model_dir="FM",
        out_dir="artifacts/model_updates/smoke",
        end_period="2025.4",
        cache_dir="",
        sources=["fred"],
        selected_vars=[],
        replace_history=False,
        extend_sample=False,
        allow_carry_forward=False,
        use_official_bundle=True,
        official_bundle_url="https://example.com/FMFP.ZIP",
        base_dir="",
    )

    rendered = " ".join(cmd)
    assert "--from-official-bundle" in rendered
    assert "--official-bundle-url https://example.com/FMFP.ZIP" in rendered
    assert "--base-dir" not in rendered


def test_data_update_page_fp_exe_check(tmp_path: Path) -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    spec = importlib.util.spec_from_file_location("data_update_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    bundle_dir = tmp_path / "out" / "FM"
    missing_fp_exe = module._fp_exe_check(bundle_dir)
    assert missing_fp_exe == (False, str(bundle_dir / "fp.exe"))

    fp_exe_path = bundle_dir / "fp.exe"
    fp_exe_path.parent.mkdir(parents=True, exist_ok=True)
    fp_exe_path.write_text("x", encoding="utf-8")
    present_fp_exe = module._fp_exe_check(bundle_dir)
    assert present_fp_exe == (True, str(fp_exe_path))


def test_data_update_page_truncate_log() -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    spec = importlib.util.spec_from_file_location("data_update_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    text = "x" * 32
    kept, omitted = module._truncate_log(text, max_chars=10)
    assert kept == "x" * 10
    assert omitted == 22


def test_data_update_page_build_parity_smoke_command() -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    spec = importlib.util.spec_from_file_location("data_update_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    cmd = module._build_parity_smoke_command(
        scenario_yaml=Path("artifacts/model_updates/demo/scenarios/baseline_smoke.yaml"),
        output_dir=Path("artifacts/model_updates/demo/parity_smoke"),
    )
    rendered = " ".join(cmd)
    assert "fp_wraptr.cli parity" in rendered
    assert "--with-drift" in rendered
    assert "--output-dir artifacts/model_updates/demo/parity_smoke" in rendered


def test_data_update_page_find_latest_parity_report(tmp_path: Path) -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    spec = importlib.util.spec_from_file_location("data_update_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    older = tmp_path / "parity_smoke" / "run_a" / "parity_report.json"
    older.parent.mkdir(parents=True, exist_ok=True)
    older.write_text(json.dumps({"status": "ok"}), encoding="utf-8")

    newer = tmp_path / "parity_smoke" / "run_b" / "parity_report.json"
    newer.parent.mkdir(parents=True, exist_ok=True)
    newer.write_text(json.dumps({"status": "hard_fail"}), encoding="utf-8")

    os.utime(older, (1, 1))
    os.utime(newer, (2, 2))

    latest = module._find_latest_parity_report(tmp_path / "parity_smoke")
    assert latest == newer


def test_data_update_page_load_parity_report_summary(tmp_path: Path) -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    spec = importlib.util.spec_from_file_location("data_update_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    report_path = tmp_path / "parity_report.json"
    report_path.write_text(
        json.dumps({
            "status": "hard_fail",
            "exit_code": 3,
            "pabev_detail": {
                "hard_fail_cell_count": 12,
                "max_abs_diff": 5.5,
                "top_first_diffs": [{"variable": "RM"}, {"variable": "RMA"}],
            },
        }),
        encoding="utf-8",
    )

    summary = module._load_parity_report_summary(report_path)
    assert summary["status"] == "hard_fail"
    assert summary["exit_code"] == 3
    assert summary["hard_fail_cell_count"] == 12
    assert summary["max_abs_diff"] == 5.5
    assert summary["top_first_diffs"] == [{"variable": "RM"}, {"variable": "RMA"}]


def test_data_update_page_extract_keyboard_patch_targets() -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    spec = importlib.util.spec_from_file_location("data_update_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    added, already_present = module._extract_keyboard_patch_targets({
        "fminput_keyboard_patch": {
            "added": ["RM", " RMA ", "", "RM"],
            "already_present": ["RMMRSL2", " RMACDZ ", "RMMRSL2"],
        }
    })
    assert added == ["RM", "RMA"]
    assert already_present == ["RMMRSL2", "RMACDZ"]


def test_data_update_page_gate_failed_caveat_helpers() -> None:
    page_path = _dashboard_root() / "pages" / "10_Data_Update.py"
    spec = importlib.util.spec_from_file_location("data_update_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    summary = {
        "status": "gate_failed",
        "hard_fail_cell_count": 0,
        "top_first_diffs": [
            {"variable": "RM"},
            {"variable": "RMA"},
            {"variable": "RM"},
            {"variable": ""},
        ],
    }
    assert module._should_show_gate_failed_caveat(summary) is True
    assert module._extract_top_diff_variables(summary) == ["RM", "RMA"]
    assert (
        module._should_show_gate_failed_caveat({"status": "hard_fail", "hard_fail_cell_count": 0})
        is False
    )
    assert (
        module._should_show_gate_failed_caveat({
            "status": "gate_failed",
            "hard_fail_cell_count": 1,
        })
        is False
    )


def test_equation_graph_discover_sources_includes_latest_pse_runs(tmp_path: Path) -> None:
    page_path = _dashboard_root() / "pages" / "4_Equation_Graph.py"
    spec = importlib.util.spec_from_file_location("equation_graph_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    artifacts_dir = tmp_path / "artifacts"
    base_old = _write_run(artifacts_dir, "pse2025_base", "20260223_120000", 2.0)
    base_new = _write_run(artifacts_dir, "pse2025_base", "20260223_120100", 2.1)
    low_run = _write_run(artifacts_dir, "pse2025_low", "20260223_120200", 2.2)
    high_run = _write_run(artifacts_dir, "pse2025_high", "20260223_120300", 2.3)
    (base_old / "work").mkdir(parents=True, exist_ok=True)
    (base_new / "work").mkdir(parents=True, exist_ok=True)
    (low_run / "work").mkdir(parents=True, exist_ok=True)
    (high_run / "work").mkdir(parents=True, exist_ok=True)
    (base_old / "work" / "fminput.txt").write_text("MODEL; EQ 1 AA BB;", encoding="utf-8")
    (base_new / "work" / "fminput.txt").write_text("MODEL; EQ 1 AA CC;", encoding="utf-8")
    (low_run / "work" / "fminput.txt").write_text("MODEL; EQ 1 LL MM;", encoding="utf-8")
    (high_run / "work" / "fminput.txt").write_text("MODEL; EQ 1 HH II;", encoding="utf-8")

    sources = module._discover_input_sources(artifacts_dir)
    labels = [source.label for source in sources]
    assert any(label.startswith("FM baseline") for label in labels)
    assert any(label == "pse2025_base (20260223_120100)" for label in labels)
    assert any(label == "pse2025_low (20260223_120200)" for label in labels)
    assert any(label == "pse2025_high (20260223_120300)" for label in labels)


def test_equation_graph_compare_parsed_inputs_and_graph_delta() -> None:
    page_path = _dashboard_root() / "pages" / "4_Equation_Graph.py"
    spec = importlib.util.spec_from_file_location("equation_graph_page", str(page_path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    left = module.parse_fp_input_text(
        "\n".join([
            "EQ 1 AA BB + CC;",
            "IDENT ZZ = AA + 1;",
            "GENR GX = ZZ;",
        ])
    )
    right = module.parse_fp_input_text(
        "\n".join([
            "EQ 1 AA BB + DD;",
            "EQ 2 NEWVAR AA + 1;",
            "IDENT ZZ = AA + 2;",
            "GENR GX = ZZ;",
            "GENR GY = GX + 1;",
        ])
    )
    diff = module._compare_parsed_inputs(left, right)
    eq_changed = [row["variable"] for row in diff["equations"]["changed"]]
    ident_changed = [row["variable"] for row in diff["identities"]["changed"]]
    assert eq_changed == ["AA"]
    assert diff["equations"]["added"] == ["NEWVAR"]
    assert ident_changed == ["ZZ"]
    assert diff["generated_vars"]["added"] == ["GY"]

    left_graph = module.build_dependency_graph(left)
    right_graph = module.build_dependency_graph(right)
    graph_delta = module._graph_diff(left_graph, right_graph)
    assert ("DD", "AA") in graph_delta["added_edges"]
    assert ("CC", "AA") in graph_delta["removed_edges"]
