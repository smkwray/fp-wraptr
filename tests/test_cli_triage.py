import json
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from fp_wraptr.cli import app

runner = CliRunner()


def _write_pabev(path: Path, *, values_by_var: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["SMPL 2025.4 2026.1;"]
    for name, values in values_by_var.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(str(float(v)) for v in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_fp_triage_anchor_acceptance_writes_outputs(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fp_wraptr.cli.assert_no_forbidden_dirs", lambda _root: None)
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"PD": [1.0, 1.0], "XX": [10.0, 10.0]})
    _write_pabev(fppy, values_by_var={"PD": [1.0, 1.0], "XX": [10.0, 14.0]})
    _write_pabev(fpr, values_by_var={"PD": [1.0, 1.0005], "XX": [10.0, 12.0]})
    out_dir = tmp_path / "anchor_acceptance"

    result = runner.invoke(
        app,
        [
            "triage",
            "anchor-acceptance",
            "--fpexe",
            str(fpexe),
            "--fppy",
            str(fppy),
            "--fpr",
            str(fpr),
            "--anchors",
            "PD",
            "--methodology",
            "XX",
            "--start",
            "2025.4",
            "--end",
            "2025.4",
            "--out-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert (out_dir / "anchor_acceptance_report.json").exists()
    assert (out_dir / "anchor_acceptance_summary.csv").exists()


def test_fp_triage_backend_defensibility_writes_outputs(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fp_wraptr.cli.assert_no_forbidden_dirs", lambda _root: None)
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"RS": [4.0, 4.0], "PCPD": [1.0, 1.0]})
    _write_pabev(fppy, values_by_var={"RS": [4.0, 4.2], "PCPD": [1.0, 3.5]})
    _write_pabev(fpr, values_by_var={"RS": [4.0, 4.21], "PCPD": [1.0, 3.54]})
    out_dir = tmp_path / "backend_defensibility"

    result = runner.invoke(
        app,
        [
            "triage",
            "backend-defensibility",
            "--fpexe",
            str(fpexe),
            "--fppy",
            str(fppy),
            "--fpr",
            str(fpr),
            "--variables",
            "RS,PCPD",
            "--focus",
            "RS",
            "--start",
            "2025.4",
            "--end",
            "2025.4",
            "--out-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert (out_dir / "backend_defensibility_report.json").exists()
    assert (out_dir / "backend_defensibility_summary.csv").exists()


def test_fp_triage_focused_series_writes_outputs(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fp_wraptr.cli.assert_no_forbidden_dirs", lambda _root: None)
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"RS": [4.0, 4.0], "RB": [5.0, 5.0]})
    _write_pabev(fppy, values_by_var={"RS": [4.0, 4.2], "RB": [5.0, 5.4]})
    _write_pabev(fpr, values_by_var={"RS": [4.0, 4.21], "RB": [5.0, 5.45]})
    out_dir = tmp_path / "focused_series"

    result = runner.invoke(
        app,
        [
            "triage",
            "focused-series",
            "--fpexe",
            str(fpexe),
            "--fppy",
            str(fppy),
            "--fpr",
            str(fpr),
            "--variables",
            "RS,RB",
            "--start",
            "2025.4",
            "--end",
            "2025.4",
            "--out-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert (out_dir / "focused_series_compare_report.json").exists()
    assert (out_dir / "focused_series_compare_rows.csv").exists()


def test_fp_triage_identity_decomposition_writes_outputs(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fp_wraptr.cli.assert_no_forbidden_dirs", lambda _root: None)
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"A": [10.0, 10.0], "B": [5.0, 5.0], "C": [2.0, 2.0], "T": [13.0, 13.0]})
    _write_pabev(fppy, values_by_var={"A": [10.5, 10.5], "B": [5.0, 5.0], "C": [2.0, 2.0], "T": [13.5, 13.5]})
    _write_pabev(fpr, values_by_var={"A": [10.0, 10.0], "B": [4.5, 4.5], "C": [2.0, 2.0], "T": [12.5, 12.5]})
    out_dir = tmp_path / "identity_decomposition"

    result = runner.invoke(
        app,
        [
            "triage",
            "identity-decomposition",
            "--fpexe",
            str(fpexe),
            "--fppy",
            str(fppy),
            "--fpr",
            str(fpr),
            "--identity",
            "T=A+B-C",
            "--period",
            "2025.4",
            "--out-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert (out_dir / "identity_decomposition_report.json").exists()
    assert (out_dir / "identity_decomposition_terms.csv").exists()


def test_fp_triage_fp_ineq_publication_writes_outputs(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fp_wraptr.cli.assert_no_forbidden_dirs", lambda _root: None)
    manifest = tmp_path / "manifest.json"
    matrix = tmp_path / "matrix.json"
    contract = tmp_path / "contract.json"
    manifest.write_text(
        json.dumps(
            {
                "runs": [
                    {"run_id": "ineq-baseline-observed", "label": "Baseline"},
                    {"run_id": "ineq-federal-transfer-relief", "label": "Federal"},
                    {
                        "run_id": "ineq-ui-relief",
                        "label": "UI (legacy split)",
                        "summary": "Shared modern branch relative to legacy fp.exe.",
                    },
                ],
                "default_run_ids": [
                    "ineq-baseline-observed",
                    "ineq-federal-transfer-relief",
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    matrix.write_text(
        json.dumps(
            {
                "lane_rows": [
                    {
                        "variant_id": "baseline-observed",
                        "classification": "modern_branch_ok",
                        "evidence_mode": "baseline",
                        "notes": "baseline ok",
                    },
                    {
                        "variant_id": "federal-transfer-relief",
                        "classification": "modern_branch_ok",
                        "evidence_mode": "federal",
                        "notes": "federal ok",
                    },
                    {
                        "variant_id": "ui-relief",
                        "classification": "modern_branch_ok_but_legacy_split",
                        "evidence_mode": "ui",
                        "notes": "ui split",
                    },
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    contract.write_text(
        json.dumps(
            {
                "run_id_prefix": "ineq-",
                "required_run_ids": [
                    "ineq-baseline-observed",
                    "ineq-federal-transfer-relief",
                    "ineq-ui-relief",
                ],
                "recommended_default_run_ids": [
                    "ineq-baseline-observed",
                    "ineq-federal-transfer-relief",
                ],
                "allow_published_legacy_split": True,
                "require_explicit_label_for_published_legacy_split": True,
                "legacy_split_label_fields": ["label", "summary"],
                "legacy_split_label_tokens": ["legacy split", "shared modern"],
                "require_default_runs_to_be_public_default_safe": True,
                "require_exact_default_run_ids": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "fp_ineq_publication"

    result = runner.invoke(
        app,
        [
            "triage",
            "fp-ineq-publication",
            "--manifest",
            str(manifest),
            "--matrix",
            str(matrix),
            "--contract",
            str(contract),
            "--out-dir",
            str(out_dir),
        ],
    )

    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert (out_dir / "fp_ineq_publication_validation_report.json").exists()
    assert (out_dir / "fp_ineq_publication_validation_rows.csv").exists()


def test_fp_triage_backend_release_shape_writes_output(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fp_wraptr.cli.assert_no_forbidden_dirs", lambda _root: None)
    anchor_report = tmp_path / "anchor_acceptance_report.json"
    anchor_report.write_text(
        json.dumps(
            {
                "status": "review",
                "counts": {"anchor_review_count": 1, "methodology_review_count": 1},
                "anchor_rows": [
                    {
                        "variable": "RS",
                        "classification": "review",
                        "review_scope": "fp_r_leading",
                        "explanation_scope": "fp_r_tail_on_shared_split",
                    }
                ],
                "methodology_rows": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "backend_release_shape"

    result = runner.invoke(
        app,
        [
            "triage",
            "backend-release-shape",
            "--anchor-report",
            str(anchor_report),
            "--stock-baseline-ok",
            "--raw-input-public-ok",
            "--modified-decks-run",
            "--docs-honest",
            "--corpus-not-green",
            "--out-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert (out_dir / "backend_release_shape_report.json").exists()


def test_fp_triage_backend_release_corpus_writes_outputs(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fp_wraptr.cli.assert_no_forbidden_dirs", lambda _root: None)
    manifest = tmp_path / "docs" / "backend-release-corpus.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    scenario = tmp_path / "examples" / "a.yaml"
    scenario.parent.mkdir(parents=True, exist_ok=True)
    scenario.write_text("name: a\n", encoding="utf-8")
    release_shape = tmp_path / "artifacts" / "a" / "backend_release_shape_report.json"
    release_shape.parent.mkdir(parents=True, exist_ok=True)
    release_shape.write_text(
        json.dumps(
            {
                "decision": {
                    "recommended_label": "preview_ready",
                    "preview_ready": True,
                    "peer_backend_ready": False,
                    "blockers": ["release_corpus_not_green"],
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    manifest.write_text(
        json.dumps(
            {
                "name": "demo",
                "entries": [
                    {
                        "name": "a",
                        "required": True,
                        "scenario": "examples/a.yaml",
                        "release_shape_report": "artifacts/a/backend_release_shape_report.json",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "backend_release_corpus"

    result = runner.invoke(
        app,
        [
            "triage",
            "backend-release-corpus",
            "--manifest",
            str(manifest),
            "--out-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert (out_dir / "backend_release_corpus_report.json").exists()
    assert (out_dir / "backend_release_corpus_summary.csv").exists()


def test_fp_triage_scenario_delta_compare_writes_outputs(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fp_wraptr.cli.assert_no_forbidden_dirs", lambda _root: None)
    left_baseline = tmp_path / "left-baseline.PABEV.TXT"
    left_scenario = tmp_path / "left-scenario.PABEV.TXT"
    right_baseline = tmp_path / "right-baseline.PABEV.TXT"
    right_scenario = tmp_path / "right-scenario.PABEV.TXT"
    _write_pabev(left_baseline, values_by_var={"UB": [9.0, 9.0], "YD": [100.0, 102.0]})
    _write_pabev(left_scenario, values_by_var={"UB": [65.0, 65.0], "YD": [150.0, 160.0]})
    _write_pabev(right_baseline, values_by_var={"UB": [9.0, 9.0], "YD": [100.0, 102.0]})
    _write_pabev(right_scenario, values_by_var={"UB": [9.18, 9.18], "YD": [112.0, 118.0]})
    out_dir = tmp_path / "scenario_delta"

    result = runner.invoke(
        app,
        [
            "triage",
            "scenario-delta-compare",
            "--baseline-left",
            str(left_baseline),
            "--scenario-left",
            str(left_scenario),
            "--baseline-right",
            str(right_baseline),
            "--scenario-right",
            str(right_scenario),
            "--left-label",
            "fpexe",
            "--right-label",
            "fpr",
            "--variables",
            "UB,YD",
            "--start",
            "2025.4",
            "--end",
            "2026.1",
            "--out-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert (out_dir / "scenario_delta_compare_report.json").exists()
    assert (out_dir / "scenario_delta_compare_summary.csv").exists()
    assert (out_dir / "scenario_delta_compare_rows.csv").exists()


def test_fp_triage_parity_hardfails_writes_outputs(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000000"
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_pabev(run_dir / "work_fpexe" / "PABEV.TXT", values_by_var={"HF": [0.5, 0.4]})
    _write_pabev(run_dir / "work_fppy" / "PABEV.TXT", values_by_var={"HF": [0.5, -0.4]})

    (run_dir / "parity_report.json").write_text(
        json.dumps({
            "pabev_detail": {
                "start": "2025.4",
                "atol": 1e-3,
                "rtol": 1e-6,
                "missing_sentinels": [-99.0],
                "discrete_eps": 1e-12,
                "signflip_eps": 1e-3,
            },
            "engine_runs": {},
        })
        + "\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["triage", "parity-hardfails", str(run_dir)])
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr

    assert (run_dir / "triage_hardfails.csv").exists()
    assert (run_dir / "triage_hardfails_summary.json").exists()


def test_fp_triage_fppy_report_writes_outputs(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000001"
    (run_dir / "work_fppy").mkdir(parents=True, exist_ok=True)
    (run_dir / "work_fppy" / "fppy_report.json").write_text(
        json.dumps({
            "issues": [
                {
                    "line": 10,
                    "statement": "GENR X=LOG(Y);",
                    "error": "KeyError: column 'Y' not present in input data",
                }
            ]
        })
        + "\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["triage", "fppy-report", str(run_dir)])
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr

    assert (run_dir / "work_fppy" / "triage_summary.json").exists()
    assert (run_dir / "work_fppy" / "triage_issues.csv").exists()


def test_fp_triage_loop_runs_parity_and_triage(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"))

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **_kwargs):
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="ok",
            exit_code=0,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)

    result = runner.invoke(
        app, ["triage", "loop", str(scenario_path), "--output-dir", str(tmp_path)]
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert (run_dir / "triage_hardfails.csv").exists()
    assert (run_dir / "triage_hardfails_summary.json").exists()
    assert (run_dir / "work_fppy" / "triage_summary.json").exists()
    assert (run_dir / "work_fppy" / "triage_issues.csv").exists()


def test_fp_triage_loop_passes_gate_pabev_end(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run_gate_end"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")
    observed = {"gate_end": None}

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"))

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **kwargs):
        gate = kwargs.get("gate")
        observed["gate_end"] = getattr(gate, "pabev_end", None)
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="ok",
            exit_code=0,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)

    result = runner.invoke(
        app,
        [
            "triage",
            "loop",
            str(scenario_path),
            "--output-dir",
            str(tmp_path),
            "--gate-pabev-end",
            "2025.4",
        ],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert observed["gate_end"] == "2025.4"


def test_fp_triage_loop_quick_sets_gate_end_to_forecast_start(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run_quick"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")
    observed = {"gate_end": None}

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"), forecast_start="2032.3")

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **kwargs):
        gate = kwargs.get("gate")
        observed["gate_end"] = getattr(gate, "pabev_end", None)
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="ok",
            exit_code=0,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)

    result = runner.invoke(
        app,
        [
            "triage",
            "loop",
            str(scenario_path),
            "--output-dir",
            str(tmp_path),
            "--quick",
        ],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    assert observed["gate_end"] == "2032.3"


def test_fp_triage_loop_strict_failure_prints_run_dir(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run_strict_fail"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"))

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **_kwargs):
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="gate_failed",
            exit_code=2,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.5},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)

    result = runner.invoke(
        app,
        ["triage", "loop", str(scenario_path), "--output-dir", str(tmp_path)],
    )
    assert result.exit_code == 2, result.stdout + "\n" + result.stderr
    output = result.stdout + result.stderr
    assert "run dir →" in output


def test_fp_triage_loop_warns_on_fpexe_solution_errors(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run_solution_warning"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")

    class _EngineSummary:
        def __init__(self):
            self.details = {"solution_errors": [{"solve": "SOL1"}]}

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"))

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **_kwargs):
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="ok",
            exit_code=0,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
            engine_runs={"fpexe": _EngineSummary()},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)

    result = runner.invoke(
        app,
        ["triage", "loop", str(scenario_path), "--output-dir", str(tmp_path)],
    )
    assert result.exit_code == 0, result.stdout + "\n" + result.stderr
    output = result.stdout + result.stderr
    assert "treat diffs as unreliable" in output


def test_fp_triage_loop_regression_failure_sets_exit_code(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "loop_run_regress"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: demo\n", encoding="utf-8")
    regression_dir = tmp_path / "golden"
    regression_dir.mkdir(parents=True, exist_ok=True)

    def _load_scenario_config(_scenario: Path):
        return SimpleNamespace(name="demo", fp_home=Path("FM"))

    def _validate_fp_home(_fp_home: Path) -> None:
        return None

    def _run_parity(*_args, **_kwargs):
        return SimpleNamespace(
            run_dir=str(run_dir),
            status="ok",
            exit_code=0,
            pabev_detail={"hard_fail_cell_count": 0, "max_abs_diff": 0.0},
        )

    def _triage_hardfails(_run_dir: Path):
        csv_path = run_dir / "triage_hardfails.csv"
        summary_path = run_dir / "triage_hardfails_summary.json"
        csv_path.write_text("variable,period\n", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        return csv_path, summary_path

    def _triage_fppy(_run_dir: Path, out_dir: Path | None = None):
        _ = out_dir
        summary_path = run_dir / "work_fppy" / "triage_summary.json"
        csv_path = run_dir / "work_fppy" / "triage_issues.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        csv_path.write_text("bucket,count\n", encoding="utf-8")
        return summary_path, csv_path

    def _compare(_run_dir: Path, _golden_dir: Path):
        return {
            "status": "new_findings",
            "counts": {
                "new_missing_left": 0,
                "new_missing_right": 0,
                "new_hard_fail_cells": 1,
                "new_diff_variables": 0,
            },
        }

    def _write(payload: dict, _run_dir: Path):
        path = run_dir / "parity_regression.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    monkeypatch.setattr("fp_wraptr.scenarios.runner.load_scenario_config", _load_scenario_config)
    monkeypatch.setattr("fp_wraptr.scenarios.runner.validate_fp_home", _validate_fp_home)
    monkeypatch.setattr("fp_wraptr.analysis.parity.run_parity", _run_parity)
    monkeypatch.setattr(
        "fp_wraptr.analysis.triage_parity_hardfails.triage_parity_hardfails", _triage_hardfails
    )
    monkeypatch.setattr("fp_wraptr.analysis.triage_fppy.triage_fppy_report", _triage_fppy)
    monkeypatch.setattr("fp_wraptr.analysis.parity_regression.compare_parity_to_golden", _compare)
    monkeypatch.setattr("fp_wraptr.analysis.parity_regression.write_regression_report", _write)

    result = runner.invoke(
        app,
        [
            "triage",
            "loop",
            str(scenario_path),
            "--output-dir",
            str(tmp_path),
            "--regression",
            str(regression_dir),
        ],
    )
    assert result.exit_code == 6, result.stdout + "\n" + result.stderr
    assert (run_dir / "parity_regression.json").exists()
