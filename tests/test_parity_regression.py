from __future__ import annotations

import json
import shutil
from pathlib import Path

from fp_wraptr.analysis.parity_regression import compare_parity_to_golden, save_parity_golden

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "pabev"


def _write_parity_report(run_dir: Path, *, scenario_name: str = "baseline") -> None:
    payload = {
        "scenario_name": scenario_name,
        "status": "ok",
        "exit_code": 0,
        "left_engine": "fpexe",
        "right_engine": "fppy",
        "pabev_detail": {
            "start": "2025.4",
            "atol": 1e-3,
            "rtol": 1e-6,
            "missing_sentinels": [-99.0],
            "discrete_eps": 1e-12,
            "signflip_eps": 1e-3,
        },
    }
    (run_dir / "parity_report.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )


def _write_pabev(path: Path, *, start: str, end: str, series: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"SMPL {start} {end};"]
    for name, values in series.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(str(value) for value in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _make_run(
    tmp_path: Path,
    run_name: str,
    *,
    fpexe_fixture: str = "golden_fpexe.pabev",
    fppy_fixture: str = "golden_fppy.pabev",
    scenario_name: str = "baseline",
) -> Path:
    run_dir = tmp_path / run_name
    (run_dir / "work_fpexe").mkdir(parents=True)
    (run_dir / "work_fppy").mkdir(parents=True)
    shutil.copy2(FIXTURE_DIR / fpexe_fixture, run_dir / "work_fpexe" / "PABEV.TXT")
    shutil.copy2(FIXTURE_DIR / fppy_fixture, run_dir / "work_fppy" / "PABEV.TXT")
    _write_parity_report(run_dir, scenario_name=scenario_name)
    return run_dir


def test_save_parity_golden_copies_report_and_pabev(tmp_path: Path) -> None:
    run_dir = _make_run(tmp_path, "run_a")
    golden_root = tmp_path / "golden"

    saved_dir = save_parity_golden(run_dir, golden_root)

    assert saved_dir == golden_root / "baseline"
    assert (saved_dir / "parity_report.json").exists()
    assert (saved_dir / "work_fpexe" / "PABEV.TXT").exists()
    assert (saved_dir / "work_fppy" / "PABEV.TXT").exists()
    gate = json.loads((saved_dir / "gate.json").read_text(encoding="utf-8"))
    assert gate["start"] == "2025.4"
    assert gate["atol"] == 1e-3
    assert gate["rtol"] == 1e-6


def test_save_parity_golden_uses_report_pabev_paths_when_default_missing(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_report_paths"
    (run_dir / "work_fpexe").mkdir(parents=True)
    (run_dir / "work_fppy").mkdir(parents=True)
    left = run_dir / "work_fpexe" / "OUT_BASE.DAT"
    right = run_dir / "work_fppy" / "OUT_BASE.DAT"
    shutil.copy2(FIXTURE_DIR / "golden_fpexe.pabev", left)
    shutil.copy2(FIXTURE_DIR / "golden_fppy.pabev", right)

    _write_parity_report(run_dir, scenario_name="baseline")
    payload = json.loads((run_dir / "parity_report.json").read_text(encoding="utf-8"))
    payload["engine_runs"] = {
        "fpexe": {"pabev_path": str(left)},
        "fppy": {"pabev_path": str(right)},
    }
    (run_dir / "parity_report.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )

    golden_root = tmp_path / "golden"
    saved_dir = save_parity_golden(run_dir, golden_root)

    assert saved_dir == golden_root / "baseline"
    assert (saved_dir / "work_fpexe" / "PABEV.TXT").exists()
    assert (saved_dir / "work_fppy" / "PABEV.TXT").exists()

    compare_payload = compare_parity_to_golden(run_dir, golden_root)
    assert compare_payload["status"] == "ok"


def test_compare_parity_to_golden_detects_new_diff_variable(tmp_path: Path) -> None:
    golden_run = _make_run(tmp_path, "golden_run")
    golden_root = tmp_path / "golden"
    save_parity_golden(golden_run, golden_root)

    current_run = _make_run(tmp_path, "current_run", fppy_fixture="current_new_diff_fppy.pabev")
    payload = compare_parity_to_golden(current_run, golden_root)

    assert payload.get("schema_version") == 1
    assert "producer_version" in payload
    assert payload["status"] == "failed"
    assert payload["counts"]["new_diff_variables"] == 1
    assert payload["new_findings"]["diff_variables"] == ["A"]


def test_compare_parity_to_golden_detects_new_missing_variable(tmp_path: Path) -> None:
    golden_run = _make_run(tmp_path, "golden_run")
    golden_root = tmp_path / "golden"
    save_parity_golden(golden_run, golden_root)

    current_run = _make_run(tmp_path, "current_run", fppy_fixture="current_new_missing_fppy.pabev")
    payload = compare_parity_to_golden(current_run, golden_root)

    assert payload["status"] == "failed"
    assert payload["counts"]["new_missing_right"] == 1
    assert payload["new_findings"]["missing_right"] == ["B"]


def test_compare_parity_to_golden_detects_new_hard_fail_cell(tmp_path: Path) -> None:
    golden_run = _make_run(tmp_path, "golden_run")
    golden_root = tmp_path / "golden"
    save_parity_golden(golden_run, golden_root)

    current_run = _make_run(
        tmp_path, "current_run", fppy_fixture="current_new_hardfail_fppy.pabev"
    )
    payload = compare_parity_to_golden(current_run, golden_root)

    assert payload["status"] == "failed"
    assert payload["counts"]["new_hard_fail_cells"] == 1
    assert payload["new_findings"]["hard_fail_cells"] == [
        {"variable": "HF", "period": "2026.1", "reason": "sign_flip"}
    ]


def test_compare_parity_to_golden_allows_improvement(tmp_path: Path) -> None:
    golden_run = _make_run(tmp_path, "golden_run", fppy_fixture="current_new_diff_fppy.pabev")
    golden_root = tmp_path / "golden"
    save_parity_golden(golden_run, golden_root)

    current_run = _make_run(tmp_path, "current_run")
    payload = compare_parity_to_golden(current_run, golden_root)

    assert payload["status"] == "ok"
    assert payload["counts"]["new_diff_variables"] == 0
    assert payload["counts"]["resolved_diff_variables"] == 1
    assert payload["resolved_findings"]["diff_variables"] == ["A"]


def test_save_parity_golden_preserves_multi_underscore_scenario_name(tmp_path: Path) -> None:
    run_dir = _make_run(
        tmp_path,
        "tight_monetary_shift_20260228_230000",
        scenario_name="",
    )
    golden_root = tmp_path / "golden"

    saved_dir = save_parity_golden(run_dir, golden_root)

    assert saved_dir == golden_root / "tight_monetary_shift"


def test_compare_parity_to_golden_period_mismatch_returns_structured_failure(
    tmp_path: Path,
) -> None:
    golden_run = _make_run(tmp_path, "golden_run")
    golden_root = tmp_path / "golden"
    save_parity_golden(golden_run, golden_root)

    current_run = _make_run(
        tmp_path,
        "current_run",
        fppy_fixture="current_period_mismatch_fppy.pabev",
    )
    payload = compare_parity_to_golden(current_run, golden_root)

    assert payload["status"] == "failed"
    assert payload["reason"] == "periods_mismatch"
    assert payload["counts"]["new_diff_variables"] == 0


def test_compare_parity_to_golden_uses_full_hard_fail_set_not_sampled(tmp_path: Path) -> None:
    periods = 16  # 2025.1..2028.4, with default start=2025.4 => 13 checked periods (>10)
    run_golden = tmp_path / "golden_run"
    run_current = tmp_path / "current_run"
    for run_dir in (run_golden, run_current):
        (run_dir / "work_fpexe").mkdir(parents=True)
        (run_dir / "work_fppy").mkdir(parents=True)
        _write_parity_report(run_dir, scenario_name="baseline")

    _write_pabev(
        run_golden / "work_fpexe" / "PABEV.TXT",
        start="2025.1",
        end="2028.4",
        series={"HF": [1.0] * periods},
    )
    _write_pabev(
        run_golden / "work_fppy" / "PABEV.TXT",
        start="2025.1",
        end="2028.4",
        series={"HF": [1.0] * periods},
    )
    _write_pabev(
        run_current / "work_fpexe" / "PABEV.TXT",
        start="2025.1",
        end="2028.4",
        series={"HF": [1.0] * periods},
    )
    _write_pabev(
        run_current / "work_fppy" / "PABEV.TXT",
        start="2025.1",
        end="2028.4",
        series={"HF": [-1.0] * periods},
    )

    golden_root = tmp_path / "golden"
    save_parity_golden(run_golden, golden_root)
    payload = compare_parity_to_golden(run_current, golden_root)

    assert payload["status"] == "failed"
    assert payload["counts"]["new_hard_fail_cells"] == 13
    assert len(payload["new_findings"]["hard_fail_cells"]) == 13


def test_save_parity_golden_supports_fpr_pair(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_fpr"
    (run_dir / "work_fpexe").mkdir(parents=True)
    (run_dir / "work_fpr").mkdir(parents=True)
    shutil.copy2(FIXTURE_DIR / "golden_fpexe.pabev", run_dir / "work_fpexe" / "PABEV.TXT")
    shutil.copy2(FIXTURE_DIR / "golden_fppy.pabev", run_dir / "work_fpr" / "PACEV.TXT")
    _write_parity_report(run_dir, scenario_name="baseline")
    payload = json.loads((run_dir / "parity_report.json").read_text(encoding="utf-8"))
    payload["left_engine"] = "fpexe"
    payload["right_engine"] = "fp-r"
    payload["engine_runs"] = {
        "fpexe": {"pabev_path": "work_fpexe/PABEV.TXT", "work_dir": "work_fpexe"},
        "fp-r": {"pabev_path": "work_fpr/PACEV.TXT", "work_dir": "work_fpr"},
    }
    (run_dir / "parity_report.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )

    golden_root = tmp_path / "golden"
    saved_dir = save_parity_golden(run_dir, golden_root)

    assert (saved_dir / "work_fpexe" / "PABEV.TXT").exists()
    assert (saved_dir / "work_fpr" / "PACEV.TXT").exists()
    saved_report = json.loads((saved_dir / "parity_report.json").read_text(encoding="utf-8"))
    assert saved_report["left_engine"] == "fpexe"
    assert saved_report["right_engine"] == "fp-r"
    assert saved_report["engine_runs"]["fp-r"]["pabev_path"] == "work_fpr/PACEV.TXT"

    compare_payload = compare_parity_to_golden(run_dir, golden_root)
    assert compare_payload["status"] == "ok"


def test_compare_parity_to_golden_fails_on_engine_pair_mismatch(tmp_path: Path) -> None:
    golden_run = _make_run(tmp_path, "golden_run")
    golden_root = tmp_path / "golden"
    save_parity_golden(golden_run, golden_root)

    current_run = _make_run(tmp_path, "current_run")
    current_payload = json.loads((current_run / "parity_report.json").read_text(encoding="utf-8"))
    current_payload["right_engine"] = "fp-r"
    current_payload["engine_runs"] = {
        "fpexe": {"pabev_path": str(current_run / "work_fpexe" / "PABEV.TXT")},
        "fp-r": {"pabev_path": str(current_run / "work_fppy" / "PABEV.TXT")},
    }
    (current_run / "parity_report.json").write_text(
        json.dumps(current_payload, indent=2) + "\n", encoding="utf-8"
    )

    payload = compare_parity_to_golden(current_run, golden_root)

    assert payload["status"] == "failed"
    assert payload["reason"] == "engine_pair_mismatch"
