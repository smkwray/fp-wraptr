import json
from pathlib import Path

from fp_wraptr.analysis.triage_fppy import (
    classify_fppy_issue,
    triage_fppy_report,
    triage_fppy_report_payload,
)
from fp_wraptr.analysis.triage_parity_hardfails import triage_parity_hardfails


def test_triage_fppy_issue_bucketing_np_dispatcher() -> None:
    issue = {
        "line": 123,
        "statement": "GENR X=LOG(Y);",
        "error": "TypeError: '>' not supported between instances of 'numpy._ArrayFunctionDispatcher' and 'int'",
    }
    assert classify_fppy_issue(issue) == "pandas_eval_np_module_attr"


def test_triage_fppy_report_payload_orders_counts() -> None:
    report = {
        "issues": [
            {"statement": "MODEQ 10 ...", "error": "parse error"},
            {"statement": "GENR A=B", "error": "KeyError: column 'B' not present in input data"},
        ]
    }
    rows, counts = triage_fppy_report_payload(report)
    assert len(rows) == 2
    assert counts["modeq_parse"] == 1
    assert counts["missing_variable"] == 1


def test_triage_fppy_report_writes_artifacts(tmp_path: Path) -> None:
    report_dir = tmp_path / "work_fppy"
    report_dir.mkdir(parents=True)
    (report_dir / "fppy_report.json").write_text(
        json.dumps({"issues": []}) + "\n", encoding="utf-8"
    )

    summary_path, csv_path = triage_fppy_report(report_dir)
    assert summary_path.exists()
    assert csv_path.exists()


def test_triage_parity_hardfails_recomputes_full_set(tmp_path: Path) -> None:
    run_dir = tmp_path / "baseline_20260301_000000"
    run_dir.mkdir(parents=True)

    left = run_dir / "left.pabev"
    right = run_dir / "right.pabev"
    fixture_root = Path(__file__).resolve().parent / "fixtures" / "pabev"
    left.write_text(
        (fixture_root / "golden_fpexe.pabev").read_text(encoding="utf-8"), encoding="utf-8"
    )
    right.write_text(
        (fixture_root / "current_new_hardfail_fppy.pabev").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

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
            "engine_runs": {
                "fpexe": {"pabev_path": str(left)},
                "fppy": {"pabev_path": str(right)},
            },
        })
        + "\n",
        encoding="utf-8",
    )

    out_csv, out_json = triage_parity_hardfails(run_dir)
    assert out_csv.exists()
    assert out_json.exists()
    assert "HF" in out_csv.read_text(encoding="utf-8")
