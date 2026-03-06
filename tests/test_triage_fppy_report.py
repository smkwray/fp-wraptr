from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "triage_fppy_report.py"


def test_triage_fppy_report_writes_summary_and_csv(tmp_path: Path) -> None:
    run_dir = tmp_path / "work_fppy"
    run_dir.mkdir(parents=True)
    report_path = run_dir / "fppy_report.json"
    report_payload = {
        "issues": [
            {
                "line": 10,
                "statement": "GENR LWF=LOG(WF);",
                "error": "TypeError: '>' not supported between instances of 'numpy._ArrayFunctionDispatcher' and 'int'",
            },
            {
                "line": 20,
                "statement": "GENR X=Y;",
                "error": "KeyError: column 'Y' not present in input data",
            },
            {
                "line": 30,
                "statement": "MODEQ 1 ...;",
                "error": "MODEQ parse error",
            },
            {
                "line": 40,
                "statement": "EQ ...;",
                "error": "did not converge after max iterations",
            },
            {
                "line": 50,
                "statement": "IDENT A=B;",
                "error": "unexpected edge case",
            },
        ],
        "summary": {},
    }
    report_path.write_text(json.dumps(report_payload, indent=2) + "\n", encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--run-dir", str(run_dir)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    summary = json.loads((run_dir / "triage_summary.json").read_text(encoding="utf-8"))
    assert summary["issue_count"] == 5
    assert summary["bucket_counts"]["pandas_eval_np_module_attr"] == 1
    assert summary["bucket_counts"]["missing_variable"] == 1
    assert summary["bucket_counts"]["modeq_parse"] == 1
    assert summary["bucket_counts"]["convergence"] == 1
    assert summary["bucket_counts"]["other"] == 1

    with (run_dir / "triage_issues.csv").open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 5
    assert rows[0]["bucket"] == "pandas_eval_np_module_attr"


def test_resolve_fppy_report_path_backend_both_parent_dir(tmp_path: Path) -> None:
    # Simulate `fp run --backend both` layout:
    # - parent contains parity_report.json
    # - engine work dirs are nested elsewhere
    from fp_wraptr.analysis.triage_fppy import resolve_fppy_report_path

    parent = tmp_path / "scenario_20260101_000000"
    nested = tmp_path / "scenario_20260101_000000" / "parity" / "inner_run" / "work_fppy"
    nested.mkdir(parents=True, exist_ok=True)
    (nested / "fppy_report.json").write_text('{"issues": [], "summary": {}}\n', encoding="utf-8")

    (parent / "parity_report.json").write_text(
        json.dumps(
            {
                "engine_runs": {
                    "fppy": {
                        "work_dir": str(nested),
                    }
                }
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    resolved = resolve_fppy_report_path(parent)
    assert resolved == nested / "fppy_report.json"
