from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from fp_wraptr.cli import app
from fp_wraptr.io.fmdata_diff import diff_fmdata_files
from fp_wraptr.io.fmdata_writer import write_fmdata

runner = CliRunner()


def _write_fixture_bundle(tmp_path: Path) -> tuple[Path, Path, Path]:
    base_path = tmp_path / "base_fmdata.txt"
    updated_path = tmp_path / "updated_fmdata.txt"
    report_path = tmp_path / "data_update_report.json"

    write_fmdata(
        sample_start="2025.3",
        sample_end="2025.4",
        series={
            "A": [100.0, 100.0],
            "B": [200.0, 200.0],
            "C": [300.0, 305.0],
        },
        newline="\n",
        path=base_path,
    )
    write_fmdata(
        sample_start="2025.3",
        sample_end="2026.1",
        series={
            "A": [100.0, 100.0, 130.0],
            "B": [200.0, 200.0, 200.0],
            "C": [300.0, 310.0, 310.0],
        },
        newline="\n",
        path=updated_path,
    )
    report_payload = {
        "start_period": "2025.4",
        "end_period": "2026.1",
        "fmdata_merge": {
            "variables_with_updates": ["A", "C"],
            "variables_with_carry_forward": ["B"],
        },
    }
    report_path.write_text(json.dumps(report_payload), encoding="utf-8")
    return base_path, updated_path, report_path


def test_diff_fmdata_sample_end_and_update_window(tmp_path: Path) -> None:
    base_path, updated_path, report_path = _write_fixture_bundle(tmp_path)

    sample_end = diff_fmdata_files(
        base_fmdata=base_path,
        updated_fmdata=updated_path,
        data_update_report=report_path,
        scope="sample_end",
    )
    assert sample_end["window_start"] == "2026.1"
    assert sample_end["window_end"] == "2026.1"
    assert sample_end["changed_variable_count"] == 2
    changed = {row["variable"]: row for row in sample_end["records"]}
    assert set(changed.keys()) == {"A", "C"}
    assert changed["A"]["change_type"] == "updated"
    assert changed["C"]["change_type"] == "updated"

    update_window = diff_fmdata_files(
        base_fmdata=base_path,
        updated_fmdata=updated_path,
        data_update_report=report_path,
        scope="update_window",
    )
    assert update_window["window_start"] == "2025.4"
    assert update_window["window_end"] == "2026.1"
    changed_window = {row["variable"]: row for row in update_window["records"]}
    assert set(changed_window.keys()) == {"A", "C"}
    assert changed_window["A"]["changed_period_count"] == 1
    assert changed_window["C"]["changed_period_count"] == 2


def test_data_diff_fmdata_cli_json(tmp_path: Path) -> None:
    base_path, updated_path, report_path = _write_fixture_bundle(tmp_path)

    result = runner.invoke(
        app,
        [
            "data",
            "diff-fmdata",
            "--base-fmdata",
            str(base_path),
            "--updated-fmdata",
            str(updated_path),
            "--data-update-report",
            str(report_path),
            "--scope",
            "sample_end",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["changed_variable_count"] == 2
    assert payload["window_start"] == "2026.1"
    assert payload["window_end"] == "2026.1"
