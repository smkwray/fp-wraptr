"""Tests for run report generation."""

from pathlib import Path

import yaml

from fp_wraptr.analysis.report import build_run_report


def _write_fmout(path: Path) -> None:
    path.write_text(
        """\
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
Variable   Periods forecast are  2025.4  TO   2025.4

                   2025.4      2025.5
                               2026.1
""",
        encoding="utf-8",
    )


def test_report_missing_scenario_yaml(tmp_path):
    run_dir = tmp_path / "run_20260223_120000"
    run_dir.mkdir()
    _write_fmout(run_dir / "fmout.txt")

    report = build_run_report(run_dir)

    assert "Run configuration not found" in report


def test_report_corrupt_scenario_yaml(tmp_path):
    run_dir = tmp_path / "run_20260223_120001"
    run_dir.mkdir()
    (run_dir / "scenario.yaml").write_text('track_variables: "not a list"\n', encoding="utf-8")
    _write_fmout(run_dir / "fmout.txt")

    report = build_run_report(run_dir)

    assert "Failed to parse scenario config" in report


def test_report_no_output_file(tmp_path):
    run_dir = tmp_path / "run_20260223_120002"
    run_dir.mkdir()
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump({"name": "test-run"}, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )

    report = build_run_report(run_dir)

    assert "No forecast output available for this run." in report


def test_report_no_track_variables(tmp_path):
    run_dir = tmp_path / "run_20260223_120003"
    run_dir.mkdir()
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump(
            {"name": "test-run", "track_variables": []},
            default_flow_style=False,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    _write_fmout(run_dir / "fmout.txt")

    report = build_run_report(run_dir)

    assert "- Track variables: <none>" in report


def test_report_with_overrides(tmp_path):
    run_dir = tmp_path / "run_20260223_120004"
    run_dir.mkdir()
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "test-run",
                "overrides": {"rate": {"method": "CHGSAMEPCT", "value": 0.5}},
            },
            default_flow_style=False,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    _write_fmout(run_dir / "fmout.txt")

    report = build_run_report(run_dir)

    assert "## Overrides" in report
    assert "`rate`" in report
    assert "`CHGSAMEPCT`" in report


def test_report_empty_levels(tmp_path):
    run_dir = tmp_path / "run_20260223_120005"
    run_dir.mkdir()
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump({"name": "test-run"}, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )
    (run_dir / "fmout.txt").write_text(
        """\
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
Variable   Periods forecast are  2025.4  TO   2025.4

                   2025.4      2025.5
                               2026.1

   1 EMPTY    P lv
             P ch
             P %ch
""",
        encoding="utf-8",
    )

    report = build_run_report(run_dir)

    assert "No level series found in output." in report


def test_report_baseline_missing_output(tmp_path):
    run_dir = tmp_path / "run_20260223_120006"
    run_dir.mkdir()
    baseline_dir = tmp_path / "baseline_20260223_120006"
    baseline_dir.mkdir()
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump({"name": "test-run"}, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )
    _write_fmout(run_dir / "fmout.txt")

    report = build_run_report(run_dir, baseline_dir=baseline_dir)

    assert "Baseline comparison" in report
    assert "Baseline output missing" in report
