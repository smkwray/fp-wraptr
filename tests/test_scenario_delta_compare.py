import json
from pathlib import Path

from fp_wraptr.analysis.scenario_delta_compare import (
    build_scenario_delta_compare_report,
    write_scenario_delta_compare_report,
)


def _write_pabev(path: Path, *, values_by_var: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["SMPL 2025.4 2026.1;"]
    for name, values in values_by_var.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(str(float(v)) for v in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_scenario_delta_compare_builds_delta_gap_summary(tmp_path: Path) -> None:
    left_baseline = tmp_path / "left-baseline.PABEV.TXT"
    left_scenario = tmp_path / "left-scenario.PABEV.TXT"
    right_baseline = tmp_path / "right-baseline.PABEV.TXT"
    right_scenario = tmp_path / "right-scenario.PABEV.TXT"
    _write_pabev(left_baseline, values_by_var={"UB": [9.0, 9.0], "YD": [100.0, 102.0]})
    _write_pabev(left_scenario, values_by_var={"UB": [65.0, 65.0], "YD": [150.0, 160.0]})
    _write_pabev(right_baseline, values_by_var={"UB": [9.0, 9.0], "YD": [100.0, 102.0]})
    _write_pabev(right_scenario, values_by_var={"UB": [9.18, 9.18], "YD": [112.0, 118.0]})

    report = build_scenario_delta_compare_report(
        baseline_left=left_baseline,
        scenario_left=left_scenario,
        baseline_right=right_baseline,
        scenario_right=right_scenario,
        left_label="fpexe",
        right_label="fpr",
        variables=["UB", "YD"],
        start="2025.4",
        end="2026.1",
    )

    assert report["common_period_count"] == 2
    rows = {row["variable"]: row for row in report["summary_rows"]}
    assert rows["UB"]["classification"] == "review"
    assert rows["UB"]["delta_max_period"] == "2025.4"
    assert rows["UB"]["fpexe_delta_at_max"] == 56.0
    assert rows["UB"]["fpr_delta_at_max"] == 0.17999999999999972
    assert rows["YD"]["classification"] == "review"


def test_scenario_delta_compare_writes_outputs(tmp_path: Path) -> None:
    report = {
        "engine_labels": {"left": "fpexe", "right": "fpr"},
        "summary_rows": [
            {
                "variable": "UB",
                "classification": "review",
                "baseline_max_abs_diff": 0.0,
                "baseline_max_period": "2025.4",
                "scenario_max_abs_diff": 55.82,
                "scenario_max_period": "2025.4",
                "delta_max_abs_diff": 55.82,
                "delta_max_period": "2025.4",
                "delta_max_rel_pct": 99.0,
                "scale_at_max": 56.0,
                "fpexe_delta_at_max": 56.0,
                "fpr_delta_at_max": 0.18,
                "fpexe_baseline_at_max": 9.0,
                "fpexe_scenario_at_max": 65.0,
                "fpr_baseline_at_max": 9.0,
                "fpr_scenario_at_max": 9.18,
            }
        ],
    }
    out_dir = tmp_path / "scenario_delta"

    json_path, csv_path = write_scenario_delta_compare_report(report, output_dir=out_dir)

    assert json_path.exists()
    assert csv_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["engine_labels"] == {"left": "fpexe", "right": "fpr"}
