from pathlib import Path

from fp_wraptr.analysis.focused_series_compare import (
    build_focused_series_compare_report,
    write_focused_series_compare_report,
)


def _write_pabev(path: Path, *, values_by_var: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["SMPL 2025.3 2025.4;"]
    for name, values in values_by_var.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(str(float(v)) for v in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_focused_series_compare_builds_period_rows(tmp_path: Path) -> None:
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"A": [1.0, 2.0], "B": [10.0, 11.0]})
    _write_pabev(fppy, values_by_var={"A": [1.5, 2.5], "B": [10.0, 10.5]})
    _write_pabev(fpr, values_by_var={"A": [0.5, 2.25], "B": [10.25, 11.25]})

    report = build_focused_series_compare_report(
        {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
        variables=["A", "B"],
        start="2025.4",
        end="2025.4",
    )

    assert report["common_period_count"] == 1
    assert report["row_count"] == 2
    row_a = next(row for row in report["rows"] if row["variable"] == "A")
    assert abs(float(row_a["fpr_minus_fppy"]) + 0.25) < 1e-12
    assert report["summary"]["A"]["first_period_present"] == "2025.4"


def test_focused_series_compare_writes_json_and_csv(tmp_path: Path) -> None:
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"A": [1.0, 2.0]})
    _write_pabev(fppy, values_by_var={"A": [1.0, 2.5]})
    _write_pabev(fpr, values_by_var={"A": [1.0, 2.25]})

    report = build_focused_series_compare_report(
        {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
        variables=["A"],
        start="2025.4",
        end="2025.4",
    )
    json_path, csv_path = write_focused_series_compare_report(report, output_dir=tmp_path / "out")

    assert json_path.exists()
    assert csv_path.exists()
    assert "variable" in csv_path.read_text(encoding="utf-8")
