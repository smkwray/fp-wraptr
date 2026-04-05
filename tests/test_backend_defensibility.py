from pathlib import Path

from fp_wraptr.analysis.backend_defensibility import (
    build_backend_defensibility_report,
    write_backend_defensibility_report,
)


def _write_pabev(path: Path, *, values_by_var: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["SMPL 2025.3 2025.4;"]
    for name, values in values_by_var.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(str(float(v)) for v in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_backend_defensibility_builds_three_way_summary(tmp_path: Path) -> None:
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"PIEF": [100.0, 101.0], "RS": [4.0, 4.0]})
    _write_pabev(fppy, values_by_var={"PIEF": [100.0, 104.0], "RS": [4.0, 4.1]})
    _write_pabev(fpr, values_by_var={"PIEF": [100.0, 102.5], "RS": [4.0, 4.0]})

    report = build_backend_defensibility_report(
        {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
        start="2025.4",
        end="2025.4",
        focus_variables=["PIEF", "RS"],
        close_rel_threshold=1e-3,
        close_abs_threshold=1.1e-3,
    )

    assert report["common_period_count"] == 1
    assert report["common_variable_count"] == 2
    assert report["focus_rows"]
    pief = next(row for row in report["summary_rows"] if row["variable"] == "PIEF")
    rs = next(row for row in report["summary_rows"] if row["variable"] == "RS")
    assert pief["classification"] == "review"
    assert pief["max_abs_pair"] == "fpexe__vs__fppy"
    assert abs(float(pief["max_abs_diff"]) - 3.0) < 1e-12
    assert rs["classification"] in {"close", "review"}


def test_backend_defensibility_writes_json_and_csv(tmp_path: Path) -> None:
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"A": [1.0, 1.0]})
    _write_pabev(fppy, values_by_var={"A": [1.0, 1.0]})
    _write_pabev(fpr, values_by_var={"A": [1.0, 1.0005]})

    report = build_backend_defensibility_report(
        {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
        start="2025.4",
        end="2025.4",
        focus_variables=["A"],
    )
    summary_json, summary_csv = write_backend_defensibility_report(report, output_dir=tmp_path / "out")

    assert summary_json.exists()
    assert summary_csv.exists()
    text = summary_csv.read_text(encoding="utf-8")
    assert "variable,classification" in text
    assert "A" in text
