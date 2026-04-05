from pathlib import Path

from fp_wraptr.analysis.identity_decomposition import (
    build_identity_decomposition_report,
    write_identity_decomposition_report,
)


def _write_pabev(path: Path, *, values_by_var: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["SMPL 2025.4 2025.4;"]
    for name, values in values_by_var.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(str(float(v)) for v in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_identity_decomposition_builds_term_report(tmp_path: Path) -> None:
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(
        fpexe,
        values_by_var={"A": [10.0], "B": [5.0], "C": [2.0], "T": [13.0]},
    )
    _write_pabev(
        fppy,
        values_by_var={"A": [10.5], "B": [5.0], "C": [2.0], "T": [13.5]},
    )
    _write_pabev(
        fpr,
        values_by_var={"A": [10.0], "B": [4.5], "C": [2.0], "T": [12.5]},
    )

    report = build_identity_decomposition_report(
        {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
        identity="T=A+B-C",
        period="2025.4",
    )

    assert report["target"] == "T"
    assert report["term_count"] == 3
    assert abs(float(report["target_pair_diffs"]["fpr_minus_fppy"]) + 1.0) < 1e-12
    top = report["term_rows"][0]
    assert top["term"] in {"A", "B"}
    assert float(report["reconstructed_totals"]["fpexe"]) == 13.0


def test_identity_decomposition_writes_json_and_csv(tmp_path: Path) -> None:
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"X": [3.0], "Y": [2.0], "Z": [5.0]})
    _write_pabev(fppy, values_by_var={"X": [3.0], "Y": [2.5], "Z": [5.5]})
    _write_pabev(fpr, values_by_var={"X": [3.0], "Y": [2.0], "Z": [5.0]})

    report = build_identity_decomposition_report(
        {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
        identity="Z=X+Y",
        period="2025.4",
    )
    report_path, csv_path = write_identity_decomposition_report(report, output_dir=tmp_path / "out")

    assert report_path.exists()
    assert csv_path.exists()
    assert "term,max_pair_abs_diff" not in csv_path.read_text(encoding="utf-8")
    assert "term" in csv_path.read_text(encoding="utf-8")
