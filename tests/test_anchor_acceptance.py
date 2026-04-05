from pathlib import Path

from fp_wraptr.analysis.anchor_acceptance import (
    build_anchor_acceptance_report,
    resolve_anchor_acceptance_preset,
    write_anchor_acceptance_report,
)


def _write_pabev(path: Path, *, values_by_var: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["SMPL 2025.3 2025.4;"]
    for name, values in values_by_var.items():
        lines.append(f"LOAD {name};")
        lines.append(" ".join(str(float(v)) for v in values))
        lines.append("'END'")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_anchor_acceptance_marks_anchor_review_separately(tmp_path: Path) -> None:
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"PD": [1.0, 1.0], "XX": [10.0, 10.0]})
    _write_pabev(fppy, values_by_var={"PD": [1.0, 1.0], "XX": [10.0, 14.0]})
    _write_pabev(fpr, values_by_var={"PD": [1.0, 1.2], "XX": [10.0, 12.0]})

    report = build_anchor_acceptance_report(
        {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
        anchor_variables=["PD"],
        methodology_variables=["XX"],
        start="2025.4",
        end="2025.4",
        close_abs_threshold=0.05,
        close_rel_threshold=1e-3,
    )

    assert report["status"] == "review"
    assert report["counts"]["anchor_review_count"] == 1
    assert report["counts"]["anchor_fp_r_review_count"] == 1
    assert report["counts"]["methodology_review_count"] == 1
    assert report["counts"]["methodology_broad_review_count"] == 1
    assert report["anchor_rows"][0]["variable"] == "PD"
    assert report["anchor_rows"][0]["review_scope"] == "fp_r_leading"
    assert report["methodology_rows"][0]["variable"] == "XX"
    assert report["methodology_rows"][0]["review_scope"] == "broad_split"
    assert abs(report["anchor_focus_summary"]["PD"]["max_abs_fpr_vs_fppy"] - 0.2) < 1e-12


def test_anchor_acceptance_marks_small_fp_r_tail_on_shared_split(tmp_path: Path) -> None:
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"RS": [4.0, 4.0]})
    _write_pabev(fppy, values_by_var={"RS": [4.0, 4.5]})
    _write_pabev(fpr, values_by_var={"RS": [4.0, 4.54]})

    report = build_anchor_acceptance_report(
        {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
        anchor_variables=["RS"],
        start="2025.4",
        end="2025.4",
        close_abs_threshold=0.001,
        close_rel_threshold=1e-3,
    )

    assert report["anchor_rows"][0]["review_scope"] == "fp_r_leading"
    assert report["anchor_rows"][0]["explanation_scope"] == "fp_r_tail_on_shared_split"


def test_anchor_acceptance_can_be_ok_with_only_methodology_review(tmp_path: Path) -> None:
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"PD": [1.0, 1.0], "XX": [10.0, 10.0]})
    _write_pabev(fppy, values_by_var={"PD": [1.0, 1.0], "XX": [10.0, 14.0]})
    _write_pabev(fpr, values_by_var={"PD": [1.0, 1.0005], "XX": [10.0, 12.0]})

    report = build_anchor_acceptance_report(
        {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
        anchor_variables=["PD"],
        methodology_variables=["XX"],
        start="2025.4",
        end="2025.4",
    )

    assert report["status"] == "ok"
    assert report["counts"]["anchor_review_count"] == 0
    assert report["counts"]["methodology_review_count"] == 1
    assert report["counts"]["methodology_broad_review_count"] == 1


def test_anchor_acceptance_writes_json_and_csv(tmp_path: Path) -> None:
    fpexe = tmp_path / "fpexe.PABEV.TXT"
    fppy = tmp_path / "fppy.PABEV.TXT"
    fpr = tmp_path / "fpr.PABEV.TXT"
    _write_pabev(fpexe, values_by_var={"PD": [1.0, 1.0], "XX": [10.0, 10.0]})
    _write_pabev(fppy, values_by_var={"PD": [1.0, 1.0], "XX": [10.0, 14.0]})
    _write_pabev(fpr, values_by_var={"PD": [1.0, 1.0005], "XX": [10.0, 12.0]})

    report = build_anchor_acceptance_report(
        {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
        anchor_variables=["PD"],
        methodology_variables=["XX"],
        start="2025.4",
        end="2025.4",
    )
    json_path, csv_path = write_anchor_acceptance_report(report, output_dir=tmp_path / "out")

    assert json_path.exists()
    assert csv_path.exists()
    csv_text = csv_path.read_text(encoding="utf-8")
    assert "section,variable,classification" in csv_text
    assert "PD" in csv_text
    assert "XX" in csv_text
    assert "explanation_scope" in csv_text


def test_anchor_acceptance_resolves_named_rs_frontier_preset() -> None:
    preset = resolve_anchor_acceptance_preset("pse_rs_frontier")

    assert preset is not None
    assert preset["name"] == "pse_rs_frontier"
    assert preset["anchors"] == ["UR", "UR1", "RS", "RB", "RM"]
    assert preset["methodology"] == ["PCPD", "PCPF", "PIEF", "E", "SG", "XX", "SUBG"]
    assert preset["start"] == "2025.4"
