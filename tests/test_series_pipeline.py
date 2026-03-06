from pathlib import Path

import pytest

from fp_wraptr.data.series_pipeline.extrapolation import extrapolate_quarterly
from fp_wraptr.data.series_pipeline.fp_targets import render_changevar_include
from fp_wraptr.data.series_pipeline.periods import PeriodError, normalize_period_token
from fp_wraptr.data.series_pipeline.runner import PipelineRunError, run_pipeline
from fp_wraptr.io.input_parser import parse_fm_data


def test_normalize_period_token_quarter_formats() -> None:
    assert normalize_period_token("2025.2") == "2025.2"
    assert normalize_period_token("2025Q2") == "2025.2"
    assert normalize_period_token("2025-Q4") == "2025.4"
    assert normalize_period_token("2025 Q1") == "2025.1"
    with pytest.raises(PeriodError):
        normalize_period_token("2025.5")


def test_extrapolate_quarterly_flat_bounds() -> None:
    result = extrapolate_quarterly(
        history_periods=["2024.3", "2024.4"],
        history_values=[0.2, 1.2],
        start="2025.1",
        end="2025.2",
        method="flat",
        bounds=(0.0, 1.0),
    )
    assert result.periods == ["2025.1", "2025.2"]
    assert all(0.0 <= v <= 1.0 for v in result.values)


def test_render_changevar_include_constant_and_series() -> None:
    text = render_changevar_include(
        variable="INTGADJ",
        fp_method="SAMEVALUE",
        smpl_start="2025.4",
        smpl_end="2026.1",
        values=[0.25, 0.3],
        mode="constant",
    )
    assert "SMPL 2025.4 2026.1;" in text
    assert "INTGADJ SAMEVALUE" in text
    assert "0.3" in text

    text2 = render_changevar_include(
        variable="INTGADJ",
        fp_method="SAMEVALUE",
        smpl_start="2025.4",
        smpl_end="2026.1",
        values=[0.25, 0.3],
        mode="series",
    )
    # series mode emits both values.
    assert "0.25" in text2
    assert "0.3" in text2


def test_pipeline_include_changevar_writes_file(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_path = data_dir / "intg.csv"
    csv_path.write_text("period,intgadj_share\n2025Q1,0.1\n2025Q2,0.2\n", encoding="utf-8")

    out_path = tmp_path / "generated" / "intgadj.txt"
    yaml_path = tmp_path / "pipe.yaml"
    yaml_path.write_text(
        f"""
name: test_intg
context:
  forecast_start: "2025.4"
  forecast_end: "2026.1"
steps:
  - id: intg
    source:
      kind: csv
      path: {csv_path.relative_to(tmp_path)}
      period_col: period
      value_col: intgadj_share
      format: long
    extrapolation:
      method: rolling_mean
      window: 2
    target:
      kind: include_changevar
      variable: INTGADJ
      fp_method: SAMEVALUE
      smpl_start: "${{context.forecast_start}}"
      smpl_end: "${{context.forecast_end}}"
      mode: constant
    write_to:
      - {out_path.relative_to(tmp_path)}
""".lstrip(),
        encoding="utf-8",
    )

    result = run_pipeline(pipeline_path=yaml_path, output_report=None, dry_run=False)
    assert out_path.exists()
    assert any(
        (s.get("target") or {}).get("kind") == "include_changevar" for s in result.report["steps"]
    )
    text = out_path.read_text(encoding="utf-8")
    assert "INTGADJ" in text


def test_pipeline_fmdata_patch_updates_values(tmp_path: Path) -> None:
    # Tiny fmdata with two periods, one series.
    fmdata_in = tmp_path / "fmdata.txt"
    fmdata_in.write_text(
        " SMPL    2020.1   2020.2 ;\n"
        " LOAD GDP      ;\n"
        "   0.10000000000E+01  0.20000000000E+01\n"
        " 'END' \n"
        " END;\n",
        encoding="utf-8",
    )

    src_csv = tmp_path / "gdp.csv"
    src_csv.write_text("period,GDP\n2020.2,3.0\n", encoding="utf-8")
    fmdata_out = tmp_path / "fmdata_out.txt"
    yaml_path = tmp_path / "pipe.yaml"
    yaml_path.write_text(
        f"""
name: test_fmdata_patch
steps:
  - id: gdp_patch
    source:
      kind: csv
      path: {src_csv.name}
      period_col: period
      variable: GDP
      format: wide
    target:
      kind: fmdata_patch
      variable: GDP
      fmdata_in: {fmdata_in.name}
      fmdata_out: {fmdata_out.name}
""".lstrip(),
        encoding="utf-8",
    )

    run_pipeline(pipeline_path=yaml_path, output_report=None, dry_run=False)
    parsed = parse_fm_data(fmdata_out)
    block = parsed["series"]["GDP"][0]
    # Second period updated to 3.0 (writer may reformat, but float parse should match).
    assert block["values"][1] == pytest.approx(3.0)


def test_pipeline_fmdata_patch_can_add_series_with_full_window(tmp_path: Path) -> None:
    fmdata_in = tmp_path / "fmdata.txt"
    fmdata_in.write_text(
        " SMPL    2020.1   2020.2 ;\n"
        " LOAD GDP      ;\n"
        "   0.10000000000E+01  0.20000000000E+01\n"
        " 'END' \n"
        " END;\n",
        encoding="utf-8",
    )

    src_csv = tmp_path / "x.csv"
    src_csv.write_text("period,value\n2020.1,10\n2020.2,20\n", encoding="utf-8")
    fmdata_out = tmp_path / "fmdata_out.txt"
    yaml_path = tmp_path / "pipe.yaml"
    yaml_path.write_text(
        f"""
name: test_add_series
steps:
  - id: add_x
    source:
      kind: csv
      path: {src_csv.name}
      period_col: period
      value_col: value
      format: long
    target:
      kind: fmdata_patch
      variable: X
      fmdata_in: {fmdata_in.name}
      fmdata_out: {fmdata_out.name}
""".lstrip(),
        encoding="utf-8",
    )

    run_pipeline(pipeline_path=yaml_path, output_report=None, dry_run=False)
    parsed = parse_fm_data(fmdata_out)
    assert "X" in parsed["series"]


def test_pipeline_errors_on_missing_source(tmp_path: Path) -> None:
    yaml_path = tmp_path / "pipe.yaml"
    yaml_path.write_text(
        """
name: bad
steps:
  - id: x
    source:
      kind: csv
      path: missing.csv
      period_col: period
      value_col: value
      format: long
    target:
      kind: include_changevar
      variable: X
      fp_method: SAMEVALUE
      smpl_start: "2025.4"
      smpl_end: "2025.4"
    write_to:
      - out.txt
""".lstrip(),
        encoding="utf-8",
    )
    with pytest.raises(PipelineRunError):
        run_pipeline(pipeline_path=yaml_path, output_report=None, dry_run=True)
