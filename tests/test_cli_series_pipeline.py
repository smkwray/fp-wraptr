from pathlib import Path

from typer.testing import CliRunner

from fp_wraptr.cli import app

runner = CliRunner()


def test_cli_data_preview_pipeline(tmp_path: Path) -> None:
    csv_path = tmp_path / "s.csv"
    csv_path.write_text("period,value\n2025Q1,1.0\n2025Q2,2.0\n", encoding="utf-8")
    out_path = tmp_path / "out.txt"
    pipe_path = tmp_path / "pipe.yaml"
    pipe_path.write_text(
        f"""
name: cli_preview
context:
  forecast_start: "2025.4"
  forecast_end: "2025.4"
steps:
  - id: s
    source:
      kind: csv
      path: {csv_path.name}
      period_col: period
      value_col: value
      format: long
    target:
      kind: include_changevar
      variable: X
      fp_method: SAMEVALUE
      smpl_start: "${{context.forecast_start}}"
      smpl_end: "${{context.forecast_end}}"
    write_to:
      - {out_path.name}
""".lstrip(),
        encoding="utf-8",
    )
    result = runner.invoke(app, ["data", "preview", str(pipe_path)])
    assert result.exit_code == 0, result.output
    assert "Pipeline preview" in result.output


def test_cli_data_run_pipeline_dry_run(tmp_path: Path) -> None:
    csv_path = tmp_path / "s.csv"
    csv_path.write_text("period,value\n2025Q1,1.0\n2025Q2,2.0\n", encoding="utf-8")
    out_path = tmp_path / "out.txt"
    pipe_path = tmp_path / "pipe.yaml"
    pipe_path.write_text(
        f"""
name: cli_run
context:
  forecast_start: "2025.4"
  forecast_end: "2025.4"
steps:
  - id: s
    source:
      kind: csv
      path: {csv_path.name}
      period_col: period
      value_col: value
      format: long
    target:
      kind: include_changevar
      variable: X
      fp_method: SAMEVALUE
      smpl_start: "${{context.forecast_start}}"
      smpl_end: "${{context.forecast_end}}"
    write_to:
      - {out_path.name}
""".lstrip(),
        encoding="utf-8",
    )
    result = runner.invoke(app, ["data", "run", str(pipe_path), "--dry-run"])
    assert result.exit_code == 0, result.output
    assert not out_path.exists()
