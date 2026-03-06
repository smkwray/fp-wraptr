from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from fp_wraptr.data.update_fred import update_model_from_fred
from fp_wraptr.io.input_parser import parse_fm_data


def _write_minimal_model_dir(model_dir: Path) -> None:
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "fmdata.txt").write_text(
        "\n".join([
            "SMPL 2024.1 2025.4;",
            "LOAD UR;",
            "1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0",
            "",
            "SMPL 2024.1 2025.4;",
            "LOAD OTHER;",
            "0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0",
            "",
            "END;",
            "",
        ]),
        encoding="utf-8",
    )
    (model_dir / "fmage.txt").write_text("AGE_PLACEHOLDER\n", encoding="utf-8")
    (model_dir / "fmexog.txt").write_text("RETURN;\n", encoding="utf-8")
    (model_dir / "fminput.txt").write_text("INPUT FILE=fmexog.txt;\nRETURN;\n", encoding="utf-8")


def test_update_model_from_fred_writes_bundle_and_report(tmp_path, monkeypatch):
    model_dir = tmp_path / "FM"
    _write_minimal_model_dir(model_dir)

    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        "\n".join([
            "UR:",
            "  description: test UR via FRED",
            "  source: fred",
            "  series_id: UNRATE",
            "  frequency: M",
            "  units: Percent",
            "  transform: level",
            "  scale: 0.01",
        ])
        + "\n",
        encoding="utf-8",
    )

    # 2025Q1 -> quarterly mean of 9.0 for UR
    fred_df = pd.DataFrame(
        {"UNRATE": [9.0, 9.0, 9.0]},
        index=pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"]),
    )
    monkeypatch.setattr(
        "fp_wraptr.data.update_fred.fetch_fred_series",
        lambda *args, **kwargs: fred_df,
    )

    out_dir = tmp_path / "artifacts" / "model_updates" / "demo"
    result = update_model_from_fred(
        model_dir=model_dir,
        out_dir=out_dir,
        end_period="2025.4",
        source_map_path=source_map_path,
        variables=["UR"],
        sources=["fred"],
        replace_history=True,
    )

    assert (out_dir / "FM" / "fmdata.txt").exists()
    assert (out_dir / "data_update_report.json").exists()
    assert result.fmdata_path == out_dir / "FM" / "fmdata.txt"
    assert result.report_path == out_dir / "data_update_report.json"

    parsed = parse_fm_data(result.fmdata_path)
    ur_blocks = parsed["series"]["UR"]
    assert ur_blocks
    # 2025.1 is index 4 when sample starts at 2024.1
    assert ur_blocks[-1]["values"][4] == 0.09

    report_payload = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report_payload["selected_variable_count"] == 1
    assert report_payload["normalized_variable_count"] == 1
    assert report_payload["fmdata_merge"]["updated_cells"] >= 1
    assert report_payload["recommended_forecast_start"] == "2026.1"
    assert report_payload["recommended_forecast_end"] == "2029.4"
    templates = report_payload["scenario_templates"]
    assert Path(templates["baseline_yaml"]).exists()
    assert Path(templates["baseline_smoke_yaml"]).exists()
    assert Path(templates["readme"]).exists()
    assert 'forecast_start: "2026.1"' in Path(templates["baseline_yaml"]).read_text(
        encoding="utf-8"
    )


def test_update_model_from_bls_writes_bundle(tmp_path, monkeypatch):
    model_dir = tmp_path / "FM"
    _write_minimal_model_dir(model_dir)

    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        "\n".join([
            "UR:",
            "  description: test UR via BLS",
            "  source: bls",
            "  series_id: TESTSERIES",
            "  frequency: M",
            "  units: Percent",
            "  transform: level",
            "  scale: 0.01",
        ])
        + "\n",
        encoding="utf-8",
    )

    bls_df = pd.DataFrame(
        {"TESTSERIES": [9.0, 9.0, 9.0]},
        index=pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"]),
    )
    monkeypatch.setattr(
        "fp_wraptr.bls.ingest.fetch_series",
        lambda *args, **kwargs: bls_df,
    )

    out_dir = tmp_path / "artifacts" / "model_updates" / "bls"
    result = update_model_from_fred(
        model_dir=model_dir,
        out_dir=out_dir,
        end_period="2025.4",
        source_map_path=source_map_path,
        variables=["UR"],
        sources=["bls"],
        replace_history=True,
    )

    parsed = parse_fm_data(result.fmdata_path)
    assert parsed["series"]["UR"][-1]["values"][4] == 0.09


def test_update_model_from_bea_writes_bundle(tmp_path, monkeypatch):
    model_dir = tmp_path / "FM"
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "fmdata.txt").write_text(
        "\n".join([
            "SMPL 2024.1 2025.4;",
            "LOAD X;",
            "1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0",
            "",
            "END;",
            "",
        ]),
        encoding="utf-8",
    )
    (model_dir / "fmage.txt").write_text("AGE_PLACEHOLDER\n", encoding="utf-8")
    (model_dir / "fmexog.txt").write_text("RETURN;\n", encoding="utf-8")
    (model_dir / "fminput.txt").write_text("INPUT FILE=fmexog.txt;\nRETURN;\n", encoding="utf-8")

    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        "\n".join([
            "X:",
            "  description: test X via BEA",
            "  source: bea",
            "  bea_table: T10106",
            "  bea_line: 1",
            "  frequency: Q",
            "  units: Level",
            "  transform: level",
        ])
        + "\n",
        encoding="utf-8",
    )

    bea_df = pd.DataFrame(
        {1: [111.0]},
        index=pd.to_datetime(["2025-01-01"]),
    )
    monkeypatch.setattr(
        "fp_wraptr.bea.ingest.fetch_nipa_table",
        lambda *args, **kwargs: bea_df,
    )

    out_dir = tmp_path / "artifacts" / "model_updates" / "bea"
    result = update_model_from_fred(
        model_dir=model_dir,
        out_dir=out_dir,
        end_period="2025.4",
        source_map_path=source_map_path,
        variables=["X"],
        sources=["bea"],
        replace_history=True,
    )

    parsed = parse_fm_data(result.fmdata_path)
    assert parsed["series"]["X"][-1]["values"][4] == 111.0


def test_update_model_from_bea_skips_failed_tables_and_continues(tmp_path, monkeypatch):
    from fp_wraptr.bea.ingest import BeaApiError

    model_dir = tmp_path / "FM"
    _write_minimal_model_dir(model_dir)

    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        "\n".join([
            "UR:",
            "  description: test UR via FRED",
            "  source: fred",
            "  series_id: UNRATE",
            "  frequency: M",
            "  units: Percent",
            "  transform: level",
            "  scale: 0.01",
            "OTHER:",
            "  description: test OTHER via BEA",
            "  source: bea",
            "  bea_table: T10106",
            "  bea_line: 1",
            "  frequency: Q",
            "  units: Level",
            "  transform: level",
        ])
        + "\n",
        encoding="utf-8",
    )

    fred_df = pd.DataFrame(
        {"UNRATE": [9.0, 9.0, 9.0]},
        index=pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"]),
    )
    monkeypatch.setattr(
        "fp_wraptr.data.update_fred.fetch_fred_series",
        lambda *args, **kwargs: fred_df,
    )

    def _raise_bea_error(*args, **kwargs):
        raise BeaApiError("BEA response missing Results.Data list")

    monkeypatch.setattr("fp_wraptr.bea.ingest.fetch_nipa_table", _raise_bea_error)

    out_dir = tmp_path / "artifacts" / "model_updates" / "bea_skip"
    result = update_model_from_fred(
        model_dir=model_dir,
        out_dir=out_dir,
        end_period="2025.4",
        source_map_path=source_map_path,
        variables=["UR", "OTHER"],
        sources=["fred", "bea"],
        replace_history=True,
    )

    parsed = parse_fm_data(result.fmdata_path)
    assert parsed["series"]["UR"][-1]["values"][4] == 0.09
    assert parsed["series"]["OTHER"][-1]["values"][4] == 0.0
    assert result.report["bea_failed_tables"] == [
        {"table": "T10106", "error": "BEA response missing Results.Data list"}
    ]
    assert result.report["selected_variables_without_observations"] == ["OTHER"]


def test_update_model_from_bea_skips_unsupported_bea_table(tmp_path, monkeypatch):
    model_dir = tmp_path / "FM"
    _write_minimal_model_dir(model_dir)

    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        "\n".join([
            "UR:",
            "  description: test UR via FRED",
            "  source: fred",
            "  series_id: UNRATE",
            "  frequency: M",
            "  units: Percent",
            "  transform: level",
            "  scale: 0.01",
            "OTHER:",
            "  description: unsupported BEA placeholder table",
            "  source: bea",
            "  bea_table: BADTABLE",
            "  bea_line: 1",
            "  frequency: Q",
            "  units: Level",
            "  transform: level",
        ])
        + "\n",
        encoding="utf-8",
    )

    fred_df = pd.DataFrame(
        {"UNRATE": [9.0, 9.0, 9.0]},
        index=pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"]),
    )
    monkeypatch.setattr(
        "fp_wraptr.data.update_fred.fetch_fred_series",
        lambda *args, **kwargs: fred_df,
    )

    out_dir = tmp_path / "artifacts" / "model_updates" / "bea_unsupported"
    result = update_model_from_fred(
        model_dir=model_dir,
        out_dir=out_dir,
        end_period="2025.4",
        source_map_path=source_map_path,
        variables=["UR", "OTHER"],
        sources=["fred", "bea"],
        replace_history=True,
    )

    assert result.report["selected_variable_count"] == 1  # UR only
    assert result.report["bea_failed_tables"] == []
    assert result.report["selected_variables_without_observations"] == []
    assert result.report["skipped_unsupported_bea"] == [
        {"variable": "OTHER", "bea_table": "BADTABLE", "reason": "unsupported_bea_table"}
    ]


def test_update_model_skips_unsupported_frequency_entries(tmp_path, monkeypatch):
    model_dir = tmp_path / "FM"
    _write_minimal_model_dir(model_dir)

    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        "\n".join([
            "UR:",
            "  description: test UR via FRED",
            "  source: fred",
            "  series_id: UNRATE",
            "  frequency: M",
            "  units: Percent",
            "  transform: level",
            "  scale: 0.01",
            "OTHER:",
            "  description: unsupported annual series",
            "  source: fred",
            "  series_id: OTHERANNUAL",
            "  frequency: A",
            "  units: Level",
            "  transform: level",
        ])
        + "\n",
        encoding="utf-8",
    )

    fred_df = pd.DataFrame(
        {"UNRATE": [9.0, 9.0, 9.0]},
        index=pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"]),
    )
    monkeypatch.setattr(
        "fp_wraptr.data.update_fred.fetch_fred_series",
        lambda *args, **kwargs: fred_df,
    )

    out_dir = tmp_path / "artifacts" / "model_updates" / "skip_annual"
    result = update_model_from_fred(
        model_dir=model_dir,
        out_dir=out_dir,
        end_period="2025.4",
        source_map_path=source_map_path,
        variables=["UR", "OTHER"],
        sources=["fred"],
        replace_history=True,
    )

    parsed = parse_fm_data(result.fmdata_path)
    assert parsed["series"]["UR"][-1]["values"][4] == 0.09
    assert result.report["selected_variable_count"] == 1
    assert result.report["skipped_unsupported_frequency"] == [
        {"variable": "OTHER", "frequency": "A", "source": "fred"}
    ]


def test_update_model_no_fetch_window_keeps_report_fields(tmp_path):
    model_dir = tmp_path / "FM"
    _write_minimal_model_dir(model_dir)

    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        "\n".join([
            "UR:",
            "  description: test UR via FRED",
            "  source: fred",
            "  series_id: UNRATE",
            "  frequency: M",
            "  units: Percent",
            "  transform: level",
            "  scale: 0.01",
        ])
        + "\n",
        encoding="utf-8",
    )

    out_dir = tmp_path / "artifacts" / "model_updates" / "no_fetch_window"
    result = update_model_from_fred(
        model_dir=model_dir,
        out_dir=out_dir,
        end_period="2025.3",
        source_map_path=source_map_path,
        variables=["UR"],
        sources=["fred"],
        replace_history=False,
    )

    assert result.report["selected_variable_count"] == 1
    assert result.report["selected_variables_without_observations"] == []
    assert result.report["bea_failed_tables"] == []
