from __future__ import annotations

import pandas as pd

from fp_wraptr.data.check_mapping import check_mapping_against_fmdata
from fp_wraptr.io.fmdata_writer import write_fmdata


def test_check_mapping_against_fmdata_multi_source(monkeypatch, tmp_path):
    model_dir = tmp_path / "FM"
    model_dir.mkdir(parents=True, exist_ok=True)
    write_fmdata(
        path=model_dir / "fmdata.txt",
        sample_start="2024.1",
        sample_end="2024.4",
        newline="\n",
        series={
            "XF": [1.0, 2.0, 3.0, 4.0],
            "XB": [10.0, 20.0, 30.0, 40.0],
            "XA": [100.0, 200.0, 300.0, 400.0],
            "AFT": [5.0, 5.0, 5.0, 5.0],
        },
    )

    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        """
XF:
  source: fred
  series_id: FRED_X
  frequency: Q
XB:
  source: bls
  series_id: BLS_X
  frequency: M
  aggregation: mean
XA:
  source: bea
  bea_table: T10106
  bea_line: 1
  frequency: Q
  annual_rate: true
AFT:
  source: fred
  series_id: AFT_SERIES
  frequency: A
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def _fake_fetch_fred_observations(*, series_ids, start_date, end_date, cache_dir):
        assert series_ids == ["FRED_X"]
        assert start_date == "2024-01-01"
        assert end_date == "2024-12-31"
        index = pd.to_datetime(["2024-01-01", "2024-04-01", "2024-07-01", "2024-10-01"])
        return pd.DataFrame({"FRED_X": [1.0, 2.0, 3.0, 4.0]}, index=index)

    def _fake_fetch_bls_observations(*, series_ids, start_date, end_date, cache_dir):
        assert series_ids == ["BLS_X"]
        assert start_date == "2024-01-01"
        assert end_date == "2024-12-31"
        index = pd.to_datetime([
            "2024-01-01",
            "2024-02-01",
            "2024-03-01",
            "2024-04-01",
            "2024-05-01",
            "2024-06-01",
            "2024-07-01",
            "2024-08-01",
            "2024-09-01",
            "2024-10-01",
            "2024-11-01",
            "2024-12-01",
        ])
        return pd.DataFrame(
            {
                "BLS_X": [
                    10.0,
                    10.0,
                    10.0,
                    20.0,
                    20.0,
                    20.0,
                    30.0,
                    30.0,
                    30.0,
                    40.0,
                    40.0,
                    40.0,
                ]
            },
            index=index,
        )

    def _fake_fetch_bea_variable_observations(*, selected_bea, cache_dir):
        assert sorted(selected_bea.keys()) == ["XA"]
        index = pd.to_datetime(["2024-01-01", "2024-04-01", "2024-07-01", "2024-10-01"])
        return pd.DataFrame({"XA": [400.0, 800.0, 1200.0, 1600.0]}, index=index), []

    monkeypatch.setattr(
        "fp_wraptr.data.check_mapping._fetch_fred_observations", _fake_fetch_fred_observations
    )
    monkeypatch.setattr(
        "fp_wraptr.data.check_mapping._fetch_bls_observations", _fake_fetch_bls_observations
    )
    monkeypatch.setattr(
        "fp_wraptr.data.check_mapping._fetch_bea_variable_observations",
        _fake_fetch_bea_variable_observations,
    )

    payload = check_mapping_against_fmdata(
        model_dir=model_dir,
        source_map_path=source_map_path,
        variables=["XF", "XB", "XA", "AFT", "MISSING"],
        periods=4,
        sources=["fred", "bls", "bea"],
    )

    rows = {str(item["variable"]): item for item in payload["rows"]}
    assert sorted(rows.keys()) == ["XA", "XB", "XF"]
    assert rows["XF"]["overlap_count"] == 4
    assert rows["XB"]["overlap_count"] == 4
    assert rows["XA"]["overlap_count"] == 4
    assert rows["XF"]["median_abs_error"] == 0.0
    assert rows["XB"]["median_abs_error"] == 0.0
    assert rows["XA"]["median_abs_error"] == 0.0

    skipped = {(item["variable"], item["reason"]) for item in payload["skipped"]}
    assert ("AFT", "unsupported_frequency") in skipped
    assert ("MISSING", "unmapped") in skipped
