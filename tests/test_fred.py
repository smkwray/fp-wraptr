"""Tests for FRED ingestion helpers and CLI commands."""

from __future__ import annotations

import json
import os
import sys
import time
import types
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from fp_wraptr.cli import app
from fp_wraptr.data import load_source_map
from fp_wraptr.fred import ingest
from fp_wraptr.fred.overlay import _period_to_date, build_overlay_data, load_fred_mapping
from fp_wraptr.io.parser import ForecastVariable, FPOutputData

runner = CliRunner()


def test_get_fred_client_missing_key(monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)

    with pytest.raises(ValueError, match="FRED_API_KEY environment variable not set"):
        ingest.get_fred_client()


def test_fetch_series_uses_cache(tmp_path, monkeypatch):
    cache_dir = tmp_path / "fred-cache"
    cache_dir.mkdir()
    cache_file = cache_dir / "GDP.json"
    cache_file.write_text(
        json.dumps({
            "fetched_at": "2026-02-23T00:00:00+00:00",
            "series_id": "GDP",
            "data": {"2026-02-23": 2.0},
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "fp_wraptr.fred.ingest.get_fred_client",
        lambda: pytest.fail("get_fred_client should not be called for fresh cache"),
    )

    df = ingest.fetch_series(["GDP"], cache_dir=cache_dir)
    assert list(df.columns) == ["GDP"]
    assert df.loc["2026-02-23", "GDP"] == 2.0


def test_clear_cache(tmp_path):
    cache_dir = tmp_path / "fred-cache"
    cache_dir.mkdir()
    (cache_dir / "GDP.json").write_text("{}", encoding="utf-8")
    (cache_dir / "CPI.json").write_text("{}", encoding="utf-8")
    (cache_dir / "README.txt").write_text("ignore", encoding="utf-8")

    deleted = ingest.clear_cache(cache_dir=cache_dir)

    assert deleted == 2
    assert not (cache_dir / "GDP.json").exists()
    assert not (cache_dir / "CPI.json").exists()


def test_fred_cli_without_fredapi(monkeypatch):
    monkeypatch.setattr(
        "fp_wraptr.cli.importlib.util.find_spec",
        lambda *_args, **_kwargs: None,
    )
    result = runner.invoke(app, ["fred", "fetch", "GDP"])

    assert result.exit_code == 1
    output = (result.stdout + result.stderr).lower()
    assert "fredapi is required" in output


def test_get_fred_client_with_key(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "test_key")

    fake_module = types.ModuleType("fredapi")
    called_with = {}

    class FakeFred:
        def __init__(self, api_key):
            called_with["api_key"] = api_key

    fake_module.Fred = FakeFred
    monkeypatch.setitem(sys.modules, "fredapi", fake_module)

    client = ingest.get_fred_client()

    assert isinstance(client, FakeFred)
    assert called_with["api_key"] == "test_key"


def test_cache_is_fresh_stale_file(tmp_path):
    cache_file = tmp_path / "stale.json"
    cache_file.write_text("{}")

    stale_timestamp = time.time() - (25 * 60 * 60)
    os.utime(cache_file, (stale_timestamp, stale_timestamp))

    assert not ingest._cache_is_fresh(cache_file)


def test_load_cached_series_wrong_id(tmp_path):
    path = tmp_path / "GDP.json"
    path.write_text(
        json.dumps(
            {"series_id": "GDP", "data": {"2026-02-23": 2.0}},
        ),
        encoding="utf-8",
    )

    assert ingest._load_cached_series(path, "CPI") is None


def test_load_cached_series_bad_data(tmp_path):
    path = tmp_path / "GDP.json"
    path.write_text(json.dumps({"series_id": "GDP", "data": "not a dict"}), encoding="utf-8")

    assert ingest._load_cached_series(path, "GDP") is None


def test_fetch_series_api_call(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    fake_module = types.ModuleType("fredapi")

    class FakeFred:
        def __init__(self, api_key):
            pass

        def get_series(self, series_id, observation_start=None, observation_end=None):
            return pd.Series(
                {
                    "2026-02-23": 2.0,
                    "2026-03-01": 2.5,
                },
                dtype=float,
            )

    fake_module.Fred = FakeFred
    monkeypatch.setitem(sys.modules, "fredapi", fake_module)
    monkeypatch.setenv("FRED_API_KEY", "test_key")

    df = ingest.fetch_series(["GDP"], cache_dir=cache_dir)

    assert list(df.columns) == ["GDP"]
    assert df.loc[pd.to_datetime("2026-02-23"), "GDP"] == 2.0
    assert (cache_dir / "GDP.json").exists()
    payload = json.loads((cache_dir / "GDP.json").read_text(encoding="utf-8"))
    assert payload["source"] == "FRED"
    assert "terms_of_use" in payload["terms_url"]


def test_fetch_series_reuses_single_client_for_multiple_series(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    calls = {"get_client": 0}

    class FakeFred:
        def get_series(self, series_id, observation_start=None, observation_end=None):
            return pd.Series({"2026-02-23": 1.0}, dtype=float)

    def _fake_get_fred_client():
        calls["get_client"] += 1
        return FakeFred()

    monkeypatch.setattr("fp_wraptr.fred.ingest.get_fred_client", _fake_get_fred_client)

    df = ingest.fetch_series(["GDP", "CPIAUCSL"], cache_dir=cache_dir)

    assert list(df.columns) == ["GDP", "CPIAUCSL"]
    assert calls["get_client"] == 1


def test_fetch_series_cache_range_miss_refetches(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cache_file = cache_dir / "GDP.json"
    cache_file.write_text(
        json.dumps({
            "fetched_at": "2026-02-23T00:00:00+00:00",
            "series_id": "GDP",
            "observation_start": "2026-01-01",
            "observation_end": "2026-01-31",
            "data": {"2026-01-15": 1.0},
        }),
        encoding="utf-8",
    )

    class FakeFred:
        def get_series(self, series_id, observation_start=None, observation_end=None):
            return pd.Series({"2026-02-01": 2.0}, dtype=float)

    monkeypatch.setattr("fp_wraptr.fred.ingest.get_fred_client", lambda: FakeFred())

    df = ingest.fetch_series(
        ["GDP"],
        start="2026-02-01",
        end="2026-02-28",
        cache_dir=cache_dir,
    )
    assert df.loc[pd.to_datetime("2026-02-01"), "GDP"] == 2.0


def test_fetch_series_cache_range_hit_filters_locally(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cache_file = cache_dir / "GDP.json"
    cache_file.write_text(
        json.dumps({
            "fetched_at": "2026-02-23T00:00:00+00:00",
            "series_id": "GDP",
            "observation_start": "2026-01-01",
            "observation_end": "2026-03-31",
            "data": {
                "2026-01-01": 1.0,
                "2026-02-01": 2.0,
                "2026-03-01": 3.0,
            },
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "fp_wraptr.fred.ingest.get_fred_client",
        lambda: pytest.fail("get_fred_client should not be called for covered cache range"),
    )

    df = ingest.fetch_series(
        ["GDP"],
        start="2026-02-01",
        end="2026-02-28",
        cache_dir=cache_dir,
    )
    assert list(df.index) == [pd.Timestamp("2026-02-01")]
    assert df.iloc[0, 0] == 2.0


def test_fetch_series_respect_tos_throttles_calls(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    sleeps: list[float] = []

    class FakeFred:
        def get_series(self, series_id, observation_start=None, observation_end=None):
            return pd.Series({"2026-02-23": 1.0}, dtype=float)

    monkeypatch.setattr("fp_wraptr.fred.ingest.get_fred_client", lambda: FakeFred())
    monkeypatch.setattr("fp_wraptr.fred.ingest.time.sleep", lambda seconds: sleeps.append(seconds))

    ingest.fetch_series(
        ["GDP", "CPIAUCSL"],
        cache_dir=cache_dir,
        min_request_interval_seconds=5.0,
        respect_tos=True,
    )

    assert len(sleeps) == 1
    assert sleeps[0] > 0


def test_fetch_series_empty_ids(tmp_path):
    df = ingest.fetch_series([], cache_dir=tmp_path / "cache")

    assert df.empty
    assert df.shape == (0, 0)


def test_clear_cache_no_dir():
    assert ingest.clear_cache(Path("/nonexistent/fp-wraptr-fred-cache")) == 0


def test_load_fred_mapping_default():
    mapping = load_fred_mapping()

    assert mapping["GDPR"] == "GDPC1"


def test_load_fred_mapping_from_yaml(tmp_path):
    path = tmp_path / "overlay_mapping.yaml"
    path.write_text("CUSTOM: CSTM\nGDP: GDP\n", encoding="utf-8")

    mapping = load_fred_mapping(path)

    assert mapping["CUSTOM"] == "CSTM"
    assert mapping["GDP"] == "GDP"


def test_load_fred_mapping_missing_file():
    mapping = load_fred_mapping(Path("/nonexistent/mapping.yaml"))

    assert mapping["GDPR"] == "GDPC1"


def test_period_to_date():
    assert _period_to_date("2025.4") == pd.Timestamp("2025-10-01")
    assert _period_to_date("2026.1") == pd.Timestamp("2026-01-01")


def test_build_overlay_data_empty_forecast():
    data = FPOutputData(periods=[])

    result = build_overlay_data(data, fred_mapping={"X": "Y"})

    assert result == {}


def test_build_overlay_data_quarterly_annual_rate_uses_div4(tmp_path, monkeypatch):
    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        """\
GDP:
  description: Gross domestic product
  source: fred
  series_id: GDP
  frequency: Q
  units: Billions of dollars, SAAR
  transform: level
  annual_rate: true
""",
        encoding="utf-8",
    )
    source_map = load_source_map(source_map_path)

    forecast = FPOutputData(
        periods=["2026.1"],
        variables={"GDP": ForecastVariable(var_id=1, name="GDP", levels=[100.0])},
    )
    fred_df = pd.DataFrame(
        {"GDP": [120.0]},
        index=pd.to_datetime(["2026-01-01"]),
    )
    monkeypatch.setattr("fp_wraptr.fred.overlay.fetch_series", lambda *args, **kwargs: fred_df)

    result = build_overlay_data(
        forecast,
        fred_mapping={"GDP": "GDP"},
        source_map=source_map,
    )

    assert result["GDP"].loc[0, "actual"] == pytest.approx(30.0)


def test_build_overlay_data_monthly_annual_rate_uses_div12_then_sum(tmp_path, monkeypatch):
    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        """\
PCS:
  description: Services consumption
  source: fred
  series_id: PCESV
  frequency: M
  units: Billions of dollars, annual rate
  transform: level
  annual_rate: true
""",
        encoding="utf-8",
    )
    source_map = load_source_map(source_map_path)

    forecast = FPOutputData(
        periods=["2026.1"],
        variables={"PCS": ForecastVariable(var_id=1, name="PCS", levels=[100.0])},
    )
    fred_df = pd.DataFrame(
        {"PCESV": [120.0, 120.0, 120.0]},
        index=pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01"]),
    )
    monkeypatch.setattr("fp_wraptr.fred.overlay.fetch_series", lambda *args, **kwargs: fred_df)

    result = build_overlay_data(
        forecast,
        fred_mapping={"PCS": "PCESV"},
        source_map=source_map,
    )

    assert result["PCS"].loc[0, "actual"] == pytest.approx(30.0)


def test_build_overlay_data_applies_window_and_zero_fill_for_ctgb(tmp_path, monkeypatch):
    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        """\
CTGB:
  description: Financial stabilization payments
  source: fred
  series_id: BOGZ1FA315410093Q
  frequency: Q
  units: Millions of dollars, SAAR
  transform: level
  annual_rate: true
  window_start: 2008Q4
  window_end: 2012Q2
  outside_window_value: 0.0
""",
        encoding="utf-8",
    )
    source_map = load_source_map(source_map_path)

    forecast = FPOutputData(
        periods=["2008.3", "2008.4", "2012.2", "2012.3"],
        variables={"CTGB": ForecastVariable(var_id=1, name="CTGB", levels=[0.0, 0.0, 0.0, 0.0])},
    )
    fred_df = pd.DataFrame(
        {"BOGZ1FA315410093Q": [16.0, 20.0, 24.0, 28.0]},
        index=pd.to_datetime(["2008-07-01", "2008-10-01", "2012-04-01", "2012-07-01"]),
    )
    monkeypatch.setattr("fp_wraptr.fred.overlay.fetch_series", lambda *args, **kwargs: fred_df)

    result = build_overlay_data(
        forecast,
        fred_mapping={"CTGB": "BOGZ1FA315410093Q"},
        source_map=source_map,
    )
    actuals = result["CTGB"]["actual"].tolist()

    assert actuals[0] == pytest.approx(0.0)
    assert actuals[1] == pytest.approx(5.0)
    assert actuals[2] == pytest.approx(6.0)
    assert actuals[3] == pytest.approx(0.0)


def test_build_overlay_data_window_end_includes_full_quarter_for_monthly_series(
    tmp_path, monkeypatch
):
    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        """\
MVAR:
  description: Monthly windowed sample
  source: fred
  series_id: MVAR
  frequency: M
  units: Index
  transform: level
  annual_rate: false
  window_end: 2026Q1
""",
        encoding="utf-8",
    )
    source_map = load_source_map(source_map_path)

    forecast = FPOutputData(
        periods=["2026.1", "2026.2"],
        variables={"MVAR": ForecastVariable(var_id=1, name="MVAR", levels=[0.0, 0.0])},
    )
    fred_df = pd.DataFrame(
        {"MVAR": [1.0, 2.0, 3.0, 10.0]},
        index=pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01", "2026-04-01"]),
    )
    monkeypatch.setattr("fp_wraptr.fred.overlay.fetch_series", lambda *args, **kwargs: fred_df)

    result = build_overlay_data(
        forecast,
        fred_mapping={"MVAR": "MVAR"},
        source_map=source_map,
    )

    assert result["MVAR"].loc[0, "actual"] == pytest.approx(2.0)
    assert pd.isna(result["MVAR"].loc[1, "actual"])
