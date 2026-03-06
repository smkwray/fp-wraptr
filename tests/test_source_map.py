"""Tests for source map loading and coverage helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from fp_wraptr.data import ModelDictionary
from fp_wraptr.data.source_map import load_source_map


def _write_source_map(path: Path) -> None:
    path.write_text(
        """\
GDP:
  description: Gross domestic product
  source: fred
  series_id: GDP
  frequency: Q
  units: Billions
  transform: level
UR:
  description: Unemployment rate
  source: fred
  series_id: UNRATE
  frequency: M
  units: Percent
  transform: level
""",
        encoding="utf-8",
    )


def _write_dictionary(path: Path) -> None:
    payload = {
        "model_version": "2025-12-23",
        "source": {"pabapa_md": "fixture", "extraction_timestamp": "2026-02-23T00:00:00Z"},
        "variables": {
            "GDP": {
                "name": "GDP",
                "description": "Gross domestic product.",
                "units": "B$",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 82,
                "used_in_equations": [84],
                "raw_data_sources": ["R11"],
                "construction": "",
            },
            "UR": {
                "name": "UR",
                "description": "Unemployment rate.",
                "units": "%",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 87,
                "used_in_equations": [1],
                "raw_data_sources": [],
                "construction": "",
            },
            "ZZ": {
                "name": "ZZ",
                "description": "",
                "units": "",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": None,
                "used_in_equations": [],
                "raw_data_sources": ["R999"],
                "construction": "",
            },
        },
        "equations": {},
        "raw_data": {
            "R11": {
                "r_number": "R11",
                "variable": "GDP",
                "source_type": "NIPA",
                "table": "1.1.5",
                "line": "1",
                "description": "Gross domestic product",
            }
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_load_source_map_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    with pytest.raises(FileNotFoundError):
        load_source_map(missing)


def test_source_map_coverage_report(tmp_path: Path) -> None:
    source_map_path = tmp_path / "source_map.yaml"
    _write_source_map(source_map_path)
    source_map = load_source_map(source_map_path)

    report = source_map.coverage_report(["GDP", "UR", "ZZ"])
    assert report["population_count"] == 3
    assert report["mapped_count"] == 2
    assert report["missing_count"] == 1
    assert report["coverage_pct"] == pytest.approx(66.67)
    assert report["missing_variables"] == ["ZZ"]


def test_source_map_resolve_variable_sources_merges_dictionary_context(tmp_path: Path) -> None:
    source_map_path = tmp_path / "source_map.yaml"
    dictionary_path = tmp_path / "dictionary.json"
    _write_source_map(source_map_path)
    _write_dictionary(dictionary_path)

    source_map = load_source_map(source_map_path)
    dictionary = ModelDictionary.load(dictionary_path)

    payload = source_map.resolve_variable_sources("GDP", dictionary=dictionary)
    assert payload["mapping_status"] == "mapped"
    assert payload["source_map_entry"]["series_id"] == "GDP"
    assert payload["source_map_entry"]["annual_rate"] is False
    assert payload["normalization"] is None
    assert payload["dictionary_raw_data_sources"] == ["R11"]
    assert payload["dictionary_raw_data_details"][0]["r_number"] == "R11"


def test_source_map_resolve_unmapped_variable_keeps_dictionary_raw_data(tmp_path: Path) -> None:
    source_map_path = tmp_path / "source_map.yaml"
    dictionary_path = tmp_path / "dictionary.json"
    _write_source_map(source_map_path)
    _write_dictionary(dictionary_path)

    source_map = load_source_map(source_map_path)
    dictionary = ModelDictionary.load(dictionary_path)

    payload = source_map.resolve_variable_sources("ZZ", dictionary=dictionary)
    assert payload["mapping_status"] == "unmapped"
    assert payload["source_map_entry"] is None
    assert payload["dictionary_raw_data_sources"] == ["R999"]
    assert payload["dictionary_raw_data_details"][0]["r_number"] == "R999"


def test_source_map_quality_report(tmp_path: Path) -> None:
    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        """\
GDP:
  description: Gross domestic product
  source: fred
  series_id: GDP
  frequency: Q
  transform: level
BADFRED:
  description: Missing fred series
  source: fred
  frequency: Q
  transform: level
BADBEA:
  description: Missing bea locator
  source: bea
  frequency: Q
  transform: level
BADSRC:
  description: Unknown source
  source: mystery
  frequency: Z
  transform: level
""",
        encoding="utf-8",
    )
    source_map = load_source_map(source_map_path)

    report = source_map.quality_report()
    assert report["population_count"] == 4
    assert report["issue_count"] == 3
    breakdown = report["issue_breakdown"]
    assert breakdown["missing_series_id"] == 1
    assert breakdown["missing_bea_locator"] == 1
    assert breakdown["invalid_source"] == 1
    assert breakdown["invalid_frequency"] == 1


def test_source_map_loads_annual_rate_flag(tmp_path: Path) -> None:
    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        """\
GDP:
  description: Gross domestic product
  source: fred
  series_id: GDP
  frequency: Q
  units: Billions SAAR
  transform: level
  annual_rate: true
""",
        encoding="utf-8",
    )
    source_map = load_source_map(source_map_path)
    entry = source_map.get("GDP")
    assert entry is not None
    assert entry.annual_rate is True


def test_source_map_monthly_annual_rate_normalization_guidance(tmp_path: Path) -> None:
    source_map_path = tmp_path / "source_map.yaml"
    source_map_path.write_text(
        """\
PCS:
  description: Services consumption proxy
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
    payload = source_map.resolve_variable_sources("PCS")

    normalization = payload["normalization"]
    assert normalization is not None
    assert normalization["annual_rate_divisor"] == 12
    assert normalization["per_period_formula"] == "value / 12"
    assert "sum(monthly_value / 12)" in normalization["quarterly_flow_formula"]


def test_source_map_loads_window_metadata(tmp_path: Path) -> None:
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
    entry = source_map.get("CTGB")

    assert entry is not None
    assert entry.window_start == "2008Q4"
    assert entry.window_end == "2012Q2"
    assert entry.outside_window_value == pytest.approx(0.0)


def test_source_map_window_assumption_report_flags_outside_window_deviation(
    tmp_path: Path,
) -> None:
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
    observations = pd.DataFrame(
        {"BOGZ1FA315410093Q": [0.0, 10.0, 0.0, 2.0]},
        index=pd.to_datetime(["2008-07-01", "2008-10-01", "2012-04-01", "2012-07-01"]),
    )

    report = source_map.window_assumption_report(observations, tolerance=0.0)

    assert report["series_checked"] == 1
    assert report["violation_count"] == 1
    assert report["status_breakdown"]["violation"] == 1
    check = report["checks"][0]
    assert check["variable"] == "CTGB"
    assert check["outside_points"] == 2
    assert check["outside_violations"] == 1
    assert check["first_violation_date"] == "2012-07-01"


def test_source_map_window_assumption_report_ok_when_outside_matches_fill(tmp_path: Path) -> None:
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
    observations = pd.DataFrame(
        {"BOGZ1FA315410093Q": [0.0, 10.0, 0.0, 0.0]},
        index=pd.to_datetime(["2008-07-01", "2008-10-01", "2012-04-01", "2012-07-01"]),
    )

    report = source_map.window_assumption_report(observations, tolerance=0.0)

    assert report["series_checked"] == 1
    assert report["violation_count"] == 0
    assert report["status_breakdown"]["ok"] == 1
    check = report["checks"][0]
    assert check["outside_points"] == 2
    assert check["outside_violations"] == 0
