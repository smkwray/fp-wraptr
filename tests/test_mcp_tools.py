"""Tests for MCP tool functions exposed by fp_wraptr.mcp_server."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from fp_wraptr.mcp_server import (
    create_scenario,
    describe_variable,
    describe_variable_sources,
    explain_equation,
    get_run_history,
    list_output_equations,
    list_output_variables,
    list_scenarios,
    parse_fp_output,
    run_batch_scenarios,
    search_dictionary,
    source_map_coverage,
    source_map_quality,
    source_map_report,
    source_map_window_check,
    update_model_from_fred,
    update_scenario,
    validate_scenario,
)

FMOUT = """\
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
Variable   Periods forecast are  2025.4  TO   2025.4

                   2025.4      2025.5
                               2026.1

   1 PCY      P lv   1.0      2.0
             P ch   0.1      0.1
             P %ch  10.0     10.0
"""


def _write_fmout(path: Path) -> None:
    path.write_text(FMOUT, encoding="utf-8")


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
                "construction": "Def., Eq. 82",
            },
            "UR": {
                "name": "UR",
                "description": "",
                "units": "",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 87,
                "used_in_equations": [1, 2],
                "raw_data_sources": [],
                "construction": "Def., Eq. 87",
            },
            "HG": {
                "name": "HG",
                "description": "Hours worked in government.",
                "units": "",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 10,
                "used_in_equations": [82],
                "raw_data_sources": [],
                "construction": "",
            },
        },
        "equations": {
            "82": {
                "id": 82,
                "type": "definitional",
                "sector_block": "nominal",
                "label": "Nominal GDP",
                "lhs_expr": "GDP",
                "rhs_variables": ["HG", "UR"],
                "formula": "HG + UR",
            }
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_dictionary_with_raw_data(path: Path) -> None:
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
                "construction": "Def., Eq. 82",
            },
            "UR": {
                "name": "UR",
                "description": "",
                "units": "",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 87,
                "used_in_equations": [1, 2],
                "raw_data_sources": [],
                "construction": "Def., Eq. 87",
            },
        },
        "equations": {
            "82": {
                "id": 82,
                "type": "definitional",
                "sector_block": "nominal",
                "label": "Nominal GDP",
                "lhs_expr": "GDP",
                "rhs_variables": ["UR"],
                "formula": "UR",
            }
        },
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


def _write_source_map(path: Path) -> None:
    payload = """\
GDP:
  description: Gross domestic product
  source: fred
  series_id: GDP
  frequency: Q
  units: Billions
  transform: level
"""
    path.write_text(payload, encoding="utf-8")


def _write_source_map_with_quality_issue(path: Path) -> None:
    payload = """\
GDP:
  description: Gross domestic product
  source: fred
  frequency: Q
  units: Billions
  transform: level
"""
    path.write_text(payload, encoding="utf-8")


def _write_source_map_with_window(path: Path) -> None:
    payload = """\
CTGB:
  description: Financial stabilization payments
  source: fred
  series_id: BOGZ1FA315410093Q
  frequency: Q
  transform: level
  annual_rate: true
  window_start: 2008Q4
  window_end: 2012Q2
  outside_window_value: 0.0
"""
    path.write_text(payload, encoding="utf-8")


def _expected_query_snapshot() -> dict:
    equation = {
        "id": 82,
        "type": "definitional",
        "sector_block": "nominal",
        "label": "Nominal GDP",
        "lhs_expr": "GDP",
        "rhs_variables": ["HG", "UR"],
        "formula": "HG + UR",
    }
    ur_payload = {
        "name": "UR",
        "description": "Construction note: Def., Eq. 87",
        "units": "",
        "sector": "",
        "category": "endogenous",
        "defined_by_equation": 87,
        "used_in_equations": [1, 2],
        "raw_data_sources": [],
        "construction": "Def., Eq. 87",
        "description_source": "construction",
        "links": {"defining_equation": 87, "used_in_equations": [1, 2]},
    }
    return {
        "query": "UR in equation 82",
        "intent": {
            "kind": "variable_in_equation",
            "raw": "UR in equation 82",
            "variable": "UR",
            "equation_id": 82,
        },
        "focus": {
            "variable_in_equation": {
                "variable": "UR",
                "equation_id": 82,
                "present_in_equation": True,
            }
        },
        "equation_matches": [
            {
                "score": 100,
                "reason": "equation_id_exact",
                "equation": equation,
                "links": {
                    "lhs_variables": ["GDP"],
                    "rhs_variables": ["HG", "UR"],
                    "related_variables": ["GDP", "HG", "UR"],
                },
            }
        ],
        "variable_matches": [
            {
                "score": 95,
                "reason": "variable_code_exact",
                "variable": ur_payload,
            }
        ],
    }


def _expected_explain_snapshot() -> dict:
    equation = {
        "id": 82,
        "type": "definitional",
        "sector_block": "nominal",
        "label": "Nominal GDP",
        "lhs_expr": "GDP",
        "rhs_variables": ["HG", "UR"],
        "formula": "HG + UR",
    }
    return {
        "equation": equation,
        "variables": [
            {
                "name": "GDP",
                "description": "Gross domestic product.",
                "units": "B$",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 82,
                "used_in_equations": [84],
                "raw_data_sources": ["R11"],
                "construction": "Def., Eq. 82",
                "description_source": "dictionary",
                "links": {"defining_equation": 82, "used_in_equations": [84]},
                "role": "lhs",
                "found_in_dictionary": True,
            },
            {
                "name": "HG",
                "description": "Hours worked in government.",
                "units": "",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 10,
                "used_in_equations": [82],
                "raw_data_sources": [],
                "construction": "",
                "description_source": "dictionary",
                "links": {"defining_equation": 10, "used_in_equations": [82]},
                "role": "rhs",
                "found_in_dictionary": True,
            },
            {
                "name": "UR",
                "description": "Construction note: Def., Eq. 87",
                "units": "",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 87,
                "used_in_equations": [1, 2],
                "raw_data_sources": [],
                "construction": "Def., Eq. 87",
                "description_source": "construction",
                "links": {"defining_equation": 87, "used_in_equations": [1, 2]},
                "role": "rhs",
                "found_in_dictionary": True,
            },
        ],
        "cross_links": {
            "equation_id": 82,
            "lhs_variables": ["GDP"],
            "rhs_variables": ["HG", "UR"],
            "variable_to_equations": {
                "GDP": {
                    "role": "lhs",
                    "defined_by_equation": 82,
                    "used_in_equations": [84],
                },
                "HG": {
                    "role": "rhs",
                    "defined_by_equation": 10,
                    "used_in_equations": [82],
                },
                "UR": {
                    "role": "rhs",
                    "defined_by_equation": 87,
                    "used_in_equations": [1, 2],
                },
            },
        },
    }


def test_validate_scenario_valid() -> None:
    payload = json.loads(validate_scenario("name: test\n"))
    assert payload["valid"] is True
    assert payload["name"] == "test"


def test_validate_scenario_invalid() -> None:
    payload = json.loads(validate_scenario("track_variables: not-a-list\n"))
    assert payload.get("valid") is False
    assert "error" in payload


def test_list_scenarios(tmp_path: Path) -> None:
    (tmp_path / "alpha.yaml").write_text("name: alpha\n", encoding="utf-8")
    (tmp_path / "beta.yaml").write_text("name: beta\n", encoding="utf-8")

    payload = json.loads(list_scenarios(str(tmp_path)))
    assert len(payload) == 2
    paths = {item["path"] for item in payload}
    assert str(tmp_path / "alpha.yaml") in paths
    assert str(tmp_path / "beta.yaml") in paths


def test_create_scenario_writes_file(tmp_path: Path) -> None:
    payload = json.loads(
        create_scenario(
            yaml_content="name: created\ntrack_variables: [PCY, UR]\n",
            filename="created_scenario",
            examples_dir=str(tmp_path),
        )
    )

    expected_path = tmp_path / "created_scenario.yaml"
    assert payload["created"] is True
    assert payload["name"] == "created"
    assert Path(payload["path"]) == expected_path
    assert expected_path.exists()


def test_create_scenario_existing_file_returns_error(tmp_path: Path) -> None:
    existing = tmp_path / "existing.yaml"
    existing.write_text("name: existing\n", encoding="utf-8")

    payload = json.loads(
        create_scenario(
            yaml_content="name: created\n",
            filename="existing.yaml",
            examples_dir=str(tmp_path),
        )
    )

    assert payload["created"] is False
    assert "already exists" in payload["error"].lower()


def test_update_scenario_updates_file(tmp_path: Path) -> None:
    scenario_path = tmp_path / "scenario.yaml"
    scenario_path.write_text("name: before\n", encoding="utf-8")

    payload = json.loads(
        update_scenario(
            scenario_path=str(scenario_path),
            yaml_content="name: after\ndescription: updated\n",
        )
    )

    assert payload["updated"] is True
    assert payload["name"] == "after"
    assert "name: after" in scenario_path.read_text(encoding="utf-8")


def test_update_scenario_missing_file_returns_error(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    payload = json.loads(
        update_scenario(
            scenario_path=str(missing),
            yaml_content="name: x\n",
        )
    )

    assert "error" in payload
    assert "not found" in payload["error"].lower()


def test_parse_fp_output_json(tmp_path: Path) -> None:
    fmout = tmp_path / "fmout.txt"
    _write_fmout(fmout)

    payload = json.loads(parse_fp_output(str(fmout), format="json"))
    assert "variables" in payload
    assert "PCY" in payload["variables"]


def test_parse_fp_output_csv(tmp_path: Path) -> None:
    fmout = tmp_path / "fmout.txt"
    _write_fmout(fmout)

    payload = parse_fp_output(str(fmout), format="csv")
    assert "PCY" in payload
    assert "2025.4" in payload


def test_list_output_variables(tmp_path: Path) -> None:
    fmout = tmp_path / "fmout.txt"
    _write_fmout(fmout)

    payload = json.loads(list_output_variables(str(fmout)))
    names = {item["name"] for item in payload["variables"]}
    assert "PCY" in names


def test_list_output_equations(tmp_path: Path) -> None:
    fmout = tmp_path / "fmout.txt"
    _write_fmout(fmout)

    payload = json.loads(list_output_equations(str(fmout)))
    assert "equations" in payload


def test_get_run_history_empty(tmp_path: Path) -> None:
    payload = json.loads(get_run_history(str(tmp_path)))
    assert payload["count"] == 0
    assert payload["runs"] == []


def test_run_batch_scenarios_with_missing_names() -> None:
    payload = json.loads(run_batch_scenarios(["does_not_exist_1", "does_not_exist_2"]))
    assert payload["total"] == 2
    assert payload["succeeded"] == 0
    assert payload["failed"] == 2


def test_describe_variable(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary.json"
    _write_dictionary(dictionary)

    payload = json.loads(describe_variable("GDP", dictionary_path=str(dictionary)))
    assert payload["name"] == "GDP"
    assert payload["defined_by_equation"] == 82


def test_search_dictionary(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary.json"
    _write_dictionary(dictionary)

    payload = json.loads(search_dictionary("eq 82", dictionary_path=str(dictionary)))
    assert payload["equation_matches"][0]["equation"]["id"] == 82


def test_search_dictionary_compact_intent(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary.json"
    _write_dictionary(dictionary)

    payload = json.loads(search_dictionary("UR in equation 82", dictionary_path=str(dictionary)))
    assert payload["intent"]["kind"] == "variable_in_equation"
    assert payload["focus"]["variable_in_equation"]["present_in_equation"] is True


def test_search_dictionary_variables_in_equation_intent(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary.json"
    _write_dictionary(dictionary)

    payload = json.loads(
        search_dictionary("variables in equation 82", dictionary_path=str(dictionary))
    )
    assert payload["intent"]["kind"] == "equation_variables_lookup"
    assert payload["focus"]["equation_variables_lookup"]["equation_id"] == 82
    assert any(match["reason"] == "equation_variable" for match in payload["variable_matches"])


def test_search_dictionary_meaning_in_equation_intent(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary.json"
    _write_dictionary(dictionary)

    payload = json.loads(
        search_dictionary("meaning of UR in equation 82", dictionary_path=str(dictionary))
    )
    assert payload["intent"]["kind"] == "variable_meaning_in_equation"
    assert payload["focus"]["variable_meaning_in_equation"]["equation_id"] == 82


def test_search_dictionary_snapshot(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary.json"
    _write_dictionary(dictionary)

    payload = json.loads(search_dictionary("UR in equation 82", dictionary_path=str(dictionary)))
    assert payload == _expected_query_snapshot()


def test_explain_equation(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary.json"
    _write_dictionary(dictionary)

    payload = json.loads(explain_equation(82, dictionary_path=str(dictionary)))
    assert payload["equation"]["id"] == 82
    names = {item["name"] for item in payload["variables"]}
    assert {"GDP", "HG", "UR"} <= names


def test_explain_equation_snapshot(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary.json"
    _write_dictionary(dictionary)

    payload = json.loads(explain_equation(82, dictionary_path=str(dictionary)))
    assert payload == _expected_explain_snapshot()


def test_describe_variable_sources(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary_raw.json"
    source_map = tmp_path / "source_map.yaml"
    _write_dictionary_with_raw_data(dictionary)
    _write_source_map(source_map)

    payload = json.loads(
        describe_variable_sources(
            "GDP",
            dictionary_path=str(dictionary),
            source_map_path=str(source_map),
        )
    )
    assert payload["variable"] == "GDP"
    assert payload["mapping_status"] == "mapped"
    assert payload["source_map_entry"]["source"] == "fred"
    assert payload["normalization"] is None
    assert payload["dictionary_raw_data_sources"] == ["R11"]
    assert payload["dictionary_raw_data_details"][0]["r_number"] == "R11"


def test_source_map_coverage(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary_raw.json"
    source_map = tmp_path / "source_map.yaml"
    _write_dictionary_with_raw_data(dictionary)
    _write_source_map(source_map)

    payload = json.loads(
        source_map_coverage(
            dictionary_path=str(dictionary),
            source_map_path=str(source_map),
            only_with_raw_data=True,
        )
    )
    assert payload["scope"] == "variables_with_raw_data"
    assert payload["population_count"] == 1
    assert payload["mapped_count"] == 1
    assert payload["missing_count"] == 0


def test_source_map_quality(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary_raw.json"
    source_map = tmp_path / "source_map_bad.yaml"
    _write_dictionary_with_raw_data(dictionary)
    _write_source_map_with_quality_issue(source_map)

    payload = json.loads(
        source_map_quality(
            dictionary_path=str(dictionary),
            source_map_path=str(source_map),
        )
    )
    assert payload["scope"] == "all_dictionary_variables"
    assert payload["issue_count"] >= 1
    assert payload["issue_breakdown"]["missing_series_id"] >= 1


def test_source_map_report(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary_raw.json"
    source_map = tmp_path / "source_map.yaml"
    _write_dictionary_with_raw_data(dictionary)
    _write_source_map(source_map)

    payload = json.loads(
        source_map_report(
            dictionary_path=str(dictionary),
            source_map_path=str(source_map),
        )
    )
    assert payload["dictionary_variable_count"] == 2
    assert payload["source_map_variable_count"] == 1
    assert payload["coverage_with_raw_data"]["mapped_count"] == 1
    assert "quality_all" in payload


def test_source_map_window_check(tmp_path: Path, monkeypatch) -> None:
    source_map = tmp_path / "source_map_window.yaml"
    _write_source_map_with_window(source_map)

    def _fake_fetch_series(series_ids, start=None, end=None, cache_dir=None):
        assert series_ids == ["BOGZ1FA315410093Q"]
        return pd.DataFrame(
            {"BOGZ1FA315410093Q": [0.0, 10.0, 0.0, 0.0]},
            index=pd.to_datetime(["2008-07-01", "2008-10-01", "2012-04-01", "2012-07-01"]),
        )

    monkeypatch.setattr("fp_wraptr.fred.ingest.fetch_series", _fake_fetch_series)

    payload = json.loads(source_map_window_check(source_map_path=str(source_map)))
    assert payload["series_checked"] == 1
    assert payload["violation_count"] == 0
    assert payload["checks"][0]["status"] == "ok"


def test_update_model_from_fred_tool_success(monkeypatch, tmp_path: Path) -> None:
    from fp_wraptr.data.update_fred import DataUpdateResult

    out_dir = tmp_path / "model_updates" / "demo"
    bundle_dir = out_dir / "FM"
    fmdata_path = bundle_dir / "fmdata.txt"
    report_path = out_dir / "data_update_report.json"

    def _fake_find_spec(name: str, *_args, **_kwargs):
        if name == "fredapi":
            return object()
        return None

    def _fake_update_model_from_fred(**_kwargs):
        bundle_dir.mkdir(parents=True, exist_ok=True)
        fmdata_path.write_text("SMPL 2025.1 2025.4;\n", encoding="utf-8")
        report_path.write_text(json.dumps({"ok": True}), encoding="utf-8")
        return DataUpdateResult(
            out_dir=out_dir,
            model_bundle_dir=bundle_dir,
            fmdata_path=fmdata_path,
            report_path=report_path,
            report={
                "selected_variable_count": 1,
                "normalized_variable_count": 1,
                "fmdata_merge": {"updated_cells": 4, "carried_cells": 0},
            },
        )

    monkeypatch.setattr("fp_wraptr.mcp_server.importlib.util.find_spec", _fake_find_spec)
    monkeypatch.setattr(
        "fp_wraptr.data.update_fred.update_model_from_fred",
        _fake_update_model_from_fred,
    )

    payload = json.loads(
        update_model_from_fred(
            model_dir="FM",
            out_dir=str(out_dir),
            end_period="2025.4",
            variables="UR",
        )
    )

    assert payload["success"] is True
    assert Path(payload["bundle_dir"]) == bundle_dir
    assert Path(payload["fmdata_path"]) == fmdata_path
    assert Path(payload["report_path"]) == report_path
    assert payload["report"]["selected_variable_count"] == 1
    assert fmdata_path.exists()
