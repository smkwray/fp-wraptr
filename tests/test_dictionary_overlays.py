"""Tests for dictionary overlay/extension merge behavior."""

from __future__ import annotations

import json
from pathlib import Path

from fp_wraptr.data.dictionary_overlays import load_dictionary_with_overlays


def _write_base_dictionary(path: Path) -> None:
    payload = {
        "model_version": "2025-12-23",
        "source": {"pabapa_md": "fixture", "extraction_timestamp": "2026-03-06T00:00:00Z"},
        "variables": {
            "GDP": {
                "name": "GDP",
                "description": "Gross domestic product.",
                "short_name": "",
                "units": "B$",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 82,
                "used_in_equations": [84],
                "raw_data_sources": [],
                "construction": "",
            },
            "UR": {
                "name": "UR",
                "description": "Unemployment rate.",
                "short_name": "",
                "units": "%",
                "sector": "",
                "category": "endogenous",
                "defined_by_equation": 87,
                "used_in_equations": [1, 2],
                "raw_data_sources": [],
                "construction": "",
            },
        },
        "equations": {},
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_dictionary_overlays_inherit_unmodified_stock_definitions(tmp_path: Path) -> None:
    base_path = tmp_path / "dictionary.json"
    _write_base_dictionary(base_path)

    scenario_overlay = tmp_path / "scenario_overlay.json"
    scenario_overlay.write_text(
        json.dumps({"variables": {"GDP": {"description": "Scenario-specific GDP definition."}}}, indent=2),
        encoding="utf-8",
    )

    merged = load_dictionary_with_overlays(base_path=base_path, overlay_paths=[scenario_overlay])

    gdp = merged.get_variable("GDP")
    ur = merged.get_variable("UR")
    assert gdp is not None
    assert ur is not None
    assert gdp.description == "Scenario-specific GDP definition."
    assert ur.description == "Unemployment rate."


def test_dictionary_shared_extension_then_scenario_override_precedence(tmp_path: Path) -> None:
    base_path = tmp_path / "dictionary.json"
    _write_base_dictionary(base_path)

    shared_extension = tmp_path / "shared_extension.json"
    shared_extension.write_text(
        json.dumps(
            {
                "variables": {
                    "GDP": {"short_name": "Real activity"},
                    "JGJ": {
                        "description": "Public jobs guarantee stock.",
                        "units": "persons",
                        "short_name": "JGs",
                    },
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    scenario_overlay = tmp_path / "scenario_overlay.json"
    scenario_overlay.write_text(
        json.dumps(
            {
                "variables": {
                    "GDP": {
                        "description": "Scenario GDP definition.",
                        "units": "index",
                    }
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    merged = load_dictionary_with_overlays(
        base_path=base_path,
        overlay_paths=[shared_extension, scenario_overlay],
    )

    gdp = merged.get_variable("GDP")
    jgj = merged.get_variable("JGJ")
    assert gdp is not None
    assert jgj is not None
    assert gdp.short_name == "Real activity"
    assert gdp.description == "Scenario GDP definition."
    assert gdp.units == "index"
    assert jgj.description == "Public jobs guarantee stock."
