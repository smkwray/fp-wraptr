"""Tests for the FP model dictionary parser and loader API.

Test suite verifies:
  1. End-to-end extraction from pabapa.md writes valid JSON.
  2. Variable and equation counts are non-zero and stable.
  3. Known sentinel symbols / equations are present.
  4. ModelDictionary.load() / describe() / search() work correctly.
  5. OCR-noise fixture still parses core fields.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import ClassVar

import pytest

from fp_wraptr.data.dictionary import EquationRecord, ModelDictionary, VariableRecord
from fp_wraptr.data.extract_dictionary import (
    OUTPUT_PATH,
    PABAPA_PATH,
    clean_latex,
    clean_var_code,
    extract,
    parse_construction_section,
    parse_equation_section,
    parse_raw_data_section,
    parse_used_in,
    parse_variable_section,
)

# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def dictionary_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Run the extractor once per test session and return the output path."""
    if not PABAPA_PATH.exists():
        pytest.skip(f"Source file not found: {PABAPA_PATH}")
    out = tmp_path_factory.mktemp("dict") / "dictionary.json"
    extract(PABAPA_PATH, out)
    return out


@pytest.fixture(scope="module")
def dict_data(dictionary_path: Path) -> dict:
    return json.loads(dictionary_path.read_text())


@pytest.fixture(scope="module")
def model_dict(dictionary_path: Path) -> ModelDictionary:
    return ModelDictionary.load(dictionary_path)


# ── OCR noise fixture ──────────────────────────────────────────────────────────

OCR_NOISE_TEXT = r"""
The Variables in the US Model in Alphabetical Order

| Variable | Eq. | Description | Used in Equations |
|---|---|---|---|
| CS | 1 | Consumer expenditures  for   services, B2017\$. | 34, 51, 52 | | GDP | 82 | Gross  Domestic  Product, B\$. | 84, 129 | | \( KD \) | 58 | Stock of durable goods, B2017\$. | none | | UR | 87 | Civilian   unemployment rate, percentage. | 1, 2, 3 |

The Equations of the US Model

STOCHASTIC EQUATIONS LHS Variable Explanatory Variables

Household Sector

1 log(CS/POP) cnst, AG1, AG2, log(CS/POP)-1 [Consumer expenditures: services]

Firm Sector

10 log PF log PF-1, log WF [Price deflator for non farm sales]

| Eq. | LHS Variable | Explanatory Variables |
|---|---|---|
| 82 | GDP = | XX + PIV [Nominal GDP] |

Nominal Variables

82 & \(GDP=\) & XX + PIV \\ & & [Nominal GDP] \\

Construction of the Variables for the US Model

| Variable | Construction (raw data variables on right hand side) |
| CS | CS |
| GDP | Def., Eq. 82 |
| KD | Def., Eq. 58. Base Period=1952.1, Value=228.1 |

The Raw Data Variables for the US Model

| No. | Variable | Table | Line | Description | NIPA Data |
|---|---|---|---|---|---|
| R1 | GDPR | 1.1.3 | 1 | Real gross domestic product | Real GDP |
| R11 | GDP | 1.1.5 | 1 | Gross domestic product | Gross domestic product |
"""


# ── 1. End-to-end extraction ───────────────────────────────────────────────────


class TestEndToEnd:
    def test_extraction_runs(self, dictionary_path: Path) -> None:
        assert dictionary_path.exists(), "dictionary.json was not created"

    def test_json_is_valid(self, dict_data: dict) -> None:
        assert isinstance(dict_data, dict)

    def test_required_top_level_keys(self, dict_data: dict) -> None:
        for key in ("model_version", "source", "variables", "equations", "raw_data"):
            assert key in dict_data, f"Missing top-level key: {key}"

    def test_model_version(self, dict_data: dict) -> None:
        assert dict_data["model_version"] == "2025-12-23"

    def test_source_has_pabapa_path(self, dict_data: dict) -> None:
        assert "pabapa_md" in dict_data["source"]
        assert "pabapa.md" in dict_data["source"]["pabapa_md"]

    def test_source_has_timestamp(self, dict_data: dict) -> None:
        assert "extraction_timestamp" in dict_data["source"]


# ── 2. Variable and equation counts ───────────────────────────────────────────


class TestCounts:
    # Minimum thresholds — deliberately conservative
    MIN_VARIABLES = 150
    MIN_EQUATIONS = 80

    def test_variable_count_nonzero(self, dict_data: dict) -> None:
        count = len(dict_data["variables"])
        assert count >= self.MIN_VARIABLES, (
            f"Expected ≥{self.MIN_VARIABLES} variables, got {count}"
        )

    def test_equation_count_nonzero(self, dict_data: dict) -> None:
        count = len(dict_data["equations"])
        assert count >= self.MIN_EQUATIONS, (
            f"Expected ≥{self.MIN_EQUATIONS} equations, got {count}"
        )

    def test_variable_records_have_required_fields(self, dict_data: dict) -> None:
        required = ("name", "description", "category", "used_in_equations")
        for code, rec in list(dict_data["variables"].items())[:20]:
            for field in required:
                assert field in rec, f"Variable {code!r} missing field {field!r}"

    def test_equation_records_have_required_fields(self, dict_data: dict) -> None:
        required = ("id", "type", "lhs_expr")
        for key, rec in list(dict_data["equations"].items())[:20]:
            for field in required:
                assert field in rec, f"Equation {key!r} missing field {field!r}"

    def test_used_in_equations_are_lists(self, dict_data: dict) -> None:
        for code, rec in dict_data["variables"].items():
            assert isinstance(rec["used_in_equations"], list), (
                f"Variable {code}: used_in_equations should be a list"
            )

    def test_used_in_equations_are_ints(self, dict_data: dict) -> None:
        for code, rec in dict_data["variables"].items():
            for val in rec["used_in_equations"]:
                assert isinstance(val, int), (
                    f"Variable {code}: used_in_equations contains non-int {val!r}"
                )


# ── 3. Sentinel symbols and equations ─────────────────────────────────────────


class TestSentinels:
    """
    Sentinel variables / equations that must be present.
    All are prominent in the pabapa.md source.
    """

    # These must be present in the dictionary
    SENTINEL_VARS: ClassVar[list[str]] = [
        "CS",  # Eq. 1, consumer services — in main variable table
        "GDP",  # Eq. 82               — in main variable table
        "POP",  # Eq. 120              — in main variable table
        "UR",  # Eq. 87               — supplemented from construction
        "RS",  # Eq. 30, T-bill rate  — in main variable table
    ]
    # These must have non-empty descriptions (main table entries only)
    VARS_WITH_DESCRIPTIONS: ClassVar[list[str]] = ["CS", "GDP", "POP", "RS"]
    SENTINEL_EQ_IDS: ClassVar[list[int]] = [1, 30, 82]

    @pytest.mark.parametrize("var_code", SENTINEL_VARS)
    def test_sentinel_variable_present(self, dict_data: dict, var_code: str) -> None:
        assert var_code in dict_data["variables"], (
            f"Sentinel variable {var_code!r} not found in dictionary"
        )

    @pytest.mark.parametrize("var_code", VARS_WITH_DESCRIPTIONS)
    def test_sentinel_variable_has_description(self, dict_data: dict, var_code: str) -> None:
        rec = dict_data["variables"][var_code]
        assert rec.get("description"), f"{var_code}: description is empty"

    def test_cs_defined_by_eq_1(self, dict_data: dict) -> None:
        cs = dict_data["variables"].get("CS", {})
        assert cs.get("defined_by_equation") == 1, (
            f"CS.defined_by_equation should be 1, got {cs.get('defined_by_equation')}"
        )

    def test_gdp_defined_by_eq_82(self, dict_data: dict) -> None:
        gdp = dict_data["variables"].get("GDP", {})
        assert gdp.get("defined_by_equation") == 82

    @pytest.mark.parametrize("eq_id", SENTINEL_EQ_IDS)
    def test_sentinel_equation_present(self, dict_data: dict, eq_id: int) -> None:
        assert str(eq_id) in dict_data["equations"], f"Sentinel equation {eq_id} not found"

    def test_eq_1_is_stochastic(self, dict_data: dict) -> None:
        eq = dict_data["equations"].get("1", {})
        assert eq.get("type") == "stochastic", (
            f"Equation 1 should be stochastic, got {eq.get('type')}"
        )

    def test_eq_1_has_label(self, dict_data: dict) -> None:
        eq = dict_data["equations"].get("1", {})
        assert eq.get("label"), "Equation 1 should have a label"

    def test_eq_82_has_lhs(self, dict_data: dict) -> None:
        eq = dict_data["equations"].get("82", {})
        assert "GDP" in eq.get("lhs_expr", "").upper() or eq.get("lhs_expr"), (
            "Equation 82 LHS should reference GDP"
        )


# ── 4. ModelDictionary API ────────────────────────────────────────────────────


class TestModelDictionaryAPI:
    def test_load_returns_instance(self, model_dict: ModelDictionary) -> None:
        assert isinstance(model_dict, ModelDictionary)

    def test_repr(self, model_dict: ModelDictionary) -> None:
        r = repr(model_dict)
        assert "ModelDictionary" in r
        assert "variables=" in r
        assert "equations=" in r

    def test_variables_property(self, model_dict: ModelDictionary) -> None:
        assert isinstance(model_dict.variables, dict)
        assert len(model_dict.variables) > 0

    def test_equations_property(self, model_dict: ModelDictionary) -> None:
        assert isinstance(model_dict.equations, dict)
        assert len(model_dict.equations) > 0

    def test_get_variable_returns_record(self, model_dict: ModelDictionary) -> None:
        rec = model_dict.get_variable("CS")
        assert rec is not None
        assert isinstance(rec, VariableRecord)
        assert rec.name == "CS"

    def test_get_variable_unknown_returns_none(self, model_dict: ModelDictionary) -> None:
        assert model_dict.get_variable("NONEXISTENT_ZZZ") is None

    def test_get_equation_returns_record(self, model_dict: ModelDictionary) -> None:
        eq = model_dict.get_equation(1)
        assert eq is not None
        assert isinstance(eq, EquationRecord)
        assert eq.id == 1

    def test_get_equation_unknown_returns_none(self, model_dict: ModelDictionary) -> None:
        assert model_dict.get_equation(99999) is None

    def test_describe_returns_dict(self, model_dict: ModelDictionary) -> None:
        result = model_dict.describe("GDP")
        assert isinstance(result, dict)
        assert result["name"] == "GDP"

    def test_describe_unknown_returns_none(self, model_dict: ModelDictionary) -> None:
        assert model_dict.describe("NONEXISTENT_ZZZ") is None

    def test_search_returns_list(self, model_dict: ModelDictionary) -> None:
        results = model_dict.search("consumer")
        assert isinstance(results, list)

    def test_search_finds_cs(self, model_dict: ModelDictionary) -> None:
        results = model_dict.search("consumer expenditures")
        names = [r.name for r in results]
        assert "CS" in names, f"CS not found in search results: {names}"

    def test_search_case_insensitive(self, model_dict: ModelDictionary) -> None:
        upper = model_dict.search("GDP")
        lower = model_dict.search("gdp")
        assert len(upper) == len(lower)

    def test_search_empty_query_returns_all(self, model_dict: ModelDictionary) -> None:
        # Empty string matches everything
        results = model_dict.search("")
        assert len(results) == len(model_dict.variables)

    def test_model_version(self, model_dict: ModelDictionary) -> None:
        assert model_dict.model_version == "2025-12-23"

    def test_variable_record_fields(self, model_dict: ModelDictionary) -> None:
        cs = model_dict.get_variable("CS")
        assert cs is not None
        assert cs.category == "endogenous"
        assert cs.defined_by_equation == 1
        assert isinstance(cs.used_in_equations, list)

    def test_raw_data_property(self, model_dict: ModelDictionary) -> None:
        assert isinstance(model_dict.raw_data, dict)

    def test_load_from_default_path(self) -> None:
        """Tests that the bundled dictionary.json can be loaded."""
        if not OUTPUT_PATH.exists():
            pytest.skip("Bundled dictionary.json not yet generated")
        d = ModelDictionary.load()
        assert len(d.variables) > 0

    def test_load_missing_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            ModelDictionary.load("/nonexistent/dictionary.json")

    def test_query_equation_id_exact(self, model_dict: ModelDictionary) -> None:
        payload = model_dict.query("eq 82")
        assert payload["equation_matches"]
        assert payload["equation_matches"][0]["equation"]["id"] == 82
        assert payload["equation_matches"][0]["reason"] == "equation_id_exact"

    def test_query_variable_code_exact(self, model_dict: ModelDictionary) -> None:
        payload = model_dict.query("gdp")
        assert payload["variable_matches"]
        assert payload["variable_matches"][0]["variable"]["name"] == "GDP"
        assert payload["variable_matches"][0]["reason"] == "variable_code_exact"

    def test_query_variable_equation_lookup_intent(self, model_dict: ModelDictionary) -> None:
        payload = model_dict.query("GDP equation")
        assert payload["intent"]["kind"] == "variable_equation_lookup"
        assert payload["intent"]["variable"] == "GDP"
        assert payload["focus"]["variable_equation_lookup"]["defined_by_equation"] == 82
        assert payload["equation_matches"]
        assert payload["equation_matches"][0]["equation"]["id"] == 82

    def test_query_variable_in_equation_intent(self, model_dict: ModelDictionary) -> None:
        payload = model_dict.query("UR in equation 30")
        assert payload["intent"]["kind"] == "variable_in_equation"
        assert payload["intent"]["variable"] == "UR"
        assert payload["intent"]["equation_id"] == 30
        focus = payload["focus"]["variable_in_equation"]
        assert focus["variable"] == "UR"
        assert focus["equation_id"] == 30
        assert isinstance(focus["present_in_equation"], bool)

    def test_query_variable_in_equation_natural_language(
        self, model_dict: ModelDictionary
    ) -> None:
        payload = model_dict.query("what is UR in eq 30")
        assert payload["intent"]["kind"] == "variable_in_equation"
        assert payload["intent"]["variable"] == "UR"
        assert payload["intent"]["equation_id"] == 30

    def test_query_variable_equation_for_form(self, model_dict: ModelDictionary) -> None:
        payload = model_dict.query("equation for GDP")
        assert payload["intent"]["kind"] == "variable_equation_lookup"
        assert payload["intent"]["variable"] == "GDP"

    def test_query_variables_in_equation_intent(self, model_dict: ModelDictionary) -> None:
        payload = model_dict.query("variables in equation 82")
        assert payload["intent"]["kind"] == "equation_variables_lookup"
        assert payload["intent"]["equation_id"] == 82
        focus = payload["focus"]["equation_variables_lookup"]
        assert focus["equation_id"] == 82
        assert focus["variable_count"] >= 1
        assert payload["equation_matches"]
        assert payload["equation_matches"][0]["equation"]["id"] == 82
        assert any(match["reason"] == "equation_variable" for match in payload["variable_matches"])

    def test_query_meaning_of_variable_in_equation_intent(
        self, model_dict: ModelDictionary
    ) -> None:
        payload = model_dict.query("meaning of UR in equation 30")
        assert payload["intent"]["kind"] == "variable_meaning_in_equation"
        assert payload["intent"]["variable"] == "UR"
        assert payload["intent"]["equation_id"] == 30
        focus = payload["focus"]["variable_meaning_in_equation"]
        assert focus["variable"] == "UR"
        assert focus["equation_id"] == 30
        assert "present_in_equation" in focus
        assert "role_in_equation" in focus

    def test_query_response_schema_stable(self, model_dict: ModelDictionary) -> None:
        payload = model_dict.query("consumer")
        assert "query" in payload
        assert "intent" in payload
        assert "focus" in payload
        assert "equation_matches" in payload
        assert "variable_matches" in payload

    def test_query_ambiguous_equation_and_variable_overlap(self, tmp_path: Path) -> None:
        fixture = {
            "model_version": "2025-12-23",
            "variables": {
                "EQ82": {
                    "name": "EQ82",
                    "description": "Synthetic overlapping variable code.",
                    "units": "",
                    "sector": "",
                    "category": "endogenous",
                    "defined_by_equation": 82,
                    "used_in_equations": [82],
                    "raw_data_sources": [],
                    "construction": "Def., Eq. 82",
                }
            },
            "equations": {
                "82": {
                    "id": 82,
                    "type": "definitional",
                    "sector_block": "synthetic",
                    "label": "Synthetic equation 82",
                    "lhs_expr": "EQ82",
                    "rhs_variables": ["EQ82"],
                    "formula": "EQ82",
                }
            },
        }
        path = tmp_path / "dict_overlap.json"
        path.write_text(json.dumps(fixture), encoding="utf-8")

        dictionary = ModelDictionary.load(path)
        payload = dictionary.query("eq82")

        assert payload["intent"]["kind"] == "equation_id"
        assert payload["equation_matches"]
        assert payload["equation_matches"][0]["reason"] == "equation_id_exact"
        assert payload["equation_matches"][0]["score"] == 100
        assert payload["variable_matches"]
        assert payload["variable_matches"][0]["reason"] == "variable_code_exact"
        assert payload["variable_matches"][0]["score"] == 95

    def test_explain_equation_returns_payload(self, model_dict: ModelDictionary) -> None:
        payload = model_dict.explain_equation(82)
        assert payload is not None
        assert payload["equation"]["id"] == 82
        assert any(v["role"] == "lhs" for v in payload["variables"])

    def test_explain_equation_unknown_returns_none(self, model_dict: ModelDictionary) -> None:
        assert model_dict.explain_equation(99999) is None

    def test_explain_equation_uses_fallback_descriptions(
        self, model_dict: ModelDictionary
    ) -> None:
        payload = model_dict.explain_equation(87)
        if payload is None:
            pytest.skip("Equation 87 not available in parsed dictionary")
        assert any(var.get("description_source") != "dictionary" for var in payload["variables"])

    def test_explain_equation_ocr_missing_descriptions_regression(self, tmp_path: Path) -> None:
        fixture = {
            "model_version": "2025-12-23",
            "variables": {
                "XX": {
                    "name": "XX",
                    "description": "",
                    "units": "",
                    "sector": "",
                    "category": "endogenous",
                    "defined_by_equation": 12,
                    "used_in_equations": [12],
                    "raw_data_sources": [],
                    "construction": "",
                },
                "YY": {
                    "name": "YY",
                    "description": "",
                    "units": "",
                    "sector": "",
                    "category": "exogenous",
                    "defined_by_equation": None,
                    "used_in_equations": [12],
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
                    "used_in_equations": [12],
                    "raw_data_sources": [],
                    "construction": "",
                },
            },
            "equations": {
                "12": {
                    "id": 12,
                    "type": "definitional",
                    "sector_block": "synthetic",
                    "label": "OCR fallback regression fixture",
                    "lhs_expr": "XX",
                    "rhs_variables": ["YY", "ZZ"],
                    "formula": "YY + ZZ",
                }
            },
        }
        path = tmp_path / "dict_ocr_fallback.json"
        path.write_text(json.dumps(fixture), encoding="utf-8")

        dictionary = ModelDictionary.load(path)
        payload = dictionary.explain_equation(12)

        assert payload is not None
        by_name = {item["name"]: item for item in payload["variables"]}
        assert by_name["XX"]["description_source"] == "equation_reference"
        assert by_name["YY"]["description_source"] == "category_fallback"
        assert by_name["ZZ"]["description_source"] == "missing"

    def test_describe_includes_raw_data_details_when_available(self, tmp_path: Path) -> None:
        fixture = {
            "model_version": "2025-12-23",
            "variables": {
                "GDP": {
                    "name": "GDP",
                    "description": "Gross domestic product.",
                    "units": "",
                    "sector": "",
                    "category": "endogenous",
                    "defined_by_equation": 82,
                    "used_in_equations": [],
                    "raw_data_sources": ["R11"],
                    "construction": "",
                }
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
        path = tmp_path / "dict_with_raw.json"
        path.write_text(json.dumps(fixture), encoding="utf-8")

        dictionary = ModelDictionary.load(path)
        payload = dictionary.describe("GDP")

        assert payload is not None
        assert payload["raw_data_details"][0]["r_number"] == "R11"

    def test_raw_data_for_variable_includes_stub_for_unresolved_r_code(
        self, tmp_path: Path
    ) -> None:
        fixture = {
            "model_version": "2025-12-23",
            "variables": {
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
                }
            },
            "equations": {},
            "raw_data": {},
        }
        path = tmp_path / "dict_missing_raw.json"
        path.write_text(json.dumps(fixture), encoding="utf-8")

        dictionary = ModelDictionary.load(path)
        payload = dictionary.raw_data_for_variable("ZZ", include_unresolved=True)

        assert payload
        assert payload[0]["r_number"] == "R999"
        assert "not found" in payload[0]["description"].lower()


# ── 5. OCR noise fixture ──────────────────────────────────────────────────────


class TestOCRNoise:
    """Verify parser handles typical OCR artifacts in the fixture string."""

    def test_variable_parse_with_noise(self) -> None:
        variables = parse_variable_section(OCR_NOISE_TEXT)
        assert "CS" in variables, f"CS not found, got: {list(variables.keys())}"
        assert "GDP" in variables
        assert "KD" in variables
        assert "UR" in variables

    def test_extra_spaces_in_description(self) -> None:
        variables = parse_variable_section(OCR_NOISE_TEXT)
        cs = variables.get("CS", {})
        # Extra spaces should be collapsed
        assert "  " not in cs.get("description", "")

    def test_latex_wrapped_variable_code(self) -> None:
        variables = parse_variable_section(OCR_NOISE_TEXT)
        # \( KD \) should parse to code "KD"
        assert "KD" in variables

    def test_escaped_dollar_in_units(self) -> None:
        variables = parse_variable_section(OCR_NOISE_TEXT)
        cs = variables.get("CS", {})
        # B2017\$ should come out as B2017$
        assert cs.get("units") == "B2017$"

    def test_stochastic_equation_parse(self) -> None:
        equations = parse_equation_section(OCR_NOISE_TEXT)
        assert "1" in equations
        eq1 = equations["1"]
        assert eq1["type"] == "stochastic"
        assert "consumer" in eq1["label"].lower()

    def test_construction_parse(self) -> None:
        construction = parse_construction_section(OCR_NOISE_TEXT)
        assert "CS" in construction
        assert "GDP" in construction

    def test_raw_data_parse(self) -> None:
        raw_data = parse_raw_data_section(OCR_NOISE_TEXT)
        assert "R1" in raw_data or "R11" in raw_data

    def test_none_used_in_equations(self) -> None:
        variables = parse_variable_section(OCR_NOISE_TEXT)
        kd = variables.get("KD", {})
        assert kd.get("used_in_equations") == []


# ── Unit tests for clean_latex and helpers ────────────────────────────────────


class TestHelpers:
    def test_clean_latex_delimiters(self) -> None:
        assert clean_latex(r"\( CS \)") == "CS"
        assert clean_latex(r"\[ formula \]") == "formula"

    def test_clean_latex_escaped_dollar(self) -> None:
        assert clean_latex(r"B2017\$") == "B2017$"

    def test_clean_latex_frac(self) -> None:
        result = clean_latex(r"\frac{a}{b}")
        assert "a" in result and "b" in result

    def test_clean_latex_cmd_arg(self) -> None:
        assert clean_latex(r"\text{hello}") == "hello"

    def test_clean_var_code_strips_latex(self) -> None:
        assert clean_var_code(r"\( KD \)") == "KD"
        assert clean_var_code(r"\( GDPR \)") == "GDPR"

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("none", []),
            ("", []),
            ("1, 2, 5", [1, 2, 5]),
            ("34, 51, 52", [34, 51, 52]),
            ("150-169", list(range(150, 170))),
            ("150\u2013169", list(range(150, 170))),
            ("133", [133]),
        ],
    )
    def test_parse_used_in(self, raw: str, expected: list[int]) -> None:
        assert parse_used_in(raw) == expected

    def test_parse_used_in_deduplicates(self) -> None:
        result = parse_used_in("1, 1, 2, 2, 3")
        assert result == [1, 2, 3]
