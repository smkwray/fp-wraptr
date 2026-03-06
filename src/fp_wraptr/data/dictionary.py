"""dictionary.py — Typed loader and query API for the FP Model dictionary.

Usage
-----
    from fp_wraptr.data import ModelDictionary

    d = ModelDictionary.load()
    print(d.describe("CS"))
    results = d.search("consumer expenditures")
    eq = d.get_equation(1)
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

__all__ = ["EquationRecord", "ModelDictionary", "RawDataRecord", "VariableRecord"]

_DEFAULT_PATH = Path(__file__).parent / "dictionary.json"


# ── Data models ───────────────────────────────────────────────────────────────


class VariableRecord(BaseModel):
    """A single variable entry from the FP model dictionary."""

    name: str = Field(description="Variable code, e.g. 'CS'")
    short_name: str = Field(
        default="",
        description="Short display label for charts/tables (defaults to `name` when blank).",
    )
    description: str = Field(default="")
    units: str = Field(default="")
    sector: str = Field(default="")
    category: str = Field(
        default="endogenous",
        description="'endogenous' or 'exogenous'",
    )
    defined_by_equation: int | None = Field(
        default=None,
        description="Equation number that defines this variable, or None if exogenous",
    )
    used_in_equations: list[int] = Field(default_factory=list)
    raw_data_sources: list[str] = Field(
        default_factory=list,
        description="R-number codes from the raw data tables",
    )
    construction: str | None = Field(
        default=None,
        description="Construction note from Appendix A Table A.7",
    )

    model_config = ConfigDict(extra="allow")


class EquationRecord(BaseModel):
    """A single equation entry from the FP model dictionary."""

    id: int = Field(description="Equation number")
    type: str = Field(
        default="definitional",
        description="'stochastic' or 'definitional'",
    )
    sector_block: str = Field(default="")
    label: str = Field(
        default="", description="Human-readable label, e.g. 'Consumer expenditures: services'"
    )
    lhs_expr: str = Field(default="")
    rhs_variables: list[str] = Field(default_factory=list)
    formula: str = Field(default="")

    model_config = ConfigDict(extra="allow")


class RawDataRecord(BaseModel):
    """A single raw-data R-variable entry from Appendix A.5."""

    r_number: str = Field(description="R-number code, e.g. 'R11'")
    variable: str = Field(default="", description="FP variable code linked to this raw-data line")
    source_type: str = Field(default="", description="Raw-data source block type")
    description: str = Field(default="")
    table: str = Field(default="")
    line: str = Field(default="")
    code: str = Field(default="")

    model_config = ConfigDict(extra="allow")


# ── Main dictionary class ─────────────────────────────────────────────────────


class ModelDictionary:
    """Typed access to the FP model dictionary.

    Examples
    --------
    >>> d = ModelDictionary.load()
    >>> rec = d.get_variable("CS")
    >>> rec.description
    'Consumer expenditures for services, B2017$.'
    >>> d.describe("GDP")
    {'name': 'GDP', 'description': ..., ...}
    >>> d.search("consumer")[0].name
    'CD'
    """

    def __init__(
        self,
        variables: dict[str, VariableRecord],
        equations: dict[int, EquationRecord],
        raw_data: dict[str, RawDataRecord] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        self._variables = variables
        self._equations = equations
        self._raw_data = raw_data or {}
        self._meta = meta or {}

    # ── Constructors ─────────────────────────────────────────────────────────

    @classmethod
    def load(cls, path: Path | str | None = None) -> ModelDictionary:
        """Load dictionary from JSON file.

        Args:
            path: Path to dictionary.json. Defaults to bundled file.

        Returns:
            ModelDictionary instance.

        Raises:
            FileNotFoundError: If the dictionary file does not exist.
            RuntimeError: If the file cannot be parsed.
        """
        resolved = Path(path) if path is not None else _DEFAULT_PATH
        if not resolved.exists():
            raise FileNotFoundError(
                f"Dictionary file not found: {resolved}. Run extract_dictionary.py first."
            )
        try:
            raw = json.loads(resolved.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"Failed to parse {resolved}: {exc}") from exc

        variables: dict[str, VariableRecord] = {}
        for code, rec in raw.get("variables", {}).items():
            try:
                variables[code] = VariableRecord(**rec)
            except Exception:
                # Soft fail: include what we can
                variables[code] = VariableRecord(name=code, description=str(rec))

        equations: dict[int, EquationRecord] = {}
        for _key, rec in raw.get("equations", {}).items():
            try:
                eq = EquationRecord(**rec)
                equations[eq.id] = eq
            except Exception:
                pass

        raw_data: dict[str, RawDataRecord] = {}
        for r_number, rec in raw.get("raw_data", {}).items():
            if not isinstance(rec, dict):
                continue
            try:
                raw_data[r_number] = RawDataRecord(**rec)
            except Exception:
                raw_data[r_number] = RawDataRecord(r_number=r_number, description=str(rec))

        meta = {k: v for k, v in raw.items() if k not in ("variables", "equations", "raw_data")}
        return cls(variables, equations, raw_data, meta)

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def variables(self) -> dict[str, VariableRecord]:
        """All variable records, keyed by variable code."""
        return self._variables

    @property
    def equations(self) -> dict[int, EquationRecord]:
        """All equation records, keyed by equation id."""
        return self._equations

    @property
    def model_version(self) -> str:
        return self._meta.get("model_version", "")

    @property
    def raw_data(self) -> dict[str, RawDataRecord]:
        """All raw-data records, keyed by R-number code."""
        return self._raw_data

    # ── Query API ─────────────────────────────────────────────────────────────

    def get_variable(self, code: str) -> VariableRecord | None:
        """Return a variable record by its code, or None if not found."""
        return self._variables.get(code)

    def get_equation(self, eq_id: int) -> EquationRecord | None:
        """Return an equation record by its id, or None if not found."""
        return self._equations.get(eq_id)

    def describe(self, code: str) -> dict[str, Any] | None:
        """Return a variable record as a plain dict, or None if not found."""
        rec = self._variables.get(code)
        if rec is None:
            return None
        payload = rec.model_dump(exclude={"_provenance"})
        raw_data_details = self.raw_data_for_variable(code)
        if raw_data_details:
            payload["raw_data_details"] = raw_data_details
        return payload

    def get_raw_data(self, r_number: str) -> RawDataRecord | None:
        """Return one raw-data record by R-number code, or None if not found."""
        return self._raw_data.get(r_number.upper())

    def raw_data_for_variable(
        self,
        code: str,
        include_unresolved: bool = False,
    ) -> list[dict[str, Any]]:
        """Return raw-data records linked to one variable."""
        record = self.get_variable(code.upper())
        if record is None:
            return []
        details: list[dict[str, Any]] = []
        for r_number in record.raw_data_sources:
            raw_data_record = self.get_raw_data(r_number)
            if raw_data_record is None:
                if include_unresolved:
                    details.append({
                        "r_number": r_number,
                        "variable": code.upper(),
                        "description": "Raw-data code not found in dictionary raw_data table.",
                        "source_type": "",
                    })
                continue
            details.append(raw_data_record.model_dump())
        return details

    def search(self, query: str) -> list[VariableRecord]:
        """Case-insensitive search over variable codes and descriptions.

        Returns list of matching VariableRecord, sorted by code.
        """
        pattern = re.compile(re.escape(query), re.IGNORECASE)
        results = [
            rec
            for rec in self._variables.values()
            if pattern.search(rec.name) or pattern.search(rec.description)
        ]
        return sorted(results, key=lambda r: r.name)

    def search_equations(self, query: str) -> list[EquationRecord]:
        """Case-insensitive search over equation labels and LHS expressions."""
        pattern = re.compile(re.escape(query), re.IGNORECASE)
        results = [
            eq
            for eq in self._equations.values()
            if pattern.search(eq.label)
            or pattern.search(eq.lhs_expr)
            or pattern.search(eq.formula)
        ]
        return sorted(results, key=lambda e: e.id)

    def query(self, query: str, limit: int = 10) -> dict[str, Any]:
        """Resolve mixed dictionary queries (equation id, variable code, free text).

        Ranking (deterministic):
            1. Exact equation id (for patterns like "82" / "eq 82")
            2. Exact variable code
            3. Prefix variable code
            4. Free-text variable/equation matches
        """
        raw = query.strip()
        normalized = raw.upper()
        intent: dict[str, Any] = {"kind": "generic", "raw": raw}
        focus: dict[str, Any] = {}
        equation_matches: list[dict[str, Any]] = []
        variable_matches: list[dict[str, Any]] = []

        if intent["kind"] == "generic":
            vars_in_eq_match = re.fullmatch(
                r"(?:VARIABLES?|VARS)\s+IN\s+EQ(?:UATION)?\s*(\d+)",
                normalized,
            )
            if vars_in_eq_match:
                intent = {
                    "kind": "equation_variables_lookup",
                    "raw": raw,
                    "equation_id": int(vars_in_eq_match.group(1)),
                }

        if intent["kind"] == "generic":
            var_in_eq_match = re.fullmatch(
                r"(?:WHAT\s+IS\s+)?([A-Z][A-Z0-9_]*)\s+IN\s+EQ(?:UATION)?\s*(\d+)",
                normalized,
            )
            if var_in_eq_match:
                intent = {
                    "kind": "variable_in_equation",
                    "raw": raw,
                    "variable": var_in_eq_match.group(1),
                    "equation_id": int(var_in_eq_match.group(2)),
                }

        if intent["kind"] == "generic":
            var_meaning_in_eq_match = re.fullmatch(
                r"(?:MEANING\s+OF\s+)([A-Z][A-Z0-9_]*)\s+IN\s+EQ(?:UATION)?\s*(\d+)",
                normalized,
            )
            if var_meaning_in_eq_match:
                intent = {
                    "kind": "variable_meaning_in_equation",
                    "raw": raw,
                    "variable": var_meaning_in_eq_match.group(1),
                    "equation_id": int(var_meaning_in_eq_match.group(2)),
                }

        if intent["kind"] == "generic":
            var_eq_match = re.fullmatch(r"([A-Z][A-Z0-9_]*)\s+EQ(?:UATION)?", normalized)
            if var_eq_match:
                intent = {
                    "kind": "variable_equation_lookup",
                    "raw": raw,
                    "variable": var_eq_match.group(1),
                }

        if intent["kind"] == "generic":
            var_eq_for_match = re.fullmatch(
                r"(?:EQUATION\s+FOR\s+)([A-Z][A-Z0-9_]*)",
                normalized,
            )
            if var_eq_for_match:
                intent = {
                    "kind": "variable_equation_lookup",
                    "raw": raw,
                    "variable": var_eq_for_match.group(1),
                }

        eq_match = re.fullmatch(r"(?:EQ(?:UATION)?\s*)?(\d+)", normalized)
        if eq_match and intent["kind"] == "generic":
            intent = {
                "kind": "equation_id",
                "raw": raw,
                "equation_id": int(eq_match.group(1)),
            }

        if intent["kind"] in {"variable_in_equation", "variable_meaning_in_equation"}:
            eq_id = int(intent["equation_id"])
            var_code = str(intent["variable"])
            eq = self.get_equation(eq_id)
            lhs_vars = self._extract_variable_tokens(eq.lhs_expr) if eq else []
            if eq is not None:
                equation_matches.append({
                    "score": 100,
                    "reason": "equation_id_exact",
                    "equation": eq.model_dump(),
                    "links": self._equation_links(eq),
                })
            var = self.get_variable(var_code)
            if var is not None:
                variable_matches.append({
                    "score": 95,
                    "reason": "variable_code_exact",
                    "variable": self._variable_payload(var),
                })
            eq_vars = set(eq.rhs_variables) | set(lhs_vars) if eq else set()
            focus_payload: dict[str, Any] = {
                "variable": var_code,
                "equation_id": eq_id,
                "present_in_equation": var_code in eq_vars,
            }
            if intent["kind"] == "variable_meaning_in_equation":
                focus_payload["role_in_equation"] = (
                    self._variable_role(var_code, lhs_vars) if var_code in eq_vars else None
                )
            focus = {intent["kind"]: focus_payload}
        elif intent["kind"] == "equation_variables_lookup":
            eq_id = int(intent["equation_id"])
            eq = self.get_equation(eq_id)
            lhs_vars = self._extract_variable_tokens(eq.lhs_expr) if eq else []
            rhs_vars = [name.upper() for name in eq.rhs_variables] if eq else []
            related_vars = sorted(set(lhs_vars + rhs_vars))
            if eq is not None:
                equation_matches.append({
                    "score": 100,
                    "reason": "equation_id_exact",
                    "equation": eq.model_dump(),
                    "links": self._equation_links(eq),
                })
                for code in related_vars[:limit]:
                    var = self.get_variable(code)
                    if var is not None:
                        variable_matches.append({
                            "score": 90,
                            "reason": "equation_variable",
                            "variable": self._variable_payload(var),
                        })
                        continue
                    variable_matches.append({
                        "score": 88,
                        "reason": "equation_variable_missing_metadata",
                        "variable": {
                            "name": code,
                            "description": "Description unavailable in current dictionary.",
                            "description_source": "missing",
                            "defined_by_equation": None,
                            "used_in_equations": [],
                            "links": {
                                "defining_equation": None,
                                "used_in_equations": [],
                            },
                        },
                    })
            focus = {
                "equation_variables_lookup": {
                    "equation_id": eq_id,
                    "lhs_variables": lhs_vars,
                    "rhs_variables": rhs_vars,
                    "variable_count": len(related_vars),
                }
            }
        elif intent["kind"] == "variable_equation_lookup":
            var_code = str(intent["variable"])
            var = self.get_variable(var_code)
            if var is not None:
                variable_matches.append({
                    "score": 95,
                    "reason": "variable_code_exact",
                    "variable": self._variable_payload(var),
                })
                if var.defined_by_equation is not None:
                    eq = self.get_equation(var.defined_by_equation)
                    if eq is not None:
                        equation_matches.append({
                            "score": 98,
                            "reason": "variable_defining_equation",
                            "equation": eq.model_dump(),
                            "links": self._equation_links(eq),
                        })
                    focus = {
                        "variable_equation_lookup": {
                            "variable": var_code,
                            "defined_by_equation": var.defined_by_equation,
                        }
                    }
                else:
                    focus = {
                        "variable_equation_lookup": {
                            "variable": var_code,
                            "defined_by_equation": None,
                        }
                    }
        elif eq_match:
            eq_id = int(eq_match.group(1))
            eq = self.get_equation(eq_id)
            if eq is not None:
                equation_matches.append({
                    "score": 100,
                    "reason": "equation_id_exact",
                    "equation": eq.model_dump(),
                    "links": self._equation_links(eq),
                })

        var = self.get_variable(normalized)
        if var is not None and not variable_matches:
            variable_matches.append({
                "score": 95,
                "reason": "variable_code_exact",
                "variable": self._variable_payload(var),
            })

        if not variable_matches:
            prefix = [
                rec
                for rec in self._variables.values()
                if rec.name.startswith(normalized) and normalized
            ]
            for rec in sorted(prefix, key=lambda item: item.name)[:limit]:
                variable_matches.append({
                    "score": 80,
                    "reason": "variable_code_prefix",
                    "variable": self._variable_payload(rec),
                })

        if not equation_matches:
            for rec in self.search_equations(raw)[:limit]:
                equation_matches.append({
                    "score": 70,
                    "reason": "equation_text_match",
                    "equation": rec.model_dump(),
                    "links": self._equation_links(rec),
                })

        if len(variable_matches) < limit:
            seen_names = {entry["variable"]["name"] for entry in variable_matches}
            remaining = limit - len(variable_matches)
            for rec in self.search(raw):
                if rec.name in seen_names:
                    continue
                variable_matches.append({
                    "score": 60,
                    "reason": "variable_text_match",
                    "variable": self._variable_payload(rec),
                })
                if len(variable_matches) >= remaining + len(seen_names):
                    break

        return {
            "query": raw,
            "intent": intent,
            "focus": focus,
            "equation_matches": equation_matches[:limit],
            "variable_matches": variable_matches[:limit],
        }

    def explain_equation(self, eq_id: int) -> dict[str, Any] | None:
        """Return equation details plus variable-level explanations."""
        equation = self.get_equation(eq_id)
        if equation is None:
            return None

        lhs_vars = self._extract_variable_tokens(equation.lhs_expr)
        rhs_vars = [name.upper() for name in equation.rhs_variables]
        all_codes = sorted(set(lhs_vars + rhs_vars))

        explained_vars = []
        for code in all_codes:
            var = self.get_variable(code)
            if var is None:
                explained_vars.append({
                    "name": code,
                    "role": self._variable_role(code, lhs_vars),
                    "description": "Description unavailable in current dictionary.",
                    "description_source": "missing",
                    "found_in_dictionary": False,
                })
                continue

            payload = self._variable_payload(var)
            payload["role"] = self._variable_role(code, lhs_vars)
            payload["found_in_dictionary"] = True
            explained_vars.append(payload)

        return {
            "equation": equation.model_dump(),
            "variables": explained_vars,
            "cross_links": {
                "equation_id": equation.id,
                "lhs_variables": lhs_vars,
                "rhs_variables": rhs_vars,
                "variable_to_equations": {
                    item["name"]: {
                        "role": item.get("role"),
                        "defined_by_equation": item.get("defined_by_equation"),
                        "used_in_equations": item.get("used_in_equations", []),
                    }
                    for item in explained_vars
                },
            },
        }

    def variables_by_sector(self, sector: str) -> list[VariableRecord]:
        """Return all variables for a given sector code ('h', 'f', etc.)."""
        return sorted(
            [v for v in self._variables.values() if v.sector == sector],
            key=lambda r: r.name,
        )

    def exogenous_variables(self) -> list[VariableRecord]:
        """Return all exogenous variables."""
        return sorted(
            [v for v in self._variables.values() if v.category == "exogenous"],
            key=lambda r: r.name,
        )

    @staticmethod
    def _equation_links(equation: EquationRecord) -> dict[str, Any]:
        lhs_variables = ModelDictionary._extract_variable_tokens(equation.lhs_expr)
        rhs_variables = [item.upper() for item in equation.rhs_variables]
        return {
            "lhs_variables": lhs_variables,
            "rhs_variables": rhs_variables,
            "related_variables": sorted(set(lhs_variables + rhs_variables)),
        }

    @staticmethod
    def _variable_links(variable: VariableRecord) -> dict[str, Any]:
        return {
            "defining_equation": variable.defined_by_equation,
            "used_in_equations": variable.used_in_equations,
        }

    def _variable_payload(self, variable: VariableRecord) -> dict[str, Any]:
        """Render variable record with description fallbacks."""
        payload = variable.model_dump(exclude={"_provenance"})
        payload["links"] = self._variable_links(variable)
        raw_data_details = self.raw_data_for_variable(variable.name)
        if raw_data_details:
            payload["raw_data_details"] = raw_data_details
        description = (variable.description or "").strip()
        if description:
            payload["description_source"] = "dictionary"
            payload["description"] = description
            return payload

        construction = (variable.construction or "").strip()
        if construction:
            payload["description_source"] = "construction"
            payload["description"] = f"Construction note: {construction}"
            return payload

        if variable.defined_by_equation is not None:
            payload["description_source"] = "equation_reference"
            payload["description"] = f"Defined by equation {variable.defined_by_equation}."
            return payload

        if variable.category == "exogenous":
            payload["description_source"] = "category_fallback"
            payload["description"] = "Exogenous variable (description unavailable)."
            return payload

        payload["description_source"] = "missing"
        payload["description"] = "Description unavailable in current dictionary."
        return payload

    @staticmethod
    def _extract_variable_tokens(expression: str) -> list[str]:
        """Extract uppercase variable codes from an equation expression."""
        return sorted(set(re.findall(r"\b([A-Z][A-Z0-9_]{1,})\b", expression or "")))

    @staticmethod
    def _variable_role(name: str, lhs_vars: list[str]) -> str:
        if name in lhs_vars:
            return "lhs"
        return "rhs"

    # ── Repr ─────────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"ModelDictionary(version={self.model_version!r}, "
            f"variables={len(self._variables)}, "
            f"equations={len(self._equations)}, "
            f"raw_data={len(self._raw_data)})"
        )
