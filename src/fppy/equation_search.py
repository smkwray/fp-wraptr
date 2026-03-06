from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
_INTENT_EQ_ID_RE = re.compile(r"^\s*(?:eq(?:uation)?\s*)?#?\s*(\d+)\s*$", re.IGNORECASE)
_INTENT_VAR_RE = re.compile(
    r"^\s*(?:var|variable)\s+([A-Za-z][A-Za-z0-9_]*)\s*$",
    re.IGNORECASE,
)
_INTENT_VARS_IN_EQ_RE = re.compile(
    r"\b(?:vars?|variables?)\s+(?:in|for)?\s*eq(?:uation)?\s*#?\s*(\d+)\b",
    re.IGNORECASE,
)
_INTENT_VAR_IN_EQ_RE = re.compile(
    r"\bwhat\s+does\s+([A-Za-z][A-Za-z0-9_]*)\s+mean\s+in\s+eq(?:uation)?\s*#?\s*(\d+)\b",
    re.IGNORECASE,
)
_INTENT_SHORT_VAR_IN_EQ_RE = re.compile(
    r"\b([A-Za-z][A-Za-z0-9_]*)\s+in\s+eq(?:uation)?\s*#?\s*(\d+)\b",
    re.IGNORECASE,
)
_VARIABLE_TOKEN_RE = re.compile(r"\b[A-Z][A-Z0-9_]*\b")
_FALLBACK_EQ_RE = re.compile(r"\bEQ\.?\s*(\d+)\b", re.IGNORECASE)
_FUNC_NAMES = {
    "ABS",
    "EXP",
    "LN",
    "LOG",
    "MAX",
    "MIN",
    "POW",
    "SQRT",
}


@dataclass(frozen=True)
class VariableRecord:
    name: str
    description: str = ""
    units: str = ""
    category: str = ""
    defined_by_equation: int | None = None
    used_in_equations: tuple[int, ...] = ()
    raw_data_sources: tuple[str, ...] = ()
    construction: str = ""


@dataclass(frozen=True)
class EquationRecord:
    id: int
    type: str = ""
    label: str = ""
    lhs_expr: str = ""
    rhs_variables: tuple[str, ...] = ()
    formula: str = ""


@dataclass(frozen=True)
class ParsedQuery:
    intent: str
    raw: str
    normalized: str
    equation_id: int | None = None
    variable_code: str | None = None
    text_terms: tuple[str, ...] = ()
    include_definitions: bool = True
    include_formula: bool = True


def _normalize_text(value: str) -> str:
    return " ".join(str(value).strip().split())


def _tokenize(text: str) -> tuple[str, ...]:
    return tuple(token.lower() for token in _TOKEN_RE.findall(_normalize_text(text)))


def _as_str_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ()
        if "," in stripped:
            return tuple(part.strip() for part in stripped.split(",") if part.strip())
        return (stripped,)
    if isinstance(value, (list, tuple, set)):
        output: list[str] = []
        for item in value:
            normalized = str(item).strip()
            if normalized:
                output.append(normalized)
        return tuple(output)
    return ()


def _as_int_tuple(value: object) -> tuple[int, ...]:
    if value is None:
        return ()
    source = value if isinstance(value, (list, tuple, set)) else (value,)
    output: list[int] = []
    for item in source:
        try:
            output.append(int(item))
        except (TypeError, ValueError):
            continue
    return tuple(output)


def _first(payload: dict[str, object], *names: str) -> object:
    for name in names:
        if name in payload:
            return payload[name]
    return None


def _coerce_variable_record(payload: dict[str, object]) -> VariableRecord | None:
    raw_name = _first(payload, "name", "code", "variable", "var")
    if raw_name is None:
        return None
    name = str(raw_name).strip().upper()
    if not name:
        return None
    defined_raw = _first(
        payload,
        "defined_by_equation",
        "defined_by_eq",
        "defined_by",
        "equation_id",
    )
    defined: int | None = None
    if defined_raw is not None:
        try:
            defined = int(defined_raw)
        except (TypeError, ValueError):
            defined = None
    return VariableRecord(
        name=name,
        description=str(_first(payload, "description", "desc", "text") or "").strip(),
        units=str(_first(payload, "units", "unit") or "").strip(),
        category=str(_first(payload, "category", "type") or "").strip(),
        defined_by_equation=defined,
        used_in_equations=_as_int_tuple(
            _first(payload, "used_in_equations", "used_in", "equations_used")
        ),
        raw_data_sources=_as_str_tuple(
            _first(payload, "raw_data_sources", "data_sources", "sources")
        ),
        construction=str(_first(payload, "construction", "construct") or "").strip(),
    )


def _coerce_equation_record(payload: dict[str, object]) -> EquationRecord | None:
    raw_id = _first(payload, "id", "equation_id", "number", "eq")
    if raw_id is None:
        return None
    try:
        equation_id = int(raw_id)
    except (TypeError, ValueError):
        return None
    rhs = _as_str_tuple(_first(payload, "rhs_variables", "rhs_vars", "rhs", "variables"))
    rhs_upper = tuple(item.strip().upper() for item in rhs if item.strip())
    return EquationRecord(
        id=equation_id,
        type=str(_first(payload, "type", "equation_type") or "").strip(),
        label=str(_first(payload, "label", "name", "title") or "").strip(),
        lhs_expr=str(_first(payload, "lhs_expr", "lhs", "lhs_variable") or "").strip(),
        rhs_variables=rhs_upper,
        formula=str(_first(payload, "formula", "equation", "expr") or "").strip(),
    )


def _extract_record_list(payload: object, *, keys: tuple[str, ...]) -> list[dict[str, object]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in keys:
        candidate = payload.get(key)
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
    return []


def _extract_lhs_variables(lhs_expr: str) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for token in _VARIABLE_TOKEN_RE.findall(lhs_expr or ""):
        normalized = token.upper()
        if normalized in _FUNC_NAMES:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return tuple(ordered)


def _ordered_union(values: tuple[str, ...], extra: tuple[str, ...]) -> tuple[str, ...]:
    output: list[str] = []
    seen: set[str] = set()
    for item in (*values, *extra):
        normalized = str(item).upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)
    return tuple(output)


@dataclass
class DictionaryStore:
    equations_by_id: dict[int, EquationRecord]
    variables_by_code: dict[str, VariableRecord]
    dictionary_version: str

    eq_label_tokens: dict[int, set[str]]
    eq_lhs_tokens: dict[int, set[str]]
    eq_formula_tokens: dict[int, set[str]]
    eq_index: dict[str, set[int]]

    var_name_tokens: dict[str, set[str]]
    var_desc_tokens: dict[str, set[str]]
    var_construction_tokens: dict[str, set[str]]
    var_index: dict[str, set[str]]

    equations_by_lhs_var: dict[str, tuple[int, ...]]

    @classmethod
    def from_records(
        cls,
        *,
        equations: list[dict[str, object]],
        variables: list[dict[str, object]],
        dictionary_version: str = "unknown",
    ) -> DictionaryStore:
        eq_map: dict[int, EquationRecord] = {}
        for record in equations:
            parsed = _coerce_equation_record(record)
            if parsed is None:
                continue
            eq_map[parsed.id] = parsed

        var_map: dict[str, VariableRecord] = {}
        for record in variables:
            parsed = _coerce_variable_record(record)
            if parsed is None:
                continue
            var_map[parsed.name] = parsed

        eq_label_tokens: dict[int, set[str]] = {}
        eq_lhs_tokens: dict[int, set[str]] = {}
        eq_formula_tokens: dict[int, set[str]] = {}
        eq_index: dict[str, set[int]] = {}
        equations_by_lhs_var_sets: dict[str, set[int]] = {}
        for equation in eq_map.values():
            label_tokens = set(_tokenize(equation.label))
            lhs_tokens = set(_tokenize(equation.lhs_expr))
            formula_tokens = set(_tokenize(equation.formula))
            eq_label_tokens[equation.id] = label_tokens
            eq_lhs_tokens[equation.id] = lhs_tokens
            eq_formula_tokens[equation.id] = formula_tokens
            for token in label_tokens | lhs_tokens | formula_tokens:
                eq_index.setdefault(token, set()).add(equation.id)
            for variable in _extract_lhs_variables(equation.lhs_expr):
                equations_by_lhs_var_sets.setdefault(variable, set()).add(equation.id)

        var_name_tokens: dict[str, set[str]] = {}
        var_desc_tokens: dict[str, set[str]] = {}
        var_construction_tokens: dict[str, set[str]] = {}
        var_index: dict[str, set[str]] = {}
        for variable in var_map.values():
            name_tokens = set(_tokenize(variable.name))
            desc_tokens = set(_tokenize(variable.description))
            construction_tokens = set(_tokenize(variable.construction))
            var_name_tokens[variable.name] = name_tokens
            var_desc_tokens[variable.name] = desc_tokens
            var_construction_tokens[variable.name] = construction_tokens
            for token in name_tokens | desc_tokens | construction_tokens:
                var_index.setdefault(token, set()).add(variable.name)

        equations_by_lhs_var = {
            key: tuple(sorted(values)) for key, values in equations_by_lhs_var_sets.items()
        }

        return cls(
            equations_by_id=eq_map,
            variables_by_code=var_map,
            dictionary_version=dictionary_version,
            eq_label_tokens=eq_label_tokens,
            eq_lhs_tokens=eq_lhs_tokens,
            eq_formula_tokens=eq_formula_tokens,
            eq_index=eq_index,
            var_name_tokens=var_name_tokens,
            var_desc_tokens=var_desc_tokens,
            var_construction_tokens=var_construction_tokens,
            var_index=var_index,
            equations_by_lhs_var=equations_by_lhs_var,
        )

    @classmethod
    def from_json_paths(
        cls,
        *,
        equations_json: Path,
        variables_json: Path,
    ) -> DictionaryStore:
        equation_payload = json.loads(equations_json.read_text(encoding="utf-8"))
        variable_payload = json.loads(variables_json.read_text(encoding="utf-8"))
        equation_records = _extract_record_list(
            equation_payload,
            keys=("equations", "equation_records", "items", "data"),
        )
        variable_records = _extract_record_list(
            variable_payload,
            keys=("variables", "variable_records", "items", "data"),
        )
        version = "unknown"
        if isinstance(equation_payload, dict):
            version = str(
                equation_payload.get("dictionary_version")
                or equation_payload.get("version")
                or version
            )
        if version == "unknown" and isinstance(variable_payload, dict):
            version = str(
                variable_payload.get("dictionary_version")
                or variable_payload.get("version")
                or version
            )
        return cls.from_records(
            equations=equation_records,
            variables=variable_records,
            dictionary_version=version,
        )


def parse_query(raw_query: str, *, known_variables: set[str] | None = None) -> ParsedQuery:
    raw = str(raw_query)
    normalized = _normalize_text(raw).lower()

    match = _INTENT_VAR_IN_EQ_RE.search(raw)
    if match is not None:
        return ParsedQuery(
            intent="VAR_IN_EQUATION",
            raw=raw,
            normalized=normalized,
            variable_code=str(match.group(1)).upper(),
            equation_id=int(match.group(2)),
        )

    match = _INTENT_SHORT_VAR_IN_EQ_RE.search(raw)
    if match is not None:
        return ParsedQuery(
            intent="VAR_IN_EQUATION",
            raw=raw,
            normalized=normalized,
            variable_code=str(match.group(1)).upper(),
            equation_id=int(match.group(2)),
        )

    match = _INTENT_VARS_IN_EQ_RE.search(raw)
    if match is not None:
        return ParsedQuery(
            intent="VARS_IN_EQUATION",
            raw=raw,
            normalized=normalized,
            equation_id=int(match.group(1)),
        )

    match = _INTENT_VAR_RE.match(raw)
    if match is not None:
        return ParsedQuery(
            intent="VARIABLE_BY_CODE",
            raw=raw,
            normalized=normalized,
            variable_code=str(match.group(1)).upper(),
        )

    match = _INTENT_EQ_ID_RE.match(raw)
    if match is not None:
        return ParsedQuery(
            intent="EQUATION_BY_ID",
            raw=raw,
            normalized=normalized,
            equation_id=int(match.group(1)),
        )

    stripped = raw.strip()
    if stripped.isdigit():
        return ParsedQuery(
            intent="EQUATION_BY_ID",
            raw=raw,
            normalized=normalized,
            equation_id=int(stripped),
        )

    tokenized = tuple(token.upper() for token in _VARIABLE_TOKEN_RE.findall(raw))
    if (
        len(tokenized) == 1
        and known_variables
        and tokenized[0] in known_variables
        and _normalize_text(raw).upper() == tokenized[0]
    ):
        return ParsedQuery(
            intent="VARIABLE_BY_CODE",
            raw=raw,
            normalized=normalized,
            variable_code=tokenized[0],
        )

    return ParsedQuery(
        intent="FREE_TEXT",
        raw=raw,
        normalized=normalized,
        text_terms=_tokenize(raw),
    )


def _resolve_variable_description(
    variable: VariableRecord,
    *,
    equations_by_id: dict[int, EquationRecord],
) -> tuple[str, bool, str]:
    description = _normalize_text(variable.description)
    if description:
        return description, False, "description"

    construction = _normalize_text(variable.construction)
    if construction:
        eq_match = _FALLBACK_EQ_RE.search(construction)
        if eq_match is not None:
            eq_id = int(eq_match.group(1))
            equation = equations_by_id.get(eq_id)
            if equation is not None and equation.label:
                return (
                    f"Defined in Eq. {eq_id} ({equation.label}) [fallback]",
                    True,
                    "construction+defined_by_equation",
                )
            return f"Defined in Eq. {eq_id} [fallback]", True, "construction"
        return construction, True, "construction"

    if variable.defined_by_equation is not None:
        equation = equations_by_id.get(variable.defined_by_equation)
        if equation is not None and equation.label:
            return (
                f"Defined in Eq. {variable.defined_by_equation} ({equation.label}) [fallback]",
                True,
                "defined_by_equation",
            )
        return (
            f"Defined in Eq. {variable.defined_by_equation} [fallback]",
            True,
            "defined_by_equation",
        )

    if variable.used_in_equations:
        listed = ", ".join(str(item) for item in variable.used_in_equations[:5])
        suffix = ""
        if len(variable.used_in_equations) > 5:
            suffix = f" (+{len(variable.used_in_equations) - 5} more)"
        return (
            f"Used in equations: {listed}{suffix} [fallback]",
            True,
            "used_in_equations",
        )

    return "No description available in dictionary.", True, "none"


def _build_variable_view(
    variable: VariableRecord | None,
    *,
    variable_code: str,
    equations_by_id: dict[int, EquationRecord],
) -> dict[str, object]:
    if variable is None:
        return {
            "name": variable_code,
            "description": "",
            "description_resolved": "No dictionary entry found for variable.",
            "units": "",
            "category": "",
            "defined_by_equation": None,
            "used_in_equations": [],
            "raw_data_sources": [],
            "construction": "",
            "quality": {
                "has_description": False,
                "used_fallback": True,
                "fallback_source": "missing_variable",
            },
        }

    description_resolved, used_fallback, fallback_source = _resolve_variable_description(
        variable,
        equations_by_id=equations_by_id,
    )
    return {
        "name": variable.name,
        "description": variable.description,
        "description_resolved": description_resolved,
        "units": variable.units,
        "category": variable.category,
        "defined_by_equation": variable.defined_by_equation,
        "used_in_equations": list(variable.used_in_equations),
        "raw_data_sources": list(variable.raw_data_sources),
        "construction": variable.construction,
        "quality": {
            "has_description": bool(_normalize_text(variable.description)),
            "used_fallback": used_fallback,
            "fallback_source": fallback_source,
        },
    }


def _build_equation_view(
    equation: EquationRecord,
    *,
    store: DictionaryStore,
    include_variable_details: bool,
) -> dict[str, object]:
    lhs_variables = _extract_lhs_variables(equation.lhs_expr)
    all_variables = _ordered_union(lhs_variables, equation.rhs_variables)
    variables_payload: list[dict[str, object]] = []
    for variable_code in all_variables:
        variable = store.variables_by_code.get(variable_code)
        if include_variable_details:
            variables_payload.append(
                _build_variable_view(
                    variable,
                    variable_code=variable_code,
                    equations_by_id=store.equations_by_id,
                )
            )
        else:
            description_resolved = ""
            defined_by: int | None = None
            if variable is not None:
                description_resolved, _used_fallback, _source = _resolve_variable_description(
                    variable,
                    equations_by_id=store.equations_by_id,
                )
                defined_by = variable.defined_by_equation
            variables_payload.append({
                "name": variable_code,
                "description_resolved": description_resolved,
                "defined_by_equation": defined_by,
            })

    label = equation.label or f"Equation {equation.id}"
    return {
        "id": equation.id,
        "type": equation.type,
        "label": label,
        "lhs_expr": equation.lhs_expr,
        "lhs_variables": list(lhs_variables),
        "rhs_variables": list(equation.rhs_variables),
        "all_variables": list(all_variables),
        "formula": equation.formula,
        "variables": variables_payload,
    }


def _rank_equation_free_text(
    equation: EquationRecord,
    *,
    terms: set[str],
    store: DictionaryStore,
) -> float:
    if not terms:
        return 0.0
    eq_id = equation.id
    label_tokens = store.eq_label_tokens.get(eq_id, set())
    lhs_tokens = store.eq_lhs_tokens.get(eq_id, set())
    formula_tokens = store.eq_formula_tokens.get(eq_id, set())
    rhs_tokens = {token.lower() for token in equation.rhs_variables}
    score = 0.0
    if terms and label_tokens == terms:
        score += 0.9
    if label_tokens:
        score += 0.8 * (len(terms & label_tokens) / len(terms))
    if lhs_tokens:
        score += 0.75 * (len(terms & lhs_tokens) / len(terms))
    if formula_tokens:
        score += 0.4 * (len(terms & formula_tokens) / len(terms))
    if rhs_tokens:
        score += 0.3 * (len(terms & rhs_tokens) / len(terms))
    return min(score, 1.0)


def _rank_variable_free_text(
    variable: VariableRecord,
    *,
    terms: set[str],
    store: DictionaryStore,
) -> float:
    if not terms:
        return 0.0
    code = variable.name
    name_tokens = store.var_name_tokens.get(code, set())
    desc_tokens = store.var_desc_tokens.get(code, set())
    construction_tokens = store.var_construction_tokens.get(code, set())
    score = 0.0
    if len(terms) == 1 and next(iter(terms)).upper() == code:
        score += 0.95
    if name_tokens:
        score += 0.9 * (len(terms & name_tokens) / len(terms))
    if desc_tokens:
        score += 0.6 * (len(terms & desc_tokens) / len(terms))
    if construction_tokens:
        score += 0.5 * (len(terms & construction_tokens) / len(terms))
    return min(score, 1.0)


def _sorted_results(results: list[dict[str, object]]) -> list[dict[str, object]]:
    def _key(item: dict[str, object]) -> tuple[float, int, int, str]:
        score = float(item.get("score", 0.0))
        kind = str(item.get("kind", ""))
        kind_rank = {
            "equation_explanation": 0,
            "var_in_equation_explanation": 1,
            "variables_in_equation": 2,
            "variable_explanation": 3,
        }.get(kind, 9)
        equation_id = int(item.get("_equation_id") or 10**9)
        variable_code = str(item.get("_variable_code") or "~")
        return (-score, kind_rank, equation_id, variable_code)

    return sorted(results, key=_key)


def search_explain(
    *,
    query: str,
    store: DictionaryStore,
    limit: int = 5,
    include_variable_details: bool = True,
) -> dict[str, object]:
    parsed = parse_query(query, known_variables=set(store.variables_by_code))
    warnings: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []
    results: list[dict[str, object]] = []

    if parsed.intent == "EQUATION_BY_ID":
        equation = (
            store.equations_by_id.get(parsed.equation_id)
            if parsed.equation_id is not None
            else None
        )
        if equation is None:
            errors.append({
                "code": "EQUATION_NOT_FOUND",
                "message": f"Equation {parsed.equation_id} not found.",
                "details": {"equation_id": parsed.equation_id},
            })
        else:
            results.append({
                "kind": "equation_explanation",
                "score": 1.0,
                "match_reason": ["eq_id_exact"],
                "_equation_id": equation.id,
                "payload": {
                    "equation": _build_equation_view(
                        equation,
                        store=store,
                        include_variable_details=include_variable_details,
                    )
                },
            })
    elif parsed.intent == "VARIABLE_BY_CODE":
        code = parsed.variable_code or ""
        variable = store.variables_by_code.get(code)
        if variable is None:
            errors.append({
                "code": "VARIABLE_NOT_FOUND",
                "message": f"Variable {code} not found.",
                "details": {"variable_code": code},
            })
        else:
            results.append({
                "kind": "variable_explanation",
                "score": 1.0,
                "match_reason": ["var_code_exact"],
                "_variable_code": code,
                "_equation_id": variable.defined_by_equation,
                "payload": {
                    "variable": _build_variable_view(
                        variable,
                        variable_code=code,
                        equations_by_id=store.equations_by_id,
                    )
                },
            })
    elif parsed.intent == "VARS_IN_EQUATION":
        equation = (
            store.equations_by_id.get(parsed.equation_id)
            if parsed.equation_id is not None
            else None
        )
        if equation is None:
            errors.append({
                "code": "EQUATION_NOT_FOUND",
                "message": f"Equation {parsed.equation_id} not found.",
                "details": {"equation_id": parsed.equation_id},
            })
        else:
            results.append({
                "kind": "variables_in_equation",
                "score": 1.0,
                "match_reason": ["parsed_intent_exact", "eq_id_exact"],
                "_equation_id": equation.id,
                "payload": {
                    "equation": _build_equation_view(
                        equation,
                        store=store,
                        include_variable_details=include_variable_details,
                    )
                },
            })
    elif parsed.intent == "VAR_IN_EQUATION":
        code = parsed.variable_code or ""
        equation = (
            store.equations_by_id.get(parsed.equation_id)
            if parsed.equation_id is not None
            else None
        )
        variable = store.variables_by_code.get(code)
        if equation is None:
            errors.append({
                "code": "EQUATION_NOT_FOUND",
                "message": f"Equation {parsed.equation_id} not found.",
                "details": {"equation_id": parsed.equation_id},
            })
        if variable is None:
            errors.append({
                "code": "VARIABLE_NOT_FOUND",
                "message": f"Variable {code} not found.",
                "details": {"variable_code": code},
            })
        if equation is not None:
            equation_view = _build_equation_view(
                equation,
                store=store,
                include_variable_details=include_variable_details,
            )
            all_variables = set(equation_view["all_variables"])
            present = code in all_variables
            suggested: list[int] = []
            if variable is not None:
                ordered_candidates: list[int] = []
                if variable.defined_by_equation is not None:
                    ordered_candidates.append(int(variable.defined_by_equation))
                ordered_candidates.extend(int(item) for item in variable.used_in_equations)
                seen_suggested: set[int] = set()
                for candidate in ordered_candidates:
                    if candidate in seen_suggested:
                        continue
                    seen_suggested.add(candidate)
                    suggested.append(candidate)
            if not present:
                warnings.append({
                    "code": "VAR_NOT_IN_EQUATION",
                    "message": (
                        f"{code} does not appear in equation {equation.id} per dictionary fields; "
                        "showing related equations instead."
                    ),
                    "details": {
                        "equation_id": equation.id,
                        "variable_code": code,
                        "suggested_equations": suggested,
                    },
                })
            results.append({
                "kind": "var_in_equation_explanation",
                "score": 1.0 if present else 0.85,
                "match_reason": ["parsed_intent_exact", "eq_id_exact", "var_code_exact"],
                "_equation_id": equation.id,
                "_variable_code": code,
                "payload": {
                    "variable": _build_variable_view(
                        variable,
                        variable_code=code,
                        equations_by_id=store.equations_by_id,
                    ),
                    "equation": equation_view,
                    "context": {
                        "variable_present_in_equation": present,
                        "presence_reason": (
                            "variable appears in lhs/rhs variable list"
                            if present
                            else "variable not found in lhs/rhs variable list"
                        ),
                        "suggested_equations_using_variable": suggested,
                    },
                },
            })
    else:
        terms = set(parsed.text_terms)
        query_has_equation_keyword = "equation" in parsed.normalized or "eq " in parsed.normalized
        variable_codes_in_query = [
            token
            for token in (item.upper() for item in _VARIABLE_TOKEN_RE.findall(parsed.raw))
            if token in store.variables_by_code
        ]
        if query_has_equation_keyword:
            for code in variable_codes_in_query:
                variable = store.variables_by_code[code]
                if variable.defined_by_equation is None:
                    continue
                equation = store.equations_by_id.get(variable.defined_by_equation)
                if equation is None:
                    continue
                results.append({
                    "kind": "equation_explanation",
                    "score": 1.0,
                    "match_reason": ["var_code_exact", "defined_by_equation_jump"],
                    "_equation_id": equation.id,
                    "_variable_code": code,
                    "payload": {
                        "equation": _build_equation_view(
                            equation,
                            store=store,
                            include_variable_details=include_variable_details,
                        )
                    },
                })

        candidate_equations: set[int] = set()
        candidate_variables: set[str] = set()
        for term in terms:
            candidate_equations.update(store.eq_index.get(term, set()))
            candidate_variables.update(store.var_index.get(term, set()))

        for equation_id in sorted(candidate_equations):
            equation = store.equations_by_id.get(equation_id)
            if equation is None:
                continue
            score = _rank_equation_free_text(equation, terms=terms, store=store)
            if score <= 0.0:
                continue
            results.append({
                "kind": "equation_explanation",
                "score": score,
                "match_reason": ["free_text_overlap"],
                "_equation_id": equation.id,
                "payload": {
                    "equation": _build_equation_view(
                        equation,
                        store=store,
                        include_variable_details=include_variable_details,
                    )
                },
            })

        for variable_code in sorted(candidate_variables):
            variable = store.variables_by_code.get(variable_code)
            if variable is None:
                continue
            score = _rank_variable_free_text(variable, terms=terms, store=store)
            if score <= 0.0:
                continue
            results.append({
                "kind": "variable_explanation",
                "score": score,
                "match_reason": ["free_text_overlap"],
                "_variable_code": variable_code,
                "_equation_id": variable.defined_by_equation,
                "payload": {
                    "variable": _build_variable_view(
                        variable,
                        variable_code=variable_code,
                        equations_by_id=store.equations_by_id,
                    )
                },
            })

    ranked = _sorted_results(results)
    if limit > 0:
        ranked = ranked[:limit]
    for item in ranked:
        item.pop("_equation_id", None)
        item.pop("_variable_code", None)

    return {
        "schema_version": "1.0",
        "query": {"raw": parsed.raw, "normalized": parsed.normalized},
        "parsed": {
            "intent": parsed.intent,
            "equation_id": parsed.equation_id,
            "variable_code": parsed.variable_code,
            "text_terms": list(parsed.text_terms),
            "flags": {
                "include_definitions": parsed.include_definitions,
                "include_formula": parsed.include_formula,
            },
        },
        "results": ranked,
        "warnings": warnings,
        "errors": errors,
        "meta": {
            "dictionary_version": store.dictionary_version,
            "timestamp": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "result_count": len(ranked),
            "truncated": len(results) > len(ranked),
        },
    }
