from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pandas as pd

from fppy.eq_solver import EqSpec
from fppy.paths import TEMPLATE_DIR

Number = int | float


@dataclass
class FmoutStructuredData:
    """Structured forecast sections from `fmout.txt`."""

    periods: tuple[str, ...]
    levels: pd.DataFrame
    changes: pd.DataFrame
    pct_changes: pd.DataFrame


_MARKER = "Variable   Periods forecast are"
_PERIOD_RE = re.compile(r"\b(?:19|20)\d{2}\.[1-4]\b")
_FLOAT_RE = re.compile(r"[+-]?(?:\d+\.\d*|\.\d+|\d+)(?:[Ee][+-]?\d+)?")
_VARIABLE_HEADER_RE = re.compile(
    r"^\s*(?:\d+\s+)?(?P<variable>[A-Za-z_][A-Za-z0-9_]*)\s+P\s+(?P<kind>lv|ch|%ch)\b(?P<values>.*)$"
)
_CONTINUATION_PREFIX_RE = re.compile(r"^\s*P\s+(?P<kind>lv|ch|%ch)\b(?P<values>.*)$")
_NUMERIC_CONTINUATION_RE = re.compile(r"^[\s0-9Ee+\-\.]+$")


def _extract_floats(text: str) -> list[float]:
    values: list[float] = []
    for match in _FLOAT_RE.finditer(text):
        try:
            values.append(float(match.group(0)))
        except ValueError:
            continue
    return values


def _materialize_metric_frame(
    rows: dict[str, list[float]],
    periods: tuple[str, ...],
) -> pd.DataFrame:
    if not periods:
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame(index=pd.Index(periods, name="period"))

    expected = len(periods)
    data = {}
    for variable, values in rows.items():
        padded = values[:expected]
        if len(padded) < expected:
            padded.extend([float("nan")] * (expected - len(padded)))
        data[variable] = padded

    return pd.DataFrame(data, index=pd.Index(periods, name="period"))


def extract_structured_forecast(text: str) -> FmoutStructuredData | None:
    """Parse a structured forecast block from text into period-indexed tables.

    The parser looks for the `Variable   Periods forecast are` section and extracts:
    - period headers such as `YYYY.Q`
    - variable ``P lv`` rows (plus continuations)
    - variable ``P ch`` rows (plus continuations)
    - variable ``P %ch`` rows (plus continuations)
    """

    lines = text.splitlines()
    marker_indices = [index for index, line in enumerate(lines) if _MARKER.lower() in line.lower()]
    if not marker_indices:
        return None
    marker_index = marker_indices[-1]

    periods: list[str] = []
    i = marker_index + 1
    while i < len(lines):
        line = lines[i]
        header_match = _VARIABLE_HEADER_RE.match(line)
        if header_match is not None:
            break

        for period in _PERIOD_RE.findall(line):
            if period not in periods:
                periods.append(period)
        i += 1

    if not periods:
        return None

    periods_tuple = tuple(periods)
    level_rows: dict[str, list[float]] = {}
    change_rows: dict[str, list[float]] = {}
    pct_rows: dict[str, list[float]] = {}
    current_variable: str | None = None
    current_kind: str | None = None
    started = False

    def append_values(kind: str, variable: str, values: list[float]) -> None:
        target = {
            "lv": level_rows,
            "ch": change_rows,
            "%ch": pct_rows,
        }.get(kind)
        if target is None or not variable or not values:
            return
        target.setdefault(variable, []).extend(values)

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        header_match = _VARIABLE_HEADER_RE.match(line)
        if header_match is not None:
            started = True
            current_variable = header_match.group("variable")
            current_kind = header_match.group("kind")
            append_values(
                current_kind, current_variable, _extract_floats(header_match.group("values"))
            )
            i += 1
            continue

        continuation_match = _CONTINUATION_PREFIX_RE.match(line)
        if continuation_match is not None and current_variable is not None:
            current_kind = continuation_match.group("kind")
            append_values(
                current_kind, current_variable, _extract_floats(continuation_match.group("values"))
            )
            i += 1
            continue

        if (
            current_variable is not None
            and current_kind is not None
            and _NUMERIC_CONTINUATION_RE.fullmatch(line)
        ):
            values = _extract_floats(line)
            if values:
                append_values(current_kind, current_variable, values)
            i += 1
            continue

        if started and (
            stripped.endswith(";")
            or stripped.upper().startswith("SETYYTOY")
            or stripped.upper().startswith("PRINTMODEL")
            or stripped.upper().startswith("QUIT")
            or stripped.upper().startswith("@")
        ):
            break

        i += 1

    if not started:
        return None

    return FmoutStructuredData(
        periods=periods_tuple,
        levels=_materialize_metric_frame(level_rows, periods_tuple),
        changes=_materialize_metric_frame(change_rows, periods_tuple),
        pct_changes=_materialize_metric_frame(pct_rows, periods_tuple),
    )


def _normalize_token(token: str) -> str:
    return token.strip().strip(";")


def _parse_float(token: str) -> Number | None:
    cleaned = _normalize_token(token)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _is_identifier(token: str) -> bool:
    if not token:
        return False
    return token[0].isalpha() and all(ch.isalnum() or ch == "_" for ch in token)


def extract_key_value_series(text: str) -> pd.DataFrame:
    """Extract simple numeric key/value sequences from a text block.

    Accepted formats:
    - ``KEY = 1.0``
    - ``KEY=1.0``
    - ``KEY 1.0 2.0``

    Multi-value non-keyed lines are normalized into positional columns, e.g.
    ``KEY 1 2`` -> columns ``KEY_0`` and ``KEY_1``.
    Unknown or non-numeric lines are ignored.
    """

    rows: list[dict[str, float]] = []

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("@"):
            continue

        # Direct ``KEY = VALUE`` form.
        if "=" in stripped:
            if stripped.count("=") == 1:
                left, right = stripped.split("=", 1)
                key = left.strip()
                if _is_identifier(key):
                    rhs_tokens = [token for token in right.split() if token]
                    values = [_parse_float(token) for token in rhs_tokens]
                    if values and all(v is not None for v in values):
                        row: dict[str, float] = {}
                        numeric_values = cast(list[float], values)
                        if len(numeric_values) == 1:
                            row[key] = numeric_values[0]
                        else:
                            for index, value in enumerate(numeric_values):
                                row[f"{key}_{index}"] = value
                        rows.append(row)
                        continue

            # If there are too many equals or the rhs contains text, ignore.
            continue

        # Form ``KEY VALUE [VALUE ...]``.
        parts = stripped.split()
        if len(parts) < 2:
            continue

        key = parts[0]
        if not _is_identifier(key):
            continue

        values = [_parse_float(token) for token in parts[1:]]
        if not values or any(value is None for value in values):
            continue

        numeric_values = cast(list[float], values)
        row = {}
        if len(numeric_values) == 1:
            row[key] = numeric_values[0]
        else:
            for index, value in enumerate(numeric_values):
                row[f"{key}_{index}"] = value
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def load_fmout(path: str | Path | None = None) -> pd.DataFrame:
    """Load and parse a Fair-Parke ``fmout.txt`` text block."""

    target = Path(path) if path is not None else TEMPLATE_DIR / "fmout.txt"
    text = target.read_text(encoding="utf-8", errors="replace")
    structured = extract_structured_forecast(text)
    if structured is not None and not structured.levels.empty:
        return structured.levels
    return extract_key_value_series(text)


def load_fmout_structured(path: str | Path | None = None) -> FmoutStructuredData | None:
    """Load the structured forecast block from ``fmout.txt`` if present."""

    target = Path(path) if path is not None else TEMPLATE_DIR / "fmout.txt"
    text = target.read_text(encoding="utf-8", errors="replace")
    return extract_structured_forecast(text)


def compare_numeric_dataframes(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    atol: float = 1e-8,
) -> pd.DataFrame:
    """Compare two DataFrames on shared numeric columns using absolute tolerance.

    Returns a summary frame indexed by shared numeric column name with:
    - ``max_abs_error``
    - ``mismatch_rows``
    - ``compared_rows``
    """

    shared = [
        column
        for column in left.columns.intersection(right.columns)
        if pd.api.types.is_numeric_dtype(left[column])
        and pd.api.types.is_numeric_dtype(right[column])
    ]

    if not shared:
        return pd.DataFrame(columns=["max_abs_error", "mismatch_rows", "compared_rows"])

    left_numeric = left[shared].apply(pd.to_numeric, errors="coerce")
    right_numeric = right[shared].apply(pd.to_numeric, errors="coerce")

    left_aligned, right_aligned = left_numeric.align(right_numeric, join="inner")
    if left_aligned.empty:
        return pd.DataFrame(columns=["max_abs_error", "mismatch_rows", "compared_rows"])

    absolute_diff = (left_aligned - right_aligned).abs()
    mismatches = absolute_diff.gt(atol) | absolute_diff.isna()

    return pd.DataFrame({
        "max_abs_error": absolute_diff.max(),
        "mismatch_rows": mismatches.sum(),
        "compared_rows": len(left_aligned),
    })


def compare_eq_specs(
    candidate: dict[str, EqSpec],
    baseline: dict[str, EqSpec],
    *,
    coef_atol: float = 1e-10,
) -> pd.DataFrame:
    """Compare fmout equation coefficient tables at equation/term level.

    The returned frame is indexed by equation lhs and includes:
    - candidate_terms / baseline_terms
    - candidate_missing_terms (present only in baseline)
    - candidate_extra_terms (present only in candidate)
    - equation_number_match
    - coef_mismatch_terms
    - max_coef_abs_error
    """

    lhs_names = sorted(set(candidate).union(baseline))
    if not lhs_names:
        return pd.DataFrame(
            columns=[
                "candidate_terms",
                "baseline_terms",
                "candidate_missing_terms",
                "candidate_extra_terms",
                "equation_number_match",
                "coef_mismatch_terms",
                "max_coef_abs_error",
            ]
        )

    rows: dict[str, dict[str, object]] = {}

    for lhs in lhs_names:
        candidate_spec = candidate.get(lhs)
        baseline_spec = baseline.get(lhs)

        candidate_terms = _term_map(candidate_spec)
        baseline_terms = _term_map(baseline_spec)
        candidate_keys = set(candidate_terms)
        baseline_keys = set(baseline_terms)
        shared = candidate_keys.intersection(baseline_keys)

        coef_mismatch_terms = 0
        max_coef_abs_error = 0.0
        for key in shared:
            candidate_coef = candidate_terms[key]
            baseline_coef = baseline_terms[key]
            error = abs(candidate_coef - baseline_coef)
            if error > coef_atol:
                coef_mismatch_terms += 1
            if error > max_coef_abs_error:
                max_coef_abs_error = error

        rows[lhs] = {
            "candidate_terms": len(candidate_terms),
            "baseline_terms": len(baseline_terms),
            "candidate_missing_terms": len(baseline_keys - candidate_keys),
            "candidate_extra_terms": len(candidate_keys - baseline_keys),
            "equation_number_match": (
                candidate_spec is not None
                and baseline_spec is not None
                and candidate_spec.equation_number == baseline_spec.equation_number
            ),
            "coef_mismatch_terms": coef_mismatch_terms,
            "max_coef_abs_error": max_coef_abs_error,
        }

    return pd.DataFrame.from_dict(rows, orient="index")


def _term_map(spec: EqSpec | None) -> dict[tuple[int | None, str, int], float]:
    if spec is None:
        return {}
    mapping: dict[tuple[int | None, str, int], float] = {}
    for term in spec.terms:
        key = (term.index, term.variable.upper(), term.lag)
        mapping[key] = term.coefficient
    return mapping
