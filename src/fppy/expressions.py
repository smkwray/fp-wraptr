"""Utilities for parsing and evaluating FP assignment expressions."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Final

import numpy as np
import pandas as pd

_ASSIGNMENT_RE: Final = re.compile(
    r"^(?P<command>GENR|IDENT|LHS|CREATE)\s+(?P<body>.+)$",
    re.IGNORECASE | re.DOTALL,
)
_LAG_RE: Final = re.compile(r"\b(?P<name>[A-Za-z_][A-Za-z0-9_]*)\(\s*(?P<lag>[+-]?\d+)\s*\)")
_FUNCTION_RE: Final = re.compile(r"\b(?P<func>LOG|EXP|ABS)\s*\(", re.IGNORECASE)
_COEF_CALL_RE: Final = re.compile(
    r"\bCOEF\s*\(\s*(?P<row>[+-]?\d+)\s*,\s*(?P<col>[+-]?\d+)\s*\)",
    re.IGNORECASE,
)
_IDENTIFIER_RE: Final = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
_SERIES_TOKEN_RE: Final = re.compile(r"[^A-Za-z0-9_]")
_TRAILING_SEMICOLON_RE: Final = re.compile(r";\s*$")
_INTERNAL_WHITESPACE_RE: Final = re.compile(r"\s+")


@dataclass(frozen=True)
class Assignment:
    """Parsed GENR/IDENT assignment statement."""

    command: str
    lhs: str
    rhs: str


@dataclass(frozen=True)
class EvalContext:
    """Optional evaluation context for legacy scalar lookups."""

    coef_values: dict[tuple[int, int], float] = field(default_factory=dict)
    coef_default: float = 0.0


@dataclass(frozen=True)
class _ScalarLagRef:
    token: str
    name: str
    lag: int


@dataclass(frozen=True)
class _PreparedScalarExpression:
    code: object
    lag_refs: tuple[_ScalarLagRef, ...]
    scalar_names: tuple[str, ...]


def parse_assignment(statement: str) -> Assignment:
    """Parse an assignment statement into command, LHS, and RHS strings."""

    text = statement.strip()
    if not text:
        raise ValueError("statement must not be empty")

    match = _ASSIGNMENT_RE.match(text)
    command = "IDENT"
    body = text
    if match:
        command = match.group("command").upper()
        body = match.group("body")

    parts = body.split("=", maxsplit=1)
    if len(parts) != 2:
        raise ValueError("assignment statement must contain '='")

    lhs = parts[0].strip()
    rhs = _TRAILING_SEMICOLON_RE.sub("", parts[1].strip())
    if not lhs or not rhs:
        raise ValueError("assignment statement must have both lhs and rhs")

    return Assignment(command=command, lhs=lhs, rhs=rhs)


def _to_lag_token(name: str, lag: int) -> str:
    normalized_name = _SERIES_TOKEN_RE.sub("_", name.upper())
    normalized_lag = f"L{lag:+d}".replace("+", "P").replace("-", "N")
    return f"__LAG_{normalized_name}_{normalized_lag}__"


def _resolve_column(data: pd.DataFrame, name: str) -> pd.Series:
    if name in data.columns:
        return data[name]

    name_lower = name.lower()
    for col in data.columns:
        if str(col).lower() == name_lower:
            return data[col]
    raise KeyError(f"column '{name}' not present in input data")


def _replace_lag_terms(expression: str, data: pd.DataFrame) -> tuple[str, dict[str, pd.Series]]:
    lagged: dict[str, pd.Series] = {}

    def _replace(match: re.Match[str]) -> str:
        series_name = match.group("name")
        lag = int(match.group("lag"))
        token = _to_lag_token(series_name, lag)
        lagged[token] = _resolve_column(data, series_name).shift(-lag)
        return token

    replaced = _LAG_RE.sub(_replace, expression)
    return replaced, lagged


def _replace_functions(expression: str) -> str:
    def _function_replacer(match: re.Match[str]) -> str:
        # Deprecated: prefer `_replace_functions_surface(..., surface_name="fp")`
        # so `pd.eval(..., engine="python")` does not hit pandas/numpy module-attr
        # edge cases (e.g. `np.log(...)`).
        #
        # Context: pandas 2.3.x can raise `TypeError: '>' not supported ...` when
        # resolving the *numpy module* name in an eval scope (pandas-dev/pandas#58041).
        return f"fp.{match.group('func').lower()}("

    return _FUNCTION_RE.sub(_function_replacer, expression)


def _replace_functions_surface(expression: str, *, surface_name: str) -> str:
    def _function_replacer(match: re.Match[str]) -> str:
        return f"{surface_name}.{match.group('func').lower()}("

    return _FUNCTION_RE.sub(_function_replacer, expression)


def _replace_legacy_scalar_calls(expression: str, eval_context: EvalContext | None) -> str:
    context = eval_context or EvalContext()

    def _coef_replacer(match: re.Match[str]) -> str:
        row = int(match.group("row"))
        col = int(match.group("col"))
        value = context.coef_values.get((row, col), context.coef_default)
        return repr(float(value))

    return _COEF_CALL_RE.sub(_coef_replacer, expression)


def _normalize_expression(expression: str) -> str:
    expr = _TRAILING_SEMICOLON_RE.sub("", expression.strip())
    expr = _INTERNAL_WHITESPACE_RE.sub(" ", expr).strip()
    if not expr:
        raise ValueError("expression must be non-empty")
    return expr


def _resolve_period_position(
    index: pd.Index,
    *,
    period: object,
    period_position: int | None = None,
) -> int:
    if period_position is not None:
        if period_position < 0 or period_position >= len(index):
            raise IndexError("period_position is out of range for data index")
        return period_position

    loc = index.get_loc(period)
    if isinstance(loc, slice):
        raise ValueError(f"period {period!r} resolves to multiple index rows")
    if isinstance(loc, np.ndarray):
        if loc.size != 1:
            raise ValueError(f"period {period!r} resolves to multiple index rows")
        loc = int(loc[0])
    return int(loc)


def _prepare_scalar_expression(
    normalized_expression: str,
    *,
    eval_context: EvalContext | None = None,
) -> _PreparedScalarExpression:
    lag_refs: list[_ScalarLagRef] = []
    seen_tokens: set[str] = set()

    def _lag_replacer(match: re.Match[str]) -> str:
        name = match.group("name")
        lag = int(match.group("lag"))
        token = _to_lag_token(name, lag)
        if token not in seen_tokens:
            lag_refs.append(_ScalarLagRef(token=token, name=name, lag=lag))
            seen_tokens.add(token)
        return token

    expr_with_lag = _LAG_RE.sub(_lag_replacer, normalized_expression)
    # Use a generic function surface ("fp") so we can swap numpy vs math backends
    # without recompiling different token spellings for each backend.
    expr_with_functions = _replace_functions_surface(expr_with_lag, surface_name="fp")
    expr_prepared = _replace_legacy_scalar_calls(expr_with_functions, eval_context)

    scalar_names: list[str] = []
    seen_names: set[str] = set()
    for match in _IDENTIFIER_RE.finditer(expr_prepared):
        token = match.group(0)
        token_lower = token.lower()
        if token == "fp" or token.startswith("__LAG_"):
            continue
        if token_lower in {"log", "exp", "abs"}:
            continue
        if token in seen_names:
            continue
        scalar_names.append(token)
        seen_names.add(token)

    return _PreparedScalarExpression(
        code=compile(expr_prepared, "<fp-scalar-expression>", "eval"),
        lag_refs=tuple(lag_refs),
        scalar_names=tuple(scalar_names),
    )


@lru_cache(maxsize=512)
def _prepare_scalar_expression_default_cached(
    normalized_expression: str,
) -> _PreparedScalarExpression:
    return _prepare_scalar_expression(normalized_expression, eval_context=None)


def _fp_surface(kind: str) -> object:
    """Return a function surface object exposing .log/.exp/.abs."""

    resolved = str(kind).strip().lower()
    if resolved == "numpy":
        # Avoid exposing numpy module-attribute calls (`np.log(...)`) to
        # pandas' expression engine; route through a plain object instead.
        # See pandas-dev/pandas#58041 for the upstream resolver bug this avoids.
        class _NumpySurface:
            @staticmethod
            def log(x: object) -> object:
                return np.log(x)

            @staticmethod
            def exp(x: object) -> object:
                return np.exp(x)

            @staticmethod
            def abs(x: object) -> object:
                return np.abs(x)

        return _NumpySurface()
    if resolved != "math":
        raise ValueError("math_backend must be 'numpy' or 'math'")

    # Mirror numpy behavior for scalar domain/overflow edges: return NaN rather
    # than throwing into the solver.
    class _MathSurface:
        @staticmethod
        def log(x: float) -> float:
            try:
                return float(math.log(float(x)))
            except (ValueError, OverflowError):
                return float("nan")

        @staticmethod
        def exp(x: float) -> float:
            try:
                return float(math.exp(float(x)))
            except (ValueError, OverflowError):
                return float("nan")

        @staticmethod
        def abs(x: float) -> float:
            try:
                return float(abs(float(x)))
            except (TypeError, ValueError):
                return float("nan")

    return _MathSurface()


def evaluate_expression(
    expression: str,
    *,
    data: pd.DataFrame,
    eval_context: EvalContext | None = None,
    raise_fp_errors: bool = False,
) -> pd.Series:
    """Evaluate an FP RHS expression over a pandas frame.

    Example:
        evaluate_expression("LOG(X)+Y(-1)", data=df)
    """

    if not isinstance(data, pd.DataFrame):
        raise TypeError("data must be a pandas DataFrame")

    if not isinstance(expression, str):
        raise TypeError("expression must be a string")

    expr = _normalize_expression(expression)

    expr_with_lag, lagged = _replace_lag_terms(expr, data)
    expr_with_functions = _replace_functions_surface(expr_with_lag, surface_name="fp")
    expr_with_functions = _replace_legacy_scalar_calls(expr_with_functions, eval_context)

    scope: dict[str, pd.Series | float | object] = {"np": np, "fp": _fp_surface("numpy")}
    scope.update(lagged)
    for column in data.columns:
        scope[str(column)] = data[column]
        if isinstance(column, str):
            scope[column.upper()] = data[column]
            scope[column.lower()] = data[column]

    fp_mode = "raise" if raise_fp_errors else "ignore"
    # FP expressions routinely hit domain/overflow edges; keep these as NaN
    # by default, but allow opt-in fail-fast trapping for diagnostics.
    with np.errstate(divide=fp_mode, invalid=fp_mode, over=fp_mode, under="ignore"):
        result = pd.eval(
            expr_with_functions, engine="python", local_dict=scope, global_dict={"np": np}
        )

    if isinstance(result, pd.Series):
        return result

    if np.isscalar(result):
        return pd.Series(result, index=data.index, dtype="float64")

    raise ValueError("expression must evaluate to a pandas Series")


def evaluate_expression_at_period(
    expression: str,
    *,
    data: pd.DataFrame,
    period: object,
    period_position: int | None = None,
    eval_context: EvalContext | None = None,
    eval_precision: str = "float64",
    math_backend: str = "numpy",
    raise_fp_errors: bool = False,
) -> float:
    """Evaluate an FP RHS expression for a single period in the frame."""

    if not isinstance(data, pd.DataFrame):
        raise TypeError("data must be a pandas DataFrame")

    if not isinstance(expression, str):
        raise TypeError("expression must be a string")

    position = _resolve_period_position(
        data.index,
        period=period,
        period_position=period_position,
    )
    resolved_eval_precision = str(eval_precision).strip().lower()
    if resolved_eval_precision not in {"float64", "longdouble"}:
        raise ValueError("eval_precision must be 'float64' or 'longdouble'")
    expr = _normalize_expression(expression)
    prepared = (
        _prepare_scalar_expression_default_cached(expr)
        if eval_context is None
        else _prepare_scalar_expression(expr, eval_context=eval_context)
    )

    source_cache: dict[str, pd.Series] = {}

    def _get_source(name: str) -> pd.Series:
        key = name.lower()
        if key in source_cache:
            return source_cache[key]
        source = _resolve_column(data, name)
        source_cache[key] = source
        return source

    fp = _fp_surface(math_backend)
    scope: dict[str, float | object] = {}
    use_longdouble = resolved_eval_precision == "longdouble"
    for lag_ref in prepared.lag_refs:
        source = _get_source(lag_ref.name)
        shifted_position = position + lag_ref.lag
        if shifted_position < 0 or shifted_position >= len(source):
            scope[lag_ref.token] = np.longdouble(np.nan) if use_longdouble else float("nan")
        else:
            raw_value = source.iat[shifted_position]
            if use_longdouble:
                scope[lag_ref.token] = np.longdouble(float(raw_value))
            else:
                scope[lag_ref.token] = float(raw_value)

    for name in prepared.scalar_names:
        source = _get_source(name)
        raw_value = source.iat[position]
        if use_longdouble:
            scope[name] = np.longdouble(float(raw_value))
        else:
            scope[name] = raw_value

    fp_mode = "raise" if raise_fp_errors else "ignore"
    with np.errstate(divide=fp_mode, invalid=fp_mode, over=fp_mode, under="ignore"):
        result = eval(prepared.code, {"fp": fp, "__builtins__": {}}, scope)

    if np.isscalar(result):
        return float(result)

    if isinstance(result, pd.Series):
        return float(result.iat[position])

    raise ValueError("expression must evaluate to a scalar value")


def apply_assignment(
    statement: str,
    *,
    data: pd.DataFrame,
    inplace: bool = False,
    eval_context: EvalContext | None = None,
) -> pd.DataFrame:
    """Apply a GENR/IDENT assignment statement to a DataFrame."""

    assignment = parse_assignment(statement)
    target = data if inplace else data.copy()

    # FP decks sometimes use `GENR X=X;` as an identity storage/materialization
    # step. Treat this as a no-op that guarantees the target column exists.
    rhs_key = assignment.rhs.strip().upper()
    lhs_key = assignment.lhs.strip().upper()
    if rhs_key == lhs_key:
        resolved_lhs = assignment.lhs
        if resolved_lhs not in target.columns:
            for col in target.columns:
                if str(col).upper() == lhs_key:
                    resolved_lhs = str(col)
                    break
        if resolved_lhs not in target.columns:
            target[assignment.lhs] = pd.Series(np.nan, index=target.index, dtype="float64")
        return target

    target[assignment.lhs] = evaluate_expression(
        assignment.rhs,
        data=target,
        eval_context=eval_context,
    )
    return target
