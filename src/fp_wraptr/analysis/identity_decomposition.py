"""Identity decomposition helpers for backend comparison.

These helpers are intentionally limited to additive FP identities such as:

    PIEF=XX+PIV*IVF+SUBS+SUBG-USOTHER-RNT

The goal is not to parse arbitrary FP syntax. It is to produce a stable,
readable artifact showing which terms drive a live backend disagreement at
one period.
"""

from __future__ import annotations

import ast
import csv
import json
import math
import re
from pathlib import Path
from typing import Any

from fp_wraptr.analysis.backend_defensibility import ENGINE_ORDER, _normalize_engine_paths
from fp_wraptr.io.loadformat import read_loadformat

_NAME_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")


def _parse_period_key(token: str) -> tuple[int, int]:
    text = str(token).strip()
    if "." not in text:
        return (0, 0)
    year_text, quarter_text = text.split(".", 1)
    try:
        return (int(year_text), int(quarter_text))
    except ValueError:
        return (0, 0)


def _split_identity(identity: str) -> tuple[str, str]:
    text = str(identity or "").strip().rstrip(";")
    if "=" not in text:
        raise ValueError("Identity must contain '='")
    lhs, rhs = text.split("=", 1)
    lhs = lhs.strip()
    rhs = rhs.strip()
    if not lhs or not rhs:
        raise ValueError("Identity must include both lhs and rhs")
    return lhs, rhs


def _split_additive_terms(rhs: str) -> list[tuple[int, str]]:
    terms: list[tuple[int, str]] = []
    current: list[str] = []
    depth = 0
    sign = 1
    index = 0
    text = rhs.strip()
    while index < len(text):
        char = text[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(depth - 1, 0)
        elif depth == 0 and char in "+-":
            previous = text[index - 1] if index > 0 else ""
            if index == 0 or previous in "+-*/(^":
                current.append(char)
                index += 1
                continue
            token = "".join(current).strip()
            if token:
                terms.append((sign, token))
            sign = 1 if char == "+" else -1
            current = []
            index += 1
            continue
        current.append(char)
        index += 1
    token = "".join(current).strip()
    if token:
        terms.append((sign, token))
    return terms


def _validate_expr(node: ast.AST) -> None:
    allowed_binary = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow)
    allowed_unary = (ast.UAdd, ast.USub)
    if isinstance(node, ast.Expression):
        _validate_expr(node.body)
        return
    if isinstance(node, ast.BinOp):
        if not isinstance(node.op, allowed_binary):
            raise ValueError("Unsupported operator in identity expression")
        _validate_expr(node.left)
        _validate_expr(node.right)
        return
    if isinstance(node, ast.UnaryOp):
        if not isinstance(node.op, allowed_unary):
            raise ValueError("Unsupported unary operator in identity expression")
        _validate_expr(node.operand)
        return
    if isinstance(node, ast.Name):
        return
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return
    raise ValueError("Unsupported syntax in identity expression")


def _evaluate_expression(expression: str, values: dict[str, float]) -> float:
    translated = expression.replace("^", "**")
    node = ast.parse(translated, mode="eval")
    _validate_expr(node)
    compiled = compile(node, "<identity>", "eval")
    names = {name: float(value) for name, value in values.items()}
    try:
        result = eval(compiled, {"__builtins__": {}}, names)
    except NameError as exc:
        raise ValueError(f"Missing series for expression '{expression}': {exc}") from None
    if not isinstance(result, (int, float)) or not math.isfinite(float(result)):
        raise ValueError(f"Non-finite result for expression '{expression}'")
    return float(result)


def _collect_period_rows(engine_paths: dict[str, Path], period: str) -> dict[str, dict[str, float]]:
    rows: dict[str, dict[str, float]] = {}
    for engine, path in engine_paths.items():
        periods, series = read_loadformat(path)
        period_tokens = [str(item) for item in periods]
        if period not in period_tokens:
            raise ValueError(f"Period {period} not found in {engine} artifact: {path}")
        index = period_tokens.index(period)
        row: dict[str, float] = {}
        for name, values in series.items():
            if index >= len(values):
                continue
            value = values[index]
            if isinstance(value, (int, float)):
                row[str(name)] = float(value)
        rows[engine] = row
    return rows


def build_identity_decomposition_report(
    engine_paths: dict[str, Path | str],
    *,
    identity: str,
    period: str,
) -> dict[str, Any]:
    normalized_paths = _normalize_engine_paths(engine_paths)
    lhs, rhs = _split_identity(identity)
    terms = _split_additive_terms(rhs)
    if not terms:
        raise ValueError("Identity rhs did not contain any terms")
    period_rows = _collect_period_rows(normalized_paths, period)

    term_rows: list[dict[str, Any]] = []
    totals = {engine: 0.0 for engine in ENGINE_ORDER}
    for sign, term in terms:
        entry: dict[str, Any] = {
            "sign": "+" if sign >= 0 else "-",
            "term": term,
            "signed_term": f"{'+' if sign >= 0 else '-'}{term}",
        }
        for engine in ENGINE_ORDER:
            value = _evaluate_expression(term, period_rows[engine])
            signed_value = float(sign) * float(value)
            entry[f"{engine}_term_value"] = value
            entry[f"{engine}_signed_value"] = signed_value
            totals[engine] += signed_value
        entry["fpr_minus_fppy"] = entry["fp-r_signed_value"] - entry["fppy_signed_value"]
        entry["fpr_minus_fpexe"] = entry["fp-r_signed_value"] - entry["fpexe_signed_value"]
        entry["fppy_minus_fpexe"] = entry["fppy_signed_value"] - entry["fpexe_signed_value"]
        entry["max_pair_abs_diff"] = max(
            abs(float(entry["fpr_minus_fppy"])),
            abs(float(entry["fpr_minus_fpexe"])),
            abs(float(entry["fppy_minus_fpexe"])),
        )
        term_rows.append(entry)

    term_rows.sort(key=lambda item: float(item["max_pair_abs_diff"]), reverse=True)

    emitted_values = {}
    for engine in ENGINE_ORDER:
        row = period_rows[engine]
        if lhs not in row:
            raise ValueError(f"Target series {lhs} not present in {engine} artifact")
        emitted_values[engine] = float(row[lhs])

    return {
        "engine_paths": {engine: str(path) for engine, path in normalized_paths.items()},
        "target": lhs,
        "identity": f"{lhs}={rhs}",
        "period": period,
        "term_count": len(term_rows),
        "term_rows": term_rows,
        "reconstructed_totals": totals,
        "emitted_target_values": emitted_values,
        "target_pair_diffs": {
            "fpr_minus_fppy": emitted_values["fp-r"] - emitted_values["fppy"],
            "fpr_minus_fpexe": emitted_values["fp-r"] - emitted_values["fpexe"],
            "fppy_minus_fpexe": emitted_values["fppy"] - emitted_values["fpexe"],
        },
    }


def write_identity_decomposition_report(
    report: dict[str, Any],
    *,
    output_dir: Path | str,
) -> tuple[Path, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "identity_decomposition_report.json"
    csv_path = out_dir / "identity_decomposition_terms.csv"

    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    fieldnames = [
        "sign",
        "term",
        "fpexe_term_value",
        "fpexe_signed_value",
        "fppy_term_value",
        "fppy_signed_value",
        "fp-r_term_value",
        "fp-r_signed_value",
        "fpr_minus_fppy",
        "fpr_minus_fpexe",
        "fppy_minus_fpexe",
        "max_pair_abs_diff",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in report.get("term_rows", []):
            writer.writerow({name: row.get(name, "") for name in fieldnames})

    return report_path, csv_path
