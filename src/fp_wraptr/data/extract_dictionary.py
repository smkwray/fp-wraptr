"""extract_dictionary.py — FP Model Appendix A Dictionary Extractor.

Deterministic, network-free parser pipeline.
Reads pabapa.md (canonical) and writes dictionary.json.

Sections parsed
---------------
  A.2  Alphabetical variable list  (Variable | Eq. | Description | Used in Equations)
  A.3  Equations list              (stochastic paragraphs + definitional tables)
  A.5  Raw data variables          (NIPA / FoF / interest / labour raw data)
  A.7  Construction of variables   (Variable | Construction)
"""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

_HERE = Path(__file__).parent
# _HERE = …/src/fp_wraptr/data
# parents[0] = …/src/fp_wraptr
# parents[1] = …/src
# parents[2] = project root (fp-wraptr/)
_PROJ = _HERE.parents[2]
_SOURCES = _PROJ / "sources" / "2025-12-23"
PABAPA_PATH = _SOURCES / "pabapa.md"
PABWRK_PATH = _SOURCES / "pabwrk.md"
OUTPUT_PATH = _HERE / "dictionary.json"

# ── LaTeX / OCR cleaning ──────────────────────────────────────────────────────

_RE_DELIM = re.compile(r"\\\(|\\\)|\\\[|\\\]")
_RE_CMD_ARG = re.compile(r"\\(?:text|mathrm|mathbf|mathit|operatorname)\{([^}]*)\}")
_RE_FRAC = re.compile(r"\\frac\{([^}]*)\}\{([^}]*)\}")
_RE_SUBSCRIPT = re.compile(r"_\{([^}]*)\}")
_RE_SUPERSCRIPT = re.compile(r"\^\{([^}]*)\}")
_RE_CMD_BARE = re.compile(r"\\([a-zA-Z]+)")

_LATEX_REPL = {
    "Delta": "\u0394",
    "delta": "\u03b4",
    "cdot": "\u00b7",
    "times": "\u00d7",
    "leq": "\u2264",
    "geq": "\u2265",
    "neq": "\u2260",
    "alpha": "\u03b1",
    "beta": "\u03b2",
    "rho": "\u03c1",
    "theta": "\u03b8",
    "hline": "",
    "begin": "",
    "end": "",
    "tabular": "",
}


def clean_latex(text: str) -> str:
    """Strip LaTeX markup, leaving readable plain text."""
    text = _RE_DELIM.sub("", text)
    text = _RE_CMD_ARG.sub(r"\1", text)
    text = _RE_FRAC.sub(r"(\1)/(\2)", text)
    for cmd, repl in _LATEX_REPL.items():
        text = text.replace(f"\\{cmd}", repl)
    text = _RE_CMD_BARE.sub(r"\1", text)
    text = _RE_SUBSCRIPT.sub(r"_\1", text)
    text = _RE_SUPERSCRIPT.sub(r"^\1", text)
    text = text.replace("\\$", "$")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_var_code(raw: str) -> str:
    """Return clean variable code, stripping LaTeX and stray spaces."""
    s = clean_latex(raw).strip()
    # Collapse internal whitespace that OCR may insert
    s = re.sub(r"\s+", "", s)
    return s


# ── Helpers ───────────────────────────────────────────────────────────────────


def _split_cells(line: str) -> list[str]:
    """Split a Markdown pipe line into non-empty stripped cells."""
    return [c.strip() for c in line.split("|") if c.strip()]


def _is_separator_cell(cell: str) -> bool:
    return bool(re.match(r"^[-:]+$", cell))


def _is_header_row(cells: list[str]) -> bool:
    if not cells:
        return False
    first = cells[0]
    return first in ("Variable", "Eq.", "No.", "No", "Var.", "Eq") or _is_separator_cell(first)


# ── Units & Sector ────────────────────────────────────────────────────────────

_UNIT_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"B2017\$"), "B2017$"),
    (re.compile(r"B\$"), "B$"),
    (re.compile(r"\bmillions?\b", re.I), "millions"),
    (re.compile(r"percentage points?", re.I), "percentage points"),
    (re.compile(r"hours? per quarter", re.I), "hours per quarter"),
    (re.compile(r"\bpercentage\b", re.I), "percentage"),
    (re.compile(r"\bratio\b", re.I), "ratio"),
    (re.compile(r"\bindex\b", re.I), "index"),
]

_SECTOR_MAP = {
    "h": "household",
    "f": "firm",
    "b": "financial",
    "r": "foreign",
    "g": "federal_government",
    "s": "state_local",
}


def extract_units(description: str) -> str:
    for patt, unit in _UNIT_PATTERNS:
        if patt.search(description):
            return unit
    return ""


def extract_sector(description: str) -> str:
    m = re.search(r"[,\-\u2212]\s*([hfbrgs])\s*[,.\s]", description)
    if m:
        return _SECTOR_MAP.get(m.group(1), m.group(1))
    return ""


# ── Parse "used in equations" field ──────────────────────────────────────────


def parse_used_in(raw: str) -> list[int]:
    """Convert 'used in equations' string to sorted list of ints."""
    raw = clean_latex(raw).strip()
    if raw.lower() in ("none", ""):
        return []
    result: list[int] = []
    for token in re.split(r"[\s,]+", raw):
        token = token.strip()
        range_m = re.match(r"(\d+)[\u2013\-](\d+)", token)
        if range_m:
            lo, hi = int(range_m.group(1)), int(range_m.group(2))
            result.extend(range(lo, hi + 1))
        elif re.match(r"^\d+$", token):
            result.append(int(token))
    return sorted(set(result))


# ── A.2 / A.3 variable alphabetical list ─────────────────────────────────────


def parse_variable_section(text: str) -> dict[str, dict[str, Any]]:
    """Parse alphabetical variable table → dict[var_code, record]."""
    start_m = "Variables in the US Model in Alphabetical Order"
    end_m = "Equations of the US Model"

    start = text.find(start_m)
    end = text.find(end_m, start)
    if start == -1:
        logger.warning("Variable section not found; skipping")
        return {}
    if end == -1:
        end = len(text)

    section = text[start:end]
    variables: dict[str, dict[str, Any]] = {}

    for line in section.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = _split_cells(line)
        if not cells or _is_header_row(cells):
            continue
        if all(_is_separator_cell(c) for c in cells):
            continue

        # Each pipe-line may pack multiple 4-column records (OCR row collapse)
        i = 0
        while i + 3 < len(cells):
            var_raw, eq_raw, desc_raw, used_raw = (
                cells[i],
                cells[i + 1],
                cells[i + 2],
                cells[i + 3],
            )
            i += 4

            # Skip embedded headers / separators
            if var_raw in ("Variable", "Eq.", "Description", "Used in Equations"):
                continue
            if _is_separator_cell(var_raw):
                continue

            var_code = clean_var_code(var_raw)
            # Validation: must be a plausible variable code
            if not var_code or not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", var_code):
                continue
            if len(var_code) > 15:
                continue

            eq_clean = clean_latex(eq_raw).strip()
            if re.match(r"^exog", eq_clean, re.I):
                eq_num: int | None = None
                category = "exogenous"
            else:
                try:
                    eq_num = int(eq_clean)
                    category = "endogenous"
                except ValueError:
                    eq_num = None
                    category = "exogenous"

            description = clean_latex(desc_raw)
            used_eqs = parse_used_in(used_raw)

            variables[var_code] = {
                "name": var_code,
                "description": description,
                "units": extract_units(description),
                "sector": extract_sector(description),
                "category": category,
                "defined_by_equation": eq_num,
                "used_in_equations": used_eqs,
                "raw_data_sources": [],  # filled by cross-reference pass
                "construction": None,  # filled by cross-reference pass
                "_provenance": {"table": "A.2"},
            }

    logger.info("Variable section: parsed %d variables", len(variables))
    return variables


# ── A.3 equations ─────────────────────────────────────────────────────────────

_SECTOR_HEADERS = {
    "Household Sector": "household",
    "Firm Sector": "firm",
    "Financial Sector": "financial",
    "Import Equation": "import",
    "Government Sectors": "government",
    "Nominal Variables": "nominal",
    "STOCHASTIC EQUATIONS": "stochastic_header",
}


def _extract_rhs_vars(text: str) -> list[str]:
    """Extract uppercase model-variable tokens from RHS text."""
    text = clean_latex(text)
    # Uppercase identifiers ≥2 chars, starting with a letter
    candidates = re.findall(r"\b([A-Z][A-Z0-9_]{1,})\b", text)
    # Exclude obvious non-variable tokens
    stopwords = {
        "RHO",
        "SE",
        "OLS",
        "IV",
        "LS",
        "AR",
        "MA",
        "ILS",
        "TSL",
        "LHS",
        "RHS",
        "NIPA",
        "BLS",
        "BOG",
        "FRED",
        "SA",
    }
    return sorted({v for v in candidates if v not in stopwords})


def _make_eq_record(
    eq_id: int,
    lhs: str,
    formula: str,
    label: str,
    eq_type: str,
    sector: str,
) -> dict[str, Any]:
    return {
        "id": eq_id,
        "type": eq_type,
        "sector_block": sector,
        "label": label,
        "lhs_expr": lhs,
        "rhs_variables": _extract_rhs_vars(formula),
        "formula": formula,
        "_provenance": {"table": "A.3"},
    }


def parse_equation_section(text: str) -> dict[str, dict[str, Any]]:
    """Parse equation section → dict[str(eq_id), record]."""
    start_m = "Equations of the US Model"
    end_m = "Coefficient Estimates"

    start = text.find(start_m)
    end = text.find(end_m, start)
    if start == -1:
        logger.warning("Equation section not found; skipping")
        return {}
    if end == -1:
        end = len(text)

    section = text[start:end]
    equations: dict[str, dict[str, Any]] = {}
    current_sector = "unknown"

    lines = section.splitlines()

    # ── Pass 1: stochastic equations (numbered paragraphs) ────────────────
    for line in lines:
        stripped = line.strip()

        # Track sector
        for header, sector_val in _SECTOR_HEADERS.items():
            if header in stripped:
                current_sector = sector_val
                break

        # Match stochastic equation line:  "N  LHS_EXPR  ... [label]"
        m = re.match(r"^(\d{1,2})\s+(.+?)\s*\[([^\]]+)\]\s*$", stripped)
        if m:
            eq_id = int(m.group(1))
            body = m.group(2)
            label = m.group(3).strip()
            # First whitespace-separated token is LHS
            parts = body.split()
            lhs = clean_latex(parts[0]) if parts else ""
            formula = clean_latex(" ".join(parts[1:]))
            equations[str(eq_id)] = _make_eq_record(
                eq_id, lhs, formula, label, "stochastic", current_sector
            )
            continue

        # Truncated stochastic eq (no label bracket)
        m2 = re.match(r"^(\d{1,2})\s+(\S+(?:\([^)]*\))?)\s+(.+)$", stripped)
        if m2 and int(m2.group(1)) <= 30 and str(m2.group(1)) not in equations:
            eq_id = int(m2.group(1))
            lhs = clean_latex(m2.group(2))
            formula = clean_latex(m2.group(3))
            equations[str(eq_id)] = _make_eq_record(
                eq_id, lhs, formula, "", "stochastic", current_sector
            )

    # ── Pass 2: markdown table equations (3-column: Eq | LHS | Formula) ──
    in_table = False
    table_rows: list[str] = []

    def flush_table(rows: list[str]) -> None:
        """Process accumulated markdown table rows."""
        for row in rows:
            cells = _split_cells(row)
            if not cells:
                continue
            # Row may pack multiple eq records (OCR collapse)
            i = 0
            while i < len(cells):
                first = cells[i].strip("* ")
                # Is this cell an equation number?
                m = re.match(r"^(\d+)$", first)
                if m and i + 2 < len(cells):
                    eq_id = int(m.group(1))
                    lhs_raw = cells[i + 1]
                    formula_raw = cells[i + 2] if i + 2 < len(cells) else ""
                    lhs = clean_latex(lhs_raw).rstrip("= ").strip()
                    formula = clean_latex(formula_raw)
                    # Extract label from brackets
                    label_m = re.search(r"\[([^\]]+)\]", formula_raw)
                    label = label_m.group(1) if label_m else ""
                    if label:
                        formula = re.sub(r"\[[^\]]+\]", "", formula).strip()
                    if str(eq_id) not in equations:
                        equations[str(eq_id)] = _make_eq_record(
                            eq_id, lhs, formula, label, "definitional", current_sector
                        )
                    i += 3
                else:
                    # Could be a label continuation | | | [label] or sector header
                    label_m = re.search(r"\[([^\]]+)\]", first)
                    if label_m and equations:
                        last_key = max(equations.keys(), key=lambda k: int(k))
                        if not equations[last_key]["label"]:
                            equations[last_key]["label"] = label_m.group(1)
                    i += 1

    for line in lines:
        stripped = line.strip()

        # Track sector in tables too
        for header, sector_val in _SECTOR_HEADERS.items():
            if header in stripped:
                current_sector = sector_val
                break

        if "| Eq. |" in stripped and "LHS Variable" in stripped:
            in_table = True
            table_rows = []
            continue

        if in_table:
            if stripped.startswith("|---"):
                continue
            if not stripped.startswith("|"):
                flush_table(table_rows)
                table_rows = []
                in_table = False
                continue
            table_rows.append(stripped)

    if in_table and table_rows:
        flush_table(table_rows)

    # ── Pass 3: LaTeX tabular equations (121-169, Nominal section) ────────
    # Format: N & LHS= & formula \\ [next row: & & [label] \\]
    _parse_latex_tabular_equations(section, equations, "nominal")

    logger.info("Equation section: parsed %d equations", len(equations))
    return equations


def _parse_latex_tabular_equations(
    text: str, equations: dict[str, dict[str, Any]], sector: str
) -> None:
    """Parse equations in LaTeX & (ampersand-separated) table rows."""
    last_id: int | None = None
    for line in text.splitlines():
        stripped = line.strip()
        # LaTeX row: tokens separated by &
        if "&" not in stripped:
            continue
        parts = [p.strip() for p in stripped.split("&")]
        if not parts:
            continue
        first = parts[0].strip("* \t\\")
        m = re.match(r"^(\d+)$", first)
        if m and len(parts) >= 2:
            eq_id = int(m.group(1))
            lhs = clean_latex(parts[1]).rstrip("= ").strip()
            formula = clean_latex(parts[2]) if len(parts) > 2 else ""
            if str(eq_id) not in equations:
                equations[str(eq_id)] = _make_eq_record(
                    eq_id, lhs, formula, "", "definitional", sector
                )
            last_id = eq_id
        elif last_id is not None and not first and len(parts) >= 3:
            # Continuation row: & & [label]
            label_text = clean_latex(parts[2])
            label_m = re.search(r"\[([^\]]+)\]", label_text)
            if label_m and not equations[str(last_id)]["label"]:
                equations[str(last_id)]["label"] = label_m.group(1)


# ── A.5 raw data variables ────────────────────────────────────────────────────


def parse_raw_data_section(text: str) -> dict[str, dict[str, Any]]:
    """Parse the raw data variables table → dict[r_code, record]."""
    start_m = "The Raw Data Variables for the US Model"
    end_m_options = [
        "Table A.6",
        "Links Between the National Income",
        "Construction of the Variables",
    ]

    start = text.find(start_m)
    if start == -1:
        logger.warning("Raw data section not found; skipping")
        return {}

    end = len(text)
    for em in end_m_options:
        pos = text.find(em, start)
        if pos != -1 and pos < end:
            end = pos

    section = text[start:end]
    raw_data: dict[str, dict[str, Any]] = {}
    current_source = "NIPA"

    for line in section.splitlines():
        stripped = line.strip()

        # Source type markers
        if "Flow of Funds" in stripped or "Code" in stripped:
            if not stripped.startswith("|"):
                current_source = "FlowOfFunds"
        elif "Interest Rate Data" in stripped and not stripped.startswith("|"):
            current_source = "InterestRate"
        elif (
            "Labor Force" in stripped or "Population Data" in stripped
        ) and not stripped.startswith("|"):
            current_source = "LaborForce"
        elif "Adjustment" in stripped and "Raw Data" in stripped and not stripped.startswith("|"):
            current_source = "Adjustment"

        if not stripped.startswith("|"):
            continue

        cells = _split_cells(stripped)
        if not cells or _is_header_row(cells):
            continue
        if all(_is_separator_cell(c) for c in cells):
            continue

        # Records may be collapsed; scan for R-number tokens
        i = 0
        while i < len(cells):
            r_match = re.match(r"^(R\d+)$", cells[i])
            if r_match and i + 1 < len(cells):
                r_code = r_match.group(1)
                var_name = cells[i + 1] if i + 1 < len(cells) else ""
                # Description varies by section type
                if current_source == "NIPA" and i + 4 < len(cells):
                    tbl = cells[i + 2]
                    line_num = cells[i + 3]
                    desc = cells[i + 4]
                    raw_data[r_code] = {
                        "r_number": r_code,
                        "variable": var_name,
                        "source_type": current_source,
                        "table": tbl,
                        "line": line_num,
                        "description": desc,
                    }
                    i += 5
                elif current_source == "FlowOfFunds" and i + 3 < len(cells):
                    code = cells[i + 2]
                    desc = cells[i + 3] if i + 3 < len(cells) else ""
                    raw_data[r_code] = {
                        "r_number": r_code,
                        "variable": var_name,
                        "source_type": current_source,
                        "code": code,
                        "description": desc,
                    }
                    i += 4
                else:
                    desc = " | ".join(cells[i + 2 : i + 4])
                    raw_data[r_code] = {
                        "r_number": r_code,
                        "variable": var_name,
                        "source_type": current_source,
                        "description": desc,
                    }
                    i += 3
            else:
                i += 1

    logger.info("Raw data section: parsed %d R-variables", len(raw_data))
    return raw_data


# ── A.7 construction of variables ─────────────────────────────────────────────


def parse_construction_section(text: str) -> dict[str, str]:
    """Parse construction table → dict[var_code, construction_text]."""
    start_m = "Construction of the Variables for the US Model"
    start = text.find(start_m)
    if start == -1:
        logger.warning("Construction section not found; skipping")
        return {}

    section = text[start:]
    construction: dict[str, str] = {}

    for line in section.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = _split_cells(stripped)
        if not cells or _is_header_row(cells):
            continue
        if all(_is_separator_cell(c) for c in cells):
            continue

        # 2-column records (may be collapsed)
        i = 0
        while i + 1 < len(cells):
            var_raw = cells[i]
            const_raw = cells[i + 1]
            i += 2

            var = clean_var_code(var_raw)
            if not var or _is_separator_cell(var):
                continue
            if not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", var):
                continue
            if len(var) > 15:
                continue

            const = clean_latex(const_raw).strip()
            # Skip header-like entries
            if var in ("Variable", "Construction"):
                continue

            construction[var] = const

    logger.info("Construction section: parsed %d entries", len(construction))
    return construction


# ── Cross-reference: attach raw_data_sources from alphabetical lookup ─────────


def _build_var_to_rnums(raw_data: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    """Map FP variable name → list of R-codes from raw data section."""
    mapping: dict[str, list[str]] = {}
    for r_code, rd in raw_data.items():
        fp_var = rd.get("variable", "").strip()
        if fp_var:
            mapping.setdefault(fp_var, []).append(r_code)
    return mapping


# ── Supplement: add variables discovered in other sections ────────────────────


def _supplement_variables(
    variables: dict[str, dict[str, Any]],
    equations: dict[str, dict[str, Any]],
    construction: dict[str, str],
) -> None:
    """Add variable stubs for codes found in construction/equations but not
    in the main alphabetical table (OCR gaps like UR, U, UB, etc.)."""

    def _add_stub(code: str, eq_id: int | None = None, const: str | None = None) -> None:
        if code in variables:
            return
        if not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", code):
            return
        if len(code) > 15:
            return
        category = "exogenous" if eq_id is None else "endogenous"
        variables[code] = {
            "name": code,
            "description": "",
            "units": "",
            "sector": "",
            "category": category,
            "defined_by_equation": eq_id,
            "used_in_equations": [],
            "raw_data_sources": [],
            "construction": const,
            "_provenance": {"table": "supplement", "source": "construction/equations"},
        }
        logger.debug("Supplemented variable stub: %s (eq=%s)", code, eq_id)

    # From construction table
    for code, const in construction.items():
        eq_id = None
        m = re.search(r"Def\.,\s*Eq\.\s*(\d+)", const)
        if m:
            eq_id = int(m.group(1))
        _add_stub(code, eq_id, const)

    # From equation LHS variables
    for eq_rec in equations.values():
        lhs = eq_rec.get("lhs_expr", "")
        # Extract primary variable code from LHS expression
        m = re.match(r"^([A-Z][A-Z0-9_]{1,})$", lhs.strip())
        if m:
            eq_id = eq_rec.get("id")
            _add_stub(m.group(1), eq_id)

    n_added = sum(
        1 for v in variables.values() if v.get("_provenance", {}).get("table") == "supplement"
    )
    if n_added:
        logger.info("Supplemented %d variable stubs from construction/equations", n_added)


# ── Main extraction orchestrator ──────────────────────────────────────────────


def extract(
    pabapa_path: Path = PABAPA_PATH,
    output_path: Path = OUTPUT_PATH,
) -> dict[str, Any]:
    """Run full pipeline and write dictionary.json.

    Args:
        pabapa_path: Path to pabapa.md source.
        output_path: Destination for dictionary.json.

    Returns:
        The dictionary data structure.
    """
    if not pabapa_path.exists():
        raise FileNotFoundError(f"Source not found: {pabapa_path}")

    logger.info("Reading %s", pabapa_path)
    text = pabapa_path.read_text(encoding="utf-8", errors="replace")

    # Parse sections
    variables = parse_variable_section(text)
    equations = parse_equation_section(text)
    raw_data = parse_raw_data_section(text)
    construction = parse_construction_section(text)

    # Supplement variables with any codes found in construction or equations
    # that the OCR-collapsed variable table may have dropped.
    _supplement_variables(variables, equations, construction)

    # Cross-reference
    var_to_rnums = _build_var_to_rnums(raw_data)
    for var_code, var_record in variables.items():
        var_record["raw_data_sources"] = var_to_rnums.get(var_code, [])
        var_record["construction"] = construction.get(var_code)

    result: dict[str, Any] = {
        "model_version": "2025-12-23",
        "source": {
            "pabapa_md": str(pabapa_path),
            "extraction_timestamp": datetime.now(UTC).isoformat(),
        },
        "variables": variables,
        "equations": equations,
        "raw_data": raw_data,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info(
        "Wrote %s  (%d variables, %d equations)",
        output_path,
        len(variables),
        len(equations),
    )
    return result


# ── CLI entry point ───────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s  %(message)s",
        stream=sys.stderr,
    )
    import argparse

    parser = argparse.ArgumentParser(description="Extract FP dictionary from pabapa.md")
    parser.add_argument("--source", type=Path, default=PABAPA_PATH)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    result = extract(args.source, args.output)
    print(f"Done — {len(result['variables'])} variables, {len(result['equations'])} equations")


if __name__ == "__main__":
    main()
