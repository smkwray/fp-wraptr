"""Parse FP input and related data-format files.

Parsers included:
- FP input file (`fminput.txt`) command-oriented parser.
- FP model data file (`fmdata.txt`) with `SMPL` / `LOAD` / values blocks.
- FM exogenous file (`fmexog.txt`) with `SMPL` / `CHANGEVAR` variable overrides.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

# Reuse a permissive number parser for packed scientific notation like
# ``0.1-0.2`` and ``-0.45280E-01-0.54776E-01``, while also accepting integer
# tokens such as ``0`` and ``1`` used in many CHANGEVAR decks.
_FLOAT_RE = re.compile(r"-?(?:\d+(?:\.\d*)?|\.\d+)(?:E[+-]?\d+)?", re.IGNORECASE)


_EXOG_METHODS = {"CHGSAMEPCT", "SAMEVALUE", "CHGSAMEABS", "ADDDIFABS"}


_KNOWN_FP_COMMANDS = {
    "SPACE",
    "SETUPSOLVE",
    "SETUPEST",
    "SMPL",
    "LOADDATA",
    "CREATE",
    "GENR",
    "EQ",
    "MODEQ",
    "LHS",
    "IDENT",
    "EXOGENOUS",
    "SOLVE",
    "EXTRAPOLATE",
    "TEST",
    "PRINTVAR",
    "PRINTMODEL",
    "PRINTNAMES",
    "CHANGEVAR",
    "INPUT",
    "RETURN",
    "QUIT",
    "EXIT",
    "END",
}


def parse_fp_input(path: Path | str) -> dict:
    """Parse an FP input file into a structured dictionary.

    Args:
        path: Path to fminput.txt or similar FP input file.

    Returns:
        Structured dictionary with parsed command sections.
    """
    path = Path(path)
    text = path.read_text(encoding="utf-8", errors="replace")
    return parse_fp_input_text(text)


def parse_fp_input_text(text: str) -> dict[str, Any]:
    """Parse FP input text into structured fields.

    The parser is line-oriented at the command level and splits on semicolons,
    which is how FP consumes commands.
    """
    lines = _strip_comments(text)
    title = _extract_title(lines)
    parse_lines = lines[1:] if title and lines and lines[0] == title else lines
    result: dict[str, Any] = {
        "title": title,
        "raw_text": text,
        "space": {},
        "commands": [],
        "commands_by_type": {},
        "samples": [],
        "load_data": [],
        "setupsolve": [],
        "setupect": [],
        "creates": [],
        "generated_vars": [],
        "equations": [],
        "equation_lhs": [],
        "identities": [],
        "modeqs": [],
        "solve": {},
        "solve_commands": [],
        "extrapolate": [],
        "printvar": [],
        "printmodel": 0,
        "printnames": 0,
        "exogenous": [],
        "changevar_blocks": [],
        "control_commands": [],
    }

    for command in _split_commands(parse_lines):
        command = command.strip()
        if not command:
            continue

        pieces = command.split(None, 1)
        command_name = pieces[0].upper()
        body = pieces[1].strip() if len(pieces) > 1 else ""

        command_record = {"name": command_name, "body": body}
        result["commands"].append(command_record)
        command_key = _normalize_command_key(command_name)
        result.setdefault("commands_by_type", {}).setdefault(command_key, []).append(
            body,
        )

        if command_name == "SPACE":
            result["space"] = _parse_param_assignments(body)
        elif command_name == "SETUPSOLVE":
            result["setupsolve"].append(_parse_param_assignments(body))
        elif command_name == "SETUPEST":
            parsed = _parse_param_assignments(body)
            result["setupect"].append(parsed)
        elif command_name == "SMPL":
            sample = _parse_sample(body)
            if sample:
                result["samples"].append(sample)
        elif command_name == "LOADDATA":
            load_data = _parse_param_assignments(body)
            filename = load_data.get("file")
            if filename:
                result["load_data"].append(filename)
        elif command_name == "CREATE":
            result["creates"].append(_parse_named_assignment(body))
        elif command_name == "GENR":
            result["generated_vars"].append(_parse_named_assignment(body))
        elif command_name == "EQ":
            equation = _parse_equation(body)
            if equation:
                result["equations"].append(equation)
        elif command_name == "MODEQ":
            modeq = body.strip()
            if modeq:
                result["modeqs"].append(modeq)
        elif command_name == "LHS":
            lhs = _parse_named_assignment(body)
            lhs["type"] = "lhs"
            result["equation_lhs"].append(lhs)
        elif command_name == "IDENT":
            result["identities"].append(_parse_named_assignment(body))
        elif command_name == "EXOGENOUS":
            result["exogenous"].append(body.strip())
        elif command_name == "SOLVE":
            parsed = _parse_solve_command(body)
            result["solve"] = parsed
            result["solve_commands"].append(parsed)
        elif command_name == "EXTRAPOLATE":
            result["extrapolate"].append({"raw": body})
        elif command_name == "TEST":
            result.setdefault("tests", []).append(_split_words(body))
        elif command_name == "PRINTVAR":
            result["printvar"].append(_parse_printvar(body))
        elif command_name == "PRINTMODEL":
            result["printmodel"] += 1
        elif command_name == "PRINTNAMES":
            result["printnames"] += 1
        elif command_name == "CHANGEVAR":
            result["changevar_blocks"].append(_parse_changevar_block(body))
        elif command_name in {"INPUT", "RETURN", "QUIT", "EXIT", "END"}:
            result["control_commands"].append(command_record)
        else:
            result.setdefault("unhandled_commands", []).append(command_record)

    # Backward-compatible summary keys expected by existing callers.
    # `solve` used to be a simple dict and is now always present.
    if not result["solve"]:
        result["solve"] = {}

    return result


def parse_fm_data(path: Path | str) -> dict[str, Any]:
    """Parse a FP model data file (fmdata.txt)."""
    path = Path(path)
    text = path.read_text(encoding="utf-8", errors="replace")
    return parse_fm_data_text(text)


def parse_fm_data_text(text: str) -> dict[str, Any]:
    """Parse fmdata text.

    FM data files are sectioned by:
    SMPL <start> <end> ;
    LOAD <name> ;
      values...
    'END'
    """
    lines = _strip_comments(text)

    result: dict[str, Any] = {
        "sample_start": "",
        "sample_end": "",
        "series": {},  # name -> list[block]
        "blocks": [],
    }

    current_sample = {"start": "", "end": ""}
    current_load: str | None = None
    current_values: list[float] = []

    def flush_load() -> None:
        nonlocal current_load, current_values
        if not current_load:
            return

        block = {
            "name": current_load,
            "sample_start": current_sample["start"],
            "sample_end": current_sample["end"],
            "values": current_values,
        }
        result["blocks"].append(block)
        result["series"].setdefault(current_load, []).append(block)
        current_load = None
        current_values = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        sample_match = re.match(
            r"^SMPL\s+(\d{4}\.\d)\s+(\d{4}\.\d)\s*;?$", stripped, re.IGNORECASE
        )
        if sample_match:
            flush_load()
            start, end = sample_match.groups()
            current_sample["start"] = start
            current_sample["end"] = end
            if not result["sample_start"]:
                result["sample_start"] = start
                result["sample_end"] = end
            continue

        if stripped.upper().startswith("END"):
            if current_load:
                flush_load()
            if stripped.upper() in {"END;", "END"}:
                continue

        load_match = re.match(r"^LOAD\s+([A-Za-z0-9_]+)\s*;?$", stripped, re.IGNORECASE)
        if load_match:
            flush_load()
            current_load = load_match.group(1)
            continue

        if current_load:
            floats = _parse_floats(stripped)
            if floats:
                current_values.extend(floats)

    flush_load()
    return result


def parse_fmexog(path: Path | str) -> dict[str, Any]:
    """Parse a FP exogenous variable file (fmexog.txt)."""
    path = Path(path)
    text = path.read_text(encoding="utf-8", errors="replace")
    return parse_fmexog_text(text)


def parse_fmexog_text(text: str) -> dict[str, Any]:
    """Parse fmexog text.

    FM exogenous files are organized as:
    SMPL <start> <end> ;
    CHANGEVAR;
    VAR METHOD
        value
    VAR2 VALUE...
    ...
    RETURN;
    """
    lines = _strip_comments(text)

    result: dict[str, Any] = {
        "sample_start": "",
        "sample_end": "",
        "blocks": [],
        "changes": [],
    }

    current_block: dict[str, Any] | None = None
    in_changevar = False
    current_change: dict[str, Any] | None = None

    def flush_change() -> None:
        nonlocal current_change
        if current_block is None or not current_change:
            current_change = None
            return
        # Keep both flattened and per-block change views.
        result["changes"].append(current_change)
        current_block["changes"].append(current_change)
        current_change = None

    def flush_block() -> None:
        nonlocal current_block, in_changevar, current_change
        if current_block is None:
            return
        flush_change()
        result["blocks"].append(current_block)
        if not result["sample_start"]:
            result["sample_start"] = current_block["sample_start"]
            result["sample_end"] = current_block["sample_end"]
        current_block = None
        in_changevar = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        sample_match = re.match(
            r"^SMPL\s+(\d{4}\.\d)\s+(\d{4}\.\d)\s*;?$", stripped, re.IGNORECASE
        )
        if sample_match:
            flush_block()
            start, end = sample_match.groups()
            current_block = {
                "sample_start": start,
                "sample_end": end,
                "changes": [],
            }
            continue

        if stripped.upper().startswith("CHANGEVAR"):
            in_changevar = True
            continue

        if stripped.upper().startswith("RETURN"):
            flush_block()
            break

        if current_block is None:
            continue

        if in_changevar:
            if _is_variable_header(stripped):
                flush_change()
                variable, method, inline_values = _parse_changevar_header(stripped)
                current_change = {
                    "sample_start": current_block["sample_start"],
                    "sample_end": current_block["sample_end"],
                    "variable": variable,
                    "method": method,
                    "values": list(inline_values),
                }
                continue

            values = _parse_floats(stripped)
            if values and current_change is not None:
                current_change["values"].extend(values)
                continue

            if stripped.upper().startswith("CHANGEVAR"):
                flush_change()
                in_changevar = True

        if stripped.upper() in {"SMPL", "RETURN"}:
            continue

    flush_block()
    return result


def _strip_comments(text: str) -> list[str]:
    """Strip comments and blank lines while preserving semantic lines."""
    lines: list[str] = []
    for raw in text.splitlines():
        line = raw.rstrip("\n\r")
        if line.lstrip().startswith("@"):  # full-line comment
            continue
        if "@" in line:
            line = line[: line.index("@")]
        line = line.strip()
        if line:
            lines.append(line)
    return lines


def _extract_title(lines: list[str]) -> str:
    """Extract the likely title line."""
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        first_token = stripped.split(None, 1)[0].upper()
        if first_token in _KNOWN_FP_COMMANDS:
            return ""
        return stripped
    return ""


def _split_commands(lines: list[str]) -> list[str]:
    """Split preprocessed lines into command texts by semicolon."""
    text = "\n".join(lines)
    parts = text.split(";")
    return [part.strip() for part in parts if part.strip()]


def _normalize_command_key(command: str) -> str:
    """Normalize command keys to canonical snake_case-style names."""
    if not command:
        return ""

    normalized = command.strip().upper()
    return normalized.lower()


def _parse_sample(text: str) -> dict[str, str] | None:
    match = re.match(r"^(\d{4}\.\d)\s+(\d{4}\.\d)", text)
    if not match:
        return None
    return {"start": match.group(1), "end": match.group(2)}


def _parse_param_assignments(text: str) -> dict[str, str]:
    params = {}
    for key, value in re.findall(r"([A-Za-z][A-Za-z0-9_]*)\s*=\s*([^\s]+)", text):
        params[_normalize_param_key(key)] = value
    return params


def _split_words(text: str) -> list[str]:
    return [token for token in text.split() if token]


def _parse_named_assignment(text: str) -> dict[str, Any]:
    if "=" in text:
        left, right = [part.strip() for part in text.split("=", 1)]
        return {
            "name": left,
            "value": right,
            "expression": right,
            "raw": text.strip(),
        }

    return {
        "name": text.strip().split()[0] if text.strip() else "",
        "value": "",
        "expression": "",
        "raw": text.strip(),
    }


def _parse_equation(text: str) -> dict[str, Any] | None:
    match = re.match(r"^(\d+)\s+(.*)$", text.strip(), re.DOTALL)
    if not match:
        return None

    number = int(match.group(1))
    body = match.group(2).strip()
    tokens = body.split()
    lhs = tokens[0] if tokens else ""
    rhs = " ".join(tokens[1:]) if len(tokens) > 1 else ""

    equation = {
        "number": number,
        "lhs": lhs,
        "rhs": rhs,
        "raw": body,
        "options": {},
    }

    rho_match = re.search(r"RHO\s*=\s*(-?\d+)", body, re.IGNORECASE)
    if rho_match:
        equation["options"][_normalize_param_key("RHO")] = int(rho_match.group(1))

    return equation


def _parse_solve_command(text: str) -> dict[str, Any]:
    tokens = _split_words(text)
    params = {}
    flags = []
    for token in tokens:
        if "=" in token:
            lhs, rhs = token.split("=", 1)
            params[_normalize_param_key(lhs)] = rhs
        else:
            flags.append(token)

    return {
        "raw": text,
        "flags": flags,
        "params": params,
    }


def _parse_printvar(text: str) -> dict[str, Any]:
    if not text:
        return {"raw": "", "variables": []}

    tokens = _split_words(text)
    result: dict[str, Any] = {"raw": text}
    if "FILEOUT=" in text.upper():
        result["file"] = _parse_param_assignments(text).get("fileout")
        result["variables"] = [
            token for token in tokens[1:] if not token.upper().startswith("FILEOUT=")
        ]
    else:
        result["variables"] = tokens
    return result


def _normalize_param_key(raw_key: str) -> str:
    """Normalize a parsed FP parameter key to canonical snake_case-like naming."""
    key = raw_key.strip()
    if not key:
        return ""
    if re.fullmatch(r"[A-Z0-9_]+", key):
        return key.lower()
    normalized = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key)
    normalized = re.sub(r"[^0-9A-Za-z_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.lower().strip("_")


def _parse_changevar_block(text: str) -> list[dict[str, Any]]:
    if not text:
        return []

    return _parse_fm_changevar_lines(text.splitlines())


def _parse_fm_changevar_lines(lines: list[str]) -> list[dict[str, Any]]:
    # Parses either raw `CHANGEVAR` blocks from fmexog or inline text blocks.
    parsed: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    def _flush() -> None:
        nonlocal current
        if current is None:
            return
        parsed.append(current)
        current = None

    for raw in lines:
        stripped = raw.strip()
        if not stripped:
            continue

        if _is_variable_header(stripped):
            _flush()
            variable, method, values = _parse_changevar_header(stripped)
            current = {"variable": variable, "method": method, "values": list(values)}
            continue

        if current is None:
            continue

        values = _parse_floats(stripped)
        if values:
            current["values"].extend(values)
            continue

        # Stop on block delimiters
        if stripped.upper().startswith("CHANGEVAR") or stripped.upper().startswith("SMPL"):
            _flush()

    _flush()
    return parsed


def _parse_changevar_header(line: str) -> tuple[str, str | None, list[float]]:
    normalized = line.replace(";", " ").strip()
    parts = normalized.split()
    if not parts:
        raise ValueError("empty changevar header")

    variable = parts[0]
    method = None
    remaining_values: list[float] = []

    if len(parts) >= 2 and parts[1].upper() in _EXOG_METHODS:
        method = parts[1].upper()
        if len(parts) > 2:
            remaining_values.extend(_parse_floats(" ".join(parts[2:])))
    elif len(parts) >= 2 and not _looks_like_float(parts[1]):
        # Rare command-like variant; keep variable-only header.
        method = None
        if len(parts) > 1:
            remaining_values.extend(_parse_floats(" ".join(parts[1:])))
    elif len(parts) >= 2:
        # Fallback: treat second token as value if it parses as float.
        remaining_values.extend(_parse_floats(" ".join(parts[1:])))

    return variable, method, remaining_values


def _is_variable_header(line: str) -> bool:
    # Matches variable names like CCQ, INTGZ, D20214.
    if not line:
        return False

    if line.upper().startswith("SMPL") or line.upper().startswith("CHANGEVAR"):
        return False

    if _looks_like_float(line.split()[0]):
        return False

    first_token = line.split()[0].replace(";", "")
    return bool(re.match(r"^[A-Za-z][A-Za-z0-9_]*$", first_token))


def _parse_floats(text: str) -> list[float]:
    # Supports packed scientific notation and trailing dots.
    matches = _FLOAT_RE.findall(text)
    values: list[float] = []
    for item in matches:
        if not item:
            continue
        try:
            values.append(float(item))
        except ValueError:
            continue
    return values


def _looks_like_float(value: str) -> bool:
    return bool(
        re.fullmatch(
            r"-?(?:\d+(?:\.\d*)?|\.\d+)(?:E[+-]?\d+)?",
            value,
            re.IGNORECASE,
        )
    )
