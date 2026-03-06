"""Utilities for generating FP input artifacts and patched inputs.

This module writes FP exogenous variable override files and applies text-level
patches to base FP input files, including `INPUT FILE` reference updates for
generated ``fmexog`` inputs.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path

_COMMAND_PATCH_KEY_RE = re.compile(
    r"^cmd:(?P<command>[A-Za-z_][A-Za-z0-9_]*)(?:\[(?P<index>\d+)\])?\.(?P<param>[A-Za-z_][A-Za-z0-9_]*)$"
)


def _normalize_scalar(value: object) -> float:
    if isinstance(value, bool):
        raise TypeError("Boolean is not a valid override value")
    if isinstance(value, (int, float)):
        return float(value)
    raise TypeError(f"Unsupported override value type: {type(value).__name__}")


def _normalize_period_value_pair(value: object) -> tuple[str, float]:
    if not isinstance(value, Sequence) or isinstance(value, str):
        raise TypeError("Expected a (period, value) pair")

    if len(value) != 2:
        raise TypeError("Expected exactly two items in a (period, value) pair")

    period, amount = value
    if not isinstance(period, str):
        raise TypeError(f"Period must be a string, got {type(period).__name__}")

    return period, _normalize_scalar(amount)


def _is_series_value(value: object) -> bool:
    if isinstance(value, str):
        return False
    if not isinstance(value, Sequence):
        return False

    return len(value) > 0 and all(isinstance(item, Sequence) and len(item) == 2 for item in value)


def _sort_period(period: str) -> tuple[int, int]:
    try:
        year, quarter = period.split(".")
        return int(year), int(quarter)
    except Exception:
        return (0, 0)


def _emit_constant_override(
    var_name: str,
    method: str,
    value: float,
    lines: list[str],
) -> None:
    lines.append(f"{var_name} {method}")
    lines.append(str(value))


def _emit_series_override(
    var_name: str,
    method: str,
    values: list[tuple[str, float]],
    lines: list[str],
) -> None:
    for period, value in sorted(values, key=lambda item: _sort_period(item[0])):
        lines.extend([
            f"SMPL {period} {period};",
            "CHANGEVAR;",
            f"{var_name} {method}",
            str(value),
            ";",
        ])


def write_exogenous_file(
    variables: dict[str, dict],
    sample_start: str,
    sample_end: str,
    output_path: Path,
) -> Path:
    """Write an FP exogenous variable file (CHANGEVAR format).

    Args:
        variables: Dict mapping variable names to {method, value} dicts.
            method: one of CHGSAMEPCT, SAMEVALUE, CHGSAMEABS
            value: numeric value or list of (period, value) pairs
        sample_start: Start period, e.g. "2025.4"
        sample_end: End period, e.g. "2029.4"
        output_path: Where to write the file.

    Returns:
        Path to the written file.
    """
    lines = [
        f"SMPL {sample_start} {sample_end};",
        "CHANGEVAR;",
    ]

    for var_name, spec in variables.items():
        method = spec.get("method", "SAMEVALUE")
        value = spec.get("value", 0.0)

        if _is_series_value(value):
            pairs = [
                _normalize_period_value_pair(item)
                for item in value  # type: ignore[arg-type]
            ]
            _emit_series_override(var_name, method, pairs, lines)
        else:
            _emit_constant_override(var_name, method, _normalize_scalar(value), lines)

    lines.append(";")
    lines.append("RETURN;")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def write_exogenous_override_file(
    *,
    base_fmexog: Path,
    variables: dict[str, dict],
    sample_start: str,
    sample_end: str,
    output_path: Path,
) -> Path:
    """Write an exogenous override file that *layers on top of* a base fmexog.

    The stock FM model ships with an `fmexog.txt` that already contains many
    baseline CHANGEVAR adjustments. Scenario overrides should not replace those
    baseline exogenous settings; they should be applied in addition.

    If base_fmexog is missing, this falls back to writing an override-only file
    (equivalent to write_exogenous_file()).
    """

    if not base_fmexog.exists():
        return write_exogenous_file(variables, sample_start, sample_end, output_path)

    # fp.exe is sensitive to newline conventions in some environments. The stock FM
    # templates ship with CRLF; preserve that when layering overrides so fp.exe
    # parses the merged file the same way it parses the baseline template.
    base_bytes = base_fmexog.read_bytes()
    newline = "\r\n" if b"\r\n" in base_bytes else "\n"

    base_lines = base_bytes.decode("utf-8", errors="replace").splitlines()
    # FP-style CHANGEVAR scripts typically stop at RETURN;. We remove the final RETURN
    # so we can append scenario overrides and then re-add RETURN at the end.
    cut = None
    for idx in range(len(base_lines) - 1, -1, -1):
        if base_lines[idx].strip().upper() == "RETURN;":
            cut = idx
            break
    if cut is not None:
        base_lines = base_lines[:cut]

    # Build override blocks.
    override_lines: list[str] = [
        f"SMPL {sample_start} {sample_end};",
        "CHANGEVAR;",
    ]

    series_blocks: list[str] = []
    for var_name, spec in variables.items():
        method = spec.get("method", "SAMEVALUE")
        value = spec.get("value", 0.0)

        if _is_series_value(value):
            pairs = [
                _normalize_period_value_pair(item)
                for item in value  # type: ignore[arg-type]
            ]
            _emit_series_override(var_name, method, pairs, series_blocks)
        else:
            _emit_constant_override(var_name, method, _normalize_scalar(value), override_lines)

    override_lines.append(";")
    merged = [*base_lines, "", *override_lines, *series_blocks, "RETURN;"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(newline.join(merged) + newline, encoding="utf-8")
    return output_path


def patch_input_file(
    base_input: Path,
    overrides: dict[str, str],
    output_path: Path,
) -> Path:
    """Create a modified FP input file by applying text-level overrides.

    Supports two patch styles:
      1) Literal replacement (legacy): ``{"old text": "new text"}``
      2) Command-aware parameter patch:
         ``{"cmd:SETUPSOLVE.MAXCHECK": "80"}``
         ``{"cmd:SETUPSOLVE[1].MINITERS": "40"}``

    Command-aware patches update FP command parameters without relying on
    fragile full-command string matches.

    Args:
        base_input: Path to the original fminput.txt.
        overrides: Dict of {search_string: replacement_string} pairs.
        output_path: Where to write the patched file.

    Returns:
        Path to the written file.

    TODO: Replace with AST-level manipulation once input parser is complete.
    """
    text = base_input.read_text(encoding="utf-8", errors="replace")

    separated = _split_command_overrides(overrides)

    for search, replace in separated["command"]:
        text = _apply_command_param_override(text, search, replace)

    for search, replace in separated["literal"]:
        text = text.replace(search, replace)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path


def _split_command_overrides(overrides: dict[str, str]) -> dict[str, list[tuple[str, str]]]:
    command: list[tuple[str, str]] = []
    literal: list[tuple[str, str]] = []

    for key, value in overrides.items():
        if key.startswith("cmd:"):
            command.append((key, value))
        else:
            literal.append((key, value))

    return {"command": command, "literal": literal}


def _apply_command_param_override(text: str, key: str, value: str) -> str:
    parsed = _parse_command_patch_key(key)
    command = parsed["command"]
    param = parsed["param"]
    index = parsed["index"]

    command_pattern = re.compile(
        rf"(?im)(^\s*{re.escape(command)}\b)(?P<body>[^;]*)(;)",
        re.IGNORECASE | re.MULTILINE,
    )
    matches = list(command_pattern.finditer(text))
    if index >= len(matches):
        raise ValueError(
            f"Command patch target not found for '{key}' "
            f"(found {len(matches)} occurrence(s) of {command})"
        )

    match = matches[index]
    body_start, body_end = match.span("body")
    updated_body = _upsert_command_param(match.group("body"), param, value)
    return text[:body_start] + updated_body + text[body_end:]


def _parse_command_patch_key(key: str) -> dict[str, str | int]:
    match = _COMMAND_PATCH_KEY_RE.match(key)
    if not match:
        raise ValueError(
            f"Invalid command patch key '{key}'. "
            "Expected format: cmd:COMMAND.PARAM or cmd:COMMAND[index].PARAM"
        )

    command = match.group("command").upper()
    param = match.group("param").upper()
    index_text = match.group("index")
    index = int(index_text) if index_text is not None else 0
    return {"command": command, "param": param, "index": index}


def _upsert_command_param(body: str, param: str, value: str) -> str:
    trailing_whitespace = body[len(body.rstrip()) :]
    core = body[: len(body.rstrip())]
    param_pattern = re.compile(
        rf"(?i)(\b{re.escape(param)}\s*=\s*)([^\s,]+)",
        re.IGNORECASE,
    )

    if param_pattern.search(core):
        core = param_pattern.sub(lambda match: f"{match.group(1)}{value}", core, count=1)
        return core + trailing_whitespace

    separator = ""
    if core and not core.endswith((" ", "\t", "\n")):
        separator = " "
    core = f"{core}{separator}{param}={value}"
    return core + trailing_whitespace


def patch_fmexog_reference(
    base_input: Path,
    fmexog_path: Path,
    output_path: Path,
) -> Path:
    """Patch INPUT FILE references to an FMEXOG-style file path.

    Args:
        base_input: Path to source input file.
        fmexog_path: Path to the replacement exogenous input file.
        output_path: Output path for patched file.

    Returns:
        Path to patched file.
    """
    text = base_input.read_text(encoding="utf-8", errors="replace")
    pattern = re.compile(
        r"(?im)^(\s*INPUT\s+FILE\s*=\s*)([^\r\n;]*)(\s*;[^\r\n]*)$",
        re.IGNORECASE,
    )
    replacement = rf"\1{fmexog_path.name}\3"

    if pattern.search(text):
        text = pattern.sub(replacement, text, count=1)
    else:
        raise ValueError(f"No INPUT FILE directive found in {base_input}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path
