"""Human-readable scenario DSL compiler.

This module provides a lightweight line-oriented DSL that compiles into
``ScenarioConfig`` so users can author scenarios without verbose YAML.

Example DSL::

    scenario baseline_plus
    description "Higher growth + lower rates"
    forecast 2025.4 to 2029.4
    track PCY,UR,GDPR
    set YS CHGSAMEPCT 0.008
    alert UR max 6.0
    patch cmd:SETUPSOLVE.MAXCHECK 80
    policy monetary_rule rate=4.0 method=SAMEVALUE
"""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

import yaml

from fp_wraptr.scenarios.config import ScenarioConfig, VariableOverride
from fp_wraptr.scenarios.policies import PolicyRegistry

__all__ = ["DSLCompileError", "compile_scenario_dsl_file", "compile_scenario_dsl_text"]

_ALLOWED_ALERT_BOUNDS = {"min", "max"}


class DSLCompileError(ValueError):
    """Raised when DSL text cannot be compiled into a valid scenario."""

    def __init__(self, message: str, line_no: int | None = None) -> None:
        self.line_no = line_no
        if line_no is None:
            super().__init__(message)
        else:
            super().__init__(f"Line {line_no}: {message}")


def compile_scenario_dsl_file(path: Path | str) -> ScenarioConfig:
    """Compile a DSL file into ``ScenarioConfig``."""
    path = Path(path)
    text = path.read_text(encoding="utf-8")
    return compile_scenario_dsl_text(text, default_name=path.stem)


def compile_scenario_dsl_text(text: str, default_name: str | None = None) -> ScenarioConfig:
    """Compile DSL text into ``ScenarioConfig``.

    Args:
        text: DSL source text.
        default_name: Optional fallback scenario name if no ``scenario`` command is present.
    """
    data: dict[str, Any] = {
        "name": default_name or "",
        "description": "",
        "fp_home": Path("FM"),
        "input_file": "fminput.txt",
        "forecast_start": "2025.4",
        "forecast_end": "2029.4",
        "overrides": {},
        "track_variables": ["PCY", "PCPF", "UR", "PIEF", "GDPR"],
        "input_patches": {},
        "alerts": {},
        "extra": {},
    }
    compiled_policy_summaries: list[dict[str, Any]] = []

    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        tokens = _tokenize_line(raw_line, line_no)
        if not tokens:
            continue

        command = tokens[0].lower()
        args = tokens[1:]

        if command == "scenario":
            _require_arg_count(command, args, min_count=1, line_no=line_no)
            data["name"] = args[0]
            continue

        if command == "description":
            _require_arg_count(command, args, min_count=1, line_no=line_no)
            data["description"] = " ".join(args)
            continue

        if command == "fp_home":
            _require_arg_count(command, args, exact_count=1, line_no=line_no)
            data["fp_home"] = Path(args[0])
            continue

        if command == "input_file":
            _require_arg_count(command, args, exact_count=1, line_no=line_no)
            data["input_file"] = args[0]
            continue

        if command == "forecast":
            start, end = _parse_forecast_args(args, line_no)
            data["forecast_start"] = start
            data["forecast_end"] = end
            continue

        if command == "track":
            _require_arg_count(command, args, min_count=1, line_no=line_no)
            data["track_variables"] = _parse_track_args(args)
            continue

        if command == "set":
            _require_arg_count(command, args, exact_count=3, line_no=line_no)
            var_name = args[0].upper()
            method = args[1].upper()
            try:
                value = float(args[2])
            except ValueError as exc:
                raise DSLCompileError(
                    f"Invalid numeric override value: {args[2]}", line_no
                ) from exc
            data["overrides"][var_name] = VariableOverride(method=method, value=value)
            continue

        if command == "alert":
            _require_arg_count(command, args, exact_count=3, line_no=line_no)
            var_name = args[0].upper()
            bound = args[1].lower()
            if bound not in _ALLOWED_ALERT_BOUNDS:
                allowed = ", ".join(sorted(_ALLOWED_ALERT_BOUNDS))
                raise DSLCompileError(f"Alert bound must be one of: {allowed}", line_no)
            try:
                value = float(args[2])
            except ValueError as exc:
                raise DSLCompileError(f"Invalid alert value: {args[2]}", line_no) from exc
            data["alerts"].setdefault(var_name, {})[bound] = value
            continue

        if command == "patch":
            _require_arg_count(command, args, min_count=2, line_no=line_no)
            key = args[0]
            value = " ".join(args[1:])
            data["input_patches"][key] = value
            continue

        if command == "policy":
            _require_arg_count(command, args, min_count=1, line_no=line_no)
            policy_type = args[0].strip()
            policy_fields = _parse_key_value_args(args[1:], line_no)
            policy_data = {"type": policy_type, **policy_fields}
            try:
                policy_block = PolicyRegistry.create(policy_data)
            except Exception as exc:
                raise DSLCompileError(f"Invalid policy block: {exc}", line_no) from exc
            data["overrides"].update(policy_block.compile())
            compiled_policy_summaries.append(policy_block.to_summary())
            continue

        if command == "extra":
            _require_arg_count(command, args, min_count=1, line_no=line_no)
            data["extra"].update(_parse_key_value_args(args, line_no))
            continue

        raise DSLCompileError(f"Unknown DSL command '{command}'", line_no)

    if not data["name"]:
        raise DSLCompileError(
            "Scenario name is required (use `scenario <name>` or provide default_name)",
            line_no=None,
        )

    if compiled_policy_summaries:
        data["extra"]["compiled_policies"] = compiled_policy_summaries

    try:
        return ScenarioConfig(**data)
    except Exception as exc:
        raise DSLCompileError(f"Compiled scenario is invalid: {exc}") from exc


def _tokenize_line(raw_line: str, line_no: int) -> list[str]:
    try:
        return shlex.split(raw_line, comments=True, posix=True)
    except ValueError as exc:
        raise DSLCompileError(f"Tokenization error: {exc}", line_no) from exc


def _require_arg_count(
    command: str,
    args: list[str],
    line_no: int,
    exact_count: int | None = None,
    min_count: int | None = None,
) -> None:
    if exact_count is not None and len(args) != exact_count:
        raise DSLCompileError(
            f"Command '{command}' expects exactly {exact_count} argument(s), got {len(args)}",
            line_no,
        )
    if min_count is not None and len(args) < min_count:
        raise DSLCompileError(
            f"Command '{command}' expects at least {min_count} argument(s), got {len(args)}",
            line_no,
        )


def _parse_forecast_args(args: list[str], line_no: int) -> tuple[str, str]:
    if len(args) == 2:
        return args[0], args[1]
    if len(args) == 3 and args[1].lower() == "to":
        return args[0], args[2]
    raise DSLCompileError(
        "Forecast syntax must be `forecast <start> <end>` or `forecast <start> to <end>`",
        line_no,
    )


def _parse_track_args(args: list[str]) -> list[str]:
    values: list[str] = []
    for token in args:
        for part in token.split(","):
            cleaned = part.strip()
            if cleaned:
                values.append(cleaned.upper())
    return values


def _parse_key_value_args(args: list[str], line_no: int) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for item in args:
        if "=" not in item:
            raise DSLCompileError(
                f"Expected key=value argument, got '{item}'",
                line_no,
            )
        key, raw_value = item.split("=", 1)
        if not key:
            raise DSLCompileError(f"Missing key in argument '{item}'", line_no)
        parsed[key] = yaml.safe_load(raw_value)
    return parsed
