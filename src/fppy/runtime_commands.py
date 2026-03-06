"""Structured parsing helpers for non-assignment FP runtime commands."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TypeAlias

from fppy.parser import FPCommand, FPCommandRecord

_FILE_ARG_RE = re.compile(
    r"\bFILE(?:OUT)?\s*=\s*(?P<value>[^\s;]+)",
    re.IGNORECASE,
)
_VARIABLE_ARG_RE = re.compile(
    r"\bVARIABLE\s*=\s*(?P<value>[^\s;]+)",
    re.IGNORECASE,
)
_WORD_RE = re.compile(r"[^\s;]+")


@dataclass(frozen=True)
class LoadDataCommand:
    file: str


@dataclass(frozen=True)
class InputCommand:
    file: str


@dataclass(frozen=True)
class ExogenousCommand:
    variable: str


@dataclass(frozen=True)
class SolveCommand:
    mode: str | None
    outside: bool
    noreset: bool
    filevar: str | None
    keyboard_targets: tuple[str, ...]
    raw_tokens: tuple[str, ...]


@dataclass(frozen=True)
class PrintVarCommand:
    fileout: str | None
    loadformat: bool
    filevar: str | None = None
    stats: bool = False
    variables: tuple[str, ...] = ()
    raw_tokens: tuple[str, ...] = ()


@dataclass(frozen=True)
class SetYyToYCommand:
    pass


@dataclass(frozen=True)
class ExtrapolateCommand:
    pass


RuntimeCommand: TypeAlias = (
    LoadDataCommand
    | InputCommand
    | ExogenousCommand
    | SolveCommand
    | PrintVarCommand
    | SetYyToYCommand
    | ExtrapolateCommand
)


def _extract_file_arg(statement: str) -> str | None:
    match = _FILE_ARG_RE.search(statement)
    if match is None:
        return None
    return str(match.group("value")).strip().strip("\"'")


def _extract_variable_arg(statement: str) -> str | None:
    match = _VARIABLE_ARG_RE.search(statement)
    if match is None:
        return None
    return str(match.group("value")).strip().strip("\"'")


def _statement_tokens(statement: str) -> tuple[str, ...]:
    return tuple(token.strip().strip("\"'") for token in _WORD_RE.findall(statement))


def _normalize_keyboard_target(token: str) -> str:
    cleaned = token.strip().strip("\"'").strip().rstrip(";").strip()
    return cleaned.upper()


def parse_runtime_command(record: FPCommandRecord) -> RuntimeCommand | None:
    """Parse a known runtime command record into a typed payload."""
    statement = record.statement
    command = record.command

    if command == FPCommand.LOADDATA:
        file_arg = _extract_file_arg(statement)
        if not file_arg:
            return None
        return LoadDataCommand(file=file_arg)

    if command == FPCommand.INPUT:
        file_arg = _extract_file_arg(statement)
        if not file_arg:
            return None
        return InputCommand(file=file_arg)

    if command == FPCommand.EXOGENOUS:
        variable = _extract_variable_arg(statement)
        if not variable:
            return None
        return ExogenousCommand(variable=variable)

    if command == FPCommand.SOLVE:
        tokens = _statement_tokens(statement)
        upper_tokens = tuple(token.upper() for token in tokens)
        mode: str | None = None
        if len(tokens) >= 2:
            second = tokens[1].upper()
            if second in {"DYNAMIC", "STATIC"}:
                mode = second.lower()

        filevar: str | None = None
        for token in tokens:
            if token.upper().startswith("FILEVAR="):
                filevar = token.split("=", 1)[1].strip().strip("\"'")
                break

        keyboard_targets: list[str] = []
        if filevar is not None and filevar.upper() == "KEYBOARD" and len(record.raw_lines) > 1:
            for raw_line in record.raw_lines[1:]:
                for token in _statement_tokens(raw_line):
                    cleaned = _normalize_keyboard_target(token)
                    if not cleaned:
                        continue
                    keyboard_targets.append(cleaned)

        return SolveCommand(
            mode=mode,
            outside=("OUTSIDE" in upper_tokens),
            noreset=("NORESET" in upper_tokens),
            filevar=filevar,
            keyboard_targets=tuple(keyboard_targets),
            raw_tokens=tokens,
        )

    if command == FPCommand.PRINTVAR:
        tokens = _statement_tokens(statement)
        upper_tokens = tuple(token.upper() for token in tokens)
        filevar: str | None = None
        for token in tokens:
            if token.upper().startswith("FILEVAR="):
                filevar = token.split("=", 1)[1].strip().strip("\"'")
                break

        variables: list[str] = []
        for token in tokens[1:]:
            token_upper = token.upper()
            if token_upper == "LOADFORMAT":
                continue
            if token_upper == "STATS":
                continue
            if "=" in token:
                continue
            variables.append(token)

        if filevar is not None and filevar.upper() == "KEYBOARD" and len(record.raw_lines) > 1:
            keyboard_variables: list[str] = []
            seen: set[str] = set()
            for raw_line in record.raw_lines[1:]:
                for raw_token in _statement_tokens(raw_line):
                    cleaned = _normalize_keyboard_target(raw_token)
                    if not cleaned:
                        continue
                    if cleaned in seen:
                        continue
                    seen.add(cleaned)
                    keyboard_variables.append(cleaned)
            if keyboard_variables:
                variables = keyboard_variables + [
                    var for var in variables if var.upper() not in seen
                ]
        return PrintVarCommand(
            fileout=_extract_file_arg(statement),
            loadformat=("LOADFORMAT" in upper_tokens),
            filevar=filevar,
            stats=("STATS" in upper_tokens),
            variables=tuple(variables),
            raw_tokens=tokens,
        )

    if command == FPCommand.SETYYTOY:
        return SetYyToYCommand()

    if command == FPCommand.EXTRAPOLATE:
        return ExtrapolateCommand()

    return None
