"""Parser utilities for FP command streams."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class FPCommand(StrEnum):
    COMMENT = "COMMENT"
    SPACE = "SPACE"
    SMPL = "SMPL"
    LOADDATA = "LOADDATA"
    CHANGEVAR = "CHANGEVAR"
    GENR = "GENR"
    EQ = "EQ"
    IDENT = "IDENT"
    LHS = "LHS"
    MODEQ = "MODEQ"
    FSR = "FSR"
    SETUPSOLVE = "SETUPSOLVE"
    CREATE = "CREATE"
    SETUPEST = "SETUPEST"
    TWOSLS = "2SLS"
    EST = "EST"
    TEST = "TEST"
    EXOGENOUS = "EXOGENOUS"
    EXTRAPOLATE = "EXTRAPOLATE"
    INPUT = "INPUT"
    SOLVE = "SOLVE"
    SETYYTOY = "SETYYTOY"
    PRINTMODEL = "PRINTMODEL"
    PRINTNAMES = "PRINTNAMES"
    PRINTVAR = "PRINTVAR"
    END = "END"
    QUIT = "QUIT"


@dataclass(frozen=True)
class FPCommandRecord:
    line_number: int
    line: str
    command: FPCommand
    statement: str
    raw_lines: tuple[str, ...]
    terminated: bool


_FIRST_TOKEN_RE = re.compile(r"^\s*(?P<token>[A-Za-z0-9]+)")
_COMMAND_BY_TOKEN: dict[str, FPCommand] = {
    "SPACE": FPCommand.SPACE,
    "SMPL": FPCommand.SMPL,
    "LOADDATA": FPCommand.LOADDATA,
    "CHANGEVAR": FPCommand.CHANGEVAR,
    "GENR": FPCommand.GENR,
    "EQ": FPCommand.EQ,
    "IDENT": FPCommand.IDENT,
    "LHS": FPCommand.LHS,
    "MODEQ": FPCommand.MODEQ,
    "FSR": FPCommand.FSR,
    "SETUPSOLVE": FPCommand.SETUPSOLVE,
    "CREATE": FPCommand.CREATE,
    "SETUPEST": FPCommand.SETUPEST,
    "2SLS": FPCommand.TWOSLS,
    "EST": FPCommand.EST,
    "TEST": FPCommand.TEST,
    "EXOGENOUS": FPCommand.EXOGENOUS,
    "EXTRAPOLATE": FPCommand.EXTRAPOLATE,
    "INPUT": FPCommand.INPUT,
    "SOLVE": FPCommand.SOLVE,
    "SETYYTOY": FPCommand.SETYYTOY,
    "PRINTMODEL": FPCommand.PRINTMODEL,
    "PRINTNAMES": FPCommand.PRINTNAMES,
    "PRINTVAR": FPCommand.PRINTVAR,
    "END": FPCommand.END,
    "QUIT": FPCommand.QUIT,
}
_MULTILINE_COMMANDS: frozenset[FPCommand] = frozenset({
    FPCommand.EQ,
    FPCommand.GENR,
    FPCommand.IDENT,
    FPCommand.LHS,
    FPCommand.CREATE,
})


def _normalize_line(raw: str) -> str:
    return raw.rstrip("\r\n")


def _classify_command(line: str) -> FPCommand | None:
    stripped = line.strip()
    if not stripped:
        return None
    if stripped.startswith("@"):
        return FPCommand.COMMENT
    match = _FIRST_TOKEN_RE.match(line)
    if match is None:
        return None
    token = match.group("token").upper()
    return _COMMAND_BY_TOKEN.get(token)


def _is_statement_terminated(line: str) -> bool:
    return ";" in line


def _is_solve_keyboard_statement(command: FPCommand, statement: str) -> bool:
    if command != FPCommand.SOLVE:
        return False
    return "FILEVAR=KEYBOARD" in statement.upper()


def _is_printvar_keyboard_statement(command: FPCommand, statement: str) -> bool:
    if command != FPCommand.PRINTVAR:
        return False
    return "FILEVAR=KEYBOARD" in statement.upper()


def parse_fp_lines(lines: Iterable[str]) -> list[FPCommandRecord]:
    records: list[FPCommandRecord] = []
    materialized = [_normalize_line(line) for line in lines]
    index = 0
    total = len(materialized)

    while index < total:
        line = materialized[index]
        command = _classify_command(line)
        if command is None:
            index += 1
            continue

        line_number = index + 1
        raw_lines = [line]
        terminated = _is_statement_terminated(line)
        index += 1

        if command == FPCommand.COMMENT:
            statement = line
            records.append(
                FPCommandRecord(
                    line_number=line_number,
                    line=line,
                    command=command,
                    statement=statement,
                    raw_lines=tuple(raw_lines),
                    terminated=True,
                )
            )
            continue

        if command == FPCommand.CHANGEVAR:
            # CHANGEVAR blocks terminate with a standalone ";" line. The
            # introductory "CHANGEVAR;" line is itself terminated, so we need a
            # bespoke collector rather than relying on _MULTILINE_COMMANDS.
            while index < total:
                continuation = materialized[index]
                raw_lines.append(continuation)
                index += 1
                if continuation.strip() == ";":
                    break
            statement = "\n".join(raw_lines)
            records.append(
                FPCommandRecord(
                    line_number=line_number,
                    line=line,
                    command=command,
                    statement=statement,
                    raw_lines=tuple(raw_lines),
                    terminated=True,
                )
            )
            continue

        if not terminated and command in _MULTILINE_COMMANDS:
            while index < total:
                continuation = materialized[index]
                continuation_command = _classify_command(continuation)
                if continuation_command is not None:
                    # Guard malformed multiline records: if the next physical line is
                    # already a new command, keep this statement unterminated and let
                    # the parser resume from that command.
                    break
                raw_lines.append(continuation)
                index += 1
                if _is_statement_terminated(continuation):
                    terminated = True
                    break

        if terminated and _is_solve_keyboard_statement(command, raw_lines[0]):
            if index < total:
                next_line = materialized[index]
                next_command = _classify_command(next_line)
                if next_command is None and next_line.strip():
                    while index < total:
                        continuation = materialized[index]
                        raw_lines.append(continuation)
                        index += 1
                        if continuation.strip() == ";":
                            break

        if _is_printvar_keyboard_statement(command, raw_lines[0]) and index < total:
            next_line = materialized[index]
            next_command = _classify_command(next_line)
            if next_command is None and next_line.strip():
                while index < total:
                    continuation = materialized[index]
                    raw_lines.append(continuation)
                    index += 1
                    if continuation.strip() == ";":
                        break

        statement = "\n".join(raw_lines)
        records.append(
            FPCommandRecord(
                line_number=line_number,
                line=line,
                command=command,
                statement=statement,
                raw_lines=tuple(raw_lines),
                terminated=terminated,
            )
        )

    return records


def parse_fminput(text: str) -> list[FPCommandRecord]:
    return parse_fp_lines(text.splitlines())


def parse_fminput_file(path: Path | str) -> list[FPCommandRecord]:
    return parse_fminput(Path(path).read_text(encoding="utf-8", errors="replace"))


def count_commands(records: Iterable[FPCommandRecord]) -> Counter[FPCommand]:
    return Counter(record.command for record in records)
