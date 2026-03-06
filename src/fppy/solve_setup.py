"""Parse SETUPSOLVE directives used by iterative EQ backfill settings."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from fppy.parser import FPCommand, FPCommandRecord

_KEY_VALUE_FIELDS: set[str] = {
    "MINITERS",
    "MAXCHECK",
    "MAXITERS",
    "TOLALL",
    "DAMPALL",
    "FILEDAMP",
    "FILETOL",
    "FILETOLABS",
    "MAXERR",
}
_FLAG_FIELDS: set[str] = {"TOLALLABS", "NOMISS"}


@dataclass(frozen=True)
class SetupSolveConfig:
    miniters: int = 1
    maxcheck: int | None = None
    maxiters: int | None = None
    tolall: float | None = None
    tolallabs: bool = False
    dampall: float | None = None
    filedamp: str | None = None
    filetol: str | None = None
    filetolabs: str | None = None
    maxerr: int | None = None
    nomiss: bool = False


def parse_setupsolve_statement(statement: str) -> dict[str, str]:
    text = statement.strip()
    if not text:
        return {}
    if text.upper().startswith("SETUPSOLVE"):
        text = text[len("SETUPSOLVE") :]
    tokens = text.replace(";", " ").split()

    parsed: dict[str, str] = {}
    for token in tokens:
        token = token.strip()
        if not token:
            continue
        if "=" in token:
            key, value = token.split("=", 1)
            key = key.strip().upper()
            value = value.strip().strip('"').strip("'")
            if key in _KEY_VALUE_FIELDS and value:
                parsed[key] = value
            continue

        normalized = token.upper()
        negative = normalized.startswith("-")
        if negative:
            normalized = normalized[1:]
        if normalized in _FLAG_FIELDS:
            parsed[normalized] = "FALSE" if negative else "TRUE"

    return parsed


def _maybe_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _maybe_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_setupsolve_config(records: Iterable[FPCommandRecord]) -> SetupSolveConfig:
    merged: dict[str, str] = {}
    for record in records:
        if record.command != FPCommand.SETUPSOLVE:
            continue
        merged.update(parse_setupsolve_statement(record.statement))

    miniters = _maybe_int(merged.get("MINITERS"))
    maxcheck = _maybe_int(merged.get("MAXCHECK"))
    maxiters = _maybe_int(merged.get("MAXITERS"))
    tolall = _maybe_float(merged.get("TOLALL"))
    dampall = _maybe_float(merged.get("DAMPALL"))
    maxerr = _maybe_int(merged.get("MAXERR"))

    return SetupSolveConfig(
        miniters=miniters if miniters is not None else 1,
        maxcheck=maxcheck,
        maxiters=maxiters,
        tolall=tolall,
        tolallabs=merged.get("TOLALLABS", "FALSE").upper() == "TRUE",
        dampall=dampall,
        filedamp=merged.get("FILEDAMP"),
        filetol=merged.get("FILETOL"),
        filetolabs=merged.get("FILETOLABS"),
        maxerr=maxerr,
        nomiss=merged.get("NOMISS", "FALSE").upper() == "TRUE",
    )


__all__ = [
    "SetupSolveConfig",
    "extract_setupsolve_config",
    "parse_setupsolve_statement",
]
