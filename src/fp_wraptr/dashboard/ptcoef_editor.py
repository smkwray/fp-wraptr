"""Helpers for parsing and safely rewriting PSE ``ptcoef.txt`` overlays."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

__all__ = [
    "PtcoefCreateEntry",
    "PtcoefDocument",
    "PtcoefWriteResult",
    "parse_ptcoef_text",
    "resolve_ptcoef_overlay_path",
    "rewrite_ptcoef_text",
    "write_ptcoef_overlay_text",
]

_CREATE_RE = re.compile(
    r"^(?P<prefix>\s*CREATE\s+)(?P<symbol>[A-Za-z][A-Za-z0-9_]*)"
    r"(?P<assign>\s*=\s*)(?P<value>[^;]*?)(?P<suffix>\s*;\s*(?:@.*)?)$",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class PtcoefCreateEntry:
    """One ``CREATE`` coefficient assignment line in ``ptcoef.txt``."""

    line_index: int
    symbol: str
    value_text: str
    value: float | None


@dataclass(frozen=True)
class PtcoefDocument:
    """Parsed ``ptcoef.txt`` text and discovered coefficient assignments."""

    lines: tuple[str, ...]
    entries: tuple[PtcoefCreateEntry, ...]


@dataclass(frozen=True)
class PtcoefWriteResult:
    """Write outcome for a ``ptcoef.txt`` overlay update."""

    target_path: Path
    backup_path: Path | None


def _line_ending(line: str) -> tuple[str, str]:
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n"):
        return line[:-1], "\n"
    return line, ""


def _parse_create_line(raw: str, *, line_index: int) -> PtcoefCreateEntry | None:
    match = _CREATE_RE.match(raw)
    if match is None:
        return None
    symbol = match.group("symbol").upper()
    value_text = match.group("value").strip()
    try:
        numeric = float(value_text)
    except ValueError:
        numeric = None
    return PtcoefCreateEntry(
        line_index=line_index,
        symbol=symbol,
        value_text=value_text,
        value=numeric,
    )


def parse_ptcoef_text(text: str) -> PtcoefDocument:
    """Parse ``ptcoef.txt`` content into lines + ``CREATE`` assignment entries."""

    lines = tuple(text.splitlines(keepends=True))
    entries: list[PtcoefCreateEntry] = []
    for idx, line in enumerate(lines):
        raw, _ = _line_ending(line)
        parsed = _parse_create_line(raw, line_index=idx)
        if parsed is not None:
            entries.append(parsed)
    return PtcoefDocument(lines=lines, entries=tuple(entries))


def _format_update_value(value: float | int | str) -> str:
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("ptcoef update values must be non-empty")
        return trimmed
    return format(float(value), ".12g")


def rewrite_ptcoef_text(
    text: str,
    updates: Mapping[str, float | int | str],
) -> tuple[str, tuple[str, ...]]:
    """Apply symbol updates while preserving non-target lines and comments.

    Returns ``(rewritten_text, missing_symbols)`` where ``missing_symbols`` are
    requested updates that did not match any ``CREATE`` line.
    """

    normalized_updates = {
        str(symbol).strip().upper(): _format_update_value(value)
        for symbol, value in updates.items()
        if str(symbol).strip()
    }
    if not normalized_updates:
        return text, ()

    rewritten_lines = list(text.splitlines(keepends=True))
    applied: set[str] = set()

    for idx, line in enumerate(rewritten_lines):
        raw, ending = _line_ending(line)
        match = _CREATE_RE.match(raw)
        if match is None:
            continue
        symbol = match.group("symbol").upper()
        if symbol not in normalized_updates:
            continue
        applied.add(symbol)
        rewritten_lines[idx] = (
            f"{match.group('prefix')}{symbol}{match.group('assign')}"
            f"{normalized_updates[symbol]}{match.group('suffix')}{ending}"
        )

    missing = tuple(sorted(symbol for symbol in normalized_updates if symbol not in applied))
    return "".join(rewritten_lines), missing


def resolve_ptcoef_overlay_path(
    *,
    overlay_dir: Path | str,
    relative_path: str = "ptcoef.txt",
) -> Path:
    """Resolve and validate the write target for overlay ``ptcoef.txt`` edits."""

    root = Path(overlay_dir).expanduser().resolve()
    rel = Path(relative_path)
    if rel.is_absolute():
        raise ValueError("relative_path must be relative to overlay_dir")

    target = (root / rel).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"refusing to write outside overlay dir: {target}") from exc

    if target.name.lower() != "ptcoef.txt":
        raise ValueError("ptcoef editor only supports writes to ptcoef.txt")

    return target


def write_ptcoef_overlay_text(
    *,
    overlay_dir: Path | str,
    text: str,
    relative_path: str = "ptcoef.txt",
    backup_timestamp: datetime | None = None,
) -> PtcoefWriteResult:
    """Write ``ptcoef.txt`` under ``overlay_dir`` with timestamped backups."""

    target = resolve_ptcoef_overlay_path(overlay_dir=overlay_dir, relative_path=relative_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    backup_path: Path | None = None
    if target.exists():
        timestamp = (backup_timestamp or datetime.now(UTC)).strftime("%Y%m%d_%H%M%S")
        backup_path = target.with_name(f"{target.name}.bak.{timestamp}")
        backup_path.write_text(target.read_text(encoding="utf-8"), encoding="utf-8")

    target.write_text(text, encoding="utf-8")
    return PtcoefWriteResult(target_path=target, backup_path=backup_path)
