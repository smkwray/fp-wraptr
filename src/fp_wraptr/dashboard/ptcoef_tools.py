"""Helpers for structured editing of PSE `ptcoef.txt` overlay decks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fp_wraptr.dashboard.ptcoef_editor import (
    parse_ptcoef_text as parse_ptcoef_document,
)
from fp_wraptr.dashboard.ptcoef_editor import (
    resolve_ptcoef_overlay_path,
    rewrite_ptcoef_text,
    write_ptcoef_overlay_text,
)

__all__ = [
    "PtcoefDeck",
    "PtcoefEntry",
    "PtcoefWriteResult",
    "load_ptcoef_deck",
    "parse_ptcoef_text",
    "write_ptcoef_overlay",
]


@dataclass(frozen=True)
class PtcoefEntry:
    name: str
    value: float
    line_index: int
    section: str | None = None


@dataclass(frozen=True)
class PtcoefDeck:
    source_path: Path
    source_kind: str
    target_path: Path
    text: str
    entries: tuple[PtcoefEntry, ...]


@dataclass(frozen=True)
class PtcoefWriteResult:
    target_path: Path
    backup_path: Path | None
    changed_names: tuple[str, ...]
    created_overlay_copy: bool


def _clean_comment(line: str) -> str:
    return line.lstrip().removeprefix("@").strip()


def parse_ptcoef_text(text: str) -> tuple[PtcoefEntry, ...]:
    """Parse coefficient entries while tracking nearby section comments."""

    document = parse_ptcoef_document(text)
    entry_by_line = {entry.line_index: entry for entry in document.entries}
    entries: list[PtcoefEntry] = []
    last_comment: str | None = None
    for line_index, raw_line in enumerate(text.splitlines()):
        stripped = raw_line.strip()
        if stripped.startswith("@"):
            cleaned = _clean_comment(raw_line)
            if cleaned:
                last_comment = cleaned
            continue
        if not stripped:
            continue

        parsed = entry_by_line.get(line_index)
        if parsed is None or parsed.value is None:
            continue
        entries.append(
            PtcoefEntry(
                name=parsed.symbol.upper(),
                value=float(parsed.value),
                line_index=line_index,
                section=last_comment,
            )
        )

    return tuple(entries)


def _safe_target_path(overlay_dir: Path) -> Path:
    return resolve_ptcoef_overlay_path(overlay_dir=overlay_dir)


def load_ptcoef_deck(*, overlay_dir: Path | str | None, fp_home: Path | str) -> PtcoefDeck:
    """Load the live ptcoef deck, preferring overlay copy over fp_home."""

    overlay_path = Path(overlay_dir) if overlay_dir is not None else None
    fp_home_path = Path(fp_home)
    target_path = (
        _safe_target_path(overlay_path)
        if overlay_path is not None
        else fp_home_path / "ptcoef.txt"
    )

    source_path: Path | None = None
    source_kind: str | None = None
    if overlay_path is not None and target_path.exists():
        source_path = target_path
        source_kind = "overlay"
    else:
        fp_source = fp_home_path / "ptcoef.txt"
        if fp_source.exists():
            source_path = fp_source
            source_kind = "fp_home"

    if source_path is None or source_kind is None:
        searched = [str(fp_home_path / "ptcoef.txt")]
        if overlay_path is not None:
            searched.insert(0, str(target_path))
        raise FileNotFoundError(f"ptcoef.txt not found (searched: {', '.join(searched)})")

    text = source_path.read_text(encoding="utf-8", errors="replace")
    entries = parse_ptcoef_text(text)
    if not entries:
        raise ValueError(f"No CREATE coefficient entries found in {source_path}")

    return PtcoefDeck(
        source_path=source_path,
        source_kind=source_kind,
        target_path=target_path,
        text=text,
        entries=entries,
    )


def _render_updated_text(
    deck: PtcoefDeck, updates: dict[str, float]
) -> tuple[str, tuple[str, ...]]:
    entry_by_name = {entry.name: entry for entry in deck.entries}
    effective_updates = {
        name.upper(): float(new_value)
        for name, new_value in updates.items()
        if name.upper() in entry_by_name
        and float(new_value) != float(entry_by_name[name.upper()].value)
    }
    rewritten_text, missing = rewrite_ptcoef_text(deck.text, effective_updates)
    changed = tuple(sorted(name for name in effective_updates if name not in set(missing)))
    return rewritten_text, changed


def write_ptcoef_overlay(deck: PtcoefDeck, updates: dict[str, float]) -> PtcoefWriteResult:
    """Write updated coefficients into the overlay copy with a timestamped backup."""

    target_path = deck.target_path
    if target_path == deck.source_path and deck.source_kind != "overlay":
        raise ValueError("Refusing to overwrite fp_home ptcoef.txt directly; use an overlay dir.")

    rendered_text, changed_names = _render_updated_text(deck, updates)
    if not changed_names:
        return PtcoefWriteResult(
            target_path=target_path,
            backup_path=None,
            changed_names=(),
            created_overlay_copy=not target_path.exists(),
        )

    created_overlay_copy = not target_path.exists()
    write_result = write_ptcoef_overlay_text(
        overlay_dir=target_path.parent,
        text=rendered_text,
    )
    return PtcoefWriteResult(
        target_path=write_result.target_path,
        backup_path=write_result.backup_path,
        changed_names=changed_names,
        created_overlay_copy=created_overlay_copy,
    )
