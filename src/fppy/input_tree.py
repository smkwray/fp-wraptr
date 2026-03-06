"""FP input-tree utilities for expanding `INPUT FILE=...;` includes.

The legacy Fair-Parke command language supports nested includes via:

    INPUT FILE=<name>;

For parity runs, we need to treat these as deck includes (not just exogenous
CHANGEVAR inputs) so the reimplementation executes the same transitive scripts
as fp.exe.
"""

from __future__ import annotations

import re
from dataclasses import replace
from pathlib import Path

from fppy.parser import FPCommand, FPCommandRecord, parse_fminput_file
from fppy.runtime_commands import InputCommand, parse_runtime_command

__all__ = ["parse_fminput_tree_file"]

_CHANGEVAR_BLOCK_RE = re.compile(r"(?im)^\\s*CHANGEVAR\\b")


def _resolve_case_insensitive(directory: Path, name: str) -> Path | None:
    direct = directory / name
    if direct.exists():
        return direct
    wanted = name.lower()
    try:
        for child in directory.iterdir():
            if child.name.lower() == wanted:
                return child
    except OSError:
        return None
    return None


def _resolve_include_path(
    include: str,
    *,
    current_dir: Path,
    runtime_base_dir: Path | None,
) -> Path:
    token = str(include or "").strip().strip("\"'").rstrip(";").strip()
    if not token:
        raise ValueError("INPUT FILE directive missing filename")

    candidate = Path(token).expanduser()
    if candidate.is_absolute():
        return candidate

    search_dirs: list[Path] = [current_dir]
    if runtime_base_dir is not None:
        search_dirs.append(runtime_base_dir)
    search_dirs.append(Path.cwd())

    for directory in search_dirs:
        resolved = _resolve_case_insensitive(directory, token)
        if resolved is not None:
            return resolved

    searched = ", ".join(str(d / token) for d in search_dirs)
    raise FileNotFoundError(f"Missing INPUT include file {token!r} (searched: {searched})")


def _looks_like_changevar_script(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return bool(_CHANGEVAR_BLOCK_RE.search(text))


def _expand_includes(
    path: Path,
    *,
    runtime_base_dir: Path | None,
    max_depth: int,
    stack: tuple[Path, ...],
) -> list[FPCommandRecord]:
    if max_depth <= 0:
        raise RecursionError("INPUT include expansion exceeded max_depth")

    resolved = path.expanduser().resolve()
    if resolved in stack:
        cycle = " -> ".join(str(p.name) for p in (*stack, resolved))
        raise RecursionError(f"INPUT include cycle detected: {cycle}")

    records = parse_fminput_file(resolved)
    out: list[FPCommandRecord] = []
    current_dir = resolved.parent

    for record in records:
        if record.command == FPCommand.INPUT:
            parsed = parse_runtime_command(record)
            if isinstance(parsed, InputCommand) and parsed.file:
                include_path = _resolve_include_path(
                    parsed.file,
                    current_dir=current_dir,
                    runtime_base_dir=runtime_base_dir,
                )
                if not _looks_like_changevar_script(include_path):
                    out.extend(
                        _expand_includes(
                            include_path,
                            runtime_base_dir=runtime_base_dir,
                            max_depth=max_depth - 1,
                            stack=(*stack, resolved),
                        )
                    )
                    continue

        out.append(record)

    return out


def _renumber(records: list[FPCommandRecord]) -> list[FPCommandRecord]:
    return [replace(record, line_number=idx + 1) for idx, record in enumerate(records)]


def parse_fminput_tree_file(
    path: Path | str,
    *,
    runtime_base_dir: Path | None = None,
    max_depth: int = 25,
) -> list[FPCommandRecord]:
    """Parse an FP input file and expand nested deck includes.

    `INPUT FILE=...;` directives are expanded *unless* the referenced file looks
    like a CHANGEVAR script (in which case the INPUT record is preserved for
    runtime parsing via `parse_fmexog_file`).
    """
    root = Path(path)
    expanded = _expand_includes(
        root,
        runtime_base_dir=Path(runtime_base_dir) if runtime_base_dir is not None else None,
        max_depth=max(1, int(max_depth)),
        stack=tuple(),
    )
    return _renumber(expanded)

