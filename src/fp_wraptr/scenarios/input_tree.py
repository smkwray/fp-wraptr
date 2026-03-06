"""FP input-tree utilities.

The Fair-Parke input language supports nested includes via:

    INPUT FILE=<name>;

For scenario runners that stage model inputs into a temporary work directory,
we need to copy all transitive include files into that work directory so fp.exe
can resolve them at runtime.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from fp_wraptr.io.input_parser import parse_fp_input_text

__all__ = [
    "InputTreeManifest",
    "InputTreeSymbols",
    "prepare_work_dir_for_fp_run",
    "scan_input_tree_symbols",
    "select_primary_loadformat_output",
]

_INPUT_FILE_RE = re.compile(r"\bFILE\s*=\s*(?P<name>[^\s]+)", re.IGNORECASE)


@dataclass(frozen=True)
class InputTreeManifest:
    """A best-effort manifest of input dependencies and outputs."""

    entry_input_file: str
    include_files: tuple[str, ...]
    load_data_files: tuple[str, ...]
    expected_output_files: tuple[str, ...]


@dataclass(frozen=True)
class InputTreeSymbols:
    """Symbols discovered while scanning an input tree (best-effort)."""

    entry_input_file: str
    files_scanned: tuple[str, ...]
    include_files: tuple[str, ...]
    variables: tuple[str, ...]
    equations: tuple[dict[str, Any], ...]


def _clean_filename(token: str) -> str:
    raw = str(token or "").strip()
    raw = raw.strip("\"'")
    raw = raw.rstrip(";")
    return raw


def _extract_input_file_from_command(body: str) -> str | None:
    match = _INPUT_FILE_RE.search(body or "")
    if not match:
        return None
    return _clean_filename(match.group("name"))


def _resolve_case_insensitive(directory: Path, name: str) -> Path | None:
    candidate = directory / name
    if candidate.exists():
        return candidate

    alt_names = {name.lower(), name.upper()}
    for alt in sorted(alt_names):
        alt_path = directory / alt
        if alt_path.exists():
            return alt_path

    # Last resort: scan directory entries for a case-insensitive match.
    want = name.lower()
    try:
        for child in directory.iterdir():
            if child.name.lower() == want:
                return child
    except OSError:
        return None
    return None


def _find_source_path(name: str, search_dirs: Iterable[Path]) -> Path | None:
    for directory in search_dirs:
        resolved = _resolve_case_insensitive(directory, name)
        if resolved is not None:
            return resolved
    return None


def _extract_symbol_vars(parsed: dict[str, Any]) -> set[str]:
    out: set[str] = set()

    for item in parsed.get("creates", []) or []:
        if isinstance(item, dict) and item.get("name"):
            out.add(str(item["name"]).strip().upper())
    for item in parsed.get("generated_vars", []) or []:
        if isinstance(item, dict) and item.get("name"):
            out.add(str(item["name"]).strip().upper())
    for item in parsed.get("identities", []) or []:
        if isinstance(item, dict) and item.get("name"):
            out.add(str(item["name"]).strip().upper())
    for item in parsed.get("equation_lhs", []) or []:
        if isinstance(item, dict) and item.get("name"):
            out.add(str(item["name"]).strip().upper())
    for item in parsed.get("equations", []) or []:
        if isinstance(item, dict) and item.get("lhs"):
            out.add(str(item["lhs"]).strip().upper())
    for item in parsed.get("changevar_blocks", []) or []:
        if isinstance(item, dict) and item.get("variable"):
            out.add(str(item["variable"]).strip().upper())
    for item in parsed.get("printvar", []) or []:
        if not isinstance(item, dict):
            continue
        raw_vars = item.get("variables", [])
        if not isinstance(raw_vars, list):
            continue
        for tok in raw_vars:
            text = str(tok).strip().upper()
            if text:
                out.add(text)
    for body in parsed.get("exogenous", []) or []:
        for tok in str(body).split():
            text = tok.strip().upper()
            if text:
                out.add(text)

    return {v for v in out if v}


def scan_input_tree_symbols(
    *,
    entry_input_file: str,
    overlay_dir: Path | str | None,
    fp_home: Path,
) -> InputTreeSymbols:
    """Scan an FP input tree directly from source directories (no staging).

    This is useful for dictionary overlays: identify symbols that appear in a
    scenario input tree but are missing from the base dictionary.
    """
    fp_home = Path(fp_home)
    overlay_path = Path(overlay_dir) if overlay_dir is not None else None

    search_dirs: list[Path] = []
    if overlay_path is not None:
        search_dirs.append(overlay_path)
    search_dirs.append(fp_home)

    visited: set[str] = set()
    queue: list[str] = [str(entry_input_file)]
    files_scanned: list[str] = []
    include_files: set[str] = set()
    variables: set[str] = set()
    equations: list[dict[str, Any]] = []

    while queue:
        name = _clean_filename(queue.pop(0))
        if not name:
            continue
        norm = name.lower()
        if norm in visited:
            continue
        visited.add(norm)

        src = _find_source_path(name, search_dirs)
        if src is None:
            searched = ", ".join(str(d / name) for d in search_dirs)
            raise FileNotFoundError(f"Missing FP input file {name!r} (searched: {searched})")

        files_scanned.append(name)
        text = src.read_text(encoding="utf-8", errors="replace")
        parsed = parse_fp_input_text(text)

        variables.update(_extract_symbol_vars(parsed))
        for eq in parsed.get("equations", []) or []:
            if isinstance(eq, dict):
                equations.append(eq)

        for cmd in parsed.get("control_commands", []):
            if not isinstance(cmd, dict):
                continue
            if str(cmd.get("name", "")).upper() != "INPUT":
                continue
            body = str(cmd.get("body", ""))
            include = _extract_input_file_from_command(body)
            if not include:
                continue
            include = _clean_filename(include)
            if not include:
                continue
            include_files.add(include)
            queue.append(include)

    return InputTreeSymbols(
        entry_input_file=str(entry_input_file),
        files_scanned=tuple(files_scanned),
        include_files=tuple(sorted(include_files)),
        variables=tuple(sorted(variables)),
        equations=tuple(equations),
    )


def prepare_work_dir_for_fp_run(
    *,
    entry_input: Path,
    work_dir: Path,
    overlay_dir: Path | str | None,
    fp_home: Path,
) -> InputTreeManifest:
    """Copy nested `INPUT FILE=...;` dependencies into a work directory.

    Args:
        entry_input: Path to the entry input script *inside* work_dir.
        work_dir: Working directory where fp.exe will be executed.
        overlay_dir: Optional overlay directory searched for include files first.
        fp_home: Base FP model directory (searched after overlay_dir).

    Returns:
        InputTreeManifest describing discovered includes and outputs.
    """
    entry_input = Path(entry_input)
    work_dir = Path(work_dir)
    fp_home = Path(fp_home)
    overlay_path = Path(overlay_dir) if overlay_dir is not None else None

    if not entry_input.exists():
        raise FileNotFoundError(f"Entry input file missing: {entry_input}")

    search_dirs: list[Path] = []
    if overlay_path is not None:
        search_dirs.append(overlay_path)
    search_dirs.append(fp_home)

    visited: set[str] = set()
    queue: list[str] = [entry_input.name]
    include_files: set[str] = set()
    load_data_files: set[str] = set()
    expected_output_files: set[str] = set()

    while queue:
        name = _clean_filename(queue.pop(0))
        if not name:
            continue
        norm = name.lower()
        if norm in visited:
            continue
        visited.add(norm)

        target = work_dir / name
        if not target.exists():
            src = _find_source_path(name, search_dirs)
            if src is None:
                searched = ", ".join(str(d / name) for d in search_dirs)
                raise FileNotFoundError(f"Missing FP include file {name!r} (searched: {searched})")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)

        try:
            text = target.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:  # pragma: no cover - extremely defensive
            raise OSError(f"Failed to read FP input file: {target}") from exc

        parsed = parse_fp_input_text(text)

        # Nested INPUT FILE=... includes
        for cmd in parsed.get("control_commands", []):
            if not isinstance(cmd, dict):
                continue
            if str(cmd.get("name", "")).upper() != "INPUT":
                continue
            body = str(cmd.get("body", ""))
            include = _extract_input_file_from_command(body)
            if not include:
                continue
            include = _clean_filename(include)
            if not include:
                continue
            include_files.add(include)
            queue.append(include)

        # LOADDATA FILE=... dependencies (copy so case-sensitive FS works)
        for raw_name in parsed.get("load_data", []) or []:
            if not isinstance(raw_name, str):
                continue
            dep_name = _clean_filename(raw_name)
            if not dep_name:
                continue
            load_data_files.add(dep_name)

        # PRINTVAR FILEOUT=... outputs
        for item in parsed.get("printvar", []) or []:
            if not isinstance(item, dict):
                continue
            fileout = item.get("file")
            if not isinstance(fileout, str):
                continue
            out_name = _clean_filename(fileout)
            if not out_name:
                continue
            expected_output_files.add(out_name)

    # Copy LOADDATA dependencies after scanning all scripts.
    for name in sorted(load_data_files):
        target = work_dir / name
        if target.exists():
            continue
        src = _find_source_path(name, search_dirs)
        if src is None:
            # LOADDATA files are often provided by fp_home; if missing, let fp.exe
            # surface the failure with its own message rather than pre-failing.
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)

    return InputTreeManifest(
        entry_input_file=entry_input.name,
        include_files=tuple(sorted(include_files)),
        load_data_files=tuple(sorted(load_data_files)),
        expected_output_files=tuple(sorted(expected_output_files)),
    )


def select_primary_loadformat_output(run_dir: Path, copied_outputs: list[Path] | None = None) -> Path | None:
    """Select the primary LOADFORMAT-style output for a run directory.

    Priority:
      1) PABEV.TXT
      2) PACEV.TXT
      3) If exactly one .DAT output exists among copied outputs, use it
      4) Else choose the largest .DAT output among copied outputs.
    """
    run_dir = Path(run_dir)
    for name in ("PABEV.TXT", "PACEV.TXT"):
        candidate = run_dir / name
        if candidate.exists():
            return candidate

    outputs = list(copied_outputs or [])
    dat_outputs = [path for path in outputs if path.suffix.lower() == ".dat" and path.exists()]
    if len(dat_outputs) == 1:
        return dat_outputs[0]
    if dat_outputs:
        return max(dat_outputs, key=lambda path: path.stat().st_size)
    return None
