"""Shared backend semantics profiles.

These profiles are intentionally small and explicit. They are meant to give
both backends a shared operator-facing mode label even when the underlying
implementation knobs differ.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = [
    "BackendSemanticsProfile",
    "build_semantics_manifest",
    "get_backend_semantics_profile",
    "normalize_backend_semantics_profile",
    "write_semantics_manifest",
]


@dataclass(frozen=True)
class BackendSemanticsProfile:
    name: str
    description: str
    helper_overlay_policy: str
    exogenous_equation_target_policy: str
    fairpy_eq_flags_preset: str
    fpr_boundary_policy: str
    fpr_solver_policy: str
    fpr_solver_active_set_start_iteration: int
    fpr_solver_active_set_delta_threshold: float


_PROFILES: dict[str, BackendSemanticsProfile] = {
    "compat": BackendSemanticsProfile(
        name="compat",
        description="Closest practical legacy FP semantics for public parity work.",
        helper_overlay_policy="base_pre_first_solve_excluding_scenario_assignments",
        exogenous_equation_target_policy="exclude_from_solve",
        fairpy_eq_flags_preset="parity",
        fpr_boundary_policy="compat",
        fpr_solver_policy="full_scan",
        fpr_solver_active_set_start_iteration=0,
        fpr_solver_active_set_delta_threshold=0.0,
    ),
    "canonical": BackendSemanticsProfile(
        name="canonical",
        description="Cleaner shared backend semantics with narrower boundary carry rules.",
        helper_overlay_policy="base_pre_first_solve_excluding_scenario_assignments",
        exogenous_equation_target_policy="exclude_from_solve",
        fairpy_eq_flags_preset="parity",
        fpr_boundary_policy="canonical",
        fpr_solver_policy="active_set_v1",
        fpr_solver_active_set_start_iteration=4,
        fpr_solver_active_set_delta_threshold=1e-6,
    ),
}


def normalize_backend_semantics_profile(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"", "default", "legacy"}:
        return "compat"
    if raw not in _PROFILES:
        valid = ", ".join(sorted(_PROFILES))
        raise ValueError(
            f"Unknown semantics profile: {value!r} (expected one of {valid})"
        )
    return raw


def get_backend_semantics_profile(value: str | None) -> BackendSemanticsProfile:
    return _PROFILES[normalize_backend_semantics_profile(value)]


_INPUT_FILE_RE = re.compile(r"\bFILE\s*=\s*(?P<name>[^\s]+)", re.IGNORECASE)


def _clean_filename(token: str) -> str:
    raw = str(token or "").strip().strip("\"'")
    return raw.rstrip(";")


def _extract_input_file_from_command(body: str) -> str | None:
    match = _INPUT_FILE_RE.search(body or "")
    if not match:
        return None
    return _clean_filename(match.group("name"))


def _resolve_case_insensitive(root: Path, relative: str) -> Path:
    rel = Path(relative)
    cur = root
    for part in rel.parts:
        candidate = cur / part
        if candidate.exists():
            cur = candidate
            continue
        alt_lower = cur / part.lower()
        if alt_lower.exists():
            cur = alt_lower
            continue
        alt_upper = cur / part.upper()
        if alt_upper.exists():
            cur = alt_upper
            continue
        want = part.lower()
        found = None
        try:
            for child in cur.iterdir():
                if child.name.lower() == want:
                    found = child
                    break
        except OSError:
            found = None
        if found is None:
            return root / rel
        cur = found
    return cur


def _scan_staged_input_files(*, work_dir: Path, entry_input: Path | None) -> list[str]:
    if entry_input is None:
        return []
    from fp_wraptr.io.input_parser import parse_fp_input_text

    visited: set[str] = set()
    queue: list[str] = [str(entry_input.name)]
    files_scanned: list[str] = []

    while queue:
        name = _clean_filename(queue.pop(0))
        if not name:
            continue
        norm = name.lower()
        if norm in visited:
            continue
        visited.add(norm)

        path = _resolve_case_insensitive(work_dir, name)
        if not path.exists():
            continue

        try:
            rel_path = path.resolve().relative_to(work_dir.resolve())
            rel_name = rel_path.as_posix()
        except ValueError:
            rel_name = path.name
        files_scanned.append(rel_name)

        parsed = parse_fp_input_text(path.read_text(encoding="utf-8", errors="replace"))
        for cmd in parsed.get("control_commands", []) or []:
            if not isinstance(cmd, dict):
                continue
            if str(cmd.get("name", "")).upper() != "INPUT":
                continue
            include = _extract_input_file_from_command(str(cmd.get("body", "")))
            if include:
                queue.append(include)

    return files_scanned


def build_semantics_manifest(
    semantics_profile: str | None,
    *,
    work_dir: Path,
    entry_input: Path | None,
) -> dict[str, Any]:
    profile = get_backend_semantics_profile(semantics_profile)
    scanned_files = _scan_staged_input_files(work_dir=work_dir, entry_input=entry_input)
    return {
        "manifest_version": 1,
        "semantics_profile": profile.name,
        "helper_overlay_policy": profile.helper_overlay_policy,
        "exogenous_equation_target_policy": profile.exogenous_equation_target_policy,
        "replay_policy": "standard_input_replay_runtime_inputs_and_changevar",
        "fallback_policy": "backend_runtime_default",
        "fairpy_eq_flags_preset": profile.fairpy_eq_flags_preset,
        "fpr_boundary_policy": profile.fpr_boundary_policy,
        "fpr_solver_policy": profile.fpr_solver_policy,
        "fpr_solver_active_set_start_iteration": profile.fpr_solver_active_set_start_iteration,
        "fpr_solver_active_set_delta_threshold": profile.fpr_solver_active_set_delta_threshold,
        "entry_input": str(entry_input.name) if entry_input is not None else "",
        "scanned_input_files": scanned_files,
    }


def write_semantics_manifest(
    *,
    work_dir: Path,
    semantics_profile: str | None,
    entry_input: Path | None,
) -> tuple[Path, dict[str, Any], str]:
    manifest = build_semantics_manifest(
        semantics_profile,
        work_dir=work_dir,
        entry_input=entry_input,
    )
    text = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    path = work_dir / "semantics_manifest.json"
    path.write_text(text, encoding="utf-8")
    return path, manifest, digest
