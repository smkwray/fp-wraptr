"""Dictionary overlay helpers.

fp-wraptr ships a bundled FP model dictionary extracted from Fair-Parke sources.
Real projects often introduce scenario-specific symbols (e.g. PSE/JG layer) or
require scenario-specific meaning/units for existing variable codes.

This module implements a small, tolerant overlay format that can be merged on
top of the bundled dictionary at runtime (dashboard, MCP, CLI).

Overlay JSON format (best-effort; unknown keys are preserved):

{
  "meta": {...},
  "variables": {
    "JGJ": {"description": "...", "units": "%", "category": "endogenous"},
    "PCPF": {"description": "...", "units": "%"}
  },
  "equations": {
    "999": {"label": "...", "formula": "..."}
  }
}
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from fp_wraptr.data.dictionary import EquationRecord, ModelDictionary, VariableRecord

__all__ = [
    "apply_dictionary_overlay",
    "load_dictionary_with_overlay",
    "load_dictionary_with_overlays",
    "read_dictionary_overlay",
    "write_dictionary_overlay",
]


def read_dictionary_overlay(path: Path | str | None) -> dict[str, Any]:
    overlay_path = Path(path) if path else None
    if overlay_path is None or not overlay_path.exists():
        return {}
    payload = json.loads(overlay_path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def write_dictionary_overlay(path: Path | str, payload: Mapping[str, Any]) -> None:
    overlay_path = Path(path)
    overlay_path.parent.mkdir(parents=True, exist_ok=True)
    overlay_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )


def _apply_variable_overrides(
    variables: dict[str, VariableRecord],
    overrides: Mapping[str, Any],
) -> dict[str, VariableRecord]:
    merged = dict(variables)
    for raw_code, raw_patch in overrides.items():
        code = str(raw_code).strip().upper()
        if not code:
            continue
        patch = raw_patch if isinstance(raw_patch, Mapping) else {}
        existing = merged.get(code)
        if existing is None:
            try:
                merged[code] = VariableRecord(name=code, **dict(patch))
            except Exception:
                merged[code] = VariableRecord(name=code, description=str(patch))
            continue
        try:
            merged[code] = VariableRecord(**{**existing.model_dump(), **dict(patch), "name": code})
        except Exception:
            # Keep the existing record if the override is malformed.
            merged[code] = existing
    return merged


def _apply_equation_overrides(
    equations: dict[int, EquationRecord],
    overrides: Mapping[str, Any],
) -> dict[int, EquationRecord]:
    merged = dict(equations)
    for raw_id, raw_patch in overrides.items():
        try:
            eq_id = int(str(raw_id).strip())
        except (TypeError, ValueError):
            continue
        patch = raw_patch if isinstance(raw_patch, Mapping) else {}
        existing = merged.get(eq_id)
        if existing is None:
            try:
                merged[eq_id] = EquationRecord(id=eq_id, **dict(patch))
            except Exception:
                merged[eq_id] = EquationRecord(id=eq_id, label=str(patch))
            continue
        try:
            merged[eq_id] = EquationRecord(**{**existing.model_dump(), **dict(patch), "id": eq_id})
        except Exception:
            merged[eq_id] = existing
    return merged


def apply_dictionary_overlay(base: ModelDictionary, overlay: Mapping[str, Any]) -> ModelDictionary:
    """Return a new ModelDictionary with `overlay` merged on top of `base`."""
    variable_overrides = overlay.get("variables") if isinstance(overlay, Mapping) else None
    equation_overrides = overlay.get("equations") if isinstance(overlay, Mapping) else None

    merged_vars = (
        _apply_variable_overrides(base.variables, variable_overrides)
        if isinstance(variable_overrides, Mapping)
        else dict(base.variables)
    )
    merged_eqs = (
        _apply_equation_overrides(base.equations, equation_overrides)
        if isinstance(equation_overrides, Mapping)
        else dict(base.equations)
    )
    return ModelDictionary(
        merged_vars, merged_eqs, raw_data=dict(base.raw_data), meta=dict(base._meta)
    )


def load_dictionary_with_overlay(
    base_path: Path | str | None = None,
    overlay_path: Path | str | None = None,
) -> ModelDictionary:
    base = ModelDictionary.load(base_path)
    overlay = read_dictionary_overlay(overlay_path)
    if not overlay:
        return base
    return apply_dictionary_overlay(base, overlay)


def load_dictionary_with_overlays(
    base_path: Path | str | None = None,
    overlay_paths: list[Path | str] | None = None,
) -> ModelDictionary:
    base = ModelDictionary.load(base_path)
    merged: ModelDictionary = base
    for path in overlay_paths or []:
        overlay = read_dictionary_overlay(path)
        if not overlay:
            continue
        merged = apply_dictionary_overlay(merged, overlay)
    return merged
