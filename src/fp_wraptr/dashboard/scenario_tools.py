"""Helpers for dashboard scenario authoring and preflight."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from fp_wraptr.scenarios.config import ScenarioConfig, VariableOverride
from fp_wraptr.scenarios.input_tree import prepare_work_dir_for_fp_run

__all__ = [
    "ScenarioInputPreflight",
    "build_tweaked_config",
    "preflight_scenario_input",
]


@dataclass(frozen=True)
class ScenarioInputPreflight:
    """Read-only preview of the staged FP input tree for one scenario."""

    input_file: str
    fp_home: Path
    overlay_dir: Path | None
    entry_source_path: Path | None
    entry_source_kind: str | None
    include_files: tuple[str, ...] = ()
    load_data_files: tuple[str, ...] = ()
    expected_output_files: tuple[str, ...] = ()
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.entry_source_path is not None


def _resolve_entry_input_source(config: ScenarioConfig) -> tuple[Path | None, str | None]:
    fp_candidate = Path(config.fp_home) / config.input_file
    if fp_candidate.exists():
        return fp_candidate, "fp_home"

    overlay_dir = getattr(config, "input_overlay_dir", None)
    if overlay_dir is not None:
        overlay_candidate = Path(overlay_dir) / config.input_file
        if overlay_candidate.exists():
            return overlay_candidate, "overlay"

    return None, None


def preflight_scenario_input(config: ScenarioConfig) -> ScenarioInputPreflight:
    """Resolve and stage an input tree in a temp dir for dashboard preview."""

    fp_home = Path(config.fp_home)
    overlay_dir = Path(config.input_overlay_dir) if config.input_overlay_dir else None
    entry_source_path, entry_source_kind = _resolve_entry_input_source(config)

    if entry_source_path is None:
        searched = [str(fp_home / config.input_file)]
        if overlay_dir is not None:
            searched.append(str(overlay_dir / config.input_file))
        return ScenarioInputPreflight(
            input_file=config.input_file,
            fp_home=fp_home,
            overlay_dir=overlay_dir,
            entry_source_path=None,
            entry_source_kind=None,
            error=(
                "Scenario input file not found. "
                f"input_file={config.input_file!r} searched: {', '.join(searched)}"
            ),
        )

    try:
        with TemporaryDirectory(prefix="fpwraptr_preflight_") as tmp:
            work_dir = Path(tmp) / "work"
            work_dir.mkdir(parents=True, exist_ok=True)
            staged_entry = work_dir / config.input_file
            staged_entry.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(entry_source_path, staged_entry)
            manifest = prepare_work_dir_for_fp_run(
                entry_input=staged_entry,
                work_dir=work_dir,
                overlay_dir=overlay_dir,
                fp_home=fp_home,
            )
    except Exception as exc:
        return ScenarioInputPreflight(
            input_file=config.input_file,
            fp_home=fp_home,
            overlay_dir=overlay_dir,
            entry_source_path=entry_source_path,
            entry_source_kind=entry_source_kind,
            error=str(exc),
        )

    return ScenarioInputPreflight(
        input_file=config.input_file,
        fp_home=fp_home,
        overlay_dir=overlay_dir,
        entry_source_path=entry_source_path,
        entry_source_kind=entry_source_kind,
        include_files=manifest.include_files,
        load_data_files=manifest.load_data_files,
        expected_output_files=manifest.expected_output_files,
    )


def build_tweaked_config(
    *,
    base_config: ScenarioConfig,
    new_name: str,
    description: str,
    backend: str,
    fppy_num_threads: int,
    overrides: dict[str, VariableOverride],
) -> ScenarioConfig:
    """Copy a scenario for dashboard tweaking while preserving deck context."""

    tweaked_config = base_config.model_copy(deep=True)
    tweaked_config.name = new_name
    tweaked_config.description = description
    tweaked_config.backend = backend
    tweaked_config.fppy = base_config.fppy.model_copy(
        update={"num_threads": int(fppy_num_threads)}
    )
    tweaked_config.overrides = dict(overrides)
    return tweaked_config
