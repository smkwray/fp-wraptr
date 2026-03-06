"""Workspace helpers for dashboard scenario/bundle authoring."""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field

from fp_wraptr.dashboard.ptcoef_editor import rewrite_ptcoef_text
from fp_wraptr.dashboard.ptcoef_tools import load_ptcoef_deck
from fp_wraptr.scenarios.config import ScenarioConfig

__all__ = [
    "AuthoringWorkspace",
    "CompiledWorkspace",
    "compile_workspace",
    "create_workspace_from_bundle_yaml",
    "create_workspace_from_scenario_config",
    "list_workspace_files",
    "load_workspace",
    "save_workspace",
    "workspace_file_path",
]


_WORKSPACE_DIR = Path("projects_local/authoring/workspaces")
_BUILD_DIR = Path("projects_local/authoring/build")


class AuthoringWorkspace(BaseModel):
    """Persisted New Run authoring workspace payload."""

    version: int = 1
    workspace_id: str
    label: str
    kind: Literal["scenario", "bundle"] = "scenario"
    source_catalog_id: str | None = None
    source_yaml_path: str | None = None
    scenario: dict[str, Any] = Field(default_factory=dict)
    bundle: dict[str, Any] = Field(default_factory=dict)
    card_values: dict[str, dict[str, float]] = Field(default_factory=dict)

    def scenario_config(self) -> ScenarioConfig:
        if self.kind != "scenario":
            raise ValueError("scenario_config is only valid for scenario workspaces")
        return ScenarioConfig(**self.scenario)


@dataclass(frozen=True)
class CompiledWorkspace:
    """Compiled workspace ready for run execution."""

    config: ScenarioConfig
    generated_overlay_dir: Path | None
    generated_files: tuple[Path, ...]


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", value.strip().lower()).strip("-_")
    return slug or "workspace"


def workspace_file_path(*, repo_root: Path, workspace_id: str) -> Path:
    return (repo_root / _WORKSPACE_DIR / f"{_slugify(workspace_id)}.yaml").resolve()


def list_workspace_files(*, repo_root: Path) -> list[Path]:
    root = (repo_root / _WORKSPACE_DIR).resolve()
    if not root.exists():
        return []
    return sorted(root.glob("*.yaml"))


def load_workspace(path: Path) -> AuthoringWorkspace:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise ValueError(f"Workspace YAML must be a mapping: {path}")
    workspace = AuthoringWorkspace(**payload)
    return _normalize_workspace_paths(workspace=workspace, repo_root=path.parents[3])


def save_workspace(workspace: AuthoringWorkspace, *, repo_root: Path) -> Path:
    path = workspace_file_path(repo_root=repo_root, workspace_id=workspace.workspace_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = workspace.model_dump(mode="json")
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def create_workspace_from_scenario_config(
    *,
    workspace_id: str,
    label: str,
    config: ScenarioConfig,
    source_yaml_path: Path,
    source_catalog_id: str | None = None,
) -> AuthoringWorkspace:
    return AuthoringWorkspace(
        workspace_id=_slugify(workspace_id),
        label=label.strip() or config.name,
        kind="scenario",
        source_catalog_id=source_catalog_id,
        source_yaml_path=str(source_yaml_path),
        scenario=config.model_dump(mode="json"),
    )


def create_workspace_from_bundle_yaml(
    *,
    workspace_id: str,
    label: str,
    bundle_yaml_path: Path,
    source_catalog_id: str | None = None,
) -> AuthoringWorkspace:
    return AuthoringWorkspace(
        workspace_id=_slugify(workspace_id),
        label=label.strip() or bundle_yaml_path.stem,
        kind="bundle",
        source_catalog_id=source_catalog_id,
        source_yaml_path=str(bundle_yaml_path),
        bundle={"yaml_path": str(bundle_yaml_path)},
    )


def _normalize_workspace_paths(*, workspace: AuthoringWorkspace, repo_root: Path) -> AuthoringWorkspace:
    if workspace.kind != "scenario":
        return workspace
    scenario = dict(workspace.scenario)
    for key in ("fp_home", "input_overlay_dir"):
        raw = scenario.get(key)
        if not raw:
            continue
        resolved = Path(raw).expanduser()
        if not resolved.is_absolute():
            resolved = (repo_root / resolved).resolve()
        scenario[key] = str(resolved)
    return workspace.model_copy(update={"scenario": scenario})


def _ptcoef_updates(workspace: AuthoringWorkspace) -> dict[str, float]:
    raw = workspace.card_values.get("deck.ptcoef", {})
    updates: dict[str, float] = {}
    for name, value in raw.items():
        try:
            updates[str(name).strip().upper()] = float(value)
        except (TypeError, ValueError):
            continue
    return updates


def compile_workspace(workspace: AuthoringWorkspace, *, repo_root: Path) -> CompiledWorkspace:
    if workspace.kind != "scenario":
        raise ValueError("Only scenario workspaces support compile.")

    normalized = _normalize_workspace_paths(workspace=workspace, repo_root=repo_root)
    config = normalized.scenario_config()
    build_root = (repo_root / _BUILD_DIR / normalized.workspace_id).resolve()
    overlay_output = build_root / "overlay"
    if overlay_output.exists():
        shutil.rmtree(overlay_output)
    overlay_output.mkdir(parents=True, exist_ok=True)

    generated_files: list[Path] = []
    base_overlay = Path(config.input_overlay_dir).resolve() if config.input_overlay_dir else None
    if base_overlay is not None and base_overlay.exists():
        shutil.copytree(base_overlay, overlay_output, dirs_exist_ok=True)

    ptcoef_updates = _ptcoef_updates(normalized)
    if ptcoef_updates:
        deck = load_ptcoef_deck(overlay_dir=base_overlay, fp_home=config.fp_home)
        rewritten, _missing = rewrite_ptcoef_text(deck.text, ptcoef_updates)
        target = overlay_output / "ptcoef.txt"
        target.write_text(rewritten, encoding="utf-8")
        generated_files.append(target)

    if any(overlay_output.iterdir()) or ptcoef_updates:
        config.input_overlay_dir = overlay_output
    else:
        config.input_overlay_dir = None
        shutil.rmtree(overlay_output, ignore_errors=True)
        overlay_output = None

    return CompiledWorkspace(
        config=config,
        generated_overlay_dir=overlay_output,
        generated_files=tuple(generated_files),
    )
