"""Managed workspace helpers for scenario and bundle authoring."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from fp_wraptr.scenarios.bundle import BundleConfig
from fp_wraptr.scenarios.catalog import CatalogEntry, load_scenario_catalog
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import load_scenario_config

from .models import (
    BundleDraft,
    BundleVariantDraft,
    DraftSourceRef,
    ScenarioDraft,
)

WORKSPACE_ROOT_RELATIVE = Path("projects_local/authoring")


@dataclass(frozen=True)
class WorkspaceInfo:
    """Minimal listing info for an authored workspace."""

    draft_kind: str
    family: str
    slug: str
    workspace_id: str
    label: str
    workspace_dir: Path
    draft_path: Path


def workspace_root(repo_root: Path | str) -> Path:
    return Path(repo_root).resolve() / WORKSPACE_ROOT_RELATIVE


def slugify(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "-", str(value).strip().lower())
    text = text.strip("-")
    return text or "workspace"


def workspace_dir(repo_root: Path | str, *, family: str, slug: str) -> Path:
    return workspace_root(repo_root) / slugify(family) / slugify(slug)


def workspace_id_for(*, family: str, slug: str) -> str:
    return f"{slugify(family)}--{slugify(slug)}"


def workspace_paths(base_dir: Path | str) -> dict[str, Path]:
    root = Path(base_dir).resolve()
    return {
        "root": root,
        "imports": root / "imports",
        "overlay": root / "overlay",
        "compiled": root / "compiled",
        "scenario_draft": root / "scenario_draft.yaml",
        "bundle_draft": root / "bundle_draft.yaml",
        "report": root / "compile_report.json",
    }


def list_workspaces(repo_root: Path | str) -> list[WorkspaceInfo]:
    root = workspace_root(repo_root)
    if not root.exists():
        return []
    found: list[WorkspaceInfo] = []
    for draft_path in sorted(root.glob("*/*/*_draft.yaml")):
        try:
            payload = draft_path.read_text(encoding="utf-8")
        except OSError:
            continue
        if draft_path.name == "scenario_draft.yaml":
            if not payload.strip():
                continue
            draft = ScenarioDraft.from_yaml(draft_path)
            found.append(
                WorkspaceInfo(
                    draft_kind="scenario",
                    family=draft.family,
                    slug=draft.slug,
                    workspace_id=_draft_workspace_id(draft),
                    label=draft.label,
                    workspace_dir=draft_path.parent,
                    draft_path=draft_path,
                )
            )
        elif draft_path.name == "bundle_draft.yaml":
            if not payload.strip():
                continue
            draft = BundleDraft.from_yaml(draft_path)
            found.append(
                WorkspaceInfo(
                    draft_kind="bundle",
                    family=draft.family,
                    slug=draft.slug,
                    workspace_id=_draft_workspace_id(draft),
                    label=draft.label,
                    workspace_dir=draft_path.parent,
                    draft_path=draft_path,
                )
            )
    return sorted(found, key=lambda item: (item.family.lower(), item.slug.lower(), item.draft_kind))


def load_workspace_draft(path: Path | str) -> ScenarioDraft | BundleDraft:
    draft_path = Path(path)
    if draft_path.name == "scenario_draft.yaml":
        return ScenarioDraft.from_yaml(draft_path)
    if draft_path.name == "bundle_draft.yaml":
        return BundleDraft.from_yaml(draft_path)
    raise ValueError(f"Unrecognized workspace draft path: {draft_path}")


def save_workspace_draft(
    draft: ScenarioDraft | BundleDraft,
    *,
    repo_root: Path | str,
    workspace: Path | str | None = None,
) -> Path:
    draft = _ensure_workspace_identity(draft)
    target = Path(workspace).resolve() if workspace is not None else workspace_dir(
        repo_root, family=draft.family, slug=draft.slug
    )
    paths = workspace_paths(target)
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["imports"].mkdir(parents=True, exist_ok=True)
    paths["overlay"].mkdir(parents=True, exist_ok=True)
    paths["compiled"].mkdir(parents=True, exist_ok=True)
    if isinstance(draft, ScenarioDraft):
        draft.to_yaml(paths["scenario_draft"])
        return paths["scenario_draft"]
    draft.to_yaml(paths["bundle_draft"])
    return paths["bundle_draft"]


def resolve_catalog_entry(
    source: DraftSourceRef,
    *,
    repo_root: Path | str,
    expected_kind: str,
) -> CatalogEntry | None:
    if source.kind != "catalog":
        return None
    catalog = load_scenario_catalog(repo_root=Path(repo_root).resolve())
    entry = catalog.find(source.value)
    if entry is None:
        raise ValueError(f"Catalog entry not found: {source.value}")
    if entry.kind != expected_kind:
        raise ValueError(f"Catalog entry {source.value!r} is {entry.kind}, expected {expected_kind}")
    return entry


def resolve_source_path(
    source: DraftSourceRef,
    *,
    repo_root: Path | str,
    expected_kind: str,
) -> Path:
    repo_root = Path(repo_root).resolve()
    if source.kind == "catalog":
        entry = resolve_catalog_entry(source, repo_root=repo_root, expected_kind=expected_kind)
        assert entry is not None
        return entry.resolved_path(repo_root=repo_root)
    raw = Path(source.value).expanduser()
    return raw if raw.is_absolute() else (repo_root / raw).resolve()


def create_scenario_draft_from_source(
    source: DraftSourceRef,
    *,
    repo_root: Path | str,
) -> ScenarioDraft:
    repo_root = Path(repo_root).resolve()
    catalog_entry = resolve_catalog_entry(source, repo_root=repo_root, expected_kind="scenario")
    path = resolve_source_path(source, repo_root=repo_root, expected_kind="scenario")
    config = load_scenario_config(path)
    family = catalog_entry.family if catalog_entry is not None else "custom"
    slug = slugify(config.name)
    label = catalog_entry.label if catalog_entry is not None else config.name
    return ScenarioDraft(
        workspace_id=workspace_id_for(family=family, slug=slug),
        family=family,
        slug=slug,
        label=label,
        description=config.description,
        source=source,
        scenario_name=config.name,
        forecast_start=config.forecast_start,
        forecast_end=config.forecast_end,
        backend=config.backend,
        fppy=dict(config.fppy or {}),
        track_variables=list(config.track_variables),
        extra={"source_path": str(path)},
    )


def create_bundle_draft_from_source(
    source: DraftSourceRef,
    *,
    repo_root: Path | str,
) -> BundleDraft:
    repo_root = Path(repo_root).resolve()
    catalog_entry = resolve_catalog_entry(source, repo_root=repo_root, expected_kind="bundle")
    path = resolve_source_path(source, repo_root=repo_root, expected_kind="bundle")
    bundle = BundleConfig.from_yaml(path)
    base_config = ScenarioConfig(**bundle.base)
    family = catalog_entry.family if catalog_entry is not None else "custom"
    slug = slugify(base_config.name)
    label = catalog_entry.label if catalog_entry is not None else base_config.name
    variants: list[BundleVariantDraft] = []
    for variant in bundle.variants:
        patch = dict(variant.patch)
        variants.append(
            BundleVariantDraft(
                variant_id=variant.name,
                label=variant.name.replace("_", " ").title(),
                scenario_name=f"{base_config.name}_{variant.name}",
                input_file=str(patch.get("input_file")) if patch.get("input_file") else None,
            )
        )
    return BundleDraft(
        workspace_id=workspace_id_for(family=family, slug=slug),
        family=family,
        slug=slug,
        label=label,
        description=str(bundle.base.get("description", "")),
        source=source,
        bundle_name=base_config.name,
        forecast_start=base_config.forecast_start,
        forecast_end=base_config.forecast_end,
        backend=base_config.backend,
        fppy=dict(base_config.fppy or {}),
        track_variables=list(base_config.track_variables),
        variants=variants,
        extra={"source_path": str(path)},
    )


def load_workspace_draft_by_id(
    workspace_id: str,
    *,
    repo_root: Path | str,
) -> tuple[ScenarioDraft | BundleDraft, Path]:
    wanted = str(workspace_id or "").strip().lower()
    if not wanted:
        raise ValueError("workspace_id must be non-empty")
    for info in list_workspaces(repo_root):
        if info.workspace_id.lower() == wanted:
            return load_workspace_draft(info.draft_path), info.draft_path
    raise FileNotFoundError(f"Workspace not found: {workspace_id}")


def _draft_workspace_id(draft: ScenarioDraft | BundleDraft) -> str:
    return draft.workspace_id or workspace_id_for(family=draft.family, slug=draft.slug)


def _ensure_workspace_identity(draft: ScenarioDraft | BundleDraft) -> ScenarioDraft | BundleDraft:
    workspace_id = _draft_workspace_id(draft)
    if draft.workspace_id == workspace_id:
        return draft
    return draft.model_copy(update={"workspace_id": workspace_id})
