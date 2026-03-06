"""Pack manifests for local/public agent-facing scenario families."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator

from fp_wraptr.scenarios.authoring.compiler import load_card_specs
from fp_wraptr.scenarios.catalog import CatalogEntry, load_scenario_catalog

__all__ = [
    "PackManifest",
    "PackRecipe",
    "PackVisualization",
    "default_pack_roots",
    "describe_pack_manifest",
    "load_pack_manifests",
]


class PackRecipe(BaseModel):
    """One named agent workflow exposed by a pack."""

    recipe_id: str
    label: str
    summary: str = ""
    steps: list[str] = Field(default_factory=list)

    @field_validator("recipe_id", "label")
    @classmethod
    def _non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("recipe fields must be non-empty")
        return text


class PackVisualization(BaseModel):
    """One saved visualization intent for a pack or scenario family."""

    view_id: str
    label: str
    description: str = ""
    variables: list[str] = Field(default_factory=list)
    chart_type: str = "forecast_overlay"

    @field_validator("view_id", "label", "chart_type")
    @classmethod
    def _non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("visualization fields must be non-empty")
        return text


class PackManifest(BaseModel):
    """One generic pack of local or public authoring assets."""

    pack_id: str
    label: str
    family: str
    description: str = ""
    visibility: str = "local"
    source_dir: str = ""
    cards_family: str | None = None
    catalog_entry_ids: list[str] = Field(default_factory=list)
    recipes: list[PackRecipe] = Field(default_factory=list)
    visualizations: list[PackVisualization] = Field(default_factory=list)

    @field_validator("pack_id", "label", "family")
    @classmethod
    def _non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("pack fields must be non-empty")
        return text

    @field_validator("visibility")
    @classmethod
    def _normalize_visibility(cls, value: str) -> str:
        text = str(value).strip().lower() or "local"
        if text not in {"public", "local"}:
            raise ValueError("visibility must be 'public' or 'local'")
        return text

    @property
    def cards_scope(self) -> str:
        return str(self.cards_family or self.family).strip()


def default_pack_roots(*, repo_root: Path | str) -> tuple[Path, ...]:
    root = Path(repo_root).resolve()
    return (
        root / "src" / "fp_wraptr" / "packs",
        root / "projects_local" / "packs",
    )


def load_pack_manifests(*, repo_root: Path | str) -> list[tuple[PackManifest, Path]]:
    manifests: list[tuple[PackManifest, Path]] = []
    seen: set[str] = set()
    for root in default_pack_roots(repo_root=repo_root):
        if not root.exists():
            continue
        for path in sorted(root.glob("*/pack.y*ml")):
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                continue
            manifest = PackManifest.model_validate(payload)
            if manifest.pack_id in seen:
                raise ValueError(f"Duplicate pack_id: {manifest.pack_id}")
            seen.add(manifest.pack_id)
            manifests.append((manifest, path.resolve()))
    manifests.sort(key=lambda item: (item[0].visibility, item[0].label.lower(), item[0].pack_id))
    return manifests


def describe_pack_manifest(pack_id: str, *, repo_root: Path | str) -> dict[str, object]:
    root = Path(repo_root).resolve()
    catalog = load_scenario_catalog(repo_root=root)
    for manifest, path in load_pack_manifests(repo_root=root):
        if manifest.pack_id != str(pack_id).strip():
            continue
        cards = load_card_specs(repo_root=root, family=manifest.cards_scope)
        entries: list[dict[str, object]] = []
        for entry_id in manifest.catalog_entry_ids:
            entry = catalog.find(entry_id)
            if entry is None:
                entries.append({"entry_id": entry_id, "missing": True})
                continue
            entries.append(_catalog_entry_payload(entry, repo_root=root))
        return {
            "pack_id": manifest.pack_id,
            "label": manifest.label,
            "family": manifest.family,
            "description": manifest.description,
            "visibility": manifest.visibility,
            "manifest_path": str(path),
            "source_dir": manifest.source_dir,
            "cards_family": manifest.cards_scope,
            "catalog_entries": entries,
            "cards": [
                {
                    "card_id": spec.card_id,
                    "kind": spec.kind,
                    "label": spec.label,
                    "description": getattr(spec, "description", ""),
                }
                for spec in cards
            ],
            "recipes": [recipe.model_dump(mode="json") for recipe in manifest.recipes],
            "visualizations": [view.model_dump(mode="json") for view in manifest.visualizations],
        }
    raise FileNotFoundError(f"Pack not found: {pack_id}")


def _catalog_entry_payload(entry: CatalogEntry, *, repo_root: Path) -> dict[str, object]:
    return {
        "entry_id": entry.entry_id,
        "label": entry.label,
        "kind": entry.kind,
        "family": entry.family,
        "path": str(entry.resolved_path(repo_root=repo_root)),
        "public": entry.public,
        "surfaces": list(entry.surfaces),
        "tags": list(entry.tags),
    }
