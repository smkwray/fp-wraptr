"""Helpers for curated scenario/bundle catalog entries."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import yaml

from fp_wraptr.hygiene import find_project_root

__all__ = [
    "CatalogEntry",
    "ScenarioCatalog",
    "ScenarioCatalogEntry",
    "default_catalog_path",
    "load_scenario_catalog",
    "resolve_catalog_or_path",
]


@dataclass(frozen=True)
class CatalogEntry:
    """One curated launchable item (scenario or bundle)."""

    entry_id: str
    label: str
    kind: str
    path: Path
    family: str
    tags: tuple[str, ...] = ()
    surfaces: tuple[str, ...] = ("home", "new_run")
    order: int = 0
    public: bool = True
    new_run_visible: bool = True

    @property
    def id(self) -> str:
        """Back-compat alias used in older callers/tests."""
        return self.entry_id

    def resolved_path(self, *, repo_root: Path) -> Path:
        """Return absolute path rooted at repo when path is relative."""
        if self.path.is_absolute():
            return self.path
        return (repo_root / self.path).resolve()


ScenarioCatalogEntry = CatalogEntry


@dataclass(frozen=True)
class ScenarioCatalog:
    """Loaded scenario catalog with simple query helpers."""

    entries: tuple[CatalogEntry, ...]
    source_path: Path
    repo_root: Path

    @classmethod
    def from_yaml(
        cls,
        path: Path | str,
        *,
        project_root: Path | None = None,
        repo_root: Path | None = None,
    ) -> "ScenarioCatalog":
        """Back-compat constructor retained for older call sites."""
        root = repo_root if repo_root is not None else project_root
        return load_scenario_catalog(path, repo_root=root)

    def find(self, entry_id: str) -> CatalogEntry | None:
        needle = str(entry_id or "").strip()
        if not needle:
            return None
        for entry in self.entries:
            if entry.entry_id == needle:
                return entry
        return None

    def filtered(
        self,
        *,
        kind: str | None = None,
        family: str | None = None,
        tags: set[str] | None = None,
        surface: str | None = None,
        public_only: bool = False,
        new_run_visible_only: bool = False,
    ) -> list[CatalogEntry]:
        """Return entries matching optional visibility/metadata filters."""
        out: list[CatalogEntry] = []
        wanted_kind = str(kind or "").strip().lower()
        wanted_family = str(family or "").strip().lower()
        wanted_surface = str(surface or "").strip().lower()
        wanted_tags = {str(tag).strip().lower() for tag in (tags or set()) if str(tag).strip()}
        for entry in self.entries:
            if wanted_kind and entry.kind != wanted_kind:
                continue
            if wanted_family and entry.family.lower() != wanted_family:
                continue
            if public_only and not entry.public:
                continue
            if new_run_visible_only and not entry.new_run_visible:
                continue
            if wanted_surface and wanted_surface not in {item.lower() for item in entry.surfaces}:
                continue
            if wanted_tags:
                lowered = {tag.lower() for tag in entry.tags}
                if not wanted_tags.issubset(lowered):
                    continue
            out.append(entry)
        return sorted(out, key=lambda entry: (entry.order, entry.label.lower(), entry.entry_id))

    def for_surface(
        self,
        surface: str,
        *,
        kind: str | None = None,
        family: str | None = None,
        tags: set[str] | None = None,
        public_only: bool = True,
    ) -> list[CatalogEntry]:
        """Back-compat helper retained for older call sites/tests."""
        return self.filtered(
            surface=surface,
            kind=kind,
            family=family,
            tags=tags,
            public_only=public_only,
        )


def default_catalog_path(*, repo_root: Path | None = None) -> Path:
    """Canonical in-repo scenario catalog path."""
    root = Path(repo_root).resolve() if repo_root is not None else Path.cwd().resolve()
    return root / "projects_local" / "scenario_catalog.yaml"


def _coerce_bool(raw: Any, *, default: bool) -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default
    if isinstance(raw, (int, float)):
        return bool(raw)
    token = str(raw).strip().lower()
    if token in {"1", "true", "yes", "y", "on"}:
        return True
    if token in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _coerce_tags(raw: Any) -> tuple[str, ...]:
    if not isinstance(raw, list):
        return ()
    return tuple(str(item).strip() for item in raw if str(item).strip())


def _coerce_surfaces(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ("home", "new_run")
    if isinstance(raw, str):
        value = raw.strip()
        return (value,) if value else ("home", "new_run")
    if isinstance(raw, list):
        values = tuple(str(item).strip() for item in raw if str(item).strip())
        return values if values else ("home", "new_run")
    return ("home", "new_run")


def _coerce_int(raw: Any, *, default: int) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _resolve_repo_root(catalog_path: Path) -> Path:
    project_root = find_project_root(catalog_path)
    if project_root is not None:
        return project_root.resolve()
    return catalog_path.parent.resolve()


def _parse_catalog_entry(raw: Any, *, index: int) -> CatalogEntry:
    if not isinstance(raw, dict):
        raise ValueError(f"Catalog entry #{index} must be a mapping")
    entry_id = str(raw.get("id") or "").strip()
    if not entry_id:
        raise ValueError(f"Catalog entry #{index} is missing required field 'id'")
    kind = str(raw.get("kind") or "").strip().lower()
    if kind not in {"scenario", "bundle"}:
        raise ValueError(
            f"Catalog entry {entry_id!r} has invalid kind {kind!r}; expected 'scenario' or 'bundle'"
        )
    path_value = raw.get("path")
    if not isinstance(path_value, str) or not path_value.strip():
        raise ValueError(f"Catalog entry {entry_id!r} is missing required field 'path'")
    label = str(raw.get("label") or entry_id).strip() or entry_id
    family = str(raw.get("family") or "").strip()
    if not family:
        raise ValueError(f"Catalog entry {entry_id!r} is missing required field 'family'")
    return CatalogEntry(
        entry_id=entry_id,
        label=label,
        kind=kind,
        path=Path(path_value).expanduser(),
        family=family,
        tags=_coerce_tags(raw.get("tags")),
        surfaces=_coerce_surfaces(raw.get("surfaces")),
        order=_coerce_int(raw.get("order"), default=0),
        public=_coerce_bool(raw.get("public"), default=True),
        new_run_visible=_coerce_bool(raw.get("new_run_visible"), default=True),
    )


def load_scenario_catalog(
    path: Path | str | None = None,
    *,
    repo_root: Path | None = None,
    project_root: Path | None = None,
) -> ScenarioCatalog:
    """Load curated scenario/bundle catalog entries from YAML."""
    root_override = repo_root if repo_root is not None else project_root
    if path is None:
        if root_override is None:
            guessed = find_project_root(Path.cwd())
            root = guessed if guessed is not None else Path.cwd()
        else:
            root = Path(root_override)
        catalog_path = default_catalog_path(repo_root=root)
    else:
        catalog_path = Path(path).expanduser().resolve()

    root = (
        Path(root_override).resolve()
        if root_override is not None
        else _resolve_repo_root(catalog_path)
    )
    if not catalog_path.exists():
        return ScenarioCatalog(entries=(), source_path=catalog_path, repo_root=root)

    with catalog_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("Scenario catalog YAML must be a mapping with an 'entries' list")
    raw_entries = data.get("entries")
    if not isinstance(raw_entries, list):
        raise ValueError("Scenario catalog YAML is missing required 'entries' list")

    entries: list[CatalogEntry] = []
    seen_ids: set[str] = set()
    for idx, raw in enumerate(raw_entries, start=1):
        entry = _parse_catalog_entry(raw, index=idx)
        if entry.entry_id in seen_ids:
            raise ValueError(f"Duplicate catalog id: {entry.entry_id}")
        seen_ids.add(entry.entry_id)
        if not entry.path.is_absolute():
            entry = replace(entry, path=(root / entry.path).resolve())
        entries.append(entry)
    entries.sort(key=lambda item: (item.order, item.label.lower(), item.entry_id))
    return ScenarioCatalog(entries=tuple(entries), source_path=catalog_path, repo_root=root)


def resolve_catalog_or_path(
    value: str,
    *,
    catalog: ScenarioCatalog | None,
    repo_root: Path | None = None,
) -> tuple[Path, CatalogEntry | None]:
    """Resolve either a catalog id or a direct path string."""
    token = str(value or "").strip()
    if not token:
        raise ValueError("Selection is empty")
    if catalog is not None:
        match = catalog.find(token)
        if match is not None:
            return match.resolved_path(repo_root=catalog.repo_root), match
    candidate = Path(token).expanduser()
    if candidate.is_absolute():
        return candidate.resolve(), None
    root = None
    if repo_root is not None:
        root = Path(repo_root).resolve()
    elif catalog is not None:
        root = catalog.repo_root
    else:
        guessed = find_project_root(Path.cwd())
        root = guessed.resolve() if guessed is not None else Path.cwd().resolve()
    return (root / candidate).resolve(), None
