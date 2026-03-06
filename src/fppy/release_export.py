from __future__ import annotations

import fnmatch
import shutil
import tarfile
import zipfile
from pathlib import Path

from fppy.release import RESTRICTED_PATHS

ARCHIVE_FORMATS: tuple[str, ...] = ("zip", "tar.gz")

DEFAULT_INCLUDE_PATTERNS: tuple[str, ...] = (
    "README.md",
    "pyproject.toml",
    "src/**",
    "tests/**",
    "docs/**",
    "examples/**",
    "scripts/**",
    ".github/**",
    "do/*.md",
    "do/cani/*.md",
    "do/feli/*.md",
    "do/spark*.md",
    "run_sparks.sh",
)

_HIDDEN_METADATA_FILENAMES: tuple[str, ...] = (
    ".DS_Store",
    "Thumbs.db",
    "desktop.ini",
)
_HIDDEN_OR_CACHE_PARTS: set[str] = {
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    ".git",
    ".venv",
    "venv",
}


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _is_restricted_path(relative_path: Path) -> bool:
    for restricted in RESTRICTED_PATHS:
        if _is_relative_to(relative_path, restricted) or _is_relative_to(
            restricted, relative_path
        ):
            return True
    return False


def _is_hidden_or_cache_path(relative_path: Path) -> bool:
    if relative_path.name in _HIDDEN_METADATA_FILENAMES:
        return True
    for part in relative_path.parts:
        if part in _HIDDEN_OR_CACHE_PARTS:
            return True
        if part.startswith("."):
            return True
    return False


def build_export_manifest(
    workspace_root: Path,
    *,
    include_patterns: tuple[str, ...] = DEFAULT_INCLUDE_PATTERNS,
) -> tuple[Path, ...]:
    root = workspace_root.resolve()
    collected: set[Path] = set()

    for candidate in root.rglob("*"):
        if not candidate.is_file():
            continue
        resolved = candidate.resolve()
        try:
            relative = resolved.relative_to(root)
        except ValueError:
            continue
        relative_text = relative.as_posix()
        if not any(fnmatch.fnmatchcase(relative_text, pattern) for pattern in include_patterns):
            continue
        if _is_restricted_path(relative):
            continue
        if _is_hidden_or_cache_path(relative):
            continue
        collected.add(resolved)

    return tuple(sorted(collected, key=str))


def export_artifact_tree(
    workspace_root: Path,
    output_dir: Path,
    *,
    include_patterns: tuple[str, ...] = DEFAULT_INCLUDE_PATTERNS,
    overwrite: bool = False,
    dry_run: bool = False,
) -> tuple[Path, ...]:
    root = workspace_root.resolve()
    destination = output_dir.resolve()
    manifest = list(build_export_manifest(root, include_patterns=include_patterns))
    if _is_relative_to(destination, root):
        manifest = [path for path in manifest if not _is_relative_to(path, destination)]
    export_targets = tuple(destination / path.relative_to(root) for path in manifest)

    if dry_run:
        return export_targets

    if destination.exists():
        if not overwrite:
            raise FileExistsError(f"Output directory already exists: {destination}")
        shutil.rmtree(destination)

    destination.mkdir(parents=True, exist_ok=True)
    for source, target in zip(manifest, export_targets, strict=False):
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)

    return export_targets


def archive_artifact_tree(
    artifact_dir: Path,
    output_path: Path,
    *,
    archive_format: str,
    overwrite: bool = False,
) -> Path:
    if archive_format not in ARCHIVE_FORMATS:
        supported = ", ".join(ARCHIVE_FORMATS)
        raise ValueError(
            f"Unsupported archive format: {archive_format!r}. Supported formats: {supported}"
        )

    source_root = artifact_dir.resolve()
    if not source_root.exists() or not source_root.is_dir():
        raise NotADirectoryError(f"Artifact directory not found: {source_root}")

    archive_path = output_path.resolve()
    if archive_path.exists() and not overwrite:
        raise FileExistsError(f"Archive path already exists: {archive_path}")
    if archive_path.exists() and archive_path.is_dir():
        raise IsADirectoryError(f"Archive output path is a directory: {archive_path}")
    archive_path.parent.mkdir(parents=True, exist_ok=True)

    file_paths = sorted((path for path in source_root.rglob("*") if path.is_file()), key=str)

    if archive_format == "zip":
        with zipfile.ZipFile(
            archive_path,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            for file_path in file_paths:
                archive.write(file_path, arcname=file_path.relative_to(source_root).as_posix())
    else:
        with tarfile.open(archive_path, mode="w:gz") as archive:
            for file_path in file_paths:
                archive.add(file_path, arcname=file_path.relative_to(source_root).as_posix())

    return archive_path


__all__ = [
    "ARCHIVE_FORMATS",
    "DEFAULT_INCLUDE_PATTERNS",
    "archive_artifact_tree",
    "build_export_manifest",
    "export_artifact_tree",
]
