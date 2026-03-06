"""Repo hygiene guardrails.

These checks are intentionally strict: the project must not create local venv/Wine
prefix directories inside the repo tree.
"""

from __future__ import annotations

from pathlib import Path


def find_project_root(start: Path) -> Path | None:
    """Walk parents looking for pyproject.toml."""
    start = Path(start).resolve()
    for candidate in (start, *tuple(start.parents)):
        if (candidate / "pyproject.toml").exists():
            return candidate
    return None


def _matches_forbidden_name(name: str, pattern: str) -> bool:
    if pattern.endswith("*"):
        return name.startswith(pattern[:-1])
    return name == pattern


def forbidden_dirs_present(
    root: Path, *, forbidden: tuple[str, ...] = (".venv", ".wine*")
) -> list[Path]:
    root = Path(root)
    present: list[Path] = []
    if not root.exists():
        return present
    for child in root.iterdir():
        if not child.is_dir():
            continue
        if any(_matches_forbidden_name(child.name, pattern) for pattern in forbidden):
            present.append(child)
    return present


def assert_no_forbidden_dirs(
    root: Path, *, forbidden: tuple[str, ...] = (".venv", ".wine*")
) -> None:
    present = forbidden_dirs_present(root, forbidden=forbidden)
    if not present:
        return
    pretty = ", ".join(str(p) for p in present)
    raise RuntimeError(
        "Forbidden repo-local environment/prefix directories are present: "
        f"{pretty}. Delete them and use an external venv and a Wine prefix outside the repo."
    )
