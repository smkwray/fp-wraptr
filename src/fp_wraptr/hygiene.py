"""Project-root discovery for source checkouts."""

from __future__ import annotations

from pathlib import Path


def find_project_root(start: Path) -> Path | None:
    """Walk parents looking for pyproject.toml."""
    start = Path(start).resolve()
    for candidate in (start, *tuple(start.parents)):
        if (candidate / "pyproject.toml").exists():
            return candidate
    return None
