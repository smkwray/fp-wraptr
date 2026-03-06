#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

FORBIDDEN_DIRS = {
    ".venv",
    ".wine*",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
}

FORBIDDEN_ONEDRIVE_ARTIFACT_GLOB = "*-BTHN*"
FORBIDDEN_ONEDRIVE_ARTIFACT_SCOPES = ("src", "tests")


def find_root(start: Path) -> Path:
    start = start.resolve()
    for candidate in (start, *tuple(start.parents)):
        if (candidate / "pyproject.toml").exists():
            return candidate
    return start


def scan(root: Path) -> list[Path]:
    found: list[Path] = []
    for name in sorted(FORBIDDEN_DIRS):
        if name == "__pycache__":
            for p in root.rglob("__pycache__"):
                if p.is_dir():
                    found.append(p)
            continue
        if name.endswith("*"):
            prefix = name[:-1]
            for p in root.iterdir():
                if p.is_dir() and p.name.startswith(prefix):
                    found.append(p)
            continue
        p = root / name
        if p.exists():
            found.append(p)

    for scope in FORBIDDEN_ONEDRIVE_ARTIFACT_SCOPES:
        scoped_root = root / scope
        if not scoped_root.exists() or not scoped_root.is_dir():
            continue
        for path in scoped_root.rglob(FORBIDDEN_ONEDRIVE_ARTIFACT_GLOB):
            if path.is_file():
                found.append(path)
    return found


def is_fixable_cache(path: Path) -> bool:
    """Return True when the path is a cache dir we can safely auto-delete."""
    name = path.name
    return name in {"__pycache__", ".pytest_cache", ".ruff_cache"}


def cleanup_fixable(found: list[Path]) -> tuple[list[Path], list[Path]]:
    removed: list[Path] = []
    skipped: list[Path] = []
    for path in found:
        if is_fixable_cache(path):
            shutil.rmtree(path, ignore_errors=True)
            removed.append(path)
        else:
            skipped.append(path)
    return removed, skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root", default=".", help="Project root (auto-detected via pyproject.toml)"
    )
    parser.add_argument(
        "--fix-caches",
        action="store_true",
        help="Delete fixable cache dirs (__pycache__, .pytest_cache, .ruff_cache) before failing.",
    )
    args = parser.parse_args(argv)

    root = find_root(Path(args.root))
    found = scan(root)
    if found and args.fix_caches:
        removed, skipped = cleanup_fixable(found)
        if removed:
            print("Removed cache dirs:")
            for p in removed:
                print(f"- {p}")
            print()
        if skipped:
            print("Skipped non-cache forbidden dirs:")
            for p in skipped:
                print(f"- {p}")
            print()
        found = scan(root)
    if not found:
        print("Hygiene: OK")
        return 0

    print("HYGIENE FAIL: forbidden cache/env dirs or sync-artifact files present:")
    for p in found:
        print(f"- {p}")
    print()
    print("Fix: delete these entries and use external venv + WINEPREFIX outside the repo.")
    print("Also remove OneDrive artifact files matching '*-BTHN*' under src/ and tests/.")
    print("Hint: configure a project-specific external venv in .env and run scripts/uvsafe ...")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
