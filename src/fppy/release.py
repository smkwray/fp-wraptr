from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

RESTRICTED_PATHS: tuple[Path, ...] = (
    Path("FM"),
    Path("sonyc"),
    Path("references") / "fortran",
    Path("data") / "templates" / "us_2025",
)


class ArtifactValidationIssue(NamedTuple):
    kind: str
    path: str


_HIDDEN_METADATA_FILENAMES: tuple[str, ...] = ("Thumbs.db", "desktop.ini")


def detect_restricted_workspace_paths(workspace_root: Path) -> tuple[Path, ...]:
    """Return detected restricted paths in deterministic order."""
    root = workspace_root.resolve()
    detected: list[Path] = []
    for candidate in RESTRICTED_PATHS:
        path = root / candidate
        if path.exists():
            detected.append(path.resolve())

    return tuple(sorted(detected, key=lambda path: str(path)))


def validate_artifact_directory(artifact_directory: Path) -> tuple[ArtifactValidationIssue, ...]:
    """Return deterministic validation issues for export artifacts."""
    root = artifact_directory.resolve()
    issues: list[ArtifactValidationIssue] = []

    for restricted_path in detect_restricted_workspace_paths(root):
        issues.append(
            ArtifactValidationIssue(
                "restricted_path",
                str(restricted_path.relative_to(root)),
            )
        )

    for entry in sorted(root.rglob("*"), key=lambda path: str(path)):
        if entry.name in _HIDDEN_METADATA_FILENAMES or entry.name.startswith("."):
            issues.append(
                ArtifactValidationIssue(
                    "hidden_metadata_file",
                    str(entry.relative_to(root)),
                )
            )

    return tuple(sorted(issues, key=lambda issue: (issue.kind, issue.path)))


def format_artifact_validation_issues(issues: tuple[ArtifactValidationIssue, ...]) -> str:
    """Format validation issues into a concise report."""
    if not issues:
        return (
            "No release artifact issues were found.\n"
            "No restricted paths or hidden metadata files were detected."
        )

    lines = [
        "Release artifact issues were found:",
        *(f"- {kind}: {path}" for kind, path in issues),
        "Remove or exclude these items before release.",
    ]
    return "\n".join(lines)


def format_release_check_report(restricted_paths: tuple[Path, ...]) -> str:
    """Format a concise report for restricted path detection."""
    if not restricted_paths:
        return (
            "No restricted upstream paths were found.\nThe workspace is clean for public release."
        )

    ordered_restricted_paths = tuple(
        sorted((path.resolve() for path in restricted_paths), key=lambda path: str(path))
    )
    lines = [
        "Restricted upstream paths were found; exclude them before public release:",
        *[f"- {path}" for path in ordered_restricted_paths],
        "Please remove or ignore these paths when building public artifacts.",
    ]
    return "\n".join(lines)
