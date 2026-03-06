"""Helpers for reading and summarizing canonical template assets."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from fppy.paths import REQUIRED_TEMPLATE_FILES, TEMPLATE_DIR

_EQ_LINE_RE = re.compile(r"(?m)^\s*EQ\b")
_IDENT_LINE_RE = re.compile(r"(?m)^\s*IDENT\b")


@dataclass(frozen=True)
class FminputSummary:
    model_header: str
    total_lines: int
    equation_count: int
    identity_count: int


def _resolve_fminput_path(path: str | Path | None) -> Path:
    if path is None:
        return TEMPLATE_DIR / "fminput.txt"
    return Path(path)


def summarize_fminput(path: str | Path | None = None) -> FminputSummary:
    source = _resolve_fminput_path(path)
    content = source.read_text(encoding="utf-8", errors="replace")
    lines = content.splitlines()
    model_header = next((line.strip() for line in lines if line.strip()), "")
    return FminputSummary(
        model_header=model_header,
        total_lines=len(lines),
        equation_count=len(_EQ_LINE_RE.findall(content)),
        identity_count=len(_IDENT_LINE_RE.findall(content)),
    )


def required_template_files_present() -> bool:
    return all(path.exists() for path in REQUIRED_TEMPLATE_FILES)


__all__ = [
    "FminputSummary",
    "required_template_files_present",
    "summarize_fminput",
]
