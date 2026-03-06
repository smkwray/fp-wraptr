"""Canonical repository paths used across FAIR Python tooling."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_DIR = PROJECT_ROOT / "data" / "templates" / "us_2025"
FORTRAN_REFERENCE = PROJECT_ROOT / "references" / "fortran" / "fp_2013.for"

REQUIRED_TEMPLATE_FILES = (
    TEMPLATE_DIR / "fminput.txt",
    TEMPLATE_DIR / "fmdata.txt",
    TEMPLATE_DIR / "fmage.txt",
    TEMPLATE_DIR / "fmexog.txt",
    TEMPLATE_DIR / "fmout.txt",
)

__all__ = [
    "FORTRAN_REFERENCE",
    "PROJECT_ROOT",
    "REQUIRED_TEMPLATE_FILES",
    "TEMPLATE_DIR",
]
