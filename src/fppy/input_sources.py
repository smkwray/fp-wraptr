"""Load tabular input sources with pluggable formats."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

import pandas as pd


class NamedSource(Protocol):
    name: str
    path: str | Path
    format: str


SupportedFormat = tuple[str, ...]
SUPPORTED_FORMATS: SupportedFormat = ("csv", "json", "parquet")


def _normalize_format(value: str) -> str:
    return value.strip().lower()


def _infer_format(path: Path) -> str:
    extension = path.suffix.lower().lstrip(".")
    if extension in SUPPORTED_FORMATS:
        return extension
    raise ValueError(f"Unsupported tabular format for auto detection: {path.suffix!r}.")


def read_tabular_source(path: str | Path, format: str = "auto") -> pd.DataFrame:
    """Read a tabular file using an explicit or inferred format."""
    source_path = Path(path)
    requested_format = _normalize_format(format)

    if requested_format == "auto":
        requested_format = _infer_format(source_path)

    if requested_format not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Unsupported tabular format: {requested_format!r}. "
            "Supported formats: csv, json, parquet, auto."
        )

    if requested_format == "csv":
        return pd.read_csv(source_path)
    if requested_format == "json":
        return pd.read_json(source_path)

    try:
        return pd.read_parquet(source_path)
    except ImportError as exc:
        raise RuntimeError(
            "Parquet engine is unavailable. Install pyarrow or fastparquet."
        ) from exc


def load_named_sources(sources: Sequence[NamedSource]) -> dict[str, pd.DataFrame]:
    """Load named sources in the provided order and return a name->DataFrame dict."""
    return {source.name: read_tabular_source(source.path, source.format) for source in sources}
