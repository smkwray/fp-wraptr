"""Execution input loading and external source merging."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from fppy.config import ModelInputConfig, load_model_config
from fppy.input_sources import load_named_sources
from fppy.io.legacy_data import parse_fmage_file, parse_fmdata_file, parse_fmexog_file

_VALID_CONFLICT_MODES = {"error", "prefix", "overwrite"}
_SMPL_RE = re.compile(r"^\d{4}\.[1-4]$")


@dataclass(frozen=True)
class ExecutionInputBundle:
    fmdata: pd.DataFrame
    fmage: pd.DataFrame
    fmexog: pd.DataFrame
    external_sources: dict[str, pd.DataFrame]
    merged_data: pd.DataFrame
    legacy_base_dir: Path | None = None


def _validate_conflict(conflict: str) -> str:
    if conflict not in _VALID_CONFLICT_MODES:
        raise ValueError("conflict must be one of 'error', 'prefix', or 'overwrite'.")
    return conflict


def normalize_external_source_frame(frame: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """Normalize an external source frame to an index-based, sorted SMPL frame."""
    if "smpl" in frame.columns:
        normalized = frame.set_index("smpl")
    elif "year" in frame.columns and "quarter" in frame.columns:
        normalized = _normalize_from_year_quarter_columns(
            frame, quarter_column="quarter", source_name=source_name
        )
    elif "year" in frame.columns and "qtr" in frame.columns:
        normalized = _normalize_from_year_quarter_columns(
            frame, quarter_column="qtr", source_name=source_name
        )
    elif "year" in frame.columns:
        normalized = _normalize_from_year_column(frame, source_name=source_name)
    else:
        normalized = frame.copy(deep=True)

    normalized.index = normalized.index.astype(str)
    if not normalized.index.is_monotonic_increasing:
        normalized = normalized.sort_index()

    normalized.index = pd.Index(normalized.index, name="smpl")
    if normalized.index.has_duplicates:
        raise ValueError(
            f"External source {source_name!r} contains duplicate 'smpl' index values."
        )
    invalid = [value for value in normalized.index if not _SMPL_RE.match(value)]
    if invalid:
        raise ValueError(
            f"External source {source_name!r} must have an 'smpl' column or index values "
            "in <year>.<quarter> format (for example, 2020.1)."
        )

    return normalized.sort_index()


def _normalize_from_year_quarter_columns(
    frame: pd.DataFrame,
    *,
    quarter_column: str,
    source_name: str,
) -> pd.DataFrame:
    converted = frame.copy(deep=True)
    smpl_values = [
        _format_smpl_period(
            _coerce_integer(year, label=f"{source_name}.year"),
            _coerce_integer(quarter, label=f"{source_name}.{quarter_column}"),
            label=source_name,
        )
        for year, quarter in zip(converted["year"], converted[quarter_column], strict=False)
    ]
    converted["smpl"] = smpl_values
    return converted.drop(columns=["year", quarter_column]).set_index("smpl")


def _normalize_from_year_column(frame: pd.DataFrame, *, source_name: str) -> pd.DataFrame:
    converted = frame.copy(deep=True)
    payload_columns = [column for column in converted.columns if column != "year"]
    expanded_rows: list[dict[str, object]] = []

    for _, row in converted.iterrows():
        year = _coerce_integer(row["year"], label=f"{source_name}.year")
        payload = {column: row[column] for column in payload_columns}
        for quarter in (1, 2, 3, 4):
            expanded_rows.append({
                "smpl": _format_smpl_period(year, quarter, label=source_name),
                **payload,
            })

    return pd.DataFrame(expanded_rows).set_index("smpl")


def _coerce_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be an integer value.")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    text = str(value).strip()
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return int(text)
    raise ValueError(f"{label} must be an integer value.")


def _format_smpl_period(year: int, quarter: int, *, label: str) -> str:
    if quarter not in (1, 2, 3, 4):
        raise ValueError(f"{label} quarter must be between 1 and 4.")
    if year < 0:
        raise ValueError(f"{label} year must be non-negative.")
    return f"{year:04d}.{quarter}"


def merge_external_sources(
    base: pd.DataFrame, sources: dict[str, pd.DataFrame], conflict: str = "error"
) -> pd.DataFrame:
    """Merge an index-aligned base frame with named external sources."""
    conflict = _validate_conflict(conflict)
    merged = base.copy(deep=True)

    for source_name, source_frame in sources.items():
        normalized = normalize_external_source_frame(source_frame, source_name=source_name)
        overlaps = merged.columns.intersection(normalized.columns)

        if conflict == "error" and len(overlaps) > 0:
            raise ValueError(
                f"External source {source_name!r} contains duplicate columns: "
                f"{tuple(overlaps.tolist())}"
            )

        if conflict == "prefix":
            renamed = {column: f"{source_name}__{column}" for column in overlaps}
            normalized = normalized.rename(columns=renamed)

        merged = merged.reindex(merged.index.union(normalized.index)).sort_index()
        for column in normalized.columns:
            merged.loc[normalized.index, column] = normalized[column]

    return merged


def load_execution_input_bundle(
    config: ModelInputConfig | None = None,
    config_path: Path | str | None = None,
    merge_external: bool = False,
    conflict: str = "error",
) -> ExecutionInputBundle:
    """Load legacy + optional external execution inputs."""
    _ = _validate_conflict(conflict)
    active_config = config or load_model_config(config_path)

    fmdata = parse_fmdata_file(active_config.legacy.fmdata)
    fmage = parse_fmage_file(active_config.legacy.fmage)
    fmexog = parse_fmexog_file(active_config.legacy.fmexog)

    external_sources = load_named_sources(active_config.external_sources)
    merged_data = fmdata
    if merge_external:
        merged_data = merge_external_sources(
            fmdata,
            external_sources,
            conflict=conflict,
        )

    return ExecutionInputBundle(
        fmdata=fmdata,
        fmage=fmage,
        fmexog=fmexog,
        external_sources=external_sources,
        merged_data=merged_data,
        legacy_base_dir=active_config.legacy.fminput.parent,
    )
