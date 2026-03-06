"""Normalize raw FRED observations into FP-period quarterly payloads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd

from fp_wraptr.data.source_map import DataSource


class FredNormalizeError(RuntimeError):
    """Raised when FRED normalization inputs are invalid or unsupported."""


def fp_period_from_timestamp(ts: pd.Timestamp) -> str:
    quarter = ((int(ts.month) - 1) // 3) + 1
    return f"{int(ts.year)}.{quarter}"


def _normalize_series_to_quarterly(
    *,
    values: pd.Series,
    frequency: str,
) -> pd.Series:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    clean.index = pd.to_datetime(clean.index, errors="coerce")
    clean = clean[clean.index.notna()]
    if clean.empty:
        return clean

    freq = str(frequency).strip().upper()
    quarter_index = clean.index.to_period("Q").to_timestamp(how="start")

    if freq == "Q":
        # Keep one value per quarter (last value wins if duplicate rows exist).
        grouped = clean.groupby(quarter_index).last()
        grouped.index = pd.to_datetime(grouped.index)
        return grouped.sort_index()

    if freq == "M":
        # Rates/indexes: monthly values aggregate to quarterly mean in MVP.
        grouped = clean.groupby(quarter_index).mean()
        grouped.index = pd.to_datetime(grouped.index)
        return grouped.sort_index()

    if freq == "A":
        raise FredNormalizeError("Unsupported frequency 'A' for MVP quarterly normalization")

    raise FredNormalizeError(f"Unsupported frequency '{frequency}'")


def normalize_fred_frame_to_fp_periods(
    *,
    observations: pd.DataFrame,
    mappings: Mapping[str, DataSource],
    variables: Sequence[str] | None = None,
) -> dict[str, dict[str, float]]:
    """Return normalized quarterly observations keyed by FP periods (`YYYY.Q`)."""
    requested = [
        str(name).strip().upper() for name in (variables or mappings.keys()) if str(name).strip()
    ]
    output: dict[str, dict[str, float]] = {}

    for var_name in requested:
        entry = mappings.get(var_name)
        if entry is None:
            raise FredNormalizeError(f"Variable '{var_name}' missing from source-map mappings")

        series_id = (entry.series_id or entry.fred_fallback).strip()
        if not series_id:
            raise FredNormalizeError(f"Variable '{var_name}' has no FRED series_id/fallback")
        if series_id not in observations.columns:
            raise FredNormalizeError(
                f"Missing FRED observations column '{series_id}' for variable '{var_name}'"
            )

        quarterly = _normalize_series_to_quarterly(
            values=observations[series_id],
            frequency=entry.frequency,
        )
        output[var_name] = {
            fp_period_from_timestamp(pd.Timestamp(idx)): float(value)
            for idx, value in quarterly.items()
            if pd.notna(value)
        }

    return output
