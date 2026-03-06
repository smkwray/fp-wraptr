"""Normalize FRED observations for writing into `fmdata.txt`.

This module is intentionally separate from `fp_wraptr.fred.overlay`:
- Overlay is for charting (forecast vs actual) and can make chart-specific
  normalization choices.
- fmdata updates must match the Fair-Parke model's historical-data conventions.

Key contract:
  Input FRED observations are timestamp-indexed (monthly or quarterly).
  Output values are quarterly and indexed by FP period strings ("YYYY.Q").
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:  # pragma: no cover
    from fp_wraptr.data.source_map import DataSource, SourceMap

__all__ = [
    "FredFmdataNormalizeError",
    "normalize_fred_for_fmdata",
    "normalize_observations_for_fmdata",
    "period_to_quarter_start",
    "quarter_start_to_period",
]


class FredFmdataNormalizeError(RuntimeError):
    """Raised when FRED normalization inputs are invalid or unsupported."""


_FP_PERIOD_RE = re.compile(r"^(?P<year>\d{4})\.(?P<quarter>[1-4])$")
_QUARTER_TOKEN_RE = re.compile(r"^(?P<year>\d{4})Q(?P<quarter>[1-4])$", re.IGNORECASE)


def period_to_quarter_start(period: str) -> pd.Timestamp:
    """Convert FP period string like '2025.4' to a quarter-start timestamp."""
    text = str(period).strip()
    match = _FP_PERIOD_RE.match(text)
    if not match:
        raise FredFmdataNormalizeError(f"Invalid FP period '{period}' (expected YYYY.Q)")
    year = int(match.group("year"))
    quarter = int(match.group("quarter"))
    month = (quarter - 1) * 3 + 1
    return pd.Timestamp(year=year, month=month, day=1)


def quarter_start_to_period(ts: pd.Timestamp) -> str:
    """Convert a quarter-start timestamp to an FP period string ('YYYY.Q')."""
    stamp = pd.Timestamp(ts)
    quarter = ((int(stamp.month) - 1) // 3) + 1
    return f"{int(stamp.year)}.{quarter}"


def _quarter_end_inclusive(period: str) -> pd.Timestamp:
    start = period_to_quarter_start(period)
    end_exclusive = start + pd.DateOffset(months=3)
    return end_exclusive - pd.Timedelta(days=1)


def _parse_quarter_token(value: str) -> pd.Timestamp | None:
    """Parse a token like 2008Q4 into a quarter-start timestamp."""
    token = str(value).strip().upper()
    if not token:
        return None
    match = _QUARTER_TOKEN_RE.match(token)
    if not match:
        return None
    year = int(match.group("year"))
    quarter = int(match.group("quarter"))
    month = (quarter - 1) * 3 + 1
    return pd.Timestamp(year=year, month=month, day=1)


def _parse_quarter_end_exclusive(value: str) -> pd.Timestamp | None:
    start = _parse_quarter_token(value)
    if start is None:
        return None
    return start + pd.DateOffset(months=3)


def _apply_window_rules(series: pd.Series, entry: DataSource) -> pd.Series:
    window_start = str(getattr(entry, "window_start", "")).strip()
    window_end = str(getattr(entry, "window_end", "")).strip()
    outside_fill = getattr(entry, "outside_window_value", None)
    if not (window_start or window_end or outside_fill is not None):
        return series

    mask = pd.Series(True, index=series.index)
    start_ts = _parse_quarter_token(window_start)
    end_exclusive_ts = _parse_quarter_end_exclusive(window_end)
    if start_ts is not None:
        mask = mask & (series.index >= start_ts)
    if end_exclusive_ts is not None:
        mask = mask & (series.index < end_exclusive_ts)

    if outside_fill is not None:
        return series.where(mask, other=float(outside_fill))
    return series[mask]


def _normalize_one_series(series: pd.Series, entry: DataSource) -> pd.Series:
    clean = pd.to_numeric(series, errors="coerce")
    clean.index = pd.to_datetime(clean.index, errors="coerce")
    clean = clean[clean.index.notna()]

    if clean.empty:
        return clean

    clean = _apply_window_rules(clean, entry)

    frequency = str(entry.frequency).strip().upper()
    divisor = entry.annual_rate_divisor()
    aggregation = str(getattr(entry, "aggregation", "mean")).strip().lower()

    if frequency == "A":
        raise FredFmdataNormalizeError(
            f"Unsupported frequency 'A' for {entry.fp_variable} (MVP requires Q or M)."
        )

    if frequency == "Q":
        quarterly = clean.resample("QS").mean()
        if divisor == 4:
            quarterly = quarterly / 4.0
        elif divisor not in (None, 1):
            raise FredFmdataNormalizeError(
                f"Unsupported annual_rate divisor {divisor} for quarterly series {entry.fp_variable}"
            )
        return quarterly

    if frequency == "M":
        if divisor == 12:
            # Monthly annual-rate flows: de-annualize monthly then quarterly sum.
            quarterly = (clean / 12.0).resample("QS").sum(min_count=1)
        else:
            if aggregation == "mean":
                quarterly = clean.resample("QS").mean()
            elif aggregation == "end":
                quarterly = clean.resample("QS").last()
            elif aggregation == "sum":
                quarterly = clean.resample("QS").sum(min_count=1)
            else:
                raise FredFmdataNormalizeError(
                    f"Unsupported aggregation '{entry.aggregation}' for {entry.fp_variable} "
                    "(expected mean/end/sum)."
                )
        return quarterly

    raise FredFmdataNormalizeError(
        f"Unsupported frequency '{entry.frequency}' for {entry.fp_variable} (expected Q or M)."
    )


def normalize_fred_for_fmdata(
    *,
    observations: pd.DataFrame,
    source_map: SourceMap,
    variables: Iterable[str],
    start_period: str,
    end_period: str,
) -> dict[str, pd.Series]:
    """Normalize raw FRED observations into per-quarter FM-data values.

    Args:
        observations: Wide DataFrame, columns are FRED series IDs.
        source_map: Loaded source-map metadata.
        variables: FP variable names to normalize (looked up in source_map).
        start_period: FP period start bound (inclusive).
        end_period: FP period end bound (inclusive).

    Returns:
        Dict mapping FP variable name -> Series indexed by FP periods ('YYYY.Q').
    """
    start_ts = period_to_quarter_start(start_period)
    end_ts = period_to_quarter_start(end_period)

    result: dict[str, pd.Series] = {}

    for raw_name in variables:
        var_name = str(raw_name).strip().upper()
        if not var_name:
            continue

        entry = source_map.get(var_name)
        if entry is None:
            raise FredFmdataNormalizeError(f"Variable '{var_name}' missing from source-map.")

        series_id = str(entry.series_id or entry.fred_fallback).strip()
        if not series_id:
            raise FredFmdataNormalizeError(
                f"Variable '{var_name}' has no FRED series_id/fallback."
            )

        if series_id not in observations.columns:
            raise FredFmdataNormalizeError(
                f"Missing FRED observations column '{series_id}' for variable '{var_name}'."
            )

        quarterly = _normalize_one_series(observations[series_id], entry)
        if quarterly.empty:
            continue

        # Apply scale/offset post aggregation + annual-rate conversion.
        scale = float(getattr(entry, "scale", 1.0))
        offset = float(getattr(entry, "offset", 0.0))
        quarterly = (quarterly.astype(float) * scale) + offset

        # Keep only requested quarters.
        quarterly = quarterly[(quarterly.index >= start_ts) & (quarterly.index <= end_ts)]
        if quarterly.empty:
            continue

        # Convert to FP period index.
        indexed = quarterly.copy()
        indexed.index = [quarter_start_to_period(pd.Timestamp(idx)) for idx in indexed.index]
        indexed = indexed.sort_index()
        indexed.name = var_name
        result[var_name] = indexed

    return result


def normalize_observations_for_fmdata(
    *,
    observations: pd.DataFrame,
    source_map: SourceMap,
    variables: Iterable[str],
    start_period: str,
    end_period: str,
) -> dict[str, pd.Series]:
    """Normalize raw observations keyed by FP variable names.

    This is the same normalization logic as `normalize_fred_for_fmdata`, but it
    assumes the input `observations` DataFrame columns are FP variables
    (uppercased), not source-specific series IDs.
    """
    start_ts = period_to_quarter_start(start_period)
    end_ts = period_to_quarter_start(end_period)

    normalized: dict[str, pd.Series] = {}
    for raw_name in variables:
        var_name = str(raw_name).strip().upper()
        if not var_name:
            continue
        entry = source_map.get(var_name)
        if entry is None:
            raise FredFmdataNormalizeError(f"Variable '{var_name}' missing from source-map.")
        if var_name not in observations.columns:
            raise FredFmdataNormalizeError(
                f"Missing observations column '{var_name}' for variable '{var_name}'."
            )
        quarterly = _normalize_one_series(observations[var_name], entry)
        if quarterly.empty:
            continue

        scale = float(getattr(entry, "scale", 1.0))
        offset = float(getattr(entry, "offset", 0.0))
        quarterly = (quarterly.astype(float) * scale) + offset

        quarterly = quarterly[(quarterly.index >= start_ts) & (quarterly.index <= end_ts)]
        if quarterly.empty:
            continue

        indexed = quarterly.copy()
        indexed.index = [quarter_start_to_period(pd.Timestamp(idx)) for idx in indexed.index]
        indexed = indexed.sort_index()
        indexed.name = var_name
        normalized[var_name] = indexed

    return normalized


def _default_cache_dir() -> Path:
    return Path.home() / ".fp-wraptr" / "fred-cache"
