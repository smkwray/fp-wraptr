"""FRED overlay — align FRED actuals with FP forecast periods for charting.

Provides utilities to fetch FRED data and align it with the FP model's
quarterly period format (e.g. "2025.4") so actuals can be plotted alongside
forecasts.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from fp_wraptr.data import load_source_map
from fp_wraptr.fred.ingest import fetch_series
from fp_wraptr.io.parser import FPOutputData

__all__ = ["build_overlay_data", "load_fred_mapping"]

if TYPE_CHECKING:
    from fp_wraptr.data.source_map import DataSource, SourceMap

# Default mapping of FP variable names to FRED series IDs.
_DEFAULT_MAPPING: dict[str, str] = {
    "GDPR": "GDPC1",
    "UR": "UNRATE",
    "PCY": "CPIAUCSL",
    "RS": "DGS10",
    "M1": "M1SL",
}


def load_fred_mapping(path: Path | str | None = None) -> dict[str, str]:
    """Load a YAML file mapping FP variable names to FRED series IDs.

    Falls back to the built-in default mapping if no path is given
    or the file doesn't exist.

    Expected YAML format::

        GDPR: GDPC1
        UR: UNRATE
        PCY: CPIAUCSL

    Args:
        path: Path to a YAML mapping file.

    Returns:
        Dict mapping FP variable names to FRED series IDs.
    """
    if path is None:
        return dict(_DEFAULT_MAPPING)

    path = Path(path)
    if not path.exists():
        return dict(_DEFAULT_MAPPING)

    import yaml

    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        return dict(_DEFAULT_MAPPING)

    return {str(k): str(v) for k, v in data.items()}


def _period_to_date(period: str) -> pd.Timestamp:
    """Convert an FP period string like '2025.4' to a pandas Timestamp.

    FP quarters: 1=Q1 (Jan), 2=Q2 (Apr), 3=Q3 (Jul), 4=Q4 (Oct).
    """
    year_str, q_str = period.split(".")
    year = int(year_str)
    quarter = int(q_str)
    month = (quarter - 1) * 3 + 1
    return pd.Timestamp(year=year, month=month, day=1)


def _to_quarterly_overlay_series(
    series: pd.Series,
    source_entry: DataSource | None,
) -> pd.Series:
    """Convert raw source frequency to quarterly values for overlay alignment."""
    if series.empty:
        return series

    clean = series.astype(float).copy()
    clean.index = pd.to_datetime(clean.index)

    if source_entry is None:
        return clean.resample("QS").mean()

    window_start = source_entry.window_start.strip()
    window_end = source_entry.window_end.strip()
    outside_fill = source_entry.outside_window_value
    if window_start or window_end or outside_fill is not None:
        mask = pd.Series(True, index=clean.index)
        start_ts = _parse_quarter_start(window_start)
        end_exclusive_ts = _parse_quarter_end_exclusive(window_end)
        if start_ts is not None:
            mask = mask & (clean.index >= start_ts)
        if end_exclusive_ts is not None:
            mask = mask & (clean.index < end_exclusive_ts)
        if outside_fill is not None:
            clean = clean.where(mask, other=float(outside_fill))
        else:
            clean = clean[mask]

    frequency = source_entry.frequency.upper()
    divisor = source_entry.annual_rate_divisor()

    if frequency == "M" and divisor == 12:
        # Monthly annual-rate flows should be de-annualized before quarterly sum.
        return (clean / 12.0).resample("QS").sum(min_count=1)

    if frequency == "Q" and divisor == 4:
        # Quarterly annual-rate flows (SAAR) convert to quarterly flow via /4.
        return clean.resample("QS").mean() / 4.0

    return clean.resample("QS").mean()


def _parse_quarter_start(value: str) -> pd.Timestamp | None:
    """Parse quarter tokens like 2008Q4 into start-of-quarter timestamps."""
    token = value.strip().upper()
    if not token:
        return None
    match = re.match(r"^(\d{4})Q([1-4])$", token)
    if not match:
        return None
    year = int(match.group(1))
    quarter = int(match.group(2))
    month = (quarter - 1) * 3 + 1
    return pd.Timestamp(year=year, month=month, day=1)


def _parse_quarter_end_exclusive(value: str) -> pd.Timestamp | None:
    """Parse quarter tokens like 2012Q2 to exclusive upper bound timestamps."""
    quarter_start = _parse_quarter_start(value)
    if quarter_start is None:
        return None
    return quarter_start + pd.DateOffset(months=3)


def build_overlay_data(
    forecast_data: FPOutputData,
    fred_mapping: dict[str, str] | None = None,
    variables: list[str] | None = None,
    cache_dir: Path | None = None,
    source_map: SourceMap | None = None,
) -> dict[str, pd.DataFrame]:
    """Fetch FRED data and align with FP forecast periods.

    For each variable in `variables` (or all mapped variables if None),
    returns a DataFrame with columns "period", "forecast", and "actual"
    where the actual values are quarterly FRED observations aligned
    to the forecast periods.

    Args:
        forecast_data: Parsed FP output with forecast variables and periods.
        fred_mapping: FP variable -> FRED series ID mapping.
            Defaults to built-in mapping.
        variables: Specific variables to overlay. If None, uses all
            variables present in both the forecast and the mapping.
        cache_dir: Cache directory for FRED data.
        source_map: Optional source-map metadata for frequency/annual-rate-aware
            normalization before quarterly alignment.

    Returns:
        Dict mapping variable name to a DataFrame with columns:
        period (str), forecast (float), actual (float or NaN).
    """
    if fred_mapping is None:
        fred_mapping = dict(_DEFAULT_MAPPING)

    if variables is None:
        variables = [v for v in forecast_data.variables if v in fred_mapping]
    else:
        variables = [v for v in variables if v in fred_mapping and v in forecast_data.variables]

    if not variables:
        return {}

    # Determine date range from forecast periods
    periods = forecast_data.periods
    if not periods:
        return {}

    start_date = _period_to_date(periods[0])
    end_date = _period_to_date(periods[-1])

    # Fetch all needed FRED series in one call
    fred_ids = [fred_mapping[v] for v in variables]
    fred_df = fetch_series(
        fred_ids,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        cache_dir=cache_dir,
    )
    map_payload = source_map
    if map_payload is None:
        try:
            map_payload = load_source_map()
        except Exception:
            map_payload = None

    result: dict[str, pd.DataFrame] = {}

    for var_name in variables:
        fred_id = fred_mapping[var_name]
        fp_var = forecast_data.variables[var_name]
        n_points = min(len(fp_var.levels), len(periods))
        source_entry = map_payload.get(var_name) if map_payload is not None else None
        quarterly: pd.Series | None = None
        if fred_id in fred_df.columns:
            quarterly = _to_quarterly_overlay_series(fred_df[fred_id], source_entry)

        records = []
        for i in range(n_points):
            period = periods[i]
            forecast_val = fp_var.levels[i]
            date = _period_to_date(period)

            actual_val = None
            if quarterly is not None:
                val = quarterly.loc[date] if date in quarterly.index else None
                if pd.notna(val):
                    actual_val = float(val)

            records.append({
                "period": period,
                "forecast": forecast_val,
                "actual": actual_val,
            })

        result[var_name] = pd.DataFrame(records)

    return result
