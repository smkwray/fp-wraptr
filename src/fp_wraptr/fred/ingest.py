"""Fetch economic time series from the FRED API.

Requires the `fredapi` optional dependency and a FRED API key
set via the FRED_API_KEY environment variable.
"""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_CACHE_TTL_SECONDS = 24 * 60 * 60
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.35
FRED_TERMS_URL = "https://fred.stlouisfed.org/docs/api/terms_of_use.html"


def get_fred_client() -> Any:
    """Create and return an authenticated Fred API client."""
    from fredapi import Fred

    key = os.environ.get("FRED_API_KEY")
    if not key:
        raise ValueError("FRED_API_KEY environment variable not set")
    return Fred(api_key=key)


def _cache_path(cache_dir: Path, series_id: str) -> Path:
    return cache_dir / f"{series_id}.json"


def _cache_is_fresh(path: Path, ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS) -> bool:
    if not path.exists():
        return False

    if ttl_seconds <= 0:
        return False

    age_seconds = datetime.now(UTC).timestamp() - path.stat().st_mtime
    return age_seconds < ttl_seconds


def _load_cached_payload(path: Path) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8") as fp:
            payload = json.load(fp)
    except (OSError, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None
    return payload


def _load_cached_series(path: Path, series_id: str) -> pd.Series | None:
    payload = _load_cached_payload(path)
    if payload is None:
        return None

    if payload.get("series_id") != series_id:
        return None

    data = payload.get("data")
    if not isinstance(data, dict):
        return None

    return pd.Series(data, dtype=float).rename(series_id)


def _parse_date_bound(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed)


def _cache_covers_request(
    payload: dict[str, Any],
    request_start: str | None,
    request_end: str | None,
) -> bool:
    start_bound = _parse_date_bound(payload.get("observation_start"))
    end_bound = _parse_date_bound(payload.get("observation_end"))
    requested_start = _parse_date_bound(request_start)
    requested_end = _parse_date_bound(request_end)

    return not (
        (requested_start is not None and start_bound is not None and requested_start < start_bound)
        or (requested_end is not None and end_bound is not None and requested_end > end_bound)
    )


def _slice_series_to_request(
    series: pd.Series,
    request_start: str | None,
    request_end: str | None,
) -> pd.Series:
    start = _parse_date_bound(request_start)
    end = _parse_date_bound(request_end)
    result = series
    if start is not None:
        result = result[result.index >= start]
    if end is not None:
        result = result[result.index <= end]
    return result


def fetch_series(
    series_ids: list[str],
    start: str | None = None,
    end: str | None = None,
    cache_dir: Path | None = None,
    cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
    respect_tos: bool = True,
    min_request_interval_seconds: float = DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
) -> pd.DataFrame:
    """Fetch one or more series from FRED as a wide DataFrame.

    Args:
        series_ids: FRED series IDs to fetch.
        start: Optional observation start date (YYYY-MM-DD).
        end: Optional observation end date (YYYY-MM-DD).
        cache_dir: Cache directory for JSON payloads.
        cache_ttl_seconds: Maximum cache age before refresh.
        respect_tos: If True, insert delay between API calls to avoid burst traffic.
        min_request_interval_seconds: Minimum delay between uncached calls.
    """
    cache_dir = cache_dir or (Path.home() / ".fp-wraptr" / "fred-cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    normalized_series_ids = list(dict.fromkeys(series_ids))
    series_data: list[pd.Series] = []
    client = None
    last_request_at: float | None = None

    for series_id in normalized_series_ids:
        path = _cache_path(cache_dir, series_id)
        if _cache_is_fresh(path, ttl_seconds=cache_ttl_seconds):
            payload = _load_cached_payload(path)
            cached = _load_cached_series(path, series_id)
            if cached is not None and (
                payload is None or _cache_covers_request(payload, start, end)
            ):
                cached.index = pd.to_datetime(cached.index)
                cached = _slice_series_to_request(cached, start, end)
                series_data.append(cached)
                continue

        if client is None:
            client = get_fred_client()

        if respect_tos and last_request_at is not None and min_request_interval_seconds > 0:
            elapsed = time.monotonic() - last_request_at
            wait_seconds = min_request_interval_seconds - elapsed
            if wait_seconds > 0:
                time.sleep(wait_seconds)

        values = client.get_series(series_id, observation_start=start, observation_end=end)
        last_request_at = time.monotonic()
        raw = pd.Series(values, name=series_id, dtype=float)
        raw.index = pd.to_datetime(raw.index)

        observation_start = ""
        observation_end = ""
        if not raw.empty:
            observation_start = raw.index.min().strftime("%Y-%m-%d")
            observation_end = raw.index.max().strftime("%Y-%m-%d")

        payload = {
            "cache_format": 2,
            "fetched_at": datetime.now(UTC).isoformat(),
            "series_id": series_id,
            "requested_start": start,
            "requested_end": end,
            "observation_start": observation_start,
            "observation_end": observation_end,
            "source": "FRED",
            "terms_url": FRED_TERMS_URL,
            "data": {index.strftime("%Y-%m-%d"): value for index, value in raw.items()},
        }
        path.write_text(json.dumps(payload), encoding="utf-8")
        series_data.append(raw)

    if not series_data:
        return pd.DataFrame()

    return pd.concat(series_data, axis=1).sort_index()


def clear_cache(cache_dir: Path | None = None) -> int:
    """Delete all cached FRED series files."""
    cache_dir = cache_dir or (Path.home() / ".fp-wraptr" / "fred-cache")
    if not cache_dir.exists():
        return 0

    deleted = 0
    for path in cache_dir.glob("*.json"):
        path.unlink()
        deleted += 1
    return deleted
