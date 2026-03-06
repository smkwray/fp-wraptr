"""Fetch economic series from the BEA API (NIPA).

This module intentionally uses the Python standard library (urllib) to avoid
adding a new runtime dependency.

Authentication:
- set `BEA_API_KEY` in the environment.

Caching:
- JSON payloads are cached under `~/.fp-wraptr/bea-cache` by default.
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.35

BEA_API_URL = "https://apps.bea.gov/api/data/"


class BeaApiError(RuntimeError):
    """Raised when BEA API requests fail."""


def _get_bea_api_key() -> str:
    key = os.environ.get("BEA_API_KEY")
    if not key:
        raise BeaApiError("BEA_API_KEY environment variable not set")
    return key


def _cache_key(*, table: str, frequency: str, year: str) -> str:
    safe_table = str(table).strip()
    safe_frequency = str(frequency).strip().upper()
    safe_year = str(year).strip().upper()
    return f"NIPA_{safe_table}_{safe_frequency}_{safe_year}"


def _cache_path(cache_dir: Path, cache_key: str) -> Path:
    return cache_dir / f"{cache_key}.json"


def _cache_is_fresh(path: Path, ttl_seconds: int) -> bool:
    if not path.exists() or ttl_seconds <= 0:
        return False
    age_seconds = datetime.now(UTC).timestamp() - path.stat().st_mtime
    return age_seconds < ttl_seconds


def _load_cached_payload(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_cache(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _request_json(params: dict[str, str], *, timeout_seconds: int = 30) -> dict[str, Any]:
    query = urllib.parse.urlencode(params)
    url = f"{BEA_API_URL}?{query}"
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as resp:
            raw = resp.read()
    except Exception as exc:  # pragma: no cover - network errors
        raise BeaApiError(f"BEA request failed: {exc}") from exc

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # pragma: no cover - defensive decode guard
        raise BeaApiError("BEA response was not valid JSON") from exc

    if not isinstance(payload, dict):
        raise BeaApiError("BEA response JSON was not an object")
    return payload


def _extract_data_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    api = payload.get("BEAAPI")
    if not isinstance(api, dict):
        raise BeaApiError("BEA response missing BEAAPI payload")
    error = api.get("Error")
    if error:
        raise BeaApiError(f"BEA API error: {error}")
    results = api.get("Results")
    if not isinstance(results, dict):
        raise BeaApiError("BEA response missing Results payload")
    data = results.get("Data")
    if not isinstance(data, list):
        raise BeaApiError("BEA response missing Results.Data list")
    return [row for row in data if isinstance(row, dict)]


def _parse_time_period(period: str, *, frequency: str) -> pd.Timestamp:
    token = str(period).strip()
    freq = str(frequency).strip().upper()
    if freq == "Q":
        # "2025Q4"
        year = int(token[:4])
        quarter = int(token[-1])
        month = (quarter - 1) * 3 + 1
        return pd.Timestamp(year=year, month=month, day=1)
    if freq == "A":
        year = int(token[:4])
        return pd.Timestamp(year=year, month=1, day=1)
    raise BeaApiError(f"Unsupported BEA frequency '{frequency}' (expected Q or A)")


@dataclass(frozen=True)
class BeaNipaRequest:
    table_name: str
    frequency: str = "Q"  # Q or A
    year: str = "ALL"


def fetch_nipa_table(
    request: BeaNipaRequest,
    *,
    cache_dir: Path | None = None,
    cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
    respect_tos: bool = True,
    min_request_interval_seconds: float = DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    """Fetch a full NIPA table from BEA.

    Returns a DataFrame with index timestamps (quarter starts for Q) and one
    column per line number (as integers), containing numeric values.
    """
    cache_dir = cache_dir or (Path.home() / ".fp-wraptr" / "bea-cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache_key = _cache_key(
        table=request.table_name,
        frequency=request.frequency,
        year=request.year,
    )
    path = _cache_path(cache_dir, cache_key)

    payload: dict[str, Any] | None = None
    if _cache_is_fresh(path, cache_ttl_seconds):
        payload = _load_cached_payload(path)

    if payload is None:
        key = _get_bea_api_key()
        if respect_tos and min_request_interval_seconds > 0:
            time.sleep(min_request_interval_seconds)
        params = {
            "UserID": key,
            "method": "GetData",
            "DataSetName": "NIPA",
            "TableName": str(request.table_name).strip(),
            "Frequency": str(request.frequency).strip().upper(),
            "Year": str(request.year).strip().upper(),
            "ResultFormat": "JSON",
        }
        payload = _request_json(params, timeout_seconds=timeout_seconds)
        payload["_cache_format"] = 1
        payload["_fetched_at"] = datetime.now(UTC).isoformat()
        payload["_request"] = {
            "table_name": request.table_name,
            "frequency": request.frequency,
            "year": request.year,
        }
        _write_cache(path, payload)

    rows = _extract_data_rows(payload)
    if not rows:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    freq = str(request.frequency).strip().upper()
    for row in rows:
        line = row.get("LineNumber")
        period = row.get("TimePeriod")
        value = row.get("DataValue")
        if line is None or period is None:
            continue
        try:
            line_no = int(line)
        except Exception:
            continue
        ts = _parse_time_period(str(period), frequency=freq)
        num = pd.to_numeric(str(value).replace(",", ""), errors="coerce")
        records.append({
            "timestamp": ts,
            "line": line_no,
            "value": float(num) if pd.notna(num) else None,
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records).dropna(subset=["value"])
    if df.empty:
        return pd.DataFrame()

    pivot = df.pivot_table(
        index="timestamp", columns="line", values="value", aggfunc="first"
    ).sort_index()
    pivot.index = pd.to_datetime(pivot.index)
    return pivot


def fetch_nipa_lines(
    *,
    table_name: str,
    line_numbers: list[int],
    frequency: str = "Q",
    year: str = "ALL",
    cache_dir: Path | None = None,
    cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
) -> dict[int, pd.Series]:
    """Fetch specific NIPA table lines as a dict of series."""
    df = fetch_nipa_table(
        BeaNipaRequest(table_name=table_name, frequency=frequency, year=year),
        cache_dir=cache_dir,
        cache_ttl_seconds=cache_ttl_seconds,
    )
    result: dict[int, pd.Series] = {}
    for line in line_numbers:
        if line in df.columns:
            series = pd.to_numeric(df[line], errors="coerce").dropna()
            series.name = f"{table_name}:{line}"
            result[int(line)] = series
    return result


def clear_cache(cache_dir: Path | None = None) -> int:
    cache_dir = cache_dir or (Path.home() / ".fp-wraptr" / "bea-cache")
    if not cache_dir.exists():
        return 0
    deleted = 0
    for path in cache_dir.glob("*.json"):
        path.unlink()
        deleted += 1
    return deleted
