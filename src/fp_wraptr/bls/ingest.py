"""Fetch economic series from the BLS Public Data API.

This module uses the Python standard library (urllib) to avoid adding new
runtime dependencies.

Authentication:
- optional key: set `BLS_API_KEY` in the environment for higher rate limits.

Caching:
- JSON payloads are cached under `~/.fp-wraptr/bls-cache` by default.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.35

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


class BlsApiError(RuntimeError):
    """Raised when BLS API requests fail."""


def _get_bls_api_key() -> str | None:
    key = os.environ.get("BLS_API_KEY")
    return key.strip() if isinstance(key, str) and key.strip() else None


def _cache_key(*, series_ids: list[str], start_year: int, end_year: int) -> str:
    safe = "_".join(series_ids)
    return f"BLS_{safe}_{start_year}_{end_year}"


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


def _post_json(payload: dict[str, Any], *, timeout_seconds: int = 30) -> dict[str, Any]:
    request = urllib.request.Request(
        BLS_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as resp:
            raw = resp.read()
    except Exception as exc:  # pragma: no cover - network errors
        raise BlsApiError(f"BLS request failed: {exc}") from exc

    try:
        parsed = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # pragma: no cover
        raise BlsApiError("BLS response was not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise BlsApiError("BLS response JSON was not an object")
    return parsed


def _status_is_success(status: Any) -> bool:
    token = str(status).strip().lower()
    if not token:
        return True
    normalized = " ".join(token.replace("_", " ").replace("-", " ").split())
    return normalized in {"request succeeded", "success"}


def _parse_period(year: int, period: str) -> pd.Timestamp | None:
    token = str(period).strip().upper()
    if not token or token == "M13":
        return None
    if token.startswith("M") and len(token) == 3:
        month = int(token[1:])
        return pd.Timestamp(year=year, month=month, day=1)
    if token.startswith("Q") and len(token) == 3:
        quarter = int(token[1:])
        month = (quarter - 1) * 3 + 1
        return pd.Timestamp(year=year, month=month, day=1)
    return None


@dataclass(frozen=True)
class BlsSeriesRequest:
    series_ids: list[str]
    start_year: int
    end_year: int


def fetch_series(
    request: BlsSeriesRequest,
    *,
    cache_dir: Path | None = None,
    cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
    respect_tos: bool = True,
    min_request_interval_seconds: float = DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    """Fetch one or more series from BLS as a wide DataFrame."""
    cache_dir = cache_dir or (Path.home() / ".fp-wraptr" / "bls-cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    series_ids = list(
        dict.fromkeys([str(s).strip() for s in request.series_ids if str(s).strip()])
    )
    if not series_ids:
        return pd.DataFrame()

    cache_key = _cache_key(
        series_ids=series_ids, start_year=request.start_year, end_year=request.end_year
    )
    path = _cache_path(cache_dir, cache_key)

    payload: dict[str, Any] | None = None
    if _cache_is_fresh(path, cache_ttl_seconds):
        payload = _load_cached_payload(path)

    if payload is None:
        key = _get_bls_api_key()
        req: dict[str, Any] = {
            "seriesid": series_ids,
            "startyear": str(int(request.start_year)),
            "endyear": str(int(request.end_year)),
        }
        if key:
            req["registrationkey"] = key

        if respect_tos and min_request_interval_seconds > 0:
            time.sleep(min_request_interval_seconds)
        payload = _post_json(req, timeout_seconds=timeout_seconds)
        payload["_cache_format"] = 1
        payload["_fetched_at"] = datetime.now(UTC).isoformat()
        payload["_request"] = {
            "series_ids": series_ids,
            "start_year": request.start_year,
            "end_year": request.end_year,
        }
        _write_cache(path, payload)

    status = payload.get("status")
    if status and not _status_is_success(status):
        message = payload.get("message")
        raise BlsApiError(f"BLS status={status!r} message={message!r}")

    results = payload.get("Results")
    if not isinstance(results, dict):
        raise BlsApiError("BLS response missing Results payload")
    series = results.get("series")
    if not isinstance(series, list):
        raise BlsApiError("BLS response missing Results.series list")

    frames: list[pd.Series] = []
    for entry in series:
        if not isinstance(entry, dict):
            continue
        series_id = str(entry.get("seriesID", "")).strip()
        data = entry.get("data")
        if not series_id or not isinstance(data, list):
            continue
        points: dict[pd.Timestamp, float] = {}
        for row in data:
            if not isinstance(row, dict):
                continue
            year_text = row.get("year")
            period_text = row.get("period")
            value_text = row.get("value")
            try:
                year = int(str(year_text))
            except Exception:
                continue
            ts = _parse_period(year, str(period_text))
            if ts is None:
                continue
            value = pd.to_numeric(str(value_text), errors="coerce")
            if pd.notna(value):
                points[ts] = float(value)
        s = pd.Series(points, name=series_id, dtype=float)
        if not s.empty:
            s.index = pd.to_datetime(s.index)
            frames.append(s.sort_index())

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=1).sort_index()


def clear_cache(cache_dir: Path | None = None) -> int:
    cache_dir = cache_dir or (Path.home() / ".fp-wraptr" / "bls-cache")
    if not cache_dir.exists():
        return 0
    deleted = 0
    for path in cache_dir.glob("*.json"):
        path.unlink()
        deleted += 1
    return deleted
