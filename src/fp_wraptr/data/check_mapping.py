"""Mapping QA helpers for comparing normalized source data to fmdata."""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import pandas as pd

from fp_wraptr.data.source_map import DataSource, SourceMap, load_source_map
from fp_wraptr.fred.normalize_for_fmdata import (
    FredFmdataNormalizeError,
    normalize_observations_for_fmdata,
    period_to_quarter_start,
)
from fp_wraptr.io.input_parser import parse_fm_data

_BEA_NIPA_TABLE_RE = re.compile(r"^T\d{5}$", re.IGNORECASE)


class MappingCheckError(RuntimeError):
    """Raised when mapping-check inputs are invalid or unsupported."""


def _is_bea_nipa_table(table: str) -> bool:
    return bool(_BEA_NIPA_TABLE_RE.match(str(table).strip()))


def _parse_period(text: str) -> tuple[int, int]:
    year_text, quarter_text = str(text).strip().split(".")
    year = int(year_text)
    quarter = int(quarter_text)
    if quarter not in {1, 2, 3, 4}:
        raise ValueError(f"Invalid quarter token in period '{text}'")
    return year, quarter


def _format_period(year: int, quarter: int) -> str:
    return f"{year}.{quarter}"


def _period_to_index(sample_start: str, period: str) -> int:
    sy, sq = _parse_period(sample_start)
    py, pq = _parse_period(period)
    return (py - sy) * 4 + (pq - sq)


def _period_add(period: str, delta_quarters: int) -> str:
    year, quarter = _parse_period(period)
    total = year * 4 + (quarter - 1) + int(delta_quarters)
    out_year, out_q0 = divmod(total, 4)
    return _format_period(out_year, out_q0 + 1)


def _default_cache_dir(cache_dir: Path | None, suffix: str) -> Path | None:
    if cache_dir is None:
        return None
    if cache_dir.name == "fred-cache":
        return cache_dir.parent / suffix
    return cache_dir / suffix


def _fetch_fred_observations(
    *,
    series_ids: list[str],
    start_date: str,
    end_date: str,
    cache_dir: Path | None,
) -> pd.DataFrame:
    from fp_wraptr.fred.ingest import fetch_series

    return fetch_series(series_ids, start=start_date, end=end_date, cache_dir=cache_dir)


def _fetch_bls_observations(
    *,
    series_ids: list[str],
    start_date: str,
    end_date: str,
    cache_dir: Path | None,
) -> pd.DataFrame:
    from fp_wraptr.bls.ingest import BlsSeriesRequest, fetch_series

    request = BlsSeriesRequest(
        series_ids=series_ids,
        start_year=int(str(start_date)[:4]),
        end_year=int(str(end_date)[:4]),
    )
    return fetch_series(
        request,
        cache_dir=_default_cache_dir(cache_dir, "bls-cache"),
    )


def _fetch_bea_variable_observations(
    *,
    selected_bea: dict[str, tuple[DataSource, str, int]],
    cache_dir: Path | None,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    from fp_wraptr.bea.ingest import BeaNipaRequest, fetch_nipa_table

    bea_failed_tables: list[dict[str, str]] = []
    tables: dict[str, set[int]] = {}
    for _, (_, table, line) in selected_bea.items():
        tables.setdefault(table, set()).add(int(line))

    bea_var_series: dict[str, pd.Series] = {}
    for table, _lines in sorted(tables.items()):
        try:
            frame = fetch_nipa_table(
                BeaNipaRequest(table_name=table, frequency="Q", year="ALL"),
                cache_dir=_default_cache_dir(cache_dir, "bea-cache"),
            )
        except Exception as exc:  # pragma: no cover - network and api behavior
            bea_failed_tables.append({"table": table, "error": str(exc)})
            continue
        if frame.empty:
            continue
        for var, (_, var_table, var_line) in selected_bea.items():
            if var_table != table:
                continue
            if int(var_line) in frame.columns:
                bea_var_series[var] = pd.to_numeric(frame[int(var_line)], errors="coerce")

    if not bea_var_series:
        return pd.DataFrame(), bea_failed_tables
    data = pd.concat(bea_var_series.values(), axis=1)
    data.columns = list(bea_var_series.keys())
    return data, bea_failed_tables


def check_mapping_against_fmdata(
    *,
    model_dir: Path,
    source_map_path: Path | None = None,
    variables: list[str] | None = None,
    periods: int = 40,
    sources: list[str] | None = None,
    cache_dir: Path | None = None,
) -> dict[str, Any]:
    """Compare fmdata history against normalized source observations."""
    if periods <= 0:
        raise MappingCheckError("--periods must be > 0")

    enabled_sources = [str(item).strip().lower() for item in (sources or ["fred"]) if str(item).strip()]
    enabled_sources = list(dict.fromkeys(enabled_sources)) or ["fred"]
    unknown_sources = sorted({name for name in enabled_sources if name not in {"fred", "bea", "bls"}})
    if unknown_sources:
        raise MappingCheckError(f"Unsupported source(s): {', '.join(unknown_sources)}")

    source_map: SourceMap = load_source_map(source_map_path)

    fmdata_path = Path(model_dir) / "fmdata.txt"
    parsed = parse_fm_data(fmdata_path)
    sample_start = str(parsed.get("sample_start", "")).strip()
    sample_end = str(parsed.get("sample_end", "")).strip()
    if not sample_start or not sample_end:
        raise MappingCheckError(f"Failed to parse sample bounds from: {fmdata_path}")

    fm_series = parsed.get("series", {})
    if not isinstance(fm_series, dict) or not fm_series:
        raise MappingCheckError(f"No series found in: {fmdata_path}")

    end_period = sample_end
    start_period = _period_add(end_period, -(periods - 1))
    if _parse_period(start_period) < _parse_period(sample_start):
        start_period = sample_start

    requested = [v.strip().upper() for v in (variables or []) if v and v.strip()]
    candidates = requested or sorted(fm_series.keys())

    selected_fred: dict[str, tuple[DataSource, str]] = {}
    selected_bls: dict[str, tuple[DataSource, str]] = {}
    selected_bea: dict[str, tuple[DataSource, str, int]] = {}
    skipped: list[dict[str, str]] = []

    for var_name in candidates:
        entry = source_map.get(var_name)
        if entry is None:
            skipped.append({"variable": var_name, "reason": "unmapped"})
            continue

        source = str(entry.source).strip().lower()
        if source not in enabled_sources:
            skipped.append({"variable": var_name, "reason": "source_not_selected", "source": source})
            continue

        frequency = str(entry.frequency).strip().upper()
        if frequency not in {"Q", "M"}:
            skipped.append({
                "variable": var_name,
                "reason": "unsupported_frequency",
                "source": source,
                "frequency": frequency,
            })
            continue

        if source == "fred":
            series_id = str(entry.series_id or entry.fred_fallback).strip()
            if not series_id:
                skipped.append({"variable": var_name, "reason": "missing_series_id", "source": source})
                continue
            selected_fred[var_name] = (entry, series_id)
            continue

        if source == "bls":
            series_id = str(entry.series_id).strip()
            if not series_id:
                skipped.append({"variable": var_name, "reason": "missing_series_id", "source": source})
                continue
            selected_bls[var_name] = (entry, series_id)
            continue

        if source == "bea":
            table = str(entry.bea_table).strip()
            line = int(getattr(entry, "bea_line", 0) or 0)
            if not table or line <= 0:
                skipped.append({"variable": var_name, "reason": "missing_bea_locator", "source": source})
                continue
            if not _is_bea_nipa_table(table):
                skipped.append({
                    "variable": var_name,
                    "reason": "unsupported_bea_table",
                    "source": source,
                    "bea_table": table,
                })
                continue
            selected_bea[var_name] = (entry, table, line)
            continue

    selected_variables = sorted({*selected_fred.keys(), *selected_bls.keys(), *selected_bea.keys()})
    if not selected_variables:
        raise MappingCheckError(
            "No variables selected with eligible mappings "
            f"(sources={enabled_sources}, requested={requested or 'all fmdata vars'})."
        )

    start_date = period_to_quarter_start(start_period).strftime("%Y-%m-%d")
    end_date = (
        period_to_quarter_start(end_period) + pd.DateOffset(months=3) - pd.Timedelta(days=1)
    ).strftime("%Y-%m-%d")

    raw_frames: list[pd.DataFrame] = []
    if selected_fred:
        fred_ids = list(dict.fromkeys(series_id for _, series_id in selected_fred.values()))
        fred_raw = _fetch_fred_observations(
            series_ids=fred_ids,
            start_date=start_date,
            end_date=end_date,
            cache_dir=cache_dir,
        )
        rename = {series_id: var for var, (_, series_id) in selected_fred.items()}
        raw_frames.append(fred_raw.rename(columns=rename))

    if selected_bls:
        bls_ids = list(dict.fromkeys(series_id for _, series_id in selected_bls.values()))
        bls_raw = _fetch_bls_observations(
            series_ids=bls_ids,
            start_date=start_date,
            end_date=end_date,
            cache_dir=cache_dir,
        )
        rename = {series_id: var for var, (_, series_id) in selected_bls.items()}
        raw_frames.append(bls_raw.rename(columns=rename))

    bea_failed_tables: list[dict[str, str]] = []
    if selected_bea:
        bea_raw, bea_failed_tables = _fetch_bea_variable_observations(
            selected_bea=selected_bea,
            cache_dir=cache_dir,
        )
        if not bea_raw.empty:
            raw_frames.append(bea_raw)

    raw_observations = pd.concat(raw_frames, axis=1).sort_index() if raw_frames else pd.DataFrame()
    available_variables = {
        str(column).strip().upper()
        for column in raw_observations.columns
        if str(column).strip()
    }
    selected_without_observations = [
        variable for variable in selected_variables if variable not in available_variables
    ]
    for variable in selected_without_observations:
        skipped.append({"variable": variable, "reason": "no_observations"})

    variables_for_normalize = [
        variable for variable in selected_variables if variable in available_variables
    ]

    try:
        normalized = normalize_observations_for_fmdata(
            observations=raw_observations,
            source_map=source_map,
            variables=variables_for_normalize,
            start_period=start_period,
            end_period=end_period,
        )
    except FredFmdataNormalizeError as exc:
        raise MappingCheckError(str(exc)) from exc

    rows: list[dict[str, Any]] = []
    for var_name in selected_variables:
        entry = source_map.get(var_name)
        if entry is None:
            continue

        blocks = fm_series.get(var_name)
        if not isinstance(blocks, list) or not blocks:
            continue
        base_values = blocks[-1].get("values")
        if not isinstance(base_values, list):
            continue

        idx_start = _period_to_index(sample_start, start_period)
        idx_end = _period_to_index(sample_start, end_period)
        period_index = [_period_add(start_period, i) for i in range(idx_end - idx_start + 1)]
        base = pd.Series(
            [float(value) for value in base_values[idx_start : idx_end + 1]],
            index=period_index,
        )
        observed = normalized.get(var_name, pd.Series(dtype=float))
        joined = pd.concat({"fmdata": base, "source": observed}, axis=1).dropna()

        overlap = int(joined.shape[0])
        corr = float(joined["fmdata"].corr(joined["source"])) if overlap >= 2 else None
        med_abs = float((joined["fmdata"] - joined["source"]).abs().median()) if overlap else None

        suggested_scale = None
        ratio = None
        nonzero = joined[(joined["fmdata"].abs() > 1e-12) & (joined["source"].abs() > 1e-12)]
        if not nonzero.empty:
            ratio = float((nonzero["fmdata"].abs() / nonzero["source"].abs()).median())
            scale_candidates = [0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0]
            best = min(
                scale_candidates,
                key=lambda candidate: abs(math.log10(ratio) - math.log10(candidate)),
            )
            if best != 1.0 and abs(ratio - best) / best < 0.25:
                suggested_scale = best

        rows.append({
            "variable": var_name,
            "source": str(entry.source),
            "series_id": str(entry.series_id or entry.fred_fallback or ""),
            "frequency": str(entry.frequency),
            "annual_rate": bool(entry.annual_rate),
            "aggregation": str(entry.aggregation),
            "scale": float(entry.scale),
            "offset": float(entry.offset),
            "overlap_count": overlap,
            "correlation": corr,
            "median_abs_error": med_abs,
            "suggested_scale": suggested_scale,
        })

    return {
        "sources": enabled_sources,
        "model_dir": str(model_dir),
        "sample_start": sample_start,
        "sample_end": sample_end,
        "start_period": start_period,
        "end_period": end_period,
        "periods_requested": int(periods),
        "rows": rows,
        "skipped": sorted(
            skipped,
            key=lambda item: (item.get("variable", ""), item.get("reason", "")),
        ),
        "selected_variable_count": len(selected_variables),
        "selected_without_observations": selected_without_observations,
        "bea_failed_tables": bea_failed_tables,
    }
