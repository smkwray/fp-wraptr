"""IO helpers for loading series inputs for pipelines."""

from __future__ import annotations

import json
from dataclasses import dataclass

import pandas as pd

from fp_wraptr.data.series_pipeline.periods import PeriodError, normalize_period_token
from fp_wraptr.data.series_pipeline.spec import ConstantSource, CsvSource, JsonSource


class SeriesIoError(RuntimeError):
    """Raised when a series source cannot be loaded."""


@dataclass(frozen=True, slots=True)
class SeriesFrame:
    periods: list[str]
    values: list[float | None]
    source_periods: list[str] | None = None

    def as_dict(self) -> dict[str, float]:
        return {
            p: float(v)  # type: ignore[arg-type]
            for p, v in zip(self.periods, self.values, strict=False)
            if v is not None
        }


def _read_csv_long(source: CsvSource) -> SeriesFrame:
    if not source.path.exists():
        raise SeriesIoError(f"CSV source not found: {source.path}")

    df = pd.read_csv(source.path)
    if source.period_col not in df.columns:
        raise SeriesIoError(f"CSV missing period_col '{source.period_col}': {source.path}")
    if not source.value_col or source.value_col not in df.columns:
        raise SeriesIoError(f"CSV missing value_col '{source.value_col}': {source.path}")

    data = df.copy()
    if source.variable_col:
        if source.variable_col not in data.columns:
            raise SeriesIoError(f"CSV missing variable_col '{source.variable_col}': {source.path}")
        desired = str(source.variable or "").strip()
        if not desired:
            raise SeriesIoError("CSV source with variable_col requires variable")
        data = data[data[source.variable_col].astype(str).str.upper() == desired.upper()]

    raw_periods = [str(v).strip() for v in data[source.period_col].tolist()]
    raw_values = data[source.value_col].tolist()

    points: dict[str, float] = {}
    for p_raw, v_raw in zip(raw_periods, raw_values, strict=False):
        if p_raw in source.missing_values or not p_raw:
            continue
        try:
            period = normalize_period_token(p_raw)
        except PeriodError as exc:
            raise SeriesIoError(f"Failed to parse period '{p_raw}' in {source.path}") from exc

        if v_raw is None:
            continue
        v_text = str(v_raw).strip()
        if v_text in source.missing_values:
            continue
        try:
            value = float(v_raw)
        except Exception:
            continue
        points[period] = value

    if not points:
        raise SeriesIoError(f"No valid observations found in {source.path}")

    periods = sorted(points.keys(), key=lambda p: (int(p.split('.')[0]), int(p.split('.')[1])))
    values: list[float | None] = [points[p] for p in periods]
    return SeriesFrame(periods=periods, values=values, source_periods=raw_periods)


def _read_csv_wide(source: CsvSource) -> SeriesFrame:
    if not source.path.exists():
        raise SeriesIoError(f"CSV source not found: {source.path}")
    desired = str(source.variable or "").strip()
    if not desired:
        raise SeriesIoError("CSV source format=wide requires variable")

    df = pd.read_csv(source.path)
    if source.period_col not in df.columns:
        raise SeriesIoError(f"CSV missing period_col '{source.period_col}': {source.path}")
    if desired not in df.columns and desired.upper() not in {c.upper() for c in df.columns}:
        raise SeriesIoError(f"CSV missing series column '{desired}': {source.path}")

    # Find the series column case-insensitively.
    series_col = next((c for c in df.columns if str(c).upper() == desired.upper()), desired)
    raw_periods = [str(v).strip() for v in df[source.period_col].tolist()]
    raw_values = df[series_col].tolist()

    points: dict[str, float] = {}
    for p_raw, v_raw in zip(raw_periods, raw_values, strict=False):
        if p_raw in source.missing_values or not p_raw:
            continue
        try:
            period = normalize_period_token(p_raw)
        except PeriodError as exc:
            raise SeriesIoError(f"Failed to parse period '{p_raw}' in {source.path}") from exc
        if v_raw is None:
            continue
        v_text = str(v_raw).strip()
        if v_text in source.missing_values:
            continue
        try:
            value = float(v_raw)
        except Exception:
            continue
        points[period] = value

    if not points:
        raise SeriesIoError(f"No valid observations found in {source.path}")
    periods = sorted(points.keys(), key=lambda p: (int(p.split('.')[0]), int(p.split('.')[1])))
    values: list[float | None] = [points[p] for p in periods]
    return SeriesFrame(periods=periods, values=values, source_periods=raw_periods)


def read_series_from_csv(source: CsvSource) -> SeriesFrame:
    fmt = str(source.format or "").strip().lower()
    if fmt == "long":
        return _read_csv_long(source)
    if fmt == "wide":
        return _read_csv_wide(source)
    raise SeriesIoError(f"Unsupported csv format '{source.format}'")


def read_series_from_json(source: JsonSource) -> SeriesFrame:
    if not source.path.exists():
        raise SeriesIoError(f"JSON source not found: {source.path}")
    try:
        payload = json.loads(source.path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SeriesIoError(f"Failed to parse JSON: {source.path}") from exc

    def get_path(obj: object, dotted: str) -> object:
        cursor = obj
        for part in [p for p in str(dotted).split(".") if p]:
            if isinstance(cursor, dict) and part in cursor:
                cursor = cursor[part]
            else:
                raise SeriesIoError(f"Missing JSON path '{dotted}' in {source.path}")
        return cursor

    raw_value = get_path(payload, source.value_path)
    try:
        value_f = float(raw_value)  # type: ignore[arg-type]
    except Exception as exc:
        raise SeriesIoError(f"Non-numeric JSON value at '{source.value_path}' in {source.path}") from exc

    period = "0.0"
    if source.period_path:
        raw_period = str(get_path(payload, source.period_path)).strip()
        try:
            period = normalize_period_token(raw_period)
        except PeriodError:
            period = raw_period

    return SeriesFrame(periods=[period], values=[value_f], source_periods=[period])


def read_series_from_constant(source: ConstantSource) -> SeriesFrame:
    period = str(source.period).strip() if source.period else "0.0"
    return SeriesFrame(periods=[period], values=[float(source.value)], source_periods=[period])
