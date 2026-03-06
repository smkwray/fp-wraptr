"""Extrapolation helpers for extending quarterly series to forecast windows."""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

from fp_wraptr.data.series_pipeline.periods import PeriodError, periods_between, period_to_ordinal


class ExtrapolationError(RuntimeError):
    """Raised when extrapolation fails."""


@dataclass(frozen=True, slots=True)
class ExtrapolationResult:
    periods: list[str]
    values: list[float]
    method: str
    notes: list[str]

    def as_dict(self) -> dict[str, float]:
        return dict(zip(self.periods, self.values, strict=False))


def _clamp(values: np.ndarray, bounds: tuple[float | None, float | None] | None) -> np.ndarray:
    if bounds is None:
        return values
    lo, hi = bounds
    out = values
    if lo is not None:
        out = np.maximum(out, float(lo))
    if hi is not None:
        out = np.minimum(out, float(hi))
    return out


def _logit(x: np.ndarray) -> np.ndarray:
    eps = 1e-12
    clipped = np.clip(x, eps, 1.0 - eps)
    return np.log(clipped / (1.0 - clipped))


def _inv_logit(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


def _apply_pre(values: np.ndarray, pre: str | None) -> tuple[np.ndarray, list[str]]:
    if not pre:
        return values, []
    if pre == "log":
        if np.any(values <= 0):
            raise ExtrapolationError("log transform requires values > 0")
        return np.log(values), ["pre=log"]
    if pre == "logit":
        return _logit(values), ["pre=logit"]
    raise ExtrapolationError(f"Unknown pre transform '{pre}'")


def _apply_post(values: np.ndarray, post: str | None) -> tuple[np.ndarray, list[str]]:
    if not post:
        return values, []
    if post == "exp":
        return np.exp(values), ["post=exp"]
    if post == "inv_logit":
        return _inv_logit(values), ["post=inv_logit"]
    raise ExtrapolationError(f"Unknown post transform '{post}'")


def _clean_history(periods: list[str], values: list[float | None]) -> tuple[list[str], np.ndarray]:
    points: list[tuple[int, float]] = []
    for p, v in zip(periods, values, strict=False):
        if v is None:
            continue
        try:
            ord_ = period_to_ordinal(p)
        except PeriodError:
            continue
        fv = float(v)
        if not math.isfinite(fv):
            continue
        points.append((ord_, fv))
    if not points:
        raise ExtrapolationError("No finite observations found")
    points.sort(key=lambda t: t[0])

    # Keep last value per period if duplicates.
    dedup: dict[int, float] = {}
    for o, v in points:
        dedup[o] = v
    ords = sorted(dedup.keys())
    cleaned_periods = [f"{o//4}.{(o%4)+1}" for o in ords]
    cleaned_values = np.array([dedup[o] for o in ords], dtype=float)
    return cleaned_periods, cleaned_values


def _flat_forecast(history: np.ndarray, n: int) -> np.ndarray:
    return np.full(shape=(n,), fill_value=float(history[-1]), dtype=float)


def _rolling_mean_forecast(history: np.ndarray, n: int, window: int) -> np.ndarray:
    w = min(int(window), len(history))
    if w <= 0:
        raise ExtrapolationError("rolling_mean window must be > 0")
    mean = float(np.mean(history[-w:]))
    return np.full(shape=(n,), fill_value=mean, dtype=float)


def _linear_trend_forecast(history: np.ndarray, n: int, tail: int) -> np.ndarray:
    t = min(int(tail), len(history))
    if t <= 1:
        return _flat_forecast(history, n)
    y = history[-t:]
    x = np.arange(t, dtype=float)
    slope, intercept = np.polyfit(x, y, deg=1)
    future_x = np.arange(t, t + n, dtype=float)
    return slope * future_x + intercept


def _ets_forecast(history: np.ndarray, n: int, seasonal_periods: int = 4) -> np.ndarray:
    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing  # type: ignore[import-not-found]
    except Exception as exc:
        raise ExtrapolationError(
            "statsmodels is required for ets extrapolation (install: fp-wraptr[forecast])"
        ) from exc
    model = ExponentialSmoothing(
        history,
        seasonal="add",
        seasonal_periods=int(seasonal_periods),
        trend="add",
        initialization_method="estimated",
    )
    fit = model.fit(optimized=True)
    forecast = fit.forecast(n)
    return np.asarray(forecast, dtype=float)


def _arima_forecast(history: np.ndarray, n: int) -> np.ndarray:
    try:
        from statsmodels.tsa.arima.model import ARIMA  # type: ignore[import-not-found]
    except Exception as exc:
        raise ExtrapolationError(
            "statsmodels is required for arima extrapolation (install: fp-wraptr[forecast])"
        ) from exc
    # MVP: a conservative quarterly-friendly order; pipeline can evolve to auto selection later.
    model = ARIMA(history, order=(1, 1, 1))
    fit = model.fit()
    forecast = fit.forecast(steps=n)
    return np.asarray(forecast, dtype=float)


def extrapolate_quarterly(
    *,
    history_periods: list[str],
    history_values: list[float | None],
    start: str,
    end: str,
    method: str,
    window: int = 8,
    tail: int = 20,
    bounds: tuple[float | None, float | None] | None = None,
    pre: str | None = None,
    post: str | None = None,
    fallback: str | None = "flat",
) -> ExtrapolationResult:
    """Return a quarterly series covering ``start..end`` (inclusive)."""
    target_periods = periods_between(start, end)
    cleaned_periods, cleaned_values = _clean_history(history_periods, history_values)

    notes: list[str] = []
    transformed, pre_notes = _apply_pre(cleaned_values, pre)
    notes.extend(pre_notes)

    n = len(target_periods)
    meth = str(method).strip().lower()
    try:
        if meth == "flat":
            forecast = _flat_forecast(transformed, n)
        elif meth == "rolling_mean":
            forecast = _rolling_mean_forecast(transformed, n, window=window)
        elif meth == "linear_trend":
            forecast = _linear_trend_forecast(transformed, n, tail=tail)
        elif meth == "ets":
            forecast = _ets_forecast(transformed, n)
        elif meth == "arima":
            forecast = _arima_forecast(transformed, n)
        else:
            raise ExtrapolationError(f"Unknown extrapolation method '{method}'")
    except Exception as exc:
        if fallback and fallback != meth:
            notes.append(f"fallback={fallback} (primary={meth} failed: {exc})")
            return extrapolate_quarterly(
                history_periods=cleaned_periods,
                history_values=cleaned_values.tolist(),
                start=start,
                end=end,
                method=fallback,
                window=window,
                tail=tail,
                bounds=bounds,
                pre=pre,
                post=post,
                fallback=None,
            )
        raise

    post_values, post_notes = _apply_post(forecast, post)
    notes.extend(post_notes)
    clamped = _clamp(post_values, bounds)
    if bounds is not None:
        notes.append(f"bounds={bounds}")

    return ExtrapolationResult(
        periods=target_periods,
        values=[float(v) for v in clamped.tolist()],
        method=meth,
        notes=notes,
    )
