from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_PABEV_SMPL_RE = re.compile(
    r"^\s*SMPL\s+(?P<start>\d{4}\.\d)\s+(?P<end>\d{4}\.\d)\s*;\s*$",
    re.IGNORECASE,
)
_PABEV_LOAD_RE = re.compile(r"^\s*LOAD\s+(?P<name>[A-Za-z0-9_]+)\s*;\s*$", re.IGNORECASE)
_PABEV_END_RE = re.compile(r"^\s*'END'\s*$", re.IGNORECASE)
_PABEV_FORTRAN_NO_E_RE = re.compile(r"^(?P<mant>[+-]?(?:\d+(?:\.\d*)?|\.\d+))(?P<exp>[+-]\d+)$")


@dataclass(frozen=True)
class PabevPeriod:
    year: int
    quarter: int

    @classmethod
    def parse(cls, text: str) -> PabevPeriod:
        raw = text.strip()
        if not re.match(r"^\d{4}\.[1-4]$", raw):
            raise ValueError(f"invalid period: {text!r}")
        year_s, quarter_s = raw.split(".", 1)
        return cls(year=int(year_s), quarter=int(quarter_s))

    def __str__(self) -> str:
        return f"{self.year}.{self.quarter}"

    def next(self) -> PabevPeriod:
        if self.quarter < 4:
            return PabevPeriod(self.year, self.quarter + 1)
        return PabevPeriod(self.year + 1, 1)


def iter_periods(start: PabevPeriod, end: PabevPeriod) -> list[PabevPeriod]:
    cur = start
    out: list[PabevPeriod] = []
    while True:
        out.append(cur)
        if cur == end:
            return out
        cur = cur.next()


def parse_pabev(path: Path) -> tuple[tuple[PabevPeriod, ...], dict[str, tuple[float, ...]]]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not lines:
        raise ValueError(f"{path}: empty file")

    smpl_match = _PABEV_SMPL_RE.match(lines[0])
    if not smpl_match:
        raise ValueError(f"{path}: expected SMPL header on first line, got: {lines[0].strip()!r}")

    start = PabevPeriod.parse(smpl_match.group("start"))
    end = PabevPeriod.parse(smpl_match.group("end"))
    periods = tuple(iter_periods(start, end))
    expected_values = len(periods)

    series: dict[str, tuple[float, ...]] = {}
    current_name: str | None = None
    current_tokens: list[str] = []
    for raw in lines[1:]:
        if current_name is None:
            match = _PABEV_LOAD_RE.match(raw)
            if match:
                current_name = match.group("name")
                current_tokens = []
            continue

        if _PABEV_END_RE.match(raw):
            if len(current_tokens) != expected_values:
                raise ValueError(
                    f"{path}: LOAD {current_name} expected {expected_values} values, got {len(current_tokens)}"
                )
            values: list[float] = []
            for tok in current_tokens:
                cleaned = tok.strip().replace("D", "E").replace("d", "E")
                # Some fp.exe builds emit Fortran-style exponents without the "E"
                # separator, e.g. "0.26630469439+102" meaning "0.26630469439E+102".
                if "E" not in cleaned and "e" not in cleaned:
                    m = _PABEV_FORTRAN_NO_E_RE.match(cleaned)
                    if m:
                        cleaned = f"{m.group('mant')}E{m.group('exp')}"
                values.append(float(cleaned))
            series[current_name] = tuple(values)
            current_name = None
            current_tokens = []
            continue

        stripped = raw.strip()
        if not stripped:
            continue
        current_tokens.extend(stripped.split())

    if current_name is not None:
        raise ValueError(f"{path}: unterminated LOAD {current_name} (missing 'END')")

    return periods, series


def period_index(periods: tuple[PabevPeriod, ...], start: PabevPeriod | None) -> int:
    if start is None:
        return 0
    for idx, p in enumerate(periods):
        if p.year > start.year or (p.year == start.year and p.quarter >= start.quarter):
            return idx
    return len(periods)


def period_stop_index(periods: tuple[PabevPeriod, ...], end: PabevPeriod | None) -> int:
    if end is None:
        return len(periods)
    stop = 0
    for idx, p in enumerate(periods):
        if p.year < end.year or (p.year == end.year and p.quarter <= end.quarter):
            stop = idx + 1
            continue
        break
    return stop


def is_diff(a: float, b: float, *, atol: float, rtol: float) -> bool:
    if math.isnan(a) and math.isnan(b):
        return False
    if math.isnan(a) != math.isnan(b):
        return True
    if math.isinf(a) or math.isinf(b):
        return a != b
    return not math.isclose(a, b, rel_tol=rtol, abs_tol=atol)


def parse_float_csv(text: str) -> list[float]:
    values: list[float] = []
    for raw in text.split(","):
        token = raw.strip()
        if not token:
            continue
        values.append(float(token))
    return values


def is_effectively_discrete_series(
    values: tuple[float, ...],
    *,
    eps: float,
    missing_sentinels: frozenset[float],
    start_idx: int,
) -> bool:
    # Avoid false positives on short horizons (e.g. `--quick` single-quarter checks)
    # where many continuous series can coincidentally land on integer-like values.
    non_missing_finite = 0
    for value in values[start_idx:]:
        if value in missing_sentinels:
            continue
        if not math.isfinite(value):
            return False
        non_missing_finite += 1
        if abs(value - round(value)) > eps:
            return False
    return non_missing_finite >= 8


def toleranced_compare(
    left_path: Path,
    right_path: Path,
    *,
    start: str | None,
    end: str | None = None,
    variables: frozenset[str] | None = None,
    atol: float,
    rtol: float,
    top: int = 10,
    hard_fail_top: int | None = 10,
    missing_sentinels: frozenset[float] = frozenset((-99.0,)),
    discrete_eps: float = 1e-12,
    signflip_eps: float = 1e-3,
    collect_period_stats: bool = False,
    period_quantiles: tuple[float, ...] = (0.5, 0.9, 0.99),
) -> tuple[bool, dict[str, Any]]:
    periods_left, series_left = parse_pabev(left_path)
    periods_right, series_right = parse_pabev(right_path)
    if periods_left != periods_right:
        return False, {
            "status": "failed",
            "reason": "periods_mismatch",
        }
    start_period = PabevPeriod.parse(start) if start and start.strip() else None
    end_period = PabevPeriod.parse(end) if end and end.strip() else None
    start_idx = period_index(periods_left, start_period)
    end_idx = period_stop_index(periods_left, end_period)
    if end_idx < start_idx:
        end_idx = start_idx

    left_keys = set(series_left)
    right_keys = set(series_right)
    if variables:
        wanted = {str(v).upper() for v in variables if str(v).strip()}
        shared_vars = sorted(left_keys & right_keys & wanted)
        missing_left = sorted(wanted - left_keys)
        missing_right = sorted(wanted - right_keys)
    else:
        shared_vars = sorted(left_keys & right_keys)
        missing_left = sorted(right_keys - left_keys)
        missing_right = sorted(left_keys - right_keys)

    hard_fails: list[dict[str, Any]] = []
    discrete_vars: set[str] = set()
    diffs: list[dict[str, Any]] = []
    abs_diffs: list[float] = []
    per_period_abs_diffs: list[list[float]] | None = None
    per_period_abs_diffs_nonzero: list[list[float]] | None = None
    if collect_period_stats:
        per_period_abs_diffs = [[] for _ in range(max(0, end_idx - start_idx))]
        per_period_abs_diffs_nonzero = [[] for _ in range(max(0, end_idx - start_idx))]

    def _quantile(sorted_values: list[float], q: float) -> float:
        if not sorted_values:
            return 0.0
        if q <= 0.0:
            return float(sorted_values[0])
        if q >= 1.0:
            return float(sorted_values[-1])
        pos = q * (len(sorted_values) - 1)
        idx = int(pos)
        frac = pos - idx
        lo = float(sorted_values[idx])
        if frac <= 0.0:
            return lo
        hi = float(sorted_values[min(idx + 1, len(sorted_values) - 1)])
        return lo + frac * (hi - lo)

    for name in shared_vars:
        left_vals = series_left[name]
        right_vals = series_right[name]

        # Hard-fail invariant: if a series is effectively discrete, require
        # integer parity on all checked periods.
        left_discrete = is_effectively_discrete_series(
            left_vals,
            eps=float(discrete_eps),
            missing_sentinels=missing_sentinels,
            start_idx=start_idx,
        )
        right_discrete = is_effectively_discrete_series(
            right_vals,
            eps=float(discrete_eps),
            missing_sentinels=missing_sentinels,
            start_idx=start_idx,
        )
        is_discrete = bool(left_discrete or right_discrete)
        if is_discrete:
            discrete_vars.add(name)

        # Optionally collect full per-period diff distributions for bounded-drift checks.
        if per_period_abs_diffs is not None:
            for idx in range(start_idx, end_idx):
                a = float(left_vals[idx])
                b = float(right_vals[idx])
                if (a in missing_sentinels) or (b in missing_sentinels):
                    continue
                if not (math.isfinite(a) and math.isfinite(b)):
                    continue
                abs_diff = abs(a - b)
                per_period_abs_diffs[idx - start_idx].append(abs_diff)
                if per_period_abs_diffs_nonzero is not None and abs_diff != 0.0:
                    per_period_abs_diffs_nonzero[idx - start_idx].append(abs_diff)

        first_diff_idx: int | None = None
        for idx in range(start_idx, end_idx):
            a = float(left_vals[idx])
            b = float(right_vals[idx])
            a_missing = a in missing_sentinels
            b_missing = b in missing_sentinels

            # Hard-fail invariant: missing sentinel pattern must match.
            if a_missing != b_missing:
                hard_fails.append({
                    "variable": name,
                    "period": str(periods_left[idx]),
                    "index": int(idx),
                    "reason": "missing_sentinel_mismatch",
                    "left": float(a),
                    "right": float(b),
                    "left_value": float(a),
                    "right_value": float(b),
                })
                continue
            if a_missing and b_missing:
                continue

            # Hard-fail invariant: sign flips away from ~0.
            if (a < 0.0) != (b < 0.0) and max(abs(a), abs(b)) > float(signflip_eps):
                hard_fails.append({
                    "variable": name,
                    "period": str(periods_left[idx]),
                    "index": int(idx),
                    "reason": "sign_flip",
                    "left": float(a),
                    "right": float(b),
                    "left_value": float(a),
                    "right_value": float(b),
                })
                continue

            # Hard-fail invariant: discrete (dummy/integer) series parity.
            if is_discrete and round(a) != round(b):
                hard_fails.append({
                    "variable": name,
                    "period": str(periods_left[idx]),
                    "index": int(idx),
                    "reason": "discrete_mismatch",
                    "left": float(a),
                    "right": float(b),
                    "left_value": float(a),
                    "right_value": float(b),
                    "left_rounded": round(a),
                    "right_rounded": round(b),
                })
                continue

            if is_diff(a, b, atol=atol, rtol=rtol):
                first_diff_idx = idx
                break

        if first_diff_idx is None:
            continue

        abs_diff = abs(left_vals[first_diff_idx] - right_vals[first_diff_idx])
        abs_diffs.append(abs_diff)
        diffs.append({
            "variable": name,
            "period": str(periods_left[first_diff_idx]),
            "index": int(first_diff_idx),
            "left_value": float(left_vals[first_diff_idx]),
            "right_value": float(right_vals[first_diff_idx]),
            "abs_diff": float(abs_diff),
        })

    diffs.sort(key=lambda row: (-float(row["abs_diff"]), int(row["index"]), str(row["variable"])))
    abs_diffs_sorted = sorted(abs_diffs)
    max_abs = float(abs_diffs_sorted[-1]) if abs_diffs_sorted else 0.0
    median_abs = float(abs_diffs_sorted[len(abs_diffs_sorted) // 2]) if abs_diffs_sorted else 0.0
    p90_abs: float = 0.0
    if abs_diffs_sorted:
        p90_abs = float(abs_diffs_sorted[int(0.9 * (len(abs_diffs_sorted) - 1))])

    hard_fails.sort(key=lambda row: (str(row["variable"]), int(row["index"]), str(row["reason"])))
    ok = not missing_left and not missing_right and not hard_fails and not diffs

    per_period_stats: list[dict[str, Any]] | None = None
    if per_period_abs_diffs is not None:
        per_period_stats = []
        for rel_idx, values in enumerate(per_period_abs_diffs):
            period = periods_left[start_idx + rel_idx]
            sorted_values = sorted(values)
            nonzero_sorted_values: list[float] = []
            if per_period_abs_diffs_nonzero is not None:
                nonzero_sorted_values = sorted(per_period_abs_diffs_nonzero[rel_idx])
            row: dict[str, Any] = {
                "period": str(period),
                "cell_count": len(sorted_values),
                "nonzero_cell_count": len(nonzero_sorted_values),
                "max_abs_diff": float(sorted_values[-1]) if sorted_values else 0.0,
            }
            for q in period_quantiles:
                q_key = f"p{round(q * 100):02d}_abs_diff" if q != 0.5 else "median_abs_diff"
                row[q_key] = float(_quantile(sorted_values, float(q)))
                q_key_nonzero = (
                    f"p{round(q * 100):02d}_abs_diff_nonzero"
                    if q != 0.5
                    else "median_abs_diff_nonzero"
                )
                row[q_key_nonzero] = float(_quantile(nonzero_sorted_values, float(q)))
            per_period_stats.append(row)

    return ok, {
        "status": "ok" if ok else "diffs_present",
        "start": str(start_period) if start_period else None,
        "end": str(end_period) if end_period else None,
        "atol": float(atol),
        "rtol": float(rtol),
        "hard_fail_top": hard_fail_top,
        "missing_sentinels": sorted(missing_sentinels),
        "discrete_eps": float(discrete_eps),
        "signflip_eps": float(signflip_eps),
        "shared_variable_count": len(shared_vars),
        "missing_left": missing_left,
        "missing_right": missing_right,
        "hard_fail_cells_count": len(hard_fails),
        "hard_fail_cell_count": len(hard_fails),
        "hard_fail_cells": hard_fails
        if hard_fail_top is None
        else hard_fails[: max(0, int(hard_fail_top))],
        "discrete_variable_count": len(discrete_vars),
        "discrete_variables_sample": sorted(discrete_vars)[:20],
        "diff_variable_count": len(diffs),
        "max_abs_diff": max_abs,
        "median_abs_diff": median_abs,
        "p90_abs_diff": p90_abs,
        "top_first_diffs": diffs[: max(0, int(top))],
        "per_period_stats": per_period_stats,
    }
