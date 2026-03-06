"""Quarterly period helpers for FP period tokens (``YYYY.Q``)."""

from __future__ import annotations

import re
from dataclasses import dataclass

_FP_PERIOD_RE = re.compile(r"^(?P<year>\d{4})\.(?P<q>[1-4])$")
_ALT_QUARTER_RE = re.compile(
    r"^(?P<year>\d{4})\s*[-_/]?\s*(?:Q(?P<q>[1-4])|(?P<q2>[1-4])Q)\s*$",
    re.IGNORECASE,
)


class PeriodError(ValueError):
    """Raised when a period token cannot be normalized to FP format."""


@dataclass(frozen=True, slots=True)
class PeriodWindow:
    start: str
    end: str


def normalize_period_token(token: str) -> str:
    """Normalize common quarter formats to FP format (``YYYY.Q``).

    Accepted:
    - ``2025.4`` (already FP)
    - ``2025Q4``, ``2025-Q4``, ``2025 Q4``
    - ``Q4 2025`` is intentionally not accepted in MVP (ambiguous in some inputs)
    """
    text = str(token).strip()
    if not text:
        raise PeriodError("Empty period token")

    m = _FP_PERIOD_RE.match(text)
    if m:
        return f"{int(m.group('year'))}.{int(m.group('q'))}"

    m = _ALT_QUARTER_RE.match(text)
    if m:
        year = int(m.group("year"))
        q = m.group("q") or m.group("q2")
        if not q:
            raise PeriodError(f"Invalid quarter token '{token}'")
        return f"{year}.{int(q)}"

    raise PeriodError(f"Unsupported period token '{token}' (expected quarterly)")


def _parse_fp_period(period: str) -> tuple[int, int]:
    fp = normalize_period_token(period)
    year_s, q_s = fp.split(".")
    year = int(year_s)
    q = int(q_s)
    return year, q


def period_to_ordinal(period: str) -> int:
    year, q = _parse_fp_period(period)
    return year * 4 + (q - 1)


def ordinal_to_period(ordinal: int) -> str:
    year = int(ordinal // 4)
    q = int((ordinal % 4) + 1)
    return f"{year}.{q}"


def periods_between(start: str, end: str) -> list[str]:
    start_ord = period_to_ordinal(start)
    end_ord = period_to_ordinal(end)
    if end_ord < start_ord:
        raise PeriodError(f"Invalid window '{start}..{end}' (end must be >= start).")
    return [ordinal_to_period(i) for i in range(start_ord, end_ord + 1)]

