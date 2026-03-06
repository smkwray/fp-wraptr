"""LOADFORMAT (PABEV-style) parser helpers.

fp.exe can emit "LOADFORMAT" outputs via PRINTVAR, often under filenames like
PABEV.TXT / PACEV.TXT or custom FILEOUT=.DAT outputs.

fp-wraptr standardizes these artifacts by copying one primary output to
`LOADFORMAT.DAT` in each run directory.
"""

from __future__ import annotations

import math
from pathlib import Path

from fppy.pabev_parity import parse_pabev

__all__ = [
    "read_loadformat",
    "add_derived_series",
]


def read_loadformat(path: Path | str) -> tuple[list[str], dict[str, list[float]]]:
    """Parse a LOADFORMAT file into (period_tokens, series).

    Returns:
        periods: list[str] of tokens like "2025.4"
        series: dict[var_name, list[float]]
    """
    periods, series = parse_pabev(Path(path))
    period_tokens = [str(p) for p in periods]
    out = {name: list(values) for name, values in series.items()}
    return period_tokens, out


def add_derived_series(series: dict[str, list[float]]) -> dict[str, list[float]]:
    """Add common derived series used in dashboards.

    Currently supported:
      - WR = WF / PF  (when both exist)
    """
    if "WF" in series and "PF" in series and "WR" not in series:
        wf = series["WF"]
        pf = series["PF"]
        n = min(len(wf), len(pf))
        wr: list[float] = []
        for idx in range(n):
            denom = pf[idx]
            if not isinstance(denom, (int, float)) or not math.isfinite(float(denom)) or denom == 0:
                wr.append(math.nan)
                continue
            wr.append(float(wf[idx]) / float(denom))
        series["WR"] = wr
    return series

