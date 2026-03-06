"""Helpers for scanning fp.exe output for solve/convergence errors."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = ["SolveErrorMatch", "scan_solution_errors"]


@dataclass(frozen=True, slots=True)
class SolveErrorMatch:
    solve: str | None
    iters: int | None
    period: str | None
    match: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "solve": self.solve,
            "iters": self.iters,
            "period": self.period,
            "match": self.match,
        }


_SOLUTION_ERROR_RE = re.compile(
    r"Solution error in\s+(?P<solve>\w+)\.\s*(?:ITERS\s*=\s*(?P<iters>\d+)\s+(?P<period>\d{4}\.[1-4]))?",
    re.IGNORECASE,
)
_ITERS_LINE_RE = re.compile(r"ITERS\s*=\s*(?P<iters>\d+)\s+(?P<period>\d{4}\.[1-4])", re.IGNORECASE)


def scan_solution_errors(work_dir: Path) -> list[SolveErrorMatch]:
    """Best-effort scan for fp.exe solve errors recorded in fmout.txt.

    fp.exe may emit:
      - `ITERS=    40  2026.1` on one line, then
      - `Solution error in SOL1.` on the next line.

    This scanner:
    - extracts direct `Solution error in ... ITERS=... <period>` patterns, and
    - falls back to pairing `Solution error in ...` with the closest preceding ITERS line.
    """
    fmout_path = Path(work_dir) / "fmout.txt"
    if not fmout_path.exists():
        return []

    try:
        lines = fmout_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []

    matches: list[SolveErrorMatch] = []
    last_iters: tuple[int | None, str | None] = (None, None)
    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        it = _ITERS_LINE_RE.search(line)
        if it:
            try:
                last_iters = (int(it.group("iters")), str(it.group("period")))
            except Exception:
                last_iters = (None, None)
            continue

        m = _SOLUTION_ERROR_RE.search(line)
        if not m:
            continue
        solve = str(m.group("solve")) if m.group("solve") else None
        iters_token = m.group("iters")
        period_token = m.group("period")
        iters = int(iters_token) if iters_token is not None else last_iters[0]
        period = str(period_token) if period_token is not None else last_iters[1]
        matches.append(SolveErrorMatch(solve=solve, iters=iters, period=period, match=line))

    if matches:
        return matches

    # Fallback: collect raw "Solution error" lines for unknown formats.
    raw_matches: list[SolveErrorMatch] = []
    for raw in lines:
        if "solution error" in raw.lower():
            raw_matches.append(SolveErrorMatch(solve=None, iters=None, period=None, match=raw.strip()))
            if len(raw_matches) >= 10:
                break
    return raw_matches

