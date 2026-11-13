"""Helpers for scanning fp.exe output for solve/convergence errors."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = ["SolveErrorMatch", "scan_run_solution_errors", "scan_solution_errors"]


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
# fp.for SOL: on hitting MAXITERS it prints this, keeps the unconverged iterate and moves on.
_ITERATION_LIMIT_RE = re.compile(
    r"Solution error\.\s*Iteration limit of\s+(?P<limit>\d+)\s+reached for observation number\s+(?P<obs>\d+)",
    re.IGNORECASE,
)
_ITERS_LINE_RE = re.compile(
    r"ITERS\s*=\s*(?P<iters>\d+)\s+(?P<period>\d{4}\.[1-4])", re.IGNORECASE
)


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

        limit = _ITERATION_LIMIT_RE.search(line)
        if limit:
            matches.append(
                SolveErrorMatch(
                    solve="iteration_limit",
                    iters=int(limit.group("limit")),
                    period=None,
                    match=line,
                )
            )
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
            raw_matches.append(
                SolveErrorMatch(solve=None, iters=None, period=None, match=raw.strip())
            )
            if len(raw_matches) >= 10:
                break
    return raw_matches


def scan_run_solution_errors(run_dir: Path) -> list[SolveErrorMatch]:
    """Scan an fp.exe run directory (its fmout.txt, else work/fmout.txt) for solve errors.

    fppy/fp-r runs (backend per their scenario.yaml) are not scanned: their work/fmout.txt
    is the staged fp.exe stock output, not their own solution.
    """
    from fp_wraptr.run_artifacts import LOADFORMAT_BACKENDS, saved_run_backend

    run_dir = Path(run_dir)
    if saved_run_backend(run_dir) in LOADFORMAT_BACKENDS:
        return []
    for candidate in (run_dir, run_dir / "work"):
        if (candidate / "fmout.txt").exists():
            return scan_solution_errors(candidate)
    return []
