"""Small comparison helpers for reduced fp-r shared-slice checks."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FpRComparisonResult:
    """Comparison summary for one fp-r series CSV against a seeded expectation."""

    status: str
    actual_path: str
    expected_path: str
    compared_periods: int
    compared_variables: list[str] = field(default_factory=list)
    atol: float = 1.1e-3
    rtol: float = 1e-6
    mismatch_count: int = 0
    max_abs_diff: float = 0.0
    missing_periods: list[str] = field(default_factory=list)
    missing_variables: list[str] = field(default_factory=list)
    mismatches: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _read_series_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    if not fieldnames or "period" not in fieldnames:
        raise ValueError(f"Series CSV missing required 'period' column: {path}")
    return fieldnames, rows


def compare_fp_r_series_csv(
    actual_path: Path | str,
    expected_path: Path | str,
    *,
    atol: float = 1.1e-3,
    rtol: float = 1e-6,
    max_mismatches: int = 20,
) -> FpRComparisonResult:
    """Compare an fp-r emitted series CSV to a seeded expected CSV."""

    actual = Path(actual_path)
    expected = Path(expected_path)
    actual_fields, actual_rows = _read_series_csv(actual)
    expected_fields, expected_rows = _read_series_csv(expected)

    expected_variables = [name for name in expected_fields if name != "period"]
    actual_variables = {name for name in actual_fields if name != "period"}
    shared_variables = [name for name in expected_variables if name in actual_variables]
    missing_variables = [name for name in expected_variables if name not in actual_variables]

    actual_by_period = {str(row["period"]): row for row in actual_rows}
    missing_periods: list[str] = []
    mismatches: list[dict[str, Any]] = []
    mismatch_count = 0
    max_abs_diff = 0.0
    compared_periods = 0

    for expected_row in expected_rows:
        period = str(expected_row["period"])
        actual_row = actual_by_period.get(period)
        if actual_row is None:
            missing_periods.append(period)
            continue
        compared_periods += 1
        for variable in shared_variables:
            expected_value = float(expected_row[variable])
            actual_value = float(actual_row[variable])
            abs_diff = abs(actual_value - expected_value)
            tolerance = float(atol) + float(rtol) * abs(expected_value)
            max_abs_diff = max(max_abs_diff, abs_diff)
            if abs_diff <= tolerance:
                continue
            mismatch_count += 1
            if len(mismatches) < max_mismatches:
                mismatches.append(
                    {
                        "period": period,
                        "variable": variable,
                        "actual": actual_value,
                        "expected": expected_value,
                        "abs_diff": abs_diff,
                        "tolerance": tolerance,
                    }
                )

    ok = not missing_periods and not missing_variables and mismatch_count == 0
    return FpRComparisonResult(
        status="ok" if ok else "mismatch",
        actual_path=str(actual),
        expected_path=str(expected),
        compared_periods=compared_periods,
        compared_variables=shared_variables,
        atol=float(atol),
        rtol=float(rtol),
        mismatch_count=mismatch_count,
        max_abs_diff=max_abs_diff,
        missing_periods=missing_periods,
        missing_variables=missing_variables,
        mismatches=mismatches,
    )


def write_fp_r_comparison_report(result: FpRComparisonResult, output_path: Path | str) -> Path:
    """Persist a JSON comparison report for a reduced fp-r slice."""

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
