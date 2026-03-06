"""Parse FP output files (fmout.txt) into structured data.

The FP output file contains:
1. Echo of input commands with program responses
2. Estimation results (coefficients, standard errors, fit stats per equation)
3. Solve iteration log
4. Forecast table: for each requested variable, rows of
   - "P lv" (level values), "P ch" (absolute change), "P %ch" (percent change)
   across quarterly periods

Example forecast block from fmout.txt:
    Variable   Periods forecast are  2025.4  TO   2029.4
     427 PCY     P lv   5.0353      4.2968      2.3212   ...
                 P ch  -0.58871    -0.73847    -1.9756   ...
                 P %ch -35.744     -46.974     -91.483   ...
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

__all__ = [
    "EstimationCoefficient",
    "EstimationResult",
    "FPOutputData",
    "ForecastVariable",
    "SolveIteration",
    "parse_fp_output",
]


@dataclass
class ForecastVariable:
    """A single variable's forecast data."""

    var_id: int
    name: str
    levels: list[float] = field(default_factory=list)
    changes: list[float] = field(default_factory=list)
    pct_changes: list[float] = field(default_factory=list)


@dataclass
class EstimationCoefficient:
    """A single coefficient from an equation's estimation results."""

    var_id: int
    var_name: str
    lag: int
    estimate: float
    std_error: float
    t_statistic: float
    mean: float


@dataclass
class EstimationResult:
    """Estimation results for a single equation."""

    equation_number: int
    dependent_var: str
    sample_start: str = ""
    sample_end: str = ""
    n_obs: int = 0
    coefficients: list[EstimationCoefficient] = field(default_factory=list)
    rho_iterations: int = 0
    se_equation: float = 0.0
    r_squared: float = 0.0
    durbin_watson: float = 0.0
    overid_pvalue: float | None = None
    overid_df: int = 0
    mean_dep_var: float = 0.0


@dataclass
class SolveIteration:
    """A single solve iteration entry."""

    period: str
    iterations: int


@dataclass
class FPOutputData:
    """Parsed FP output data."""

    model_title: str = ""
    forecast_start: str = ""
    forecast_end: str = ""
    base_period: str = ""
    periods: list[str] = field(default_factory=list)
    variables: dict[str, ForecastVariable] = field(default_factory=dict)
    estimations: list[EstimationResult] = field(default_factory=list)
    solve_iterations: list[SolveIteration] = field(default_factory=list)
    raw_text: str = ""

    def to_dataframe(self) -> pd.DataFrame:
        """Convert forecast data to a pandas DataFrame.

        Returns:
            DataFrame with periods as index and variables as columns (level values).
        """
        if not self.variables:
            return pd.DataFrame()

        data = {}
        for name, var in self.variables.items():
            data[name] = var.levels

        # periods includes the base period as first entry
        return pd.DataFrame(data, index=self.periods[: len(next(iter(data.values())))])

    def to_dict(self) -> dict:
        """Convert to a JSON-serializable dictionary."""
        return {
            "model_title": self.model_title,
            "forecast_start": self.forecast_start,
            "forecast_end": self.forecast_end,
            "base_period": self.base_period,
            "periods": self.periods,
            "variables": {
                name: {
                    "var_id": v.var_id,
                    "name": v.name,
                    "levels": v.levels,
                    "changes": v.changes,
                    "pct_changes": v.pct_changes,
                }
                for name, v in self.variables.items()
            },
            "estimations": [
                {
                    "equation_number": e.equation_number,
                    "dependent_var": e.dependent_var,
                    "sample_start": e.sample_start,
                    "sample_end": e.sample_end,
                    "n_obs": e.n_obs,
                    "r_squared": e.r_squared,
                    "durbin_watson": e.durbin_watson,
                    "se_equation": e.se_equation,
                    "n_coefficients": len(e.coefficients),
                }
                for e in self.estimations
            ],
            "solve_iterations": [
                {"period": s.period, "iterations": s.iterations} for s in self.solve_iterations
            ],
        }


def parse_fp_output(path: Path | str) -> FPOutputData:
    """Parse an FP output file.

    Args:
        path: Path to fmout.txt or similar FP output file.

    Returns:
        FPOutputData with extracted forecast information.
    """
    path = Path(path)
    text = path.read_text(encoding="utf-8", errors="replace")
    return parse_fp_output_text(text)


def parse_fp_output_text(text: str) -> FPOutputData:
    """Parse FP output from a text string."""
    result = FPOutputData(raw_text=text)

    # Extract model title (first non-blank, non-command line after SPACE)
    result.model_title = _parse_model_title(text)

    # Parse estimation results
    result.estimations = _parse_estimations(text)

    # Parse solve iterations
    result.solve_iterations = _parse_solve_iterations(text)

    # Find the forecast section
    # Pattern: "Variable   Periods forecast are  YYYY.Q  TO   YYYY.Q"
    forecast_header = re.search(
        r"Variable\s+Periods forecast are\s+(\d{4}\.\d)\s+TO\s+(\d{4}\.\d)",
        text,
    )
    if not forecast_header:
        return result

    result.forecast_start = forecast_header.group(1)
    result.forecast_end = forecast_header.group(2)

    # Generate period list
    result.periods = _generate_periods(result.forecast_start, result.forecast_end)

    # Find the period header line to get base period
    forecast_pos = forecast_header.end()
    period_section = text[forecast_pos:]

    # Parse actual period headers from the column header lines
    parsed_periods = _parse_period_headers(period_section)
    if parsed_periods:
        result.base_period = parsed_periods[0]
        result.periods = parsed_periods
    else:
        # Fallback: extract base period from header line
        base_match = re.search(r"^\s+(\d{4}\.\d)\s+\d{4}\.\d", period_section, re.MULTILINE)
        if base_match:
            result.base_period = base_match.group(1)
            result.periods = [result.base_period, *result.periods]

    # Parse variable blocks
    # Each variable block starts with: "<id> <NAME>     P lv  <values...>"
    # followed by continuation lines and P ch / P %ch rows
    var_pattern = re.compile(
        r"^\s*(\d+)\s+(\w+)\s+P lv\s+(.+?)$",
        re.MULTILINE,
    )

    for match in var_pattern.finditer(period_section):
        var_id = int(match.group(1))
        var_name = match.group(2).strip()
        var = ForecastVariable(var_id=var_id, name=var_name)

        # Extract the block of lines for this variable
        block_start = match.start()
        # Find the next variable block or end
        next_var = var_pattern.search(period_section, match.end())
        block_end = next_var.start() if next_var else len(period_section)
        block = period_section[block_start:block_end]

        # Parse the three row types: P lv, P ch, P %ch
        var.levels = _parse_value_rows(block, "P lv")
        var.changes = _parse_value_rows(block, "P ch")
        var.pct_changes = _parse_value_rows(block, r"P %ch")

        result.variables[var_name] = var

    return result


def _parse_value_rows(block: str, row_type: str) -> list[float]:
    """Extract numeric values from a row type (P lv, P ch, P %ch) and its continuations.

    The format is:
        <prefix>  P lv   5.0353      4.2968      2.3212      2.2759      2.2212
                                     2.0090      2.0472      2.1677      2.2854
                                     ...

    Values on the first line start after "P lv" (or P ch, P %ch).
    Continuation lines are indented and start with numeric values.
    A new row type (P lv, P ch, P %ch) or a blank line ends the section.
    """
    values: list[float] = []
    lines = block.split("\n")

    in_section = False
    for line in lines:
        if row_type in line:
            # First line of this row type
            in_section = True
            # Extract values after the row type marker
            after_marker = line.split(row_type, 1)[1]
            values.extend(_extract_floats(after_marker))
        elif in_section:
            stripped = line.strip()
            if not stripped:
                # Blank line ends the section
                in_section = False
                continue
            # Check if this is a continuation (starts with whitespace + numbers)
            # or a new row type
            if re.match(r"^\s+P (lv|ch|%ch)", line):
                in_section = False
                continue
            if re.match(r"^\s*\d+\s+\w+\s+P", line):
                # Next variable block
                in_section = False
                continue
            # Continuation line -- extract floats
            floats = _extract_floats(stripped)
            if floats:
                values.extend(floats)
            else:
                in_section = False

    return values


def _extract_floats(text: str) -> list[float]:
    """Extract floating point numbers from a text string.

    Handles FORTRAN-style scientific notation like:
        0.58871E-01  -0.73847E+00  5.0353
    """
    # Match numbers including FORTRAN-style scientific notation
    pattern = re.compile(r"-?\d+\.?\d*(?:E[+-]?\d+)?", re.IGNORECASE)
    matches = pattern.findall(text)
    result = []
    for m in matches:
        try:
            result.append(float(m))
        except ValueError:
            continue
    return result


def _generate_periods(start: str, end: str) -> list[str]:
    """Generate quarterly period strings from start to end inclusive.

    Args:
        start: e.g. "2025.4"
        end: e.g. "2029.4"

    Returns:
        List of period strings like ["2025.4", "2026.1", "2026.2", ...]
    """
    start_year, start_q = int(start.split(".")[0]), int(start.split(".")[1])
    end_year, end_q = int(end.split(".")[0]), int(end.split(".")[1])

    periods = []
    y, q = start_year, start_q
    while (y, q) <= (end_year, end_q):
        periods.append(f"{y}.{q}")
        q += 1
        if q > 4:
            q = 1
            y += 1

    return periods


def _parse_period_headers(text: str) -> list[str]:
    """Parse the actual period column headers from the forecast section.

    The format is multiple lines of YYYY.Q values:
        2025.3      2025.4      2026.1      2026.2      2026.3
                    2026.4      2027.1      2027.2      2027.3
                    ...
    """
    period_pat = re.compile(r"\d{4}\.\d")
    periods: list[str] = []
    lines = text.split("\n")

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if periods:
                # Blank line after period headers means we're done
                break
            continue
        # Check if this line contains only period values (and whitespace)
        tokens = stripped.split()
        if all(period_pat.fullmatch(t) for t in tokens) and tokens:
            periods.extend(tokens)
        elif periods:
            # Non-period line after collecting periods means we're done
            break

    return periods


def _parse_model_title(text: str) -> str:
    """Extract the model title from the output header.

    The title appears after the SPACE command line, e.g.:
        SPACE MAXVAR=500 ...;
        US MODEL DECEMBER 23, 2025
    """
    match = re.search(
        r"^SPACE\s+MAXVAR=.+?;\s*\n(.+?)$",
        text,
        re.MULTILINE,
    )
    if match:
        return match.group(1).strip()
    return ""


def _parse_estimations(text: str) -> list[EstimationResult]:
    """Parse all equation estimation result blocks from the output."""
    results: list[EstimationResult] = []

    # Find each "Equation number = N" block
    eq_pattern = re.compile(
        r"^Equation number =\s+(\d+)\s*\n"
        r"\*+\s*\n\s*\n"
        r"Dependent variable = (\w+)",
        re.MULTILINE,
    )

    matches = list(eq_pattern.finditer(text))
    for i, match in enumerate(matches):
        eq_num = int(match.group(1))
        dep_var = match.group(2).strip()
        est = EstimationResult(equation_number=eq_num, dependent_var=dep_var)

        # Determine the block extent (to next equation or end of estimation section)
        block_start = match.start()
        block_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        block = text[block_start:block_end]

        # Sample period and observations
        smpl_match = re.search(
            r"SMPL and No\. Obs\.\s+=\s+(\d{4}\.\d)\s+(\d{4}\.\d)\s+(\d+)",
            block,
        )
        if smpl_match:
            est.sample_start = smpl_match.group(1)
            est.sample_end = smpl_match.group(2)
            est.n_obs = int(smpl_match.group(3))

        # RHO iterations
        rho_match = re.search(r"Number of iterations for RHO =\s+(\d+)", block)
        if rho_match:
            est.rho_iterations = int(rho_match.group(1))

        # Mean of dependent variable
        mean_match = re.search(r"Mean of dependent variable\s+([-\d.]+)", block)
        if mean_match:
            est.mean_dep_var = float(mean_match.group(1))

        # Equation statistics
        se_match = re.search(r"SE of equation\s+=\s+([\d.]+)", block)
        if se_match:
            est.se_equation = float(se_match.group(1))

        r2_match = re.search(r"R squared\s+=\s+([\d.]+)", block)
        if r2_match:
            est.r_squared = float(r2_match.group(1))

        dw_match = re.search(r"Durbin-Watson statistic\s+=\s+([\d.]+)", block)
        if dw_match:
            est.durbin_watson = float(dw_match.group(1))

        overid_match = re.search(r"Overid test: p-value\s+=\s+([\d.]+)\s+\(df =\s+(\d+)\)", block)
        if overid_match:
            est.overid_pvalue = float(overid_match.group(1))
            est.overid_df = int(overid_match.group(2))

        # Parse coefficient table
        est.coefficients = _parse_coefficients(block)

        results.append(est)

    return results


def _parse_coefficients(block: str) -> list[EstimationCoefficient]:
    """Parse the coefficient table from an equation estimation block.

    Format:
        328 LPF     ( -1)       0.886852027       0.011513387  77.02790    -0.765587
          0 RHO     ( -1)       0.234101712       0.060202352   3.88858     0.000000
    """
    coefficients: list[EstimationCoefficient] = []

    # Find the coefficient header line
    header_match = re.search(r"Coef est\s+SE\s+T statistic\s+Mean", block)
    if not header_match:
        return coefficients

    coef_section = block[header_match.end() :]
    coef_pattern = re.compile(
        r"^\s*(\d+)\s+(\w+)\s+\(\s*(-?\d+)\)\s+"
        r"(-?[\d.]+)\s+(-?[\d.]+)\s+(-?[\d.]+)\s+(-?[\d.]+)",
        re.MULTILINE,
    )

    for m in coef_pattern.finditer(coef_section):
        # Stop if we hit equation statistics
        preceding = coef_section[: m.start()]
        if "SE of equation" in preceding:
            break

        coefficients.append(
            EstimationCoefficient(
                var_id=int(m.group(1)),
                var_name=m.group(2).strip(),
                lag=int(m.group(3)),
                estimate=float(m.group(4)),
                std_error=float(m.group(5)),
                t_statistic=float(m.group(6)),
                mean=float(m.group(7)),
            )
        )

    return coefficients


def _parse_solve_iterations(text: str) -> list[SolveIteration]:
    """Parse ITERS= lines from the solve section.

    Format:
        ITERS=    40  2025.4
        ITERS=    40  2026.1
    """
    iterations: list[SolveIteration] = []
    pattern = re.compile(r"^ITERS=\s+(\d+)\s+(\d{4}\.\d)", re.MULTILINE)

    for match in pattern.finditer(text):
        iterations.append(
            SolveIteration(
                iterations=int(match.group(1)),
                period=match.group(2),
            )
        )

    return iterations
