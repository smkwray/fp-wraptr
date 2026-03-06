"""Writer utilities for FP ``fmdata.txt`` files.

fp.exe can be sensitive to formatting/newlines. The stock Fair-Parke model ships
`fmdata.txt` with:
- CRLF newlines
- fixed-format scientific notation using a `0.xxxxxE+YY` mantissa style
- one global `SMPL` header followed by many `LOAD` blocks, then `'END'` and `END;`.

This writer emits the same conventions by default so regenerated fmdata files
can be consumed by fp.exe without surprises.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal

_PERIOD_RE = re.compile(r"^\d{4}\.[1-4]$")


def _validate_period(period: str) -> str:
    text = str(period).strip()
    if not _PERIOD_RE.match(text):
        raise ValueError(f"Invalid FP period '{period}' (expected YYYY.Q)")
    return text


def _format_value(value: float, *, decimals: int = 11) -> str:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"Non-finite fmdata value is not supported: {value!r}")

    # fp.exe style: mantissa in [0.1, 1.0) and exponent adjusted accordingly.
    # Example: 258.2 -> 0.25820000000E+03 (instead of 2.582E+02).
    if number == 0.0:
        return f"0.{('0' * decimals)}E+00"

    sign = "-" if number < 0 else ""
    abs_value = abs(number)
    exponent = math.floor(math.log10(abs_value))
    mantissa = abs_value / (10.0**exponent)
    mantissa /= 10.0
    exponent += 1

    rendered = f"{mantissa:.{decimals}f}"
    # Guard rounding edge-case where mantissa becomes 1.000000... due to formatting.
    if rendered.startswith("1."):
        mantissa /= 10.0
        exponent += 1
        rendered = f"{mantissa:.{decimals}f}"

    exp = f"{exponent:+03d}"
    return f"{sign}{rendered}E{exp}"


def render_fmdata_text(
    *,
    sample_start: str,
    sample_end: str,
    series: Mapping[str, Sequence[float]],
    values_per_line: int = 4,
) -> str:
    """Render FP ``fmdata.txt`` content in stock-FM style (global SMPL + LOAD blocks)."""
    start = _validate_period(sample_start)
    end = _validate_period(sample_end)
    if values_per_line <= 0:
        raise ValueError("values_per_line must be > 0")

    lines: list[str] = []
    lines.append(f" SMPL    {start}   {end} ;")

    for name, values in series.items():
        var = str(name).strip().upper()
        if not var:
            continue
        # Base FM uses a padded 9-character field for the variable name.
        lines.append(f" LOAD {var.ljust(9)};")
        rendered_values = [_format_value(float(v)) for v in values]
        for idx in range(0, len(rendered_values), values_per_line):
            chunk = rendered_values[idx : idx + values_per_line]
            lines.append("   " + "  ".join(chunk))
        # Stock FM format terminates each LOAD block with a literal 'END' line.
        lines.append(" 'END' ")

    lines.append(" END;")
    return "\n".join(lines)


def write_fmdata_file(
    path: Path | str,
    *,
    sample_start: str,
    sample_end: str,
    series: Mapping[str, Sequence[float]],
    values_per_line: int = 4,
) -> Path:
    """Write ``fmdata.txt`` content to disk."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        render_fmdata_text(
            sample_start=sample_start,
            sample_end=sample_end,
            series=series,
            values_per_line=values_per_line,
        )
        + "\n",
        encoding="utf-8",
    )
    return target


def write_fmdata(
    *,
    sample_start: str,
    sample_end: str,
    series: dict[str, list[float]],
    newline: Literal["\n", "\r\n"],
    path: Path,
) -> None:
    """Write `fmdata.txt` content using the requested newline convention."""
    text = render_fmdata_text(
        sample_start=sample_start,
        sample_end=sample_end,
        series=series,
        values_per_line=4,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.replace("\n", newline) + newline, encoding="utf-8")
