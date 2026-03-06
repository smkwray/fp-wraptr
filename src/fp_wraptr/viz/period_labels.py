"""Period label helpers for Plotly/Streamlit.

FP often uses period tokens like "2025.4". Plotly can misinterpret these as
numeric values, which leads to axis tick labels like "2,025.5". Converting
tokens to non-numeric strings (e.g. "2025Q4") forces categorical axes.
"""

from __future__ import annotations

import re

__all__ = ["format_period_label"]

_PERIOD_TOKEN_RE = re.compile(r"^(?P<year>\d{4})\.(?P<sub>\d+)$")


def format_period_label(token: str) -> str:
    """Format period tokens into non-numeric labels.

    Rules:
      - "YYYY.<1-4>" -> "YYYYQ<digit>"
      - "YYYY.<other>" -> "YYYYP<digit>"
      - Otherwise return the original token unchanged.
    """
    raw = str(token).strip()
    match = _PERIOD_TOKEN_RE.match(raw)
    if not match:
        return raw
    year = match.group("year")
    sub = match.group("sub")
    if sub in {"1", "2", "3", "4"}:
        return f"{year}Q{sub}"
    return f"{year}P{sub}"

