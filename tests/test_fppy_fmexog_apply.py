from __future__ import annotations

import pandas as pd

from fppy.io.legacy_data import parse_fmexog_text
from fppy.mini_run import _apply_fmexog_rows


def test_changevar_scalar_chgsamepct_single_period_uses_scalar_semantics() -> None:
    # When SMPL window is a single period, scalar CHANGEVAR instructions still
    # need to apply method semantics (not be misread as a 1-value vector).
    base = pd.DataFrame(
        {
            "TBGQ": [40.0, 40.0],
        },
        index=pd.Index(["2025.3", "2025.4"], name="smpl"),
    )
    fmexog = "\n".join([
        "SMPL 2025.4 2025.4;",
        "CHANGEVAR;",
        "TBGQ CHGSAMEPCT",
        "0.01",
        ";",
        "RETURN;",
        "",
    ])
    rows = parse_fmexog_text(fmexog)
    out = _apply_fmexog_rows(base, rows)
    assert float(out.loc["2025.4", "TBGQ"]) == 40.0 * 1.01


def test_changevar_vector_without_header_semicolon_parses_all_values() -> None:
    base = pd.DataFrame(index=pd.Index(["2025.4", "2026.1"], name="smpl"))
    fmexog = "\n".join([
        "SMPL 2025.4 2026.1;",
        "CHANGEVAR;",
        "JGPHASE ADDDIFABS",
        "0.2",
        "0.4",
        "JGCOLA ADDDIFABS",
        "0.0",
        "0.1",
        ";",
        "RETURN;",
        "",
    ])

    rows = parse_fmexog_text(fmexog)
    jgphase = rows.loc[rows["variable"] == "JGPHASE"].iloc[0]
    assert bool(jgphase["is_vector"]) is True
    assert tuple(jgphase["values"]) == (0.2, 0.4)

    out = _apply_fmexog_rows(base, rows)
    assert float(out.loc["2025.4", "JGPHASE"]) == 0.2
    assert float(out.loc["2026.1", "JGPHASE"]) == 0.4
    assert float(out.loc["2025.4", "JGCOLA"]) == 0.0
    assert float(out.loc["2026.1", "JGCOLA"]) == 0.1
