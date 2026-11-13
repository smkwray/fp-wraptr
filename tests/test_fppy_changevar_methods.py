"""CHANGEVAR ADD*/CHGDIF* methods must adjust existing values the way fp.exe does.

fp.for GET1 defines ADD* as changes to the existing value Y(J) and CHGDIF* as
per-period changes chained from Y(J-1). Expected values below were checked
against fp.exe output on the stock model. Treating these methods as plain
replacement silently drops scenario shocks: for example, a "double PIM" deck that
adds PIM's own level with ADDDIFABS would leave PIM at its baseline level.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fppy.io.legacy_data import parse_fmexog_text
from fppy.mini_run import _apply_fmexog_rows


def _apply(body: list[str], base_values: list[float]) -> list[float]:
    base = pd.DataFrame(
        {"X": base_values},
        index=pd.Index(["2025.4", "2026.1", "2026.2"], name="smpl"),
    )
    text = "\n".join(["SMPL 2026.1 2026.2;", "CHANGEVAR;", *body, ";", "RETURN;", ""])
    out = _apply_fmexog_rows(base, parse_fmexog_text(text))
    assert float(out.loc["2025.4", "X"]) == base_values[0]  # outside the SMPL window
    return [float(out.loc["2026.1", "X"]), float(out.loc["2026.2", "X"])]


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        # Y(J) = Y(J) + XC
        (["X ADDSAMEABS", "1.0"], [101.0, 201.0]),
        # Y(J) = (1 + XPC) * Y(J)
        (["X ADDSAMEPCT", "0.1"], [110.0, 220.0]),
        # Y(J) = Y(J) + DUM(J)
        (["X ADDDIFABS", "1.0", "2.0"], [101.0, 202.0]),
        # Y(J) = (1 + DUM(J)) * Y(J)
        (["X ADDDIFPCT", "0.1", "0.2"], [110.0, 240.0]),
        # Y(J) = Y(J-1) + DUM(J)
        (["X CHGDIFABS", "1.0", "2.0"], [51.0, 53.0]),
        # Y(J) = (1 + DUM(J)) * Y(J-1)
        (["X CHGDIFPCT", "0.1", "0.1"], [55.0, 60.5]),
    ],
)
def test_relative_changevar_methods_follow_fp_semantics(body, expected) -> None:
    got = _apply(body, [50.0, 100.0, 200.0])
    assert got == pytest.approx(expected, rel=1e-12)


def test_single_period_adddifabs_adds_to_existing_level() -> None:
    # Per-period scenario blocks (`SMPL p p; X ADDDIFABS; v;`) add v to the value
    # already present; a 0.0 delta must leave the series untouched.
    base = pd.DataFrame(
        {"PIM": [1.10, 1.12]},
        index=pd.Index(["2025.4", "2026.1"], name="smpl"),
    )
    text = "\n".join([
        "SMPL 2026.1 2026.1;",
        "CHANGEVAR;",
        "PIM ADDDIFABS",
        "1.12",
        ";",
        "SMPL 2025.4 2025.4;",
        "CHANGEVAR;",
        "PIM ADDDIFABS",
        "0.0",
        ";",
        "RETURN;",
        "",
    ])
    out = _apply_fmexog_rows(base, parse_fmexog_text(text))
    assert float(out.loc["2026.1", "PIM"]) == pytest.approx(2.24)
    assert float(out.loc["2025.4", "PIM"]) == pytest.approx(1.10)


def test_line_without_method_reuses_previous_method_in_block() -> None:
    # fp.exe applies the previous line's option to a CHANGEVAR line that names
    # no option (checked on the stock model: `X SAMEVALUE 5` then `Y` / `0.1`
    # sets Y to 0.1). Defaulting to CHGSAMEPCT instead compounds Y by 10% per
    # period. The stock deck's `ISZQ` line follows a CHGSAMEPCT line, so both
    # readings agree there.
    base = pd.DataFrame(
        {"A": [0.0, 0.0, 0.0], "B": [10.0, 10.0, 10.0]},
        index=pd.Index(["2025.4", "2026.1", "2026.2"], name="smpl"),
    )
    text = "\n".join([
        "SMPL 2026.1 2026.2;",
        "CHANGEVAR;",
        "A SAMEVALUE",
        "5",
        "B",
        "0.1",
        ";",
        "RETURN;",
        "",
    ])
    out = _apply_fmexog_rows(base, parse_fmexog_text(text))
    assert out.loc["2026.1":"2026.2", "A"].tolist() == [5.0, 5.0]
    assert out.loc["2026.1":"2026.2", "B"].tolist() == [0.1, 0.1]


def test_digits_in_variable_name_are_not_read_as_its_value() -> None:
    # `POP1` alone on a line used to parse as value 1.0 for POP1, and the real
    # value line `0.5` as an instruction for a variable named "0.5".
    rows = parse_fmexog_text(
        "\n".join([
            "SMPL 2026.1 2026.2;",
            "CHANGEVAR;",
            "POP1 SAMEVALUE",
            "2",
            "POP2",
            "0.5",
            ";",
            "RETURN;",
            "",
        ])
    )
    assert rows["variable"].tolist() == ["POP1", "POP2"]
    assert [tuple(v) for v in rows["values"]] == [(2.0,), (0.5,)]
