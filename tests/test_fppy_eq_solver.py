from __future__ import annotations

import pandas as pd

from fppy.eq_solver import EqSpec, EqTerm, apply_eq_backfill, parse_eq_specs_from_fmout_text
from fppy.parser import FPCommand, FPCommandRecord


def _record(line_number: int, command: FPCommand, statement: str) -> FPCommandRecord:
    return FPCommandRecord(
        line_number=line_number,
        line=statement,
        command=command,
        statement=statement,
        raw_lines=(statement,),
        terminated=True,
    )


def test_parse_eq_specs_header_equation_first() -> None:
    specs = parse_eq_specs_from_fmout_text(
        "\n".join([
            "EQUATION 12 LCSZ",
            "  1.0 1 LYDZ",
            "",
        ])
    )
    assert "LCSZ" in specs
    assert specs["LCSZ"].equation_number == 12
    assert len(specs["LCSZ"].terms) == 1
    assert specs["LCSZ"].terms[0].variable == "LYDZ"
    assert specs["LCSZ"].terms[0].index == 1


def test_parse_eq_specs_header_lhs_first() -> None:
    specs = parse_eq_specs_from_fmout_text(
        "\n".join([
            "LCSZ EQUATION 12",
            "  1.0 1 LYDZ",
            "",
        ])
    )
    assert "LCSZ" in specs
    assert specs["LCSZ"].equation_number == 12
    assert len(specs["LCSZ"].terms) == 1
    assert specs["LCSZ"].terms[0].variable == "LYDZ"


def test_apply_eq_backfill_numpy_structural_read_cache_matches_default() -> None:
    frame = pd.DataFrame(
        {
            "B": [0.0, 0.0],
            "C": [5.0, 7.0],
            "Y": [0.0, 0.0],
        },
        index=["2025.1", "2025.2"],
    )
    specs = {
        "Y": EqSpec(
            lhs="Y",
            equation_number=1,
            terms=(EqTerm(variable="B", coefficient=1.0, lag=-1, index=1),),
        )
    }
    records = [
        _record(1, FPCommand.EQ, "EQ 1 ;"),
        _record(2, FPCommand.IDENT, "IDENT B = C ;"),
    ]

    default_result = apply_eq_backfill(records, frame, specs, period_sequential=True)
    cached_result = apply_eq_backfill(
        records,
        frame,
        specs,
        period_sequential=True,
        period_sequential_eq_structural_read_cache="numpy_columns",
    )

    assert default_result.frame.equals(cached_result.frame)
    assert cached_result.frame.loc["2025.2", "Y"] == 5.0
    assert cached_result.structural_read_cache_mode == "numpy_columns"
    assert cached_result.structural_read_cache_column_count >= 3
    assert cached_result.structural_scalar_reads_cached > 0
    assert cached_result.structural_scalar_reads_frame == 0
