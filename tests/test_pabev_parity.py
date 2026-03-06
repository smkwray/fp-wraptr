from __future__ import annotations

from pathlib import Path

import pytest

from fppy.pabev_parity import parse_pabev


def test_parse_pabev_accepts_fortran_no_e_exponent(tmp_path: Path) -> None:
    path = tmp_path / "PABEV.TXT"
    path.write_text(
        "SMPL 2025.4 2025.4;\nLOAD RM;\n0.26630469439+102\n'END'\n",
        encoding="utf-8",
    )

    periods, series = parse_pabev(path)
    assert len(periods) == 1
    assert "RM" in series
    assert series["RM"][0] == pytest.approx(0.26630469439e102)
