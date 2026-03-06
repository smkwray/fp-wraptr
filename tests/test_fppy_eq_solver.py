from __future__ import annotations

from fppy.eq_solver import parse_eq_specs_from_fmout_text


def test_parse_eq_specs_header_equation_first() -> None:
    specs = parse_eq_specs_from_fmout_text(
        "\n".join(
            [
                "EQUATION 12 LCSZ",
                "  1.0 1 LYDZ",
                "",
            ]
        )
    )
    assert "LCSZ" in specs
    assert specs["LCSZ"].equation_number == 12
    assert len(specs["LCSZ"].terms) == 1
    assert specs["LCSZ"].terms[0].variable == "LYDZ"
    assert specs["LCSZ"].terms[0].index == 1


def test_parse_eq_specs_header_lhs_first() -> None:
    specs = parse_eq_specs_from_fmout_text(
        "\n".join(
            [
                "LCSZ EQUATION 12",
                "  1.0 1 LYDZ",
                "",
            ]
        )
    )
    assert "LCSZ" in specs
    assert specs["LCSZ"].equation_number == 12
    assert len(specs["LCSZ"].terms) == 1
    assert specs["LCSZ"].terms[0].variable == "LYDZ"
