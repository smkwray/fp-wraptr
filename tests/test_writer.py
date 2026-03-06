"""Tests for FP writer helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from fp_wraptr.io.input_parser import parse_fmexog, parse_fmexog_text


def test_write_exogenous_scalar_value(tmp_path: Path) -> None:
    output = tmp_path / "fmexog_scalar.txt"

    from fp_wraptr.io.writer import write_exogenous_file

    write_exogenous_file(
        variables={
            "YS": {
                "method": "CHGSAMEPCT",
                "value": 0.008,
            },
            "RS": {
                "method": "SAMEVALUE",
                "value": 4.0,
            },
        },
        sample_start="2025.4",
        sample_end="2029.4",
        output_path=output,
    )

    parsed = parse_fmexog(output)
    values = {item["variable"]: item["values"] for item in parsed["changes"]}

    assert values["YS"] == [0.008]
    assert values["RS"] == [4.0]
    assert parsed["sample_start"] == "2025.4"
    assert parsed["sample_end"] == "2029.4"


def test_write_exogenous_series_value_by_period(tmp_path: Path) -> None:
    output = tmp_path / "fmexog_series.txt"

    from fp_wraptr.io.writer import write_exogenous_file

    write_exogenous_file(
        variables={
            "PIM": {
                "method": "CHGSAMEABS",
                "value": [
                    ("2025.4", 0.25),
                    ("2026.1", 0.20),
                    ("2026.2", 0.10),
                ],
            }
        },
        sample_start="2025.4",
        sample_end="2029.4",
        output_path=output,
    )

    parsed = parse_fmexog(output)
    changes = [item for item in parsed["changes"] if item["variable"] == "PIM"]

    assert len(changes) == 3
    assert [change["sample_start"] for change in changes] == [
        "2025.4",
        "2026.1",
        "2026.2",
    ]
    assert [change["values"][0] for change in changes] == [0.25, 0.20, 0.10]


def test_parse_fmexog_handles_integer_vector_values() -> None:
    parsed = parse_fmexog_text(
        "\n".join(
            [
                "SMPL 2025.4 2026.1;",
                "CHANGEVAR;",
                "JGSWITCH SAMEVALUE",
                "1",
                "JGPHASE ADDDIFABS",
                "0",
                "1",
                ";",
                "RETURN;",
                "",
            ]
        )
    )

    changes = {item["variable"]: item["values"] for item in parsed["changes"]}
    assert changes["JGSWITCH"] == [1.0]
    assert changes["JGPHASE"] == [0.0, 1.0]


def test_write_exogenous_invalid_method_is_supported(tmp_path: Path) -> None:
    output = tmp_path / "fmexog_invalid.txt"
    from fp_wraptr.io.writer import write_exogenous_file

    write_exogenous_file(
        variables={"BAD": {"method": "INVALID", "value": 1.0}},
        sample_start="2025.4",
        sample_end="2029.4",
        output_path=output,
    )

    text = output.read_text(encoding="utf-8")
    assert "BAD INVALID" in text


def test_write_exogenous_empty_overrides(tmp_path: Path) -> None:
    output = tmp_path / "fmexog_empty.txt"
    from fp_wraptr.io.writer import write_exogenous_file

    write_exogenous_file(
        variables={},
        sample_start="2025.4",
        sample_end="2029.4",
        output_path=output,
    )

    text = output.read_text(encoding="utf-8")
    assert text.startswith("SMPL 2025.4 2029.4;\nCHANGEVAR;")
    assert ";\nRETURN;" in text


def test_write_exogenous_override_preserves_crlf_newlines(tmp_path: Path) -> None:
    base = tmp_path / "fmexog.txt"
    # Simulate FM template newline style (CRLF) for fp.exe compatibility.
    base.write_bytes(b"SMPL 2025.4 2029.4;\r\nCHANGEVAR;\r\nRETURN;\r\n")
    output = tmp_path / "fmexog_override.txt"

    from fp_wraptr.io.writer import write_exogenous_override_file

    write_exogenous_override_file(
        base_fmexog=base,
        variables={"TBGQ": {"method": "CHGSAMEPCT", "value": 0.01}},
        sample_start="2025.4",
        sample_end="2029.4",
        output_path=output,
    )

    data = output.read_bytes()
    assert b"\r\n" in data
    assert b"\r" in data
    # No bare LF outside CRLF sequences.
    assert b"\n" not in data.replace(b"\r\n", b"")


def test_patch_input_file_command_patch_updates_existing_param(tmp_path: Path) -> None:
    base = tmp_path / "fminput.txt"
    base.write_text("SETUPSOLVE MINITERS=3 MAXCHECK=30 ;\n", encoding="utf-8")
    output = tmp_path / "patched.txt"

    from fp_wraptr.io.writer import patch_input_file

    patch_input_file(
        base_input=base,
        overrides={"cmd:SETUPSOLVE.MINITERS": "40"},
        output_path=output,
    )

    patched = output.read_text(encoding="utf-8")
    assert "MINITERS=40" in patched
    assert "MAXCHECK=30" in patched


def test_patch_input_file_command_patch_appends_missing_param(tmp_path: Path) -> None:
    base = tmp_path / "fminput.txt"
    base.write_text("SETUPSOLVE MINITERS=3 ;\n", encoding="utf-8")
    output = tmp_path / "patched.txt"

    from fp_wraptr.io.writer import patch_input_file

    patch_input_file(
        base_input=base,
        overrides={"cmd:SETUPSOLVE.MAXCHECK": "99"},
        output_path=output,
    )

    patched = output.read_text(encoding="utf-8")
    assert "MINITERS=3" in patched
    assert "MAXCHECK=99" in patched


def test_patch_input_file_command_patch_occurrence_index(tmp_path: Path) -> None:
    base = tmp_path / "fminput.txt"
    base.write_text(
        "SETUPSOLVE MINITERS=3 ;\nSETUPSOLVE MINITERS=4 ;\n",
        encoding="utf-8",
    )
    output = tmp_path / "patched.txt"

    from fp_wraptr.io.writer import patch_input_file

    patch_input_file(
        base_input=base,
        overrides={"cmd:SETUPSOLVE[1].MINITERS": "77"},
        output_path=output,
    )

    patched = output.read_text(encoding="utf-8")
    assert "SETUPSOLVE MINITERS=3 ;" in patched
    assert "SETUPSOLVE MINITERS=77 ;" in patched


def test_patch_input_file_command_patch_missing_target_raises(tmp_path: Path) -> None:
    base = tmp_path / "fminput.txt"
    base.write_text("SMPL 2025.4 2029.4;\n", encoding="utf-8")
    output = tmp_path / "patched.txt"

    from fp_wraptr.io.writer import patch_input_file

    with pytest.raises(ValueError, match="Command patch target not found"):
        patch_input_file(
            base_input=base,
            overrides={"cmd:SETUPSOLVE.MAXCHECK": "30"},
            output_path=output,
        )
