from __future__ import annotations

from pathlib import Path

import pandas as pd

from fppy.input_tree import parse_fminput_tree_file
from fppy.mini_run import run_mini_run
from fppy.parser import FPCommand, parse_fminput


def test_parser_collects_changevar_block() -> None:
    records = parse_fminput(
        "\n".join(
            [
                "SMPL 2025.4 2025.4;",
                "CHANGEVAR;",
                "X SAMEVALUE",
                "2",
                ";",
                "QUIT;",
                "",
            ]
        )
    )
    changevar = [r for r in records if r.command == FPCommand.CHANGEVAR]
    assert len(changevar) == 1
    assert "X SAMEVALUE" in changevar[0].statement


def test_mini_run_applies_inline_changevar_block(tmp_path) -> None:
    deck = "\n".join(
        [
            "SMPL 2025.4 2025.4;",
            "CHANGEVAR;",
            "X SAMEVALUE",
            "2",
            ";",
            "QUIT;",
            "",
        ]
    )
    records = parse_fminput(deck)
    data = pd.DataFrame(index=pd.Index(["2025.4"], name="smpl"))
    result = run_mini_run(records, data=data, runtime_base_dir=tmp_path)
    assert float(result.frame.loc["2025.4", "X"]) == 2.0


def test_parse_fminput_tree_expands_deck_includes(tmp_path) -> None:
    base_dir = Path(tmp_path)
    (base_dir / "inc.txt").write_text(
        "\n".join(
            [
                "SMPL 2025.4 2025.4;",
                "CREATE X=2;",
                "QUIT;",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (base_dir / "fminput.txt").write_text(
        "\n".join(
            [
                "SMPL 2025.4 2025.4;",
                "INPUT FILE=inc.txt;",
                "GENR Y=X+1;",
                "QUIT;",
                "",
            ]
        ),
        encoding="utf-8",
    )

    records = parse_fminput_tree_file(base_dir / "fminput.txt", runtime_base_dir=base_dir)

    assert any(record.command == FPCommand.CREATE and "X=2" in record.statement for record in records)
    assert not any(record.command == FPCommand.INPUT and "inc.txt" in record.statement for record in records)
