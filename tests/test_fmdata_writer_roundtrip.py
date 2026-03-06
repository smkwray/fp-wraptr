from __future__ import annotations

import re
from pathlib import Path

from fp_wraptr.io.fmdata_writer import write_fmdata
from fp_wraptr.io.input_parser import parse_fm_data_text


def test_write_fmdata_roundtrip_and_crlf(tmp_path: Path) -> None:
    out_path = tmp_path / "fmdata.txt"
    write_fmdata(
        sample_start="2024.1",
        sample_end="2024.2",
        series={
            "X": [0.0, 258.2],
            "Y": [-99.0, 0.043],
        },
        newline="\r\n",
        path=out_path,
    )

    raw = out_path.read_bytes()
    assert b"\r\n" in raw

    text = raw.decode("utf-8", errors="replace")
    # Stock FM format ends each LOAD block with a literal 'END' marker line.
    assert text.count("'END'") >= 2
    assert "END;" in text
    parsed = parse_fm_data_text(text)
    assert parsed["sample_start"] == "2024.1"
    assert parsed["sample_end"] == "2024.2"
    assert parsed["series"]["X"][-1]["values"] == [0.0, 258.2]
    assert parsed["series"]["Y"][-1]["values"] == [-99.0, 0.043]

    # Spot-check that we emitted the 0.xxxxxE+YY style.
    first_value_line = next(line for line in text.splitlines() if re.search(r"E[+-]\d\d", line))
    assert re.search(r"0\.\d{11}E[+-]\d\d", first_value_line)
