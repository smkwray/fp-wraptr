from __future__ import annotations

from pathlib import Path

from fp_wraptr.data.update_fred import apply_normalized_observations, merge_fmdata_observations
from fp_wraptr.io.fmdata_writer import render_fmdata_text
from fp_wraptr.io.input_parser import parse_fm_data_text


def _base_fmdata_text() -> str:
    return render_fmdata_text(
        sample_start="2024.1",
        sample_end="2025.4",
        series={
            "UR": [1.0, 2.0, 3.0, 4.0, 5.0, -99.0, -99.0, -99.0],
            "PCY": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0],
        },
        values_per_line=4,
    )


def _observations() -> dict[str, dict[str, float]]:
    return {
        "UR": {"2024.2": 22.0, "2025.3": 33.0, "2026.1": 44.0},
        "PCY": {"2024.2": 111.0, "2025.4": 222.0, "2026.1": 333.0},
        "MISSING": {"2025.1": 1.0},
    }


def test_merge_fmdata_observations_extend_only_and_replace_history() -> None:
    base = _base_fmdata_text()

    extend_text, extend_report = merge_fmdata_observations(
        base_fmdata_text=base,
        observations=_observations(),
        replace_history=False,
    )
    extend_parsed = parse_fm_data_text(extend_text)
    ur = extend_parsed["series"]["UR"][-1]["values"]
    pcy = extend_parsed["series"]["PCY"][-1]["values"]

    assert extend_report["sample_end_before"] == "2025.4"
    assert extend_report["sample_end_after"] == "2026.1"
    assert extend_report["extended_periods"] == 1
    assert extend_report["updated_points"] == 3
    assert extend_report["missing_vars"] == ["MISSING"]
    assert ur[1] == 2.0
    assert ur[6] == 33.0
    assert ur[8] == 44.0
    assert pcy[1] == 11.0
    assert pcy[7] == 17.0
    assert pcy[8] == 333.0

    replace_text, replace_report = merge_fmdata_observations(
        base_fmdata_text=base,
        observations=_observations(),
        replace_history=True,
    )
    replace_parsed = parse_fm_data_text(replace_text)
    ur_replace = replace_parsed["series"]["UR"][-1]["values"]
    pcy_replace = replace_parsed["series"]["PCY"][-1]["values"]

    assert replace_report["updated_points"] == 6
    assert ur_replace[1] == 22.0
    assert ur_replace[6] == 33.0
    assert ur_replace[8] == 44.0
    assert pcy_replace[1] == 111.0
    assert pcy_replace[7] == 222.0
    assert pcy_replace[8] == 333.0


def test_apply_normalized_observations_writes_output_and_report(tmp_path: Path) -> None:
    base_path = tmp_path / "base_fmdata.txt"
    out_path = tmp_path / "updated_fmdata.txt"
    base_path.write_text(_base_fmdata_text(), encoding="utf-8")

    report = apply_normalized_observations(
        base_fmdata_path=base_path,
        out_fmdata_path=out_path,
        observations=_observations(),
        replace_history=False,
    )

    assert out_path.exists()
    parsed = parse_fm_data_text(out_path.read_text(encoding="utf-8"))
    assert parsed["sample_end"] == "2026.1"
    assert report["updated_points"] == 3
    assert report["extended_periods"] == 1
    assert report["missing_vars"] == ["MISSING"]
    assert report["observation_variable_count"] == 3
