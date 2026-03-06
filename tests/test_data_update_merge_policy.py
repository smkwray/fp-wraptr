from __future__ import annotations

import pandas as pd
import pytest

from fp_wraptr.data.update_fmdata_from_observations import update_fmdata_from_observations
from fp_wraptr.io.fmdata_writer import render_fmdata_text
from fp_wraptr.io.input_parser import parse_fm_data_text


def _base_parsed() -> dict:
    text = render_fmdata_text(
        sample_start="2024.1",
        sample_end="2024.4",
        series={
            "X": [1.0, 2.0, 3.0, 4.0],
            "Y": [10.0, 11.0, 12.0, 13.0],
        },
        values_per_line=4,
    )
    return parse_fm_data_text(text)


def test_extend_sample_requires_carry_forward_when_unmapped() -> None:
    parsed = _base_parsed()
    obs = {"X": pd.Series({"2025.1": 99.0})}
    with pytest.raises(RuntimeError):
        update_fmdata_from_observations(
            parsed_fmdata=parsed,
            observations=obs,
            end_period="2025.1",
            extend_sample=True,
            allow_carry_forward=False,
        )


def test_extend_sample_with_carry_forward() -> None:
    parsed = _base_parsed()
    obs = {"X": pd.Series({"2025.1": 99.0})}
    result = update_fmdata_from_observations(
        parsed_fmdata=parsed,
        observations=obs,
        end_period="2025.1",
        extend_sample=True,
        allow_carry_forward=True,
    )

    assert result.sample_end_after == "2025.1"
    assert result.series["X"][-1] == 99.0
    # Y is carried forward from 2024.4.
    assert result.series["Y"][-1] == 13.0
    assert result.report["updated_cells"] >= 1
    assert result.report["carried_cells"] >= 1


def test_replace_history_updates_in_range() -> None:
    parsed = _base_parsed()
    obs = {"X": pd.Series({"2024.2": 222.0})}
    result = update_fmdata_from_observations(
        parsed_fmdata=parsed,
        observations=obs,
        end_period="2024.4",
        replace_history=True,
        extend_sample=False,
    )
    # 2024.2 is index 1 when sample starts at 2024.1
    assert result.series["X"][1] == 222.0


def test_duplicate_load_blocks_are_tolerated_if_identical() -> None:
    parsed = _base_parsed()
    block = dict(parsed["blocks"][0])
    block["values"] = list(block["values"])
    parsed["blocks"].append(block)

    result = update_fmdata_from_observations(
        parsed_fmdata=parsed,
        observations={},
        end_period="2024.4",
        replace_history=False,
        extend_sample=False,
    )
    assert result.series["X"][0] == 1.0


def test_duplicate_load_blocks_raise_if_non_identical() -> None:
    parsed = _base_parsed()
    block = dict(parsed["blocks"][0])
    values = list(block["values"])
    values[0] = values[0] + 1.0
    block["values"] = values
    parsed["blocks"].append(block)

    with pytest.raises(RuntimeError):
        update_fmdata_from_observations(
            parsed_fmdata=parsed,
            observations={},
            end_period="2024.4",
            replace_history=False,
            extend_sample=False,
        )
