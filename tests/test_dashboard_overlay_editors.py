from __future__ import annotations

from pathlib import Path

from fp_wraptr.dashboard.overlay_editors import available_scenario_cards
from fp_wraptr.dashboard.scenario_tools import ScenarioInputPreflight
from fp_wraptr.scenarios.config import ScenarioConfig


def test_available_scenario_cards_discovers_overlay_backed_deck_cards() -> None:
    config = ScenarioConfig(
        name="pse_card_test",
        fp_home=Path("FM"),
        input_file="psebase.txt",
        input_overlay_dir=Path("projects_local/pse2025"),
        forecast_start="2025.4",
        forecast_end="2029.4",
    )
    preflight = ScenarioInputPreflight(
        input_file="psebase.txt",
        fp_home=Path("FM"),
        overlay_dir=Path("projects_local/pse2025"),
        entry_source_path=Path("projects_local/pse2025/psebase.txt"),
        entry_source_kind="overlay",
        include_files=("pse_common.txt", "ptcoef.txt", "intgadj.txt"),
    )

    cards = available_scenario_cards(config, preflight)

    assert [card.card_id for card in cards] == [
        "deck.ptcoef",
        "deck.jg_policy_create",
        "series.intgadj",
    ]


def test_available_scenario_cards_hides_cards_without_overlay_context() -> None:
    config = ScenarioConfig(
        name="baseline",
        fp_home=Path("FM"),
        input_file="fminput.txt",
        forecast_start="2025.4",
        forecast_end="2029.4",
    )
    preflight = ScenarioInputPreflight(
        input_file="fminput.txt",
        fp_home=Path("FM"),
        overlay_dir=None,
        entry_source_path=Path("FM/fminput.txt"),
        entry_source_kind="fp_home",
        include_files=("fminput.txt",),
    )

    assert available_scenario_cards(config, preflight) == []
