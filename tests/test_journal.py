"""Tests for the run journal SQLite store."""

from __future__ import annotations

import pytest

from fp_wraptr.io.parser import ForecastVariable, FPOutputData
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import ScenarioResult
from fp_wraptr.storage.journal import JournalEntry, RunJournal, _parse_conditions


@pytest.fixture()
def journal(tmp_path):
    db = tmp_path / "test_runs.db"
    j = RunJournal(db_path=db)
    yield j
    j.close()


@pytest.fixture()
def sample_result(tmp_path):
    config = ScenarioConfig(name="test_scenario", track_variables=["GDPR", "UR"])
    output = FPOutputData(
        forecast_start="2025.4",
        forecast_end="2026.3",
        periods=["2025.3", "2025.4", "2026.1", "2026.2", "2026.3"],
        variables={
            "GDPR": ForecastVariable(
                var_id=1, name="GDPR", levels=[100.0, 101.0, 102.0, 103.0, 104.0]
            ),
            "UR": ForecastVariable(var_id=2, name="UR", levels=[4.0, 3.8, 3.6, 3.4, 3.2]),
        },
    )
    return ScenarioResult(
        config=config,
        output_dir=tmp_path / "run_out",
        parsed_output=output,
    )


def test_journal_create(journal):
    assert journal.db_path.exists()


def test_log_run(journal, sample_result):
    entry = journal.log_run(sample_result)
    assert entry.id is not None
    assert entry.scenario_name == "test_scenario"
    assert entry.key_outputs["GDPR"] == 104.0
    assert entry.key_outputs["UR"] == 3.2


def test_list_runs(journal, sample_result):
    journal.log_run(sample_result)
    journal.log_run(sample_result)

    runs = journal.list_runs(last=10)
    assert len(runs) == 2
    # Most recent first
    assert runs[0].id > runs[1].id


def test_list_runs_limit(journal, sample_result):
    for _ in range(5):
        journal.log_run(sample_result)

    runs = journal.list_runs(last=3)
    assert len(runs) == 3


def test_search_runs_gt(journal, sample_result):
    journal.log_run(sample_result)

    matches = journal.search_runs("GDPR > 100")
    assert len(matches) == 1

    no_matches = journal.search_runs("GDPR > 200")
    assert len(no_matches) == 0


def test_search_runs_lt(journal, sample_result):
    journal.log_run(sample_result)

    matches = journal.search_runs("UR < 4")
    assert len(matches) == 1


def test_search_runs_compound(journal, sample_result):
    journal.log_run(sample_result)

    matches = journal.search_runs("GDPR > 100 AND UR < 4")
    assert len(matches) == 1

    no_matches = journal.search_runs("GDPR > 100 AND UR > 10")
    assert len(no_matches) == 0


def test_get_run(journal, sample_result):
    entry = journal.log_run(sample_result)
    fetched = journal.get_run(entry.id)
    assert fetched is not None
    assert fetched.scenario_name == "test_scenario"


def test_get_run_missing(journal):
    assert journal.get_run(999) is None


def test_delete_run(journal, sample_result):
    entry = journal.log_run(sample_result)
    assert journal.delete_run(entry.id)
    assert journal.get_run(entry.id) is None


def test_delete_run_missing(journal):
    assert not journal.delete_run(999)


def test_parse_conditions():
    conds = _parse_conditions("UR > 5.0 AND PCY < 0")
    assert len(conds) == 2
    assert conds[0].variable == "UR"
    assert conds[0].operator == ">"
    assert conds[0].value == 5.0
    assert conds[1].variable == "PCY"
    assert conds[1].operator == "<"


def test_log_run_no_output(journal, tmp_path):
    config = ScenarioConfig(name="empty")
    result = ScenarioResult(config=config, output_dir=tmp_path / "empty_run")
    entry = journal.log_run(result)
    assert entry.n_variables == 0
    assert entry.key_outputs == {}


def test_journal_entry_defaults():
    entry = JournalEntry()
    assert entry.id is None
    assert entry.scenario_name == ""
    assert entry.key_outputs == {}
