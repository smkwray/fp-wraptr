from __future__ import annotations

import json

from typer.testing import CliRunner

from fp_wraptr.cli import app

runner = CliRunner()


def test_data_check_mapping_json(monkeypatch):
    original_find_spec = __import__("importlib").util.find_spec

    def _fake_find_spec(name, *args, **kwargs):
        if name == "fredapi":
            return object()
        return original_find_spec(name, *args, **kwargs)

    monkeypatch.setattr("fp_wraptr.cli.importlib.util.find_spec", _fake_find_spec)

    def _fake_check_mapping_against_fmdata(**kwargs):
        assert kwargs["sources"] == ["fred", "bea"]
        return {
            "rows": [{"variable": "GDP", "source": "fred", "overlap_count": 4}],
            "skipped": [{"variable": "AFT", "reason": "unsupported_frequency"}],
            "sources": ["fred", "bea"],
        }

    monkeypatch.setattr(
        "fp_wraptr.data.check_mapping.check_mapping_against_fmdata",
        _fake_check_mapping_against_fmdata,
    )

    result = runner.invoke(
        app,
        [
            "data",
            "check-mapping",
            "--sources",
            "fred",
            "--sources",
            "bea",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["variable"] == "GDP"
    assert payload["skipped"][0]["reason"] == "unsupported_frequency"


def test_data_check_fred_mapping_json_backward_compat(monkeypatch):
    original_find_spec = __import__("importlib").util.find_spec

    def _fake_find_spec(name, *args, **kwargs):
        if name == "fredapi":
            return object()
        return original_find_spec(name, *args, **kwargs)

    monkeypatch.setattr("fp_wraptr.cli.importlib.util.find_spec", _fake_find_spec)

    def _fake_check_mapping_against_fmdata(**kwargs):
        assert kwargs["sources"] == ["fred"]
        return {
            "rows": [{"variable": "GDP", "series_id": "GDPC1", "overlap_count": 4}],
            "skipped": [
                {"variable": "X1", "reason": "unmapped"},
                {"variable": "X2", "reason": "missing_series_id"},
            ],
        }

    monkeypatch.setattr(
        "fp_wraptr.data.check_mapping.check_mapping_against_fmdata",
        _fake_check_mapping_against_fmdata,
    )

    result = runner.invoke(app, ["data", "check-fred-mapping", "--format", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["variable"] == "GDP"
    assert sorted(payload["skipped"]) == ["X1", "X2"]
