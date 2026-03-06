"""Tests for BLS ingestion helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from fp_wraptr.bls import ingest


def test_fetch_series_accepts_request_succeeded_status(tmp_path, monkeypatch):
    def _fake_post_json(_payload, *, timeout_seconds=30):
        return {
            "status": "REQUEST_SUCCEEDED",
            "Results": {
                "series": [
                    {
                        "seriesID": "LNS11000000",
                        "data": [{"year": "2025", "period": "M12", "value": "4.1"}],
                    },
                ],
            },
        }

    monkeypatch.setattr("fp_wraptr.bls.ingest._post_json", _fake_post_json)

    df = ingest.fetch_series(
        ingest.BlsSeriesRequest(series_ids=["LNS11000000"], start_year=2025, end_year=2025),
        cache_dir=tmp_path / "cache",
        respect_tos=False,
    )

    assert list(df.columns) == ["LNS11000000"]
    assert df.loc[pd.Timestamp("2025-12-01"), "LNS11000000"] == 4.1


def test_fetch_series_raises_on_non_success_status(tmp_path, monkeypatch):
    def _fake_post_json(_payload, *, timeout_seconds=30):
        return {
            "status": "REQUEST_FAILED",
            "message": ["bad request"],
            "Results": {"series": []},
        }

    monkeypatch.setattr("fp_wraptr.bls.ingest._post_json", _fake_post_json)

    with pytest.raises(ingest.BlsApiError, match="REQUEST_FAILED"):
        ingest.fetch_series(
            ingest.BlsSeriesRequest(series_ids=["LNS11000000"], start_year=2025, end_year=2025),
            cache_dir=tmp_path / "cache",
            respect_tos=False,
        )
