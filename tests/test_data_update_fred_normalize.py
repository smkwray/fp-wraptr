from __future__ import annotations

import pandas as pd
import pytest

from fp_wraptr.data.source_map import DataSource
from fp_wraptr.fred.normalize import (
    FredNormalizeError,
    fp_period_from_timestamp,
    normalize_fred_frame_to_fp_periods,
)


def test_normalize_monthly_to_quarterly_mean() -> None:
    observations = pd.DataFrame(
        {"UNRATE": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]},
        index=pd.to_datetime([
            "2025-01-01",
            "2025-02-01",
            "2025-03-01",
            "2025-04-01",
            "2025-05-01",
            "2025-06-01",
        ]),
    )
    mappings = {
        "UR": DataSource(
            fp_variable="UR",
            source="fred",
            series_id="UNRATE",
            frequency="M",
        )
    }

    out = normalize_fred_frame_to_fp_periods(observations=observations, mappings=mappings)
    assert out["UR"] == {"2025.1": 2.0, "2025.2": 5.0}


def test_normalize_quarterly_identity() -> None:
    observations = pd.DataFrame(
        {"GDPC1": [100.0, 110.0]},
        index=pd.to_datetime(["2025-01-01", "2025-04-01"]),
    )
    mappings = {
        "GDPR": DataSource(
            fp_variable="GDPR",
            source="fred",
            series_id="GDPC1",
            frequency="Q",
        )
    }

    out = normalize_fred_frame_to_fp_periods(observations=observations, mappings=mappings)
    assert out["GDPR"] == {"2025.1": 100.0, "2025.2": 110.0}


def test_fp_period_mapping_q4() -> None:
    period = fp_period_from_timestamp(pd.Timestamp("2025-10-01"))
    assert period == "2025.4"


def test_normalize_annual_frequency_raises() -> None:
    observations = pd.DataFrame(
        {"A191RL1Q225SBEA": [1.0]},
        index=pd.to_datetime(["2025-01-01"]),
    )
    mappings = {
        "GDPR": DataSource(
            fp_variable="GDPR",
            source="fred",
            series_id="A191RL1Q225SBEA",
            frequency="A",
        )
    }

    with pytest.raises(FredNormalizeError, match="Unsupported frequency 'A'"):
        normalize_fred_frame_to_fp_periods(observations=observations, mappings=mappings)
