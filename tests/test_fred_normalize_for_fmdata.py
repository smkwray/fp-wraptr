from __future__ import annotations

import pandas as pd

from fp_wraptr.data.source_map import DataSource, SourceMap
from fp_wraptr.fred.normalize_for_fmdata import normalize_fred_for_fmdata


def test_normalize_quarterly_annual_rate_div4() -> None:
    # 400.0 SAAR -> 100.0 per-quarter after /4.
    observations = pd.DataFrame(
        {"GDPC1": [400.0]},
        index=pd.to_datetime(["2025-01-01"]),
    )
    mapping = SourceMap({
        "GDPR": DataSource(
            fp_variable="GDPR",
            source="fred",
            series_id="GDPC1",
            frequency="Q",
            annual_rate=True,
        )
    })
    out = normalize_fred_for_fmdata(
        observations=observations,
        source_map=mapping,
        variables=["GDPR"],
        start_period="2025.1",
        end_period="2025.1",
    )
    assert out["GDPR"].loc["2025.1"] == 100.0


def test_normalize_monthly_annual_rate_div12_then_sum() -> None:
    # 1200.0 annual rate each month -> 100.0 monthly -> 300.0 for quarter sum.
    observations = pd.DataFrame(
        {"FLOW": [1200.0, 1200.0, 1200.0]},
        index=pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"]),
    )
    mapping = SourceMap({
        "X": DataSource(
            fp_variable="X",
            source="fred",
            series_id="FLOW",
            frequency="M",
            annual_rate=True,
        )
    })
    out = normalize_fred_for_fmdata(
        observations=observations,
        source_map=mapping,
        variables=["X"],
        start_period="2025.1",
        end_period="2025.1",
    )
    assert out["X"].loc["2025.1"] == 300.0


def test_normalize_monthly_aggregation_mean_end_sum() -> None:
    observations = pd.DataFrame(
        {"M": [1.0, 2.0, 3.0]},
        index=pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"]),
    )

    mapping_mean = SourceMap({
        "A": DataSource(
            fp_variable="A", source="fred", series_id="M", frequency="M", aggregation="mean"
        ),
        "B": DataSource(
            fp_variable="B", source="fred", series_id="M", frequency="M", aggregation="end"
        ),
        "C": DataSource(
            fp_variable="C", source="fred", series_id="M", frequency="M", aggregation="sum"
        ),
    })

    out = normalize_fred_for_fmdata(
        observations=observations,
        source_map=mapping_mean,
        variables=["A", "B", "C"],
        start_period="2025.1",
        end_period="2025.1",
    )
    assert out["A"].loc["2025.1"] == 2.0
    assert out["B"].loc["2025.1"] == 3.0
    assert out["C"].loc["2025.1"] == 6.0


def test_normalize_applies_scale_and_offset() -> None:
    observations = pd.DataFrame(
        {"UNRATE": [5.0, 5.0, 5.0]},
        index=pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"]),
    )
    mapping = SourceMap({
        "UR": DataSource(
            fp_variable="UR",
            source="fred",
            series_id="UNRATE",
            frequency="M",
            scale=0.01,
            offset=0.0,
        )
    })
    out = normalize_fred_for_fmdata(
        observations=observations,
        source_map=mapping,
        variables=["UR"],
        start_period="2025.1",
        end_period="2025.1",
    )
    assert out["UR"].loc["2025.1"] == 0.05
