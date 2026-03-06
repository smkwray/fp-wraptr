"""Data source map — maps FP model variables to external data sources.

Provides lookup from FP variable names to their real-world data sources
(FRED, BEA/NIPA, BLS) for the data update pipeline.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from pydantic import BaseModel, Field

__all__ = ["DataSource", "SourceMap", "load_source_map"]

_SOURCE_MAP_PATH = Path(__file__).parent / "source_map.yaml"


class DataSource(BaseModel):
    """Metadata for a single FP variable's external data source."""

    fp_variable: str = Field(description="FP model variable name")
    description: str = Field(default="")
    source: str = Field(description="Primary source: fred, bea, bls, census, treas")
    series_id: str = Field(default="", description="FRED series ID (if source is fred)")
    frequency: str = Field(default="Q", description="Q=quarterly, M=monthly, A=annual")
    aggregation: str = Field(
        default="mean",
        description="Monthly-to-quarterly aggregation: mean, end, sum (only used when frequency=M).",
    )
    units: str = Field(default="")
    transform: str = Field(default="level", description="level, growth_rate, index, cumulative")
    annual_rate: bool = Field(
        default=False,
        description="Whether source values are annualized rates (for example SAAR flows).",
    )
    scale: float = Field(
        default=1.0,
        description="Multiplicative scale applied after aggregation and annual-rate conversion.",
    )
    offset: float = Field(
        default=0.0,
        description="Additive offset applied after scaling.",
    )
    bea_table: str = Field(default="", description="BEA/NIPA table ID")
    bea_line: int = Field(default=0, description="BEA/NIPA line number")
    fred_fallback: str = Field(default="", description="Fallback FRED series if primary is BEA")
    notes: str = Field(default="")
    raw_data_codes: list[str] = Field(
        default_factory=list,
        description="Optional raw-data R-codes linked to this FP variable",
    )
    window_start: str = Field(
        default="",
        description="Optional active window start quarter (for example 2008Q4).",
    )
    window_end: str = Field(
        default="",
        description="Optional active window end quarter (for example 2012Q2).",
    )
    outside_window_value: float | None = Field(
        default=None,
        description="Optional fill value applied outside active window.",
    )

    def annual_rate_divisor(self) -> int | None:
        """Return divisor for converting annualized-rate values to period flow."""
        if not self.annual_rate:
            return None
        frequency = self.frequency.upper()
        if frequency == "Q":
            return 4
        if frequency == "M":
            return 12
        if frequency == "A":
            return 1
        return None

    def normalization_guidance(self) -> dict[str, Any] | None:
        """Return deterministic normalization rules for downstream consumers."""
        divisor = self.annual_rate_divisor()
        if divisor is None:
            return None

        frequency = self.frequency.upper()
        if frequency == "Q":
            return {
                "annual_rate": True,
                "annual_rate_divisor": 4,
                "per_period_formula": "value / 4",
                "quarterly_flow_formula": "quarterly_flow = value / 4",
                "requires_temporal_disaggregation": False,
                "notes": "Quarterly annualized-rate flow (for example SAAR).",
            }

        if frequency == "M":
            return {
                "annual_rate": True,
                "annual_rate_divisor": 12,
                "per_period_formula": "value / 12",
                "quarterly_flow_formula": "quarterly_flow = sum(monthly_value / 12) over the quarter",
                "requires_temporal_disaggregation": False,
                "notes": "Monthly annualized-rate flow; de-annualize monthly before quarterly aggregation.",
            }

        return {
            "annual_rate": True,
            "annual_rate_divisor": 1,
            "per_period_formula": "value",
            "quarterly_flow_formula": "",
            "requires_temporal_disaggregation": True,
            "notes": "Annual frequency requires an explicit disaggregation rule before quarterly use.",
        }


class SourceMap:
    """Registry of FP variable → data source mappings."""

    def __init__(self, sources: dict[str, DataSource]) -> None:
        self._sources = {name.upper(): payload for name, payload in sources.items()}

    def __len__(self) -> int:
        return len(self._sources)

    def __contains__(self, var_name: str) -> bool:
        return var_name.upper() in self._sources

    def get(self, var_name: str) -> DataSource | None:
        """Look up a data source by FP variable name."""
        return self._sources.get(var_name.upper())

    def list_variables(self) -> list[str]:
        """Return all mapped variable names, sorted."""
        return sorted(self._sources.keys())

    def by_source(self, source: str) -> list[DataSource]:
        """Return all variables from a specific source (fred, bea, bls, etc.)."""
        return [ds for ds in self._sources.values() if ds.source == source]

    def fred_series_ids(self) -> dict[str, str]:
        """Return FP variable → FRED series ID mapping for all FRED-sourced vars."""
        result: dict[str, str] = {}
        for var_name, ds in self._sources.items():
            if ds.series_id:
                result[var_name] = ds.series_id
            elif ds.fred_fallback:
                result[var_name] = ds.fred_fallback
        return result

    def windowed_fred_entries(self) -> list[tuple[str, DataSource]]:
        """Return mapped FRED entries that declare window metadata."""
        entries: list[tuple[str, DataSource]] = []
        for var_name in self.list_variables():
            payload = self.get(var_name)
            if payload is None:
                continue
            if payload.source != "fred" or not payload.series_id:
                continue
            if (
                payload.window_start
                or payload.window_end
                or payload.outside_window_value is not None
            ):
                entries.append((var_name, payload))
        return entries

    def window_assumption_report(
        self,
        observations: pd.DataFrame,
        tolerance: float = 0.0,
    ) -> dict[str, Any]:
        """Audit windowed-series assumptions against observed data."""
        checks: list[dict[str, Any]] = []
        status_counts: dict[str, int] = {}
        for var_name, entry in self.windowed_fred_entries():
            record: dict[str, Any] = {
                "variable": var_name,
                "series_id": entry.series_id,
                "window_start": entry.window_start,
                "window_end": entry.window_end,
                "outside_window_value": entry.outside_window_value,
                "status": "unknown",
                "outside_points": 0,
                "outside_violations": 0,
                "max_abs_deviation": 0.0,
                "first_violation_date": "",
                "last_violation_date": "",
                "observation_start": "",
                "observation_end": "",
                "latest_observation_date": "",
                "latest_value": None,
            }

            if entry.series_id not in observations.columns:
                record["status"] = "series_missing"
                checks.append(record)
                status_counts["series_missing"] = status_counts.get("series_missing", 0) + 1
                continue

            series = pd.to_numeric(observations[entry.series_id], errors="coerce")
            series.index = pd.to_datetime(series.index, errors="coerce")
            series = series[series.index.notna()]
            non_missing = series.dropna()
            if non_missing.empty:
                record["status"] = "no_observations"
                checks.append(record)
                status_counts["no_observations"] = status_counts.get("no_observations", 0) + 1
                continue

            record["observation_start"] = non_missing.index.min().strftime("%Y-%m-%d")
            record["observation_end"] = non_missing.index.max().strftime("%Y-%m-%d")
            record["latest_observation_date"] = non_missing.index[-1].strftime("%Y-%m-%d")
            record["latest_value"] = float(non_missing.iloc[-1])

            mask = pd.Series(True, index=series.index)
            start_ts = _parse_quarter_start(entry.window_start)
            end_exclusive_ts = _parse_quarter_end_exclusive(entry.window_end)
            if start_ts is not None:
                mask = mask & (series.index >= start_ts)
            if end_exclusive_ts is not None:
                mask = mask & (series.index < end_exclusive_ts)

            outside_values = series[~mask].dropna()
            record["outside_points"] = int(outside_values.size)

            if entry.outside_window_value is None:
                record["status"] = "no_outside_fill_rule"
                checks.append(record)
                status_counts["no_outside_fill_rule"] = (
                    status_counts.get("no_outside_fill_rule", 0) + 1
                )
                continue

            deviations = (outside_values - float(entry.outside_window_value)).abs()
            violating = deviations[deviations > max(tolerance, 0.0)]
            record["outside_violations"] = int(violating.size)
            if not deviations.empty:
                record["max_abs_deviation"] = float(deviations.max())
            if not violating.empty:
                record["first_violation_date"] = violating.index.min().strftime("%Y-%m-%d")
                record["last_violation_date"] = violating.index.max().strftime("%Y-%m-%d")
                record["status"] = "violation"
            else:
                record["status"] = "ok"

            checks.append(record)
            status = str(record["status"])
            status_counts[status] = status_counts.get(status, 0) + 1

        violation_count = sum(1 for item in checks if item.get("status") == "violation")
        return {
            "series_checked": len(checks),
            "violation_count": violation_count,
            "status_breakdown": dict(sorted(status_counts.items())),
            "tolerance": max(tolerance, 0.0),
            "checks": checks,
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialize to JSON-compatible dict."""
        return {var: ds.model_dump() for var, ds in self._sources.items()}

    def coverage_report(self, variable_names: list[str]) -> dict[str, Any]:
        """Compute mapping coverage for a target set of FP variables."""
        population = sorted({name.upper() for name in variable_names if name})
        mapped = [name for name in population if name in self]
        missing = [name for name in population if name not in self]

        by_source: dict[str, int] = {}
        for name in mapped:
            payload = self.get(name)
            if payload is None:
                continue
            by_source[payload.source] = by_source.get(payload.source, 0) + 1

        population_count = len(population)
        mapped_count = len(mapped)
        coverage_pct = (
            round((mapped_count / population_count) * 100, 2) if population_count > 0 else 0.0
        )
        return {
            "population_count": population_count,
            "mapped_count": mapped_count,
            "missing_count": len(missing),
            "coverage_pct": coverage_pct,
            "mapped_by_source": dict(sorted(by_source.items())),
            "mapped_variables": mapped,
            "missing_variables": missing,
        }

    def quality_report(self, variable_names: list[str] | None = None) -> dict[str, Any]:
        """Report mapping quality issues for all or a subset of variables."""
        if variable_names is None:
            population = self.list_variables()
        else:
            wanted = {name.upper() for name in variable_names if name}
            population = [name for name in self.list_variables() if name in wanted]

        findings: list[dict[str, Any]] = []
        issue_counts: dict[str, int] = {}
        valid_sources = {"fred", "bea", "bls", "census", "treas"}
        valid_frequencies = {"Q", "M", "A"}
        valid_aggregations = {"mean", "end", "sum"}

        for name in population:
            entry = self.get(name)
            if entry is None:
                continue

            issues: list[str] = []
            if entry.source not in valid_sources:
                issues.append("invalid_source")
            if entry.frequency not in valid_frequencies:
                issues.append("invalid_frequency")
            if (
                entry.frequency == "M"
                and entry.aggregation.strip().lower() not in valid_aggregations
            ):
                issues.append("invalid_aggregation")
            if entry.source == "fred" and not (entry.series_id or entry.fred_fallback):
                issues.append("missing_series_id")
            if entry.source == "bea" and not (
                entry.bea_table or entry.series_id or entry.fred_fallback
            ):
                issues.append("missing_bea_locator")
            if not entry.description.strip():
                issues.append("missing_description")

            if not issues:
                continue

            for issue in issues:
                issue_counts[issue] = issue_counts.get(issue, 0) + 1

            findings.append({
                "variable": name,
                "source": entry.source,
                "frequency": entry.frequency,
                "issues": issues,
            })

        return {
            "population_count": len(population),
            "issue_count": len(findings),
            "clean_count": len(population) - len(findings),
            "issue_breakdown": dict(sorted(issue_counts.items())),
            "findings": findings,
        }

    def resolve_variable_sources(
        self,
        variable: str,
        dictionary: Any | None = None,
    ) -> dict[str, Any]:
        """Return merged source-map + dictionary raw-data context for one variable."""
        var_name = variable.upper()
        source_entry = self.get(var_name)
        payload: dict[str, Any] = {
            "variable": var_name,
            "mapping_status": "mapped" if source_entry is not None else "unmapped",
            "source_map_entry": source_entry.model_dump() if source_entry is not None else None,
            "normalization": source_entry.normalization_guidance()
            if source_entry is not None
            else None,
            "dictionary_raw_data_sources": [],
            "dictionary_raw_data_details": [],
        }
        if dictionary is None:
            return payload

        var_record = dictionary.get_variable(var_name)
        if var_record is None:
            payload["mapping_status"] = "variable_not_in_dictionary"
            return payload

        payload["dictionary_raw_data_sources"] = var_record.raw_data_sources
        raw_details_getter = getattr(dictionary, "raw_data_for_variable", None)
        if callable(raw_details_getter):
            payload["dictionary_raw_data_details"] = raw_details_getter(
                var_name,
                include_unresolved=True,
            )
        return payload


def load_source_map(path: Path | str | None = None) -> SourceMap:
    """Load the data source map from YAML.

    Args:
        path: Path to source map YAML. Defaults to the bundled source_map.yaml.

    Returns:
        SourceMap instance with all mapped variables.
    """
    if path is None:
        path = _SOURCE_MAP_PATH
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Source map file not found: {path}")

    try:
        with path.open(encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except Exception as exc:  # pragma: no cover - defensive parse guard
        raise RuntimeError(f"Failed to parse source map YAML: {exc}") from exc

    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise RuntimeError("Source map YAML must be a mapping of variable -> metadata.")
    sources: dict[str, DataSource] = {}
    for var_name, entry in raw.items():
        if isinstance(entry, dict):
            sources[var_name.upper()] = DataSource(fp_variable=var_name.upper(), **entry)

    return SourceMap(sources)


def _parse_quarter_start(value: str) -> pd.Timestamp | None:
    token = value.strip().upper()
    if not token:
        return None
    match = re.match(r"^(\d{4})Q([1-4])$", token)
    if not match:
        return None
    year = int(match.group(1))
    quarter = int(match.group(2))
    month = (quarter - 1) * 3 + 1
    return pd.Timestamp(year=year, month=month, day=1)


def _parse_quarter_end_exclusive(value: str) -> pd.Timestamp | None:
    quarter_start = _parse_quarter_start(value)
    if quarter_start is None:
        return None
    return quarter_start + pd.DateOffset(months=3)
