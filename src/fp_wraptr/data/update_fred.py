"""Build updated FM model bundles using external source mappings (FRED/BEA/BLS)."""

from __future__ import annotations

import datetime as dt
import json
import re
import shutil
import textwrap
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from fp_wraptr.data.source_map import DataSource, SourceMap, load_source_map
from fp_wraptr.data.update_fmdata_from_observations import (
    FmdataUpdateError,
    update_fmdata_from_observations,
)
from fp_wraptr.fred.ingest import fetch_series as fetch_fred_series
from fp_wraptr.fred.normalize_for_fmdata import (
    FredFmdataNormalizeError,
    normalize_observations_for_fmdata,
    period_to_quarter_start,
)
from fp_wraptr.io.fmdata_writer import render_fmdata_text, write_fmdata
from fp_wraptr.io.input_parser import parse_fm_data, parse_fm_data_text


class DataUpdateError(RuntimeError):
    """Raised when data-update inputs are invalid or unsupported."""


_BEA_NIPA_TABLE_RE = re.compile(r"^T\d{5}$", re.IGNORECASE)


def _is_bea_nipa_table(table: str) -> bool:
    """Return True only for NIPA TableName values (eg T10106).

    `source_map.yaml` contains many BEA placeholders seeded from raw-data links;
    the current BEA ingest only supports NIPA tables. Treat non-NIPA values as
    unsupported for now so multi-source updates don't crash.
    """
    return bool(_BEA_NIPA_TABLE_RE.match(str(table).strip()))


@dataclass
class DataUpdateResult:
    """Output paths and metadata for one data-update run."""

    out_dir: Path
    model_bundle_dir: Path
    fmdata_path: Path
    report_path: Path
    report: dict[str, Any]


def _parse_period(period: str) -> tuple[int, int]:
    text = str(period).strip()
    try:
        year_text, quarter_text = text.split(".")
        year = int(year_text)
        quarter = int(quarter_text)
    except Exception as exc:  # pragma: no cover - defensive parse guard
        raise DataUpdateError(f"Invalid period '{period}' (expected YYYY.Q)") from exc

    if quarter not in {1, 2, 3, 4}:
        raise DataUpdateError(f"Invalid period '{period}' (quarter must be 1..4)")
    return year, quarter


def _format_period(year: int, quarter: int) -> str:
    return f"{year}.{quarter}"


def _period_to_index(sample_start: str, period: str) -> int:
    sy, sq = _parse_period(sample_start)
    py, pq = _parse_period(period)
    return (py - sy) * 4 + (pq - sq)


def _period_gt(left: str, right: str) -> bool:
    return _parse_period(left) > _parse_period(right)


def _quarter_start_timestamp(period: str) -> pd.Timestamp:
    year, quarter = _parse_period(period)
    month = (quarter - 1) * 3 + 1
    return pd.Timestamp(year=year, month=month, day=1)


def _timestamp_to_period(ts: pd.Timestamp) -> str:
    quarter = ((int(ts.month) - 1) // 3) + 1
    return _format_period(int(ts.year), quarter)


def _next_period(period: str) -> str:
    year, quarter = _parse_period(period)
    quarter += 1
    if quarter > 4:
        year += 1
        quarter = 1
    return _format_period(year, quarter)


def _quarter_end_inclusive_timestamp(period: str) -> pd.Timestamp:
    start = _quarter_start_timestamp(period)
    end_exclusive = start + pd.DateOffset(months=3)
    return end_exclusive - pd.Timedelta(days=1)


def _period_distance(start: str, end: str) -> int:
    sy, sq = _parse_period(start)
    ey, eq = _parse_period(end)
    return (ey - sy) * 4 + (eq - sq)


def _max_period(left: str, right: str) -> str:
    return left if _parse_period(left) >= _parse_period(right) else right


def _render_scenario_yaml(*, payload: dict[str, Any]) -> str:
    """Render a small, deterministic YAML snippet without depending on PyYAML.

    This is operator-facing output (under artifacts/), not a general-purpose YAML
    serializer. Keep it intentionally minimal and stable.
    """
    lines: list[str] = []
    for key in (
        "name",
        "description",
        "fp_home",
        "input_file",
        "forecast_start",
        "forecast_end",
        "backend",
    ):
        if key not in payload:
            continue
        value = payload[key]
        if value is None:
            continue
        if isinstance(value, str):
            if key in {"forecast_start", "forecast_end"}:
                lines.append(f'{key}: "{value}"')
            else:
                lines.append(f"{key}: {value}")
        else:
            lines.append(f"{key}: {value}")

    fppy = payload.get("fppy")
    if isinstance(fppy, dict) and fppy:
        lines.append("fppy:")
        for k in sorted(fppy.keys()):
            v = fppy[k]
            lines.append(f"  {k}: {v}")

    patches = payload.get("input_patches")
    if isinstance(patches, dict) and patches:
        lines.append("input_patches:")
        for search, replace in patches.items():
            lines.append(f'  "{search}": "{replace}"')

    track = payload.get("track_variables")
    if isinstance(track, list) and track:
        lines.append("track_variables:")
        for item in track:
            lines.append(f"  - {item}")

    return "\n".join(lines).rstrip() + "\n"


def _patch_fminput_smpl_endpoints(
    *,
    fminput_path: Path,
    old_end: str,
    new_end: str,
) -> dict[str, Any]:
    """Rewrite a small, safe subset of SMPL endpoints in fminput.txt.

    Goal: when `fmdata.txt` is extended (for example 2025.3 -> 2025.4), ensure fp.exe
    actually *loads* the new history by updating the single `SMPL ... <end>;` that is
    in effect when the deck reaches `LOADDATA FILE=FMDATA.TXT;`.

    We intentionally do NOT rewrite every `SMPL ... <old_end>;` in the file. The Fair
    model contains many pinned historical-window SMPLs that should remain unchanged
    unless you know exactly what you are doing. Patching only the LOADDATA SMPL avoids
    destabilizing unrelated windows.
    """
    payload: dict[str, Any] = {
        "path": str(fminput_path),
        "old_end": str(old_end),
        "new_end": str(new_end),
        "patched_count": 0,
        "patched_lines_sample": [],
    }
    try:
        text = fminput_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        payload["error"] = str(exc)
        return payload

    smpl_re = re.compile(r"^(\s*SMPL\s+)(\d{4}\.\d)\s+(\d{4}\.\d)(\s*;.*)$", re.IGNORECASE)
    loaddata_re = re.compile(r"\bLOADDATA\b.*\bFILE\s*=\s*FMDATA\.TXT\b", re.IGNORECASE)

    lines = text.splitlines(keepends=True)
    # Track the most recent SMPL line so we can patch only the one that applies to LOADDATA.
    last_smpl_idx: int | None = None
    last_smpl_lineno: int | None = None
    last_smpl_match: tuple[str, str, str, str] | None = None

    patched: list[dict[str, Any]] = []
    for i, line in enumerate(lines):
        lineno = i + 1
        stripped = line.rstrip("\r\n")
        if stripped.lstrip().startswith("@"):
            continue
        m = smpl_re.match(stripped)
        if m:
            last_smpl_idx = i
            last_smpl_lineno = lineno
            last_smpl_match = m.groups()
            continue
        if loaddata_re.search(stripped):
            # Patch only the SMPL that is active for LOADDATA.
            if last_smpl_idx is None or last_smpl_lineno is None or last_smpl_match is None:
                break
            prefix, start_q, end_q, suffix = last_smpl_match
            if str(end_q).strip() != str(old_end).strip():
                break

            before = lines[last_smpl_idx].rstrip("\r\n")
            after = f"{prefix}{start_q} {new_end}{suffix}"
            newline = (
                "\r\n"
                if lines[last_smpl_idx].endswith("\r\n")
                else ("\n" if lines[last_smpl_idx].endswith("\n") else "")
            )
            lines[last_smpl_idx] = after + newline
            patched.append({"line": last_smpl_lineno, "before": before, "after": after})
            break

    payload["patched_count"] = len(patched)
    payload["patched_lines_sample"] = patched[:50]
    if not patched:
        return payload

    fminput_path.write_text("".join(lines), encoding="utf-8")
    return payload


def _extract_fminput_fmdata_load_end(fminput_text: str) -> str | None:
    """Return the SMPL end quarter in effect for `LOADDATA FILE=FMDATA.TXT;`.

    The Fair model fminput typically sets SMPL, then issues LOADDATA for fmdata.
    fp.exe will only load history through that SMPL end (even if fmdata.txt itself
    contains additional quarters).
    """
    last_smpl_end: str | None = None
    smpl_re = re.compile(r"^\s*SMPL\s+\d{4}\.\d\s+(?P<end>\d{4}\.\d)\s*;\s*$", re.IGNORECASE)
    # LOADDATA lines vary in case and sometimes include spaces around '='.
    loaddata_re = re.compile(r"^\s*LOADDATA\b.*\bFILE\s*=\s*FMDATA\.TXT\b", re.IGNORECASE)
    for raw in str(fminput_text or "").splitlines():
        if raw.lstrip().startswith("@"):
            continue
        m = smpl_re.match(raw)
        if m:
            last_smpl_end = str(m.group("end")).strip()
            continue
        if loaddata_re.search(raw):
            return last_smpl_end
    return None


def _augment_fminput_keyboard_targets(
    *,
    fminput_path: Path,
    extra_targets: tuple[str, ...],
) -> dict[str, Any]:
    """Append extra targets to a Fair-style KEYBOARD list in `fminput.txt`.

    Expected pattern:
      SOLVE ... FILEVAR=KEYBOARD ...;
      <var>
      <var>
      ;
    """
    payload: dict[str, Any] = {
        "path": str(fminput_path),
        "extra_targets": [str(t) for t in extra_targets],
        "found": False,
        "added": [],
        "already_present": [],
    }
    normalized_extra = [str(t).strip().upper() for t in extra_targets if str(t).strip()]
    if not normalized_extra:
        return payload

    try:
        raw = fminput_path.read_bytes()
    except OSError as exc:
        payload["error"] = str(exc)
        return payload

    newline = "\r\n" if b"\r\n" in raw else "\n"
    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines(keepends=True)

    solve_re = re.compile(r"^\s*SOLVE\b.*\bFILEVAR\s*=\s*KEYBOARD\b", re.IGNORECASE)
    i = 0
    while i < len(lines):
        if lines[i].lstrip().startswith("@"):
            i += 1
            continue
        if not solve_re.search(lines[i]):
            i += 1
            continue

        payload["found"] = True
        j = i + 1
        existing: list[str] = []
        while j < len(lines):
            token = lines[j].strip()
            if token.startswith("@"):
                j += 1
                continue
            if token == ";":
                break
            if token:
                existing.append(str(token).upper())
            j += 1
        if j >= len(lines) or lines[j].strip() != ";":
            payload["error"] = "Could not find KEYBOARD terminator ';' after SOLVE FILEVAR=KEYBOARD"
            return payload

        existing_set = set(existing)
        to_add: list[str] = []
        for name in normalized_extra:
            if name in existing_set:
                payload["already_present"].append(name)
            else:
                to_add.append(name)

        if not to_add:
            return payload

        insert_lines = [f"{name}{newline}" for name in to_add]
        lines[j:j] = insert_lines
        payload["added"] = to_add
        break

    if not payload["found"]:
        return payload

    fminput_path.write_text("".join(lines), encoding="utf-8")
    return payload


def _write_update_scenario_templates(
    *,
    out_dir: Path,
    recommended_forecast_start: str,
    recommended_forecast_end: str,
) -> dict[str, str]:
    """Emit ready-to-run scenario templates next to an updated bundle.

    This avoids operators having to hand-edit forecast windows after extending
    sample_end. Templates are written under `<out_dir>/scenarios/`.
    """
    scenarios_dir = out_dir / "scenarios"
    scenarios_dir.mkdir(parents=True, exist_ok=True)

    # Heuristic patch: this exact string exists in the stock FAIR model fminput.
    # If a future model changes it, the scenario still runs; the patch simply
    # won't apply (and the operator can adjust manually).
    smpl_search = "SMPL 2025.4 2029.4;"
    smpl_full_replace = f"SMPL {recommended_forecast_start} {recommended_forecast_end};"
    smpl_smoke_replace = f"SMPL {recommended_forecast_start} {recommended_forecast_start};"

    track = ["PCY", "PCPF", "UR", "PIEF", "GDPR"]

    baseline_payload = {
        "name": "baseline_updated_data",
        "description": "Baseline run using an updated FM bundle (auto-generated by fp data update-fred).",
        "fp_home": "../FM",
        "input_file": "fminput.txt",
        "forecast_start": recommended_forecast_start,
        "forecast_end": recommended_forecast_end,
        "backend": "both",
        "fppy": {"eq_flags_preset": "parity", "timeout_seconds": 2400},
        "input_patches": {smpl_search: smpl_full_replace},
        "track_variables": track,
    }
    smoke_payload = {
        "name": "baseline_smoke_updated_data",
        "description": "Single-quarter smoke run using an updated FM bundle (auto-generated by fp data update-fred).",
        "fp_home": "../FM",
        "input_file": "fminput.txt",
        "forecast_start": recommended_forecast_start,
        "forecast_end": recommended_forecast_start,
        "backend": "both",
        "fppy": {"eq_flags_preset": "parity", "timeout_seconds": 900},
        "input_patches": {smpl_search: smpl_smoke_replace},
        "track_variables": track,
    }

    baseline_path = scenarios_dir / "baseline.yaml"
    baseline_path.write_text(_render_scenario_yaml(payload=baseline_payload), encoding="utf-8")

    smoke_path = scenarios_dir / "baseline_smoke.yaml"
    smoke_path.write_text(_render_scenario_yaml(payload=smoke_payload), encoding="utf-8")

    readme = scenarios_dir / "README.txt"
    readme.write_text(
        textwrap.dedent(
            f"""\
            These scenario templates were generated by `fp data update-fred`.

            Why: extending `FM/fmdata.txt` to a newer `sample_end` shifts the first
            forecast quarter. The stock examples in the repo are pinned to the
            repo's baseline FM sample_end and may not be appropriate for a newly
            updated bundle without adjusting forecast windows.

            Recommended forecast_start: {recommended_forecast_start}

            Run:
              fp parity scenarios/baseline.yaml --with-drift --output-dir artifacts
              fp parity scenarios/baseline_smoke.yaml --with-drift --output-dir artifacts

            Notes:
            - These templates assume the stock fminput contains `{smpl_search}`.
              If it does not, the `input_patches` entry will not apply; update it
              to match your model's solve SMPL line.
            """
        ).rstrip()
        + "\n",
        encoding="utf-8",
    )

    return {
        "scenarios_dir": str(scenarios_dir),
        "baseline_yaml": str(baseline_path),
        "baseline_smoke_yaml": str(smoke_path),
        "readme": str(readme),
    }


def _last_non_missing_index(values: list[float], missing_sentinels: set[float]) -> int:
    last = -1
    for idx, value in enumerate(values):
        if float(value) not in missing_sentinels:
            last = idx
    return last


def _sorted_period_items(values: Mapping[str, float]) -> list[tuple[str, float]]:
    items: list[tuple[str, float]] = []
    for period, value in values.items():
        text = str(period).strip()
        _parse_period(text)
        items.append((text, float(value)))
    items.sort(key=lambda row: _parse_period(row[0]))
    return items


def merge_fmdata_observations(
    *,
    base_fmdata_text: str,
    observations: Mapping[str, Mapping[str, float]],
    replace_history: bool = False,
    missing_sentinels: tuple[float, ...] = (-99.0,),
    fill_value: float = -99.0,
) -> tuple[str, dict[str, Any]]:
    """Merge normalized quarterly observations into fmdata text.

    `observations` is a mapping of `VAR -> {period: value}` where period is
    `YYYY.Q`. When `replace_history=False`, updates apply only to periods after
    the last non-missing value for each variable.
    """

    parsed = parse_fm_data_text(base_fmdata_text)
    sample_start = str(parsed.get("sample_start", "")).strip()
    sample_end_before = str(parsed.get("sample_end", "")).strip()
    if not sample_start or not sample_end_before:
        raise DataUpdateError("Base fmdata is missing sample bounds")

    blocks = parsed.get("blocks") or []
    if not isinstance(blocks, list) or not blocks:
        raise DataUpdateError("Base fmdata contains no LOAD blocks")

    series_values: dict[str, list[float]] = {}
    series_order: list[str] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        name = str(block.get("name", "")).strip().upper()
        if not name:
            continue
        values = block.get("values")
        if not isinstance(values, list):
            continue
        if name not in series_order:
            series_order.append(name)
        series_values[name] = [float(v) for v in values]

    if not series_values:
        raise DataUpdateError("Base fmdata contains no parseable series")

    max_observation_period = sample_end_before
    normalized_obs: dict[str, list[tuple[str, float]]] = {}
    for variable in sorted(observations):
        values = observations.get(variable)
        if not isinstance(values, Mapping):
            continue
        name = str(variable).strip().upper()
        if not name:
            continue
        normalized = _sorted_period_items(values)
        if normalized:
            normalized_obs[name] = normalized
            max_observation_period = _max_period(max_observation_period, normalized[-1][0])

    sample_end_after = max_observation_period
    before_len = _period_distance(sample_start, sample_end_before) + 1
    after_len = _period_distance(sample_start, sample_end_after) + 1
    extension_periods = max(0, after_len - before_len)

    missing_set = {float(item) for item in missing_sentinels}
    fill = float(fill_value)
    updated_points = 0
    missing_vars: list[str] = []
    variable_stats: dict[str, dict[str, int]] = {}

    for name in series_order:
        values = series_values[name]
        if len(values) < before_len:
            values.extend([fill] * (before_len - len(values)))
        elif len(values) > before_len:
            values[:] = values[:before_len]
        if extension_periods:
            values.extend([fill] * extension_periods)

    for name in sorted(normalized_obs):
        period_values = normalized_obs[name]
        if name not in series_values:
            missing_vars.append(name)
            continue

        values = series_values[name]
        last_history_idx = _last_non_missing_index(values[:before_len], missing_set)
        stats = {"updated": 0, "skipped_history": 0, "out_of_range": 0}

        for period, value in period_values:
            idx = _period_to_index(sample_start, period)
            if idx < 0 or idx >= len(values):
                stats["out_of_range"] += 1
                continue
            if not replace_history and idx <= last_history_idx:
                stats["skipped_history"] += 1
                continue
            current = float(values[idx])
            if current != float(value):
                values[idx] = float(value)
                stats["updated"] += 1
                updated_points += 1

        variable_stats[name] = stats

    rendered = render_fmdata_text(
        sample_start=sample_start,
        sample_end=sample_end_after,
        series={name: series_values[name] for name in series_order},
        values_per_line=4,
    )
    report: dict[str, Any] = {
        "sample_start": sample_start,
        "sample_end_before": sample_end_before,
        "sample_end_after": sample_end_after,
        "replace_history": bool(replace_history),
        "extended_periods": int(extension_periods),
        "updated_points": int(updated_points),
        "missing_vars": sorted(missing_vars),
        "variable_stats": {
            name: variable_stats.get(name, {"updated": 0, "skipped_history": 0, "out_of_range": 0})
            for name in sorted(normalized_obs)
        },
    }
    return rendered, report


def apply_normalized_observations(
    *,
    base_fmdata_path: Path,
    out_fmdata_path: Path,
    observations: Mapping[str, Mapping[str, float]],
    replace_history: bool = False,
    missing_sentinels: tuple[float, ...] = (-99.0,),
    fill_value: float = -99.0,
) -> dict[str, Any]:
    """Apply normalized observations to a base fmdata file and write output."""
    base_text = Path(base_fmdata_path).read_text(encoding="utf-8", errors="replace")
    rendered, report = merge_fmdata_observations(
        base_fmdata_text=base_text,
        observations=observations,
        replace_history=replace_history,
        missing_sentinels=missing_sentinels,
        fill_value=fill_value,
    )
    target = Path(out_fmdata_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(rendered, encoding="utf-8")
    return {
        **report,
        "base_fmdata_path": str(Path(base_fmdata_path)),
        "out_fmdata_path": str(target),
        "observation_variable_count": len(observations),
    }


def _load_source_map(source_map_path: Path | None) -> SourceMap:
    return load_source_map(source_map_path) if source_map_path is not None else load_source_map()


def update_model_from_fred(
    *,
    model_dir: Path,
    out_dir: Path,
    end_period: str,
    source_map_path: Path | None = None,
    cache_dir: Path | None = None,
    variables: list[str] | None = None,
    sources: list[str] | None = None,
    replace_history: bool = False,
    extend_sample: bool = False,
    allow_carry_forward: bool = False,
    patch_fminput_smpl_endpoints: bool = False,
    keyboard_augment_targets: tuple[str, ...] = tuple(),
    start_date: str | None = None,
    end_date: str | None = None,
) -> DataUpdateResult:
    """Create an updated model bundle with refreshed `fmdata.txt` values.

    The update writes a new `FM/`-shaped directory under `out_dir` and never
    mutates `model_dir`.
    """

    model_dir = Path(model_dir)
    out_dir = Path(out_dir)
    _parse_period(end_period)  # validate early

    fmdata_path = model_dir / "fmdata.txt"
    if not model_dir.exists():
        raise DataUpdateError(f"Model directory not found: {model_dir}")
    if not fmdata_path.exists():
        raise DataUpdateError(f"Missing fmdata.txt in model directory: {fmdata_path}")

    # Build bundle dir first so we never touch the repo model dir.
    out_dir.mkdir(parents=True, exist_ok=True)
    bundle_dir = out_dir / "FM"
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    shutil.copytree(model_dir, bundle_dir)

    bundle_fmdata_path = bundle_dir / "fmdata.txt"
    parsed = parse_fm_data(bundle_fmdata_path)
    blocks_raw = parsed.get("blocks")
    if not isinstance(blocks_raw, list) or not blocks_raw:
        raise DataUpdateError(f"Failed to parse any LOAD blocks from {fmdata_path}")

    series_blocks: dict[str, dict[str, Any]] = {}
    for payload in blocks_raw:
        if not isinstance(payload, dict):
            continue
        name = str(payload.get("name", "")).strip().upper()
        if not name:
            continue
        series_blocks[name] = payload

    sample_start = str(parsed.get("sample_start", "")).strip()
    sample_end_before = str(parsed.get("sample_end", "")).strip()
    if not sample_start:
        raise DataUpdateError(f"Parsed fmdata is missing sample_start: {bundle_fmdata_path}")
    if not sample_end_before:
        raise DataUpdateError(f"Parsed fmdata is missing sample_end: {bundle_fmdata_path}")

    source_map = _load_source_map(source_map_path)
    requested = [name.strip().upper() for name in (variables or []) if str(name).strip()]
    enabled_sources = [str(s).strip().lower() for s in (sources or ["fred"]) if str(s).strip()]
    enabled_sources = list(dict.fromkeys(enabled_sources)) or ["fred"]
    if requested:
        candidates = requested
    else:
        # Default: only series explicitly sourced from the enabled source set.
        candidates = [
            var_name
            for var_name in sorted(series_blocks.keys())
            if (entry := source_map.get(var_name)) is not None
            and str(entry.source).strip().lower() in enabled_sources
            and (
                (
                    str(entry.source).strip().lower() != "bea"
                    and (entry.series_id or entry.fred_fallback)
                )
                or (
                    str(entry.source).strip().lower() == "bea"
                    and entry.bea_table
                    and entry.bea_line
                    and _is_bea_nipa_table(str(entry.bea_table))
                )
            )
        ]

    selected_fred: dict[str, tuple[DataSource, str]] = {}
    selected_bls: dict[str, tuple[DataSource, str]] = {}
    selected_bea: dict[str, tuple[DataSource, str, int]] = {}
    skipped_unmapped: list[str] = []
    skipped_unsupported_frequency: list[dict[str, str]] = []
    skipped_unsupported_bea: list[dict[str, str]] = []
    for var_name in candidates:
        block = series_blocks.get(var_name)
        if block is None:
            skipped_unmapped.append(var_name)
            continue
        entry = source_map.get(var_name)
        if entry is None:
            skipped_unmapped.append(var_name)
            continue
        source = str(entry.source).strip().lower()
        if source not in enabled_sources:
            skipped_unmapped.append(var_name)
            continue
        frequency = str(entry.frequency).strip().upper()
        if frequency and frequency not in {"Q", "M"}:
            skipped_unsupported_frequency.append({
                "variable": var_name,
                "frequency": frequency,
                "source": source,
            })
            continue

        if source == "fred":
            series_id = (entry.series_id or entry.fred_fallback).strip()
            if not series_id:
                skipped_unmapped.append(var_name)
                continue
            selected_fred[var_name] = (entry, series_id)
            continue

        if source == "bls":
            series_id = str(entry.series_id).strip()
            if not series_id:
                skipped_unmapped.append(var_name)
                continue
            selected_bls[var_name] = (entry, series_id)
            continue

        if source == "bea":
            table = str(entry.bea_table).strip()
            line = int(getattr(entry, "bea_line", 0) or 0)
            if not table or line <= 0:
                skipped_unmapped.append(var_name)
                continue
            if not _is_bea_nipa_table(table):
                skipped_unsupported_bea.append({
                    "variable": var_name,
                    "bea_table": table,
                    "reason": "unsupported_bea_table",
                })
                continue
            selected_bea[var_name] = (entry, table, line)
            continue

        skipped_unmapped.append(var_name)

    selected_total = len(selected_fred) + len(selected_bls) + len(selected_bea)
    if not selected_total:
        raise DataUpdateError(
            "No variables with eligible source mappings were eligible for update "
            f"(sources={enabled_sources}, requested={requested or 'all fmdata vars'})."
        )

    fred_series_ids = list(dict.fromkeys(series_id for _, series_id in selected_fred.values()))
    bls_series_ids = list(dict.fromkeys(series_id for _, series_id in selected_bls.values()))
    update_start_period = sample_start if replace_history else _next_period(sample_end_before)

    normalized: dict[str, pd.Series] = {}
    effective_start_date = ""
    effective_end_date = ""
    selected_without_observations: list[str] = []
    bea_failed_tables: list[dict[str, str]] = []

    if not _period_gt(update_start_period, end_period):
        effective_start_date = start_date or period_to_quarter_start(update_start_period).strftime(
            "%Y-%m-%d"
        )
        effective_end_date = end_date or _quarter_end_inclusive_timestamp(end_period).strftime(
            "%Y-%m-%d"
        )
        raw_frames: list[pd.DataFrame] = []

        if selected_fred:
            try:
                fetched = fetch_fred_series(
                    fred_series_ids,
                    start=effective_start_date,
                    end=effective_end_date,
                    cache_dir=cache_dir,
                )
            except ValueError as exc:
                raise DataUpdateError(str(exc)) from exc
            rename = {series_id: var for var, (_, series_id) in selected_fred.items()}
            raw_frames.append(fetched.rename(columns=rename))

        if selected_bls:
            from fp_wraptr.bls.ingest import BlsSeriesRequest
            from fp_wraptr.bls.ingest import fetch_series as fetch_bls_series

            start_year = int(str(effective_start_date)[:4])
            end_year = int(str(effective_end_date)[:4])
            cache_root = (
                (cache_dir.parent if cache_dir and cache_dir.name == "fred-cache" else cache_dir)
                if cache_dir
                else None
            )
            bls_cache_dir = (cache_root / "bls-cache") if isinstance(cache_root, Path) else None
            fetched = fetch_bls_series(
                BlsSeriesRequest(
                    series_ids=bls_series_ids,
                    start_year=start_year,
                    end_year=end_year,
                ),
                cache_dir=bls_cache_dir,
            )
            rename = {series_id: var for var, (_, series_id) in selected_bls.items()}
            raw_frames.append(fetched.rename(columns=rename))

        if selected_bea:
            from fp_wraptr.bea.ingest import BeaNipaRequest, fetch_nipa_table

            cache_root = (
                (cache_dir.parent if cache_dir and cache_dir.name == "fred-cache" else cache_dir)
                if cache_dir
                else None
            )
            bea_cache_dir = (cache_root / "bea-cache") if isinstance(cache_root, Path) else None

            tables: dict[str, set[int]] = {}
            for _, (_, table, line) in selected_bea.items():
                tables.setdefault(table, set()).add(int(line))

            bea_var_series: dict[str, pd.Series] = {}
            for table, _lines in sorted(tables.items()):
                try:
                    df = fetch_nipa_table(
                        BeaNipaRequest(table_name=table, frequency="Q", year="ALL"),
                        cache_dir=bea_cache_dir,
                    )
                except Exception as exc:
                    bea_failed_tables.append({"table": table, "error": str(exc)})
                    continue
                if df.empty:
                    continue
                for var, (_, var_table, var_line) in selected_bea.items():
                    if var_table != table:
                        continue
                    if int(var_line) in df.columns:
                        bea_var_series[var] = pd.to_numeric(df[int(var_line)], errors="coerce")

            if bea_var_series:
                bea_df = pd.concat(bea_var_series.values(), axis=1)
                bea_df.columns = list(bea_var_series.keys())
                raw_frames.append(bea_df)
        else:
            bea_failed_tables = []

        raw_observations = (
            pd.concat(raw_frames, axis=1).sort_index() if raw_frames else pd.DataFrame()
        )
        selected_variables = sorted({
            *selected_fred.keys(),
            *selected_bls.keys(),
            *selected_bea.keys(),
        })
        available_variables = {
            str(column).strip().upper()
            for column in raw_observations.columns
            if str(column).strip()
        }
        variables_for_normalize = [
            name for name in selected_variables if name in available_variables
        ]
        selected_without_observations = [
            name for name in selected_variables if name not in available_variables
        ]
        try:
            normalized = normalize_observations_for_fmdata(
                observations=raw_observations,
                source_map=source_map,
                variables=variables_for_normalize,
                start_period=update_start_period,
                end_period=end_period,
            )
        except FredFmdataNormalizeError as exc:
            raise DataUpdateError(str(exc)) from exc

    try:
        merged = update_fmdata_from_observations(
            parsed_fmdata=parsed,
            observations=normalized,
            end_period=end_period,
            replace_history=replace_history,
            extend_sample=extend_sample,
            allow_carry_forward=allow_carry_forward,
        )
    except FmdataUpdateError as exc:
        raise DataUpdateError(str(exc)) from exc

    # Preserve CRLF if base uses CRLF.
    base_bytes = bundle_fmdata_path.read_bytes()
    newline = "\r\n" if b"\r\n" in base_bytes else "\n"
    write_fmdata(
        sample_start=merged.sample_start,
        sample_end=merged.sample_end_after,
        series=merged.series,
        newline=newline,  # type: ignore[arg-type]
        path=bundle_fmdata_path,
    )

    from fp_wraptr import __version__ as producer_version

    fminput_smpl_patch: dict[str, Any] | None = None
    if (
        patch_fminput_smpl_endpoints
        and extend_sample
        and merged.sample_end_after != merged.sample_end_before
    ):
        fminput_path = bundle_dir / "fminput.txt"
        if fminput_path.exists():
            fminput_smpl_patch = _patch_fminput_smpl_endpoints(
                fminput_path=fminput_path,
                old_end=merged.sample_end_before,
                new_end=merged.sample_end_after,
            )
    fminput_keyboard_patch: dict[str, Any] | None = None
    if keyboard_augment_targets:
        fminput_path = bundle_dir / "fminput.txt"
        if fminput_path.exists():
            fminput_keyboard_patch = _augment_fminput_keyboard_targets(
                fminput_path=fminput_path,
                extra_targets=tuple(keyboard_augment_targets),
            )

    fminput_fmdata_load_end: str | None = None
    recommended_forecast_end = "2029.4"
    try:
        fminput_text = (bundle_dir / "fminput.txt").read_text(encoding="utf-8", errors="replace")
        fminput_fmdata_load_end = _extract_fminput_fmdata_load_end(fminput_text)
        match = re.search(r"\bLASTPER\s*=\s*(\d{4}\.\d)\s*;", fminput_text, re.IGNORECASE)
        if match:
            recommended_forecast_end = str(match.group(1)).strip()
    except OSError:
        fminput_text = ""

    # Recommended forecast start is the quarter after the fmdata sample_end we wrote.
    # Note: fp.exe may still be pinned to a different LOADDATA SMPL end in fminput.txt;
    # we persist `fminput_fmdata_load_end` in the report so operators can see that mismatch.
    recommended_forecast_start = _next_period(merged.sample_end_after)

    scenario_templates = _write_update_scenario_templates(
        out_dir=out_dir,
        recommended_forecast_start=recommended_forecast_start,
        recommended_forecast_end=recommended_forecast_end,
    )

    report_payload: dict[str, Any] = {
        "schema_version": 1,
        "producer_version": str(producer_version),
        "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        "model_dir": str(model_dir),
        "bundle_dir": str(bundle_dir),
        "fmdata_path": str(bundle_fmdata_path),
        "end_period": end_period,
        "start_period": update_start_period,
        "sample_start": merged.sample_start,
        "sample_end_before": merged.sample_end_before,
        "sample_end_after": merged.sample_end_after,
        "fminput_fmdata_load_end": fminput_fmdata_load_end,
        "recommended_forecast_start": recommended_forecast_start,
        "recommended_forecast_end": recommended_forecast_end,
        "replace_history": bool(replace_history),
        "extend_sample": bool(extend_sample),
        "allow_carry_forward": bool(allow_carry_forward),
        "start_date": effective_start_date,
        "end_date": effective_end_date,
        "requested_variables": requested,
        "enabled_sources": enabled_sources,
        "selected_variable_count": selected_total,
        "selected_fred_variable_count": len(selected_fred),
        "selected_bls_variable_count": len(selected_bls),
        "selected_bea_variable_count": len(selected_bea),
        "skipped_unmapped": sorted(skipped_unmapped),
        "skipped_unsupported_frequency": skipped_unsupported_frequency,
        "skipped_unsupported_bea": skipped_unsupported_bea,
        "selected_variables_without_observations": selected_without_observations,
        "fred_series_ids": fred_series_ids,
        "bls_series_ids": bls_series_ids,
        "bea_tables": sorted({table for _, (_, table, _) in selected_bea.items()}),
        "bea_failed_tables": bea_failed_tables,
        "normalized_variable_count": len(normalized),
        "fmdata_merge": merged.report,
        "fminput_smpl_patch": fminput_smpl_patch,
        "fminput_keyboard_patch": fminput_keyboard_patch,
        "scenario_templates": scenario_templates,
    }

    report_path = out_dir / "data_update_report.json"
    report_path.write_text(
        json.dumps(report_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    return DataUpdateResult(
        out_dir=out_dir,
        model_bundle_dir=bundle_dir,
        fmdata_path=bundle_fmdata_path,
        report_path=report_path,
        report=report_payload,
    )
