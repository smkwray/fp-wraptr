"""Helpers for discovering and loading fp-wraptr dashboard run artifacts."""

from __future__ import annotations

import datetime as dt
import json
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fp_wraptr.io.loadformat import add_derived_series, read_loadformat
from fp_wraptr.io.parser import FPOutputData, parse_fp_output
from fp_wraptr.scenarios.config import ScenarioConfig

_RUN_DIR_RE = re.compile(r"^(?P<scenario_name>.+)_(?P<timestamp>\d{8}_\d{6})$")
_PERIOD_TOKEN_RE = re.compile(r"^(?P<year>\d{4})\.(?P<sub>\d+)$")


@dataclass
class RunArtifact:
    """Metadata for one scenario run artifact directory."""

    run_dir: Path
    scenario_name: str
    timestamp: str
    has_output: bool
    has_chart: bool
    config: ScenarioConfig | None
    backend_hint: str = ""

    @property
    def display_name(self) -> str:
        if self.timestamp:
            return f"{self.scenario_name} ({self.timestamp})"
        return self.scenario_name

    @property
    def backend_name(self) -> str:
        """Return the configured backend for the run (fpexe/fppy/both/unknown)."""
        if self.config is None:
            return "unknown"
        value = str(getattr(self.config, "backend", "") or "").strip().lower()
        return value or "unknown"

    def load_output(self) -> FPOutputData | None:
        """Load and parse output data for this run.

        Preference order:
          1) ``fmout.txt`` (rich FP console output)
          2) ``LOADFORMAT.DAT`` / ``PABEV.TXT`` / ``PACEV.TXT`` (PRINTVAR LOADFORMAT series)
        """
        fmout = self.run_dir / "fmout.txt"
        if fmout.exists():
            return parse_fp_output(fmout)

        config = self.config
        if config is None:
            scenario_file = self.run_dir / "scenario.yaml"
            if scenario_file.exists():
                try:
                    config = ScenarioConfig.from_yaml(scenario_file)
                except Exception:
                    config = None

        loadformat_path = _first_existing(
            self.run_dir / "LOADFORMAT.DAT",
            self.run_dir / "PABEV.TXT",
            self.run_dir / "PACEV.TXT",
        )
        if loadformat_path is None:
            return None

        try:
            period_tokens, series = read_loadformat(loadformat_path)
        except Exception:
            return None

        add_derived_series(series)
        start = (
            str(getattr(config, "forecast_start", "") or "").strip() if config is not None else ""
        )
        end = str(getattr(config, "forecast_end", "") or "").strip() if config is not None else ""
        model_title = str(getattr(config, "name", "") or "").strip() if config is not None else ""
        if not model_title:
            model_title = self.scenario_name
        return _fp_output_from_series(
            period_tokens,
            series,
            forecast_start=start or None,
            forecast_end=end or None,
            model_title=model_title,
        )

    def load_series_output(self) -> FPOutputData | None:
        """Load run output preferring LOADFORMAT-style series over fmout.

        This is useful for side-by-side comparisons between fpexe/fppy runs,
        since both engines emit comparable LOADFORMAT artifacts.
        """
        config = self.config
        if config is None:
            scenario_file = self.run_dir / "scenario.yaml"
            if scenario_file.exists():
                try:
                    config = ScenarioConfig.from_yaml(scenario_file)
                except Exception:
                    config = None

        loadformat_path = _first_existing(
            self.run_dir / "LOADFORMAT.DAT",
            self.run_dir / "PABEV.TXT",
            self.run_dir / "PACEV.TXT",
        )
        if loadformat_path is not None:
            try:
                period_tokens, series = read_loadformat(loadformat_path)
            except Exception:
                period_tokens, series = [], {}
            if period_tokens and series:
                add_derived_series(series)
                start = (
                    str(getattr(config, "forecast_start", "") or "").strip()
                    if config is not None
                    else ""
                )
                end = (
                    str(getattr(config, "forecast_end", "") or "").strip()
                    if config is not None
                    else ""
                )
                model_title = (
                    str(getattr(config, "name", "") or "").strip() if config is not None else ""
                )
                if not model_title:
                    model_title = self.scenario_name
                return _fp_output_from_series(
                    period_tokens,
                    series,
                    forecast_start=start or None,
                    forecast_end=end or None,
                    model_title=model_title,
                )

        fmout = self.run_dir / "fmout.txt"
        if fmout.exists():
            return parse_fp_output(fmout)
        return None


def _first_existing(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _fp_output_from_series(
    period_tokens: list[str],
    series: dict[str, list[float]],
    *,
    forecast_start: str | None,
    forecast_end: str | None,
    model_title: str,
) -> FPOutputData:
    """Convert (period_tokens, series) into FPOutputData for dashboard diffs/plots."""

    start_idx, end_idx = _slice_bounds(period_tokens, start=forecast_start, end=forecast_end)
    periods = period_tokens[start_idx : end_idx + 1]

    output = FPOutputData(
        model_title=model_title,
        forecast_start=forecast_start or (periods[0] if periods else ""),
        forecast_end=forecast_end or (periods[-1] if periods else ""),
        base_period=periods[0] if periods else "",
        periods=periods,
        variables={},
        raw_text="",
    )

    from fp_wraptr.io.parser import ForecastVariable

    for idx, (name, values) in enumerate(series.items(), start=1):
        changes, pct_changes = _derive_change_series(values, start_idx=start_idx, end_idx=end_idx)
        sliced = values[start_idx : end_idx + 1]
        output.variables[name] = ForecastVariable(
            var_id=idx,
            name=name,
            levels=list(sliced),
            changes=changes,
            pct_changes=pct_changes,
        )
    return output


def _derive_change_series(
    values: list[float],
    *,
    start_idx: int,
    end_idx: int,
) -> tuple[list[float], list[float]]:
    """Compute sliced change/pct-change series from full level values."""
    changes: list[float] = []
    pct_changes: list[float] = []
    for idx in range(start_idx, end_idx + 1):
        current = values[idx] if idx < len(values) else float("nan")
        prev = values[idx - 1] if 0 < idx <= len(values) else float("nan")
        if idx == 0 or not math.isfinite(current) or not math.isfinite(prev):
            changes.append(float("nan"))
            pct_changes.append(float("nan"))
            continue

        delta = current - prev
        changes.append(delta)
        if prev == 0.0:
            pct_changes.append(float("nan"))
        else:
            pct_changes.append((delta / prev) * 100.0)
    return changes, pct_changes


def _slice_bounds(
    period_tokens: list[str], *, start: str | None, end: str | None
) -> tuple[int, int]:
    """Return inclusive slice bounds (start_idx, end_idx) for YYYY.Q tokens."""
    if not period_tokens:
        return 0, -1

    start_key = _period_index(start) if start else None
    end_key = _period_index(end) if end else None

    start_idx = 0
    if start_key is not None:
        for idx, token in enumerate(period_tokens):
            key = _period_index(token)
            if key is not None and key >= start_key:
                start_idx = idx
                break

    end_idx = len(period_tokens) - 1
    if end_key is not None:
        for idx in range(len(period_tokens) - 1, -1, -1):
            key = _period_index(period_tokens[idx])
            if key is not None and key <= end_key:
                end_idx = idx
                break

    if end_idx < start_idx:
        end_idx = start_idx
    return start_idx, end_idx


def _split_run_dir_name(run_dir: Path) -> tuple[str, str]:
    """Split `<scenario>_<YYYYMMDD_HHMMSS>` into scenario name and timestamp."""
    match = _RUN_DIR_RE.match(run_dir.name)
    if match:
        return match.group("scenario_name"), match.group("timestamp")
    return run_dir.name, ""


def _artifact_sort_key(run: RunArtifact) -> str:
    return run.timestamp if run.timestamp else "00000000_000000"


def backend_name(run: object) -> str:
    """Best-effort backend label for a run (fpexe/fppy/both/unknown)."""
    hint = getattr(run, "backend_hint", None)
    if isinstance(hint, str) and hint.strip():
        return hint.strip().lower()
    backend = getattr(run, "backend_name", None)
    if isinstance(backend, str) and backend.strip():
        return backend.strip().lower()
    cfg = getattr(run, "config", None)
    value = str(getattr(cfg, "backend", "") or "").strip().lower()
    return value or "unknown"


def run_dir_key(path: Path) -> str:
    """Stable normalized key for run directory lookups in session state."""
    try:
        return str(path.expanduser().resolve())
    except Exception:
        return str(path)


def filter_runs_by_keys(runs: list[RunArtifact], selected_keys: set[str]) -> list[RunArtifact]:
    """Return runs constrained to selected run keys (or all runs when unset)."""
    if not selected_keys:
        return list(runs)
    out: list[RunArtifact] = []
    for run in runs:
        if run_dir_key(run.run_dir) in selected_keys:
            out.append(run)
    return out


def bundle_label(run: RunArtifact, artifacts_dir: Path) -> str:
    """Human label for run grouping under artifacts (top-level dir or root)."""
    try:
        relative = run.run_dir.relative_to(artifacts_dir)
        if len(relative.parts) <= 1:
            return "(root)"
        return relative.parts[0]
    except Exception:
        return "(external)"


def parse_run_timestamp(value: str) -> dt.datetime | None:
    """Parse run timestamp token (YYYYMMDD_HHMMSS)."""
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return dt.datetime.strptime(raw, "%Y%m%d_%H%M%S")
    except ValueError:
        return None


def latest_runs(
    runs: list[RunArtifact], *, limit: int = 3, has_output: bool = False
) -> list[RunArtifact]:
    """Pick latest N runs, optionally constrained to runs with output."""
    selected = [run for run in runs if (run.has_output if has_output else True)]
    selected.sort(key=_artifact_sort_key, reverse=True)
    return selected[: max(0, int(limit))]


def _period_index(token: str | None) -> int | None:
    raw = str(token or "").strip()
    if not raw:
        return None
    match = _PERIOD_TOKEN_RE.match(raw)
    if not match:
        return None
    try:
        year = int(match.group("year"))
        sub = int(match.group("sub"))
    except (TypeError, ValueError):
        return None
    return year * 10 + sub


def forecast_period_count(run: object) -> int | None:
    """Best-effort number of configured forecast periods for a run."""
    config = getattr(run, "config", None)
    if config is None:
        return None
    start = _period_index(str(getattr(config, "forecast_start", "") or ""))
    end = _period_index(str(getattr(config, "forecast_end", "") or ""))
    if start is None or end is None or end < start:
        return None
    return end - start + 1


def has_multi_period_forecast(run: object, *, min_periods: int = 2) -> bool:
    count = forecast_period_count(run)
    return bool(count is not None and count >= int(min_periods))


def latest_preferred_runs(
    runs: list[RunArtifact],
    *,
    limit: int = 3,
    has_output: bool = False,
) -> list[RunArtifact]:
    """Pick latest runs, preferring runs with multi-period forecast windows."""
    selected = [run for run in runs if (run.has_output if has_output else True)]
    if not selected:
        return []
    preferred = [run for run in selected if has_multi_period_forecast(run)]
    source = preferred if preferred else selected
    source.sort(key=_artifact_sort_key, reverse=True)
    return source[: max(0, int(limit))]


def _scenario_overlay_aliases(run: object) -> list[str]:
    config = getattr(run, "config", None)
    scenario_name = str(getattr(run, "scenario_name", "") or "").strip()
    config_name = str(getattr(config, "name", "") or "").strip() if config is not None else ""
    aliases: list[str] = []

    def _append_alias(value: str) -> None:
        token = str(value or "").strip()
        if token and token not in aliases:
            aliases.append(token)

    for value in (config_name, scenario_name):
        _append_alias(value)
        folded = str(value or "").strip().casefold()
        if folded in {"baseline", "baseline_smoke", "stock_fm_baseline"} or folded.endswith(
            "_baseline"
        ):
            _append_alias("baseline")
    if not aliases:
        aliases.append("scenario")
    return aliases


def _existing_unique_paths(candidates: Sequence[Path]) -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for candidate in candidates:
        try:
            resolved = candidate.expanduser().resolve()
        except Exception:
            resolved = candidate
        if resolved in seen:
            continue
        seen.add(resolved)
        if candidate.exists():
            out.append(candidate)
    return out


def recommended_overlay_path(run: object | None) -> Path | None:
    """Best-effort scenario dictionary overlay path for one run."""
    if run is None:
        return None
    config = getattr(run, "config", None)
    if config is None:
        return None
    run_dir = Path(run.run_dir)
    legacy = run_dir / "dictionary_overlay.json"
    if legacy.exists():
        return legacy
    aliases = _scenario_overlay_aliases(run)
    primary = aliases[0]
    overlay_dir = getattr(config, "input_overlay_dir", None)
    if overlay_dir:
        return Path(overlay_dir) / "dictionary_overlays" / f"{primary}.json"
    return Path("projects_local") / "dictionary_overlays" / f"{primary}.json"


def shared_extension_paths(run: object | None) -> list[Path]:
    """Return shared dictionary extension JSON files for one run context."""
    if run is None:
        return []
    config = getattr(run, "config", None)
    if config is None:
        return []

    roots: list[Path] = []
    overlay_dir = getattr(config, "input_overlay_dir", None)
    if overlay_dir:
        roots.append(Path(overlay_dir) / "dictionary_extensions")
    roots.append(Path("projects_local") / "dictionary_extensions")

    candidates: list[Path] = []
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        candidates.extend(sorted(root.glob("*.json")))
    return _existing_unique_paths(candidates)


def existing_overlay_paths(run: object | None) -> list[Path]:
    """Return existing dictionary extension+overlay paths relevant to a run.

    Merge order is deterministic: shared extensions first, then scenario-specific
    overlays. This preserves stock dictionary inheritance while allowing
    scenario overlays to override shared extension definitions.
    """
    if run is None:
        return []
    config = getattr(run, "config", None)
    if config is None:
        return []
    run_dir = Path(run.run_dir)
    aliases = _scenario_overlay_aliases(run)
    shared_root = Path("projects_local") / "dictionary_overlays"
    candidates: list[Path] = [
        *shared_extension_paths(run),
        shared_root / "baseline.json",
        run_dir / "dictionary_overlay.json",
    ]
    overlay_dir = getattr(config, "input_overlay_dir", None)
    if overlay_dir:
        overlay_root = Path(overlay_dir) / "dictionary_overlays"
        for alias in aliases:
            candidates.append(overlay_root / f"{alias}.json")
    for alias in aliases:
        candidates.append(shared_root / f"{alias}.json")

    return _existing_unique_paths(candidates)


def overlay_paths_for_runs(runs: Sequence[object]) -> list[Path]:
    """Collect existing dictionary overlay files for selected runs."""
    seen: set[Path] = set()
    out: list[Path] = []
    for run in runs:
        for path in existing_overlay_paths(run):
            try:
                resolved = path.expanduser().resolve()
            except Exception:
                resolved = path
            if resolved in seen:
                continue
            seen.add(resolved)
            out.append(path)
    return out


def scan_artifacts(artifacts_dir: Path) -> list[RunArtifact]:
    """Scan for run directories up to three levels deep.

    A run is identified by a `scenario.yaml` file present in the directory.
    """
    artifacts_root = Path(artifacts_dir)
    if not artifacts_root.exists():
        return []

    runs: list[RunArtifact] = []
    for scenario_file in artifacts_root.rglob("scenario.yaml"):
        parts = scenario_file.relative_to(artifacts_root).parts
        if not parts:
            continue
        top = str(parts[0])
        if top == "parity":
            continue
        if top == "sensitivity":
            continue
        if top.startswith("."):
            continue
        if len(parts) > 4:
            continue

        run_dir = scenario_file.parent
        scenario_name, timestamp = _split_run_dir_name(run_dir)
        has_output = (run_dir / "fmout.txt").exists() or (run_dir / "LOADFORMAT.DAT").exists()
        has_chart = (run_dir / "forecast.png").exists()
        try:
            config = ScenarioConfig.from_yaml(scenario_file)
        except Exception:
            config = None

        runs.append(
            RunArtifact(
                run_dir=run_dir,
                scenario_name=scenario_name,
                timestamp=timestamp,
                has_output=has_output,
                has_chart=has_chart,
                config=config,
            )
        )

    runs.sort(key=_artifact_sort_key, reverse=True)
    return runs


@dataclass
class ParityRunArtifact:
    """Metadata for one parity run artifact directory."""

    run_dir: Path
    scenario_name: str
    timestamp: str
    parity_report_path: Path
    status: str
    exit_code: int | None

    @property
    def display_name(self) -> str:
        status = self.status or "unknown"
        code = "?" if self.exit_code is None else str(self.exit_code)
        if self.timestamp:
            return f"{self.scenario_name} ({self.timestamp}) [{status}:{code}]"
        return f"{self.scenario_name} [{status}:{code}]"

    def load_report(self) -> dict[str, Any]:
        payload = json.loads(self.parity_report_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Invalid parity report JSON at {self.parity_report_path}")
        return payload


def scan_parity_artifacts(artifacts_dir: Path) -> list[ParityRunArtifact]:
    """Scan for directories containing `parity_report.json` (kept shallow for performance)."""
    artifacts_root = Path(artifacts_dir)
    if not artifacts_root.exists():
        return []

    runs: list[ParityRunArtifact] = []
    seen_dirs: set[Path] = set()
    for report_file in artifacts_root.rglob("parity_report.json"):
        parts = report_file.relative_to(artifacts_root).parts
        # Keep the scan shallow for performance, but include common grouped layouts like:
        #   artifacts/<group>/<backend>/<scenario_ts>/parity_report.json
        if len(parts) > 4:
            continue
        run_dir = report_file.parent
        if run_dir in seen_dirs:
            continue
        seen_dirs.add(run_dir)

        scenario_name, timestamp = _split_run_dir_name(run_dir)
        status = "unknown"
        exit_code: int | None = None
        try:
            report = json.loads(report_file.read_text(encoding="utf-8"))
            if isinstance(report, dict):
                status = str(report.get("status", "unknown"))
                raw_code = report.get("exit_code")
                if raw_code is not None:
                    try:
                        exit_code = int(raw_code)
                    except (TypeError, ValueError):
                        exit_code = None
        except Exception:
            status = "invalid_report"
            exit_code = None

        runs.append(
            ParityRunArtifact(
                run_dir=run_dir,
                scenario_name=scenario_name,
                timestamp=timestamp,
                parity_report_path=report_file,
                status=status,
                exit_code=exit_code,
            )
        )

    runs.sort(key=lambda run: run.timestamp if run.timestamp else "00000000_000000", reverse=True)
    return runs
