"""Discover scenario run directories under an artifacts root."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from fp_wraptr.scenarios.config import ScenarioConfig

_RUN_DIR_RE = re.compile(r"^(?P<scenario_name>.+)_(?P<timestamp>\d{8}_\d{6})$")


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


def _split_run_dir_name(run_dir: Path) -> tuple[str, str]:
    """Split `<scenario>_<YYYYMMDD_HHMMSS>` into scenario name and timestamp."""
    match = _RUN_DIR_RE.match(run_dir.name)
    if match:
        return match.group("scenario_name"), match.group("timestamp")
    return run_dir.name, ""


def _artifact_sort_key(run: RunArtifact) -> str:
    return run.timestamp if run.timestamp else "00000000_000000"


def latest_runs(
    runs: list[RunArtifact], *, limit: int = 3, has_output: bool = False
) -> list[RunArtifact]:
    """Pick latest N runs, optionally constrained to runs with output."""
    selected = [run for run in runs if (run.has_output if has_output else True)]
    selected.sort(key=_artifact_sort_key, reverse=True)
    return selected[: max(0, int(limit))]


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
