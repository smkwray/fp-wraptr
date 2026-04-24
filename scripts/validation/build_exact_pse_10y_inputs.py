#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from collections import OrderedDict
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ARTIFACTS_ROOT = REPO_ROOT / "artifacts-pse2026"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "projects_local" / "pse2025_exact_10y"

TARGET_END = "2034.4"
SOURCE_END = "2029.4"

PSE_RUN_IDS = (
    "pse2025_base",
    "pse2025_base_rsexog",
    "pse2025_high_20h_3pct",
    "pse2025_low_20h_3pct",
    "pse2025_high_15h_3pct",
    "pse2025_low_15h_3pct",
    "pse2025_high_20h_3pct_rsexog",
    "pse2025_low_20h_3pct_rsexog",
    "pse2025_high_15h_3pct_rsexog",
    "pse2025_low_15h_3pct_rsexog",
)

SUPPORT_FILES = (
    "fmage.txt",
    "fmdata.txt",
    "fmexog.txt",
    "intgadj.txt",
    "pse_common.txt",
    "ptcoef.txt",
)

NUMERIC_LINE_RE = r"^[+\-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+\-]?\d+)?$"


def _period_to_parts(token: str) -> tuple[int, int]:
    year_text, quarter_text = token.split(".", 1)
    return int(year_text), int(quarter_text)


def _parts_to_period(year: int, quarter: int) -> str:
    return f"{year}.{quarter}"


def _next_period(token: str) -> str:
    year, quarter = _period_to_parts(token)
    if quarter >= 4:
        return _parts_to_period(year + 1, 1)
    return _parts_to_period(year, quarter + 1)


def _period_range(start_exclusive: str, end_inclusive: str) -> list[str]:
    out: list[str] = []
    current = start_exclusive
    while current != end_inclusive:
        current = _next_period(current)
        out.append(current)
    return out


def _latest_source_run_dir(run_id: str, artifacts_root: Path) -> Path:
    candidates = sorted(
        (
            path
            for path in artifacts_root.glob(f"{run_id}_*")
            if path.is_dir() and (path / "scenario.yaml").exists() and (path / "work").exists()
        ),
        key=lambda path: path.name,
    )
    if not candidates:
        raise FileNotFoundError(
            f"Missing source artifact for {run_id} under {artifacts_root}"
        )
    return candidates[-1]


def _copy_support_tree(*, source_work_dir: Path, dest_overlay_dir: Path, entry_input_file: str) -> None:
    if dest_overlay_dir.exists():
        shutil.rmtree(dest_overlay_dir)
    dest_overlay_dir.mkdir(parents=True, exist_ok=True)
    for filename in (*SUPPORT_FILES, entry_input_file):
        source_path = source_work_dir / filename
        if not source_path.exists():
            raise FileNotFoundError(f"Missing required support file: {source_path}")
        shutil.copy2(source_path, dest_overlay_dir / filename)


def _extend_repeated_series_block(text: str, *, name: str, total_values: int) -> str:
    import re

    lines = text.splitlines()
    header_index = next((idx for idx, line in enumerate(lines) if line.strip() == f"{name} ;"), -1)
    if header_index < 0:
        return text

    values_start = header_index + 1
    values_end = values_start
    while values_end < len(lines):
        stripped = lines[values_end].strip()
        if not stripped or not re.fullmatch(NUMERIC_LINE_RE, stripped):
            break
        values_end += 1
    values = [line.strip() for line in lines[values_start:values_end] if line.strip()]
    if not values:
        return text

    if len(values) < total_values:
        values.extend([values[-1]] * (total_values - len(values)))

    prefix = ""
    if values_start < len(lines):
        original_line = lines[values_start]
        prefix = original_line[: len(original_line) - len(original_line.lstrip(" "))]

    rebuilt = [lines[header_index], *[f"{prefix}{value}" for value in values]]
    next_lines = [*lines[:header_index], *rebuilt, *lines[values_end:]]
    return "\n".join(next_lines) + "\n"


def _extend_constant_changevar_series(text: str, *, name: str, sample_start: str, sample_end: str) -> str:
    lines = text.splitlines()
    sample_index = next(
        (
            idx
            for idx, line in enumerate(lines)
            if line.strip().replace(";", "") == f"SMPL {sample_start} {SOURCE_END}"
        ),
        -1,
    )
    if sample_index < 0:
        return text

    lines[sample_index] = f"SMPL {sample_start} {sample_end};"
    header_index = next(
        (idx for idx in range(sample_index + 1, len(lines)) if lines[idx].strip() == f"{name} SAMEVALUE"),
        -1,
    )
    if header_index < 0:
        return "\n".join(lines) + "\n"

    value_start = header_index + 1
    value_end = value_start
    import re

    while value_end < len(lines) and re.fullmatch(NUMERIC_LINE_RE, lines[value_end].strip()):
        value_end += 1
    values = [line.strip() for line in lines[value_start:value_end] if line.strip()]
    if not values:
        return "\n".join(lines) + "\n"

    total_values = len(_period_range(_previous_period(sample_start), sample_end))
    if len(values) < total_values:
        values.extend([values[-1]] * (total_values - len(values)))

    prefix = lines[value_start][: len(lines[value_start]) - len(lines[value_start].lstrip(" "))] if value_start < len(lines) else ""
    rebuilt = [f"{prefix}{value}" for value in values]
    next_lines = [*lines[:value_start], *rebuilt, *lines[value_end:]]
    return "\n".join(next_lines) + "\n"


def _previous_period(token: str) -> str:
    year, quarter = _period_to_parts(token)
    if quarter <= 1:
        return _parts_to_period(year - 1, 4)
    return _parts_to_period(year, quarter - 1)


def _format_load_value(value: float) -> str:
    return f"{value:.11E}"


def _wrap_load_values(values: list[float], *, indent: str = "   ", per_line: int = 4) -> list[str]:
    out: list[str] = []
    for index in range(0, len(values), per_line):
        chunk = values[index:index + per_line]
        out.append(indent + "  ".join(_format_load_value(value) for value in chunk))
    return out


def _extend_fmage(text: str) -> str:
    periods_to_add = len(_period_range(SOURCE_END, TARGET_END))
    if periods_to_add <= 0:
        return text

    lines = text.splitlines()
    if lines:
        lines[0] = lines[0].replace(SOURCE_END, TARGET_END)

    output: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        output.append(line)
        stripped = line.strip()
        if not stripped.startswith("LOAD "):
            index += 1
            continue

        index += 1
        value_lines: list[str] = []
        while index < len(lines):
            candidate = lines[index]
            if candidate.strip() == "'END'":
                break
            value_lines.append(candidate)
            index += 1
        values: list[float] = []
        for value_line in value_lines:
            for token in value_line.split():
                values.append(float(token))
        if len(values) >= 2:
            delta = values[-1] - values[-2]
            last_value = values[-1]
            for _ in range(periods_to_add):
                last_value = last_value + delta
                values.append(last_value)
        output.extend(_wrap_load_values(values))
        if index < len(lines):
            output.append(lines[index])
        index += 1

    return "\n".join(output).rstrip() + "\n"


def _extend_adddifabs_list(text: str, *, variable_name: str, total_values: int, fill_value: str) -> str:
    lines = text.splitlines()
    header_index = next(
        (
            idx
            for idx, line in enumerate(lines)
            if line.strip() == f"{variable_name} ADDDIFABS"
        ),
        -1,
    )
    if header_index < 0:
        return text

    value_start = header_index + 1
    value_end = value_start
    import re

    while value_end < len(lines):
        stripped = lines[value_end].strip()
        if stripped == ";":
            break
        if not stripped or not re.fullmatch(NUMERIC_LINE_RE, stripped):
            break
        value_end += 1

    values = [line.strip() for line in lines[value_start:value_end] if line.strip()]
    if len(values) < total_values:
        values.extend([fill_value] * (total_values - len(values)))
    next_lines = [*lines[:value_start], *values, *lines[value_end:]]
    return "\n".join(next_lines) + "\n"


def _extend_single_period_fmexog_blocks(text: str, *, target_end: str) -> str:
    lines = text.splitlines()
    last_blocks: OrderedDict[str, list[str]] = OrderedDict()
    latest_period = SOURCE_END
    index = 0
    while index < len(lines):
        line = lines[index].strip()
        if not line.startswith("SMPL "):
            index += 1
            continue
        parts = line.replace(";", "").split()
        if len(parts) != 3 or parts[1] != parts[2]:
            index += 1
            continue
        period = parts[1]
        if index + 2 >= len(lines) or lines[index + 1].strip() != "CHANGEVAR;":
            index += 1
            continue
        body: list[str] = []
        cursor = index + 2
        while cursor < len(lines):
            body.append(lines[cursor])
            if lines[cursor].strip() == ";":
                break
            cursor += 1
        if not body or body[0].strip() == ";" or body[-1].strip() != ";":
            index = cursor + 1
            continue
        head_tokens = body[0].split()
        if head_tokens:
            variable_name = head_tokens[0]
            last_blocks[variable_name] = body[:-1]
            latest_period = max(latest_period, period, key=_period_to_parts)
        index = cursor + 1

    extension_periods = _period_range(latest_period, target_end)
    if not extension_periods or not last_blocks:
        return text

    lines = text.splitlines()
    return_index = next((idx for idx, line in enumerate(lines) if line.strip() == "RETURN;"), -1)
    base_lines = lines[:] if return_index < 0 else lines[:return_index]
    suffix_lines = [] if return_index < 0 else lines[return_index:]

    appended: list[str] = [*base_lines]
    if appended and appended[-1].strip():
        appended.append("")
    for variable_name, body in last_blocks.items():
        for period in extension_periods:
            appended.append(f"SMPL {period} {period};")
            appended.append("CHANGEVAR;")
            appended.extend(body)
            appended.append(";")
        appended.append("")
    if suffix_lines:
        appended.extend(suffix_lines)
    return "\n".join(appended).rstrip() + "\n"


def _patch_top_level_input(text: str) -> str:
    replacements = {
        "LASTPER=2029.4;": f"LASTPER={TARGET_END};",
        "SMPL 2025.4 2029.4;": f"SMPL 2025.4 {TARGET_END};",
        "SMPL 1952.1 2029.4;": f"SMPL 1952.1 {TARGET_END};",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    total_values = len(_period_range(_previous_period("2025.4"), TARGET_END))
    text = _extend_adddifabs_list(text, variable_name="JGPHASE", total_values=total_values, fill_value="1")
    text = _extend_adddifabs_list(text, variable_name="JGCOLA", total_values=total_values, fill_value="0")
    return text


def _patch_pse_common(text: str) -> str:
    replacements = {
        "forecast through 2029.4": f"forecast through {TARGET_END}",
        "SMPL 1952.1 2029.4;": f"SMPL 1952.1 {TARGET_END};",
        "SMPL 2025.4 2029.4;": f"SMPL 2025.4 {TARGET_END};",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def _patch_fmexog(text: str) -> str:
    replacements = {
        "SMPL 2026.1 2029.4;": f"SMPL 2026.1 {TARGET_END};",
        "SMPL 2025.4 2029.4;": f"SMPL 2025.4 {TARGET_END};",
        "JGPHASE ADDDIFABS": "JGPHASE SAMEVALUE",
        "JGWPHASE ADDDIFABS": "JGWPHASE SAMEVALUE",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)

    quarter_count = len(_period_range("2025.4", TARGET_END))
    for name in ("STAT", "INTS", "INTF"):
        text = _extend_repeated_series_block(text, name=name, total_values=quarter_count)
    return _extend_single_period_fmexog_blocks(text, target_end=TARGET_END)


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _write_scenario_yaml(
    *,
    scenario_path: Path,
    source_scenario: dict[str, object],
    scenario_name: str,
    overlay_dir: Path,
    input_file: str,
    artifacts_root: str,
) -> None:
    payload = {
        "name": scenario_name,
        "description": source_scenario.get("description", ""),
        "fp_home": source_scenario.get("fp_home", ""),
        "input_overlay_dir": str(overlay_dir),
        "input_file": input_file,
        "forecast_start": source_scenario.get("forecast_start", "2025.4"),
        "forecast_end": TARGET_END,
        "backend": source_scenario.get("backend", "fpexe"),
        "fppy": source_scenario.get("fppy", {}),
        "fpr": source_scenario.get("fpr", {}),
        "overrides": source_scenario.get("overrides", {}),
        "track_variables": source_scenario.get("track_variables", []),
        "input_patches": source_scenario.get("input_patches", {}),
        "alerts": source_scenario.get("alerts", {}),
        "extra": source_scenario.get("extra", {}),
        "artifacts_root": artifacts_root,
    }
    scenario_path.parent.mkdir(parents=True, exist_ok=True)
    scenario_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def build_exact_inputs(
    *,
    output_root: Path,
    artifacts_root: str,
    source_artifacts_root: Path,
) -> list[Path]:
    scenarios_root = output_root / "scenarios"
    overlays_root = output_root / "overlays"
    created: list[Path] = []

    for run_id in PSE_RUN_IDS:
        source_run_dir = _latest_source_run_dir(run_id, source_artifacts_root)
        source_work_dir = source_run_dir / "work"
        source_scenario_path = source_run_dir / "scenario.yaml"
        source_scenario = yaml.safe_load(source_scenario_path.read_text(encoding="utf-8")) or {}
        input_file = str(source_scenario.get("input_file", "") or "").strip()
        if not input_file:
            raise ValueError(f"scenario.yaml missing input_file: {source_scenario_path}")

        scenario_name = f"{run_id}_10y"
        overlay_dir = overlays_root / scenario_name
        _copy_support_tree(
            source_work_dir=source_work_dir,
            dest_overlay_dir=overlay_dir,
            entry_input_file=input_file,
        )

        _write_text(
            overlay_dir / input_file,
            _patch_top_level_input((overlay_dir / input_file).read_text(encoding="utf-8")),
        )
        _write_text(
            overlay_dir / "pse_common.txt",
            _patch_pse_common((overlay_dir / "pse_common.txt").read_text(encoding="utf-8")),
        )
        _write_text(
            overlay_dir / "fmexog.txt",
            _patch_fmexog((overlay_dir / "fmexog.txt").read_text(encoding="utf-8")),
        )
        _write_text(
            overlay_dir / "intgadj.txt",
            _extend_constant_changevar_series(
                (overlay_dir / "intgadj.txt").read_text(encoding="utf-8"),
                name="INTGADJ",
                sample_start="2025.4",
                sample_end=TARGET_END,
            ),
        )
        _write_text(
            overlay_dir / "fmage.txt",
            _extend_fmage((overlay_dir / "fmage.txt").read_text(encoding="utf-8")),
        )

        scenario_path = scenarios_root / f"{scenario_name}.yaml"
        _write_scenario_yaml(
            scenario_path=scenario_path,
            source_scenario=source_scenario,
            scenario_name=scenario_name,
            overlay_dir=overlay_dir,
            input_file=input_file,
            artifacts_root=artifacts_root,
        )
        created.append(scenario_path)
    return created


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build exact 10-year PSE input trees from the published 5-year artifacts."
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Directory where overlays and scenario yamls will be written.",
    )
    parser.add_argument(
        "--artifacts-root",
        default="artifacts-pse2026",
        help="Artifacts root written into the generated scenario yamls.",
    )
    parser.add_argument(
        "--source-artifacts-root",
        type=Path,
        default=SOURCE_ARTIFACTS_ROOT,
        help="Directory containing the latest 5-year source artifacts to extend.",
    )
    args = parser.parse_args()

    scenario_paths = build_exact_inputs(
        output_root=args.output_root.resolve(),
        artifacts_root=str(args.artifacts_root),
        source_artifacts_root=args.source_artifacts_root.resolve(),
    )
    for path in scenario_paths:
        print(path)


if __name__ == "__main__":
    main()
