#!/usr/bin/env python3
from __future__ import annotations

from collections import OrderedDict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
OVERLAYS_ROOT = REPO_ROOT / "do" / "pse_rebased_2026" / "overlays"
FMAGE_SOURCE = REPO_ROOT / "FM" / "FMAGE.TXT"

TARGET_END = "2034.4"
SOURCE_END = "2029.4"
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

    return_index = next((idx for idx, line in enumerate(lines) if line.strip() == "RETURN;"), -1)
    base_lines = lines[:] if return_index < 0 else lines[:return_index]
    suffix_lines = [] if return_index < 0 else lines[return_index:]

    appended: list[str] = [*base_lines]
    if appended and appended[-1].strip():
        appended.append("")
    for _, body in last_blocks.items():
        for period in extension_periods:
            appended.append(f"SMPL {period} {period};")
            appended.append("CHANGEVAR;")
            appended.extend(body)
            appended.append(";")
        appended.append("")
    if suffix_lines:
        appended.extend(suffix_lines)
    return "\n".join(appended).rstrip() + "\n"


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


def patch_rebased_10y_inputs() -> list[Path]:
    if not OVERLAYS_ROOT.exists():
        raise FileNotFoundError(f"Missing overlays root: {OVERLAYS_ROOT}")
    if not FMAGE_SOURCE.exists():
        raise FileNotFoundError(f"Missing FMAGE source: {FMAGE_SOURCE}")

    fmage_text = _extend_fmage(FMAGE_SOURCE.read_text(encoding="utf-8"))
    updated: list[Path] = []
    for overlay_dir in sorted(path for path in OVERLAYS_ROOT.glob("*_10y") if path.is_dir()):
        fmexog_path = overlay_dir / "fmexog.txt"
        if not fmexog_path.exists():
            raise FileNotFoundError(f"Missing fmexog in overlay: {overlay_dir}")
        fmexog_path.write_text(_patch_fmexog(fmexog_path.read_text(encoding="utf-8")), encoding="utf-8")
        (overlay_dir / "fmage.txt").write_text(fmage_text, encoding="utf-8")
        updated.append(overlay_dir)
    return updated


def main() -> None:
    updated = patch_rebased_10y_inputs()
    for path in updated:
        print(path)


if __name__ == "__main__":
    main()
