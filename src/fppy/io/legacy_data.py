"""Parsers for legacy FAIR template data blocks."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from fppy.config import default_legacy_config

_NUMBER_RE = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?")
_WORD_RE = re.compile(r"[^\s;]+")


@dataclass(frozen=True)
class LegacyTemplateBundle:
    """Parsed legacy template artifacts loaded from default paths."""

    fmdata: pd.DataFrame
    fmage: pd.DataFrame
    fmexog: pd.DataFrame


def generate_smpl_period_index(start: str | float | int, end: str | float | int) -> pd.Index:
    """Generate a quarter-period index from an inclusive SMPL window."""

    start_year, start_q = _parse_smpl_token(str(start))
    end_year, end_q = _parse_smpl_token(str(end))
    start_code = _smpl_code(start_year, start_q)
    end_code = _smpl_code(end_year, end_q)

    if end_code < start_code:
        raise ValueError("SMPL end period must not come before start period.")

    periods: list[str] = []
    for code in range(start_code, end_code + 1):
        year, quarter = _smpl_from_code(code)
        periods.append(f"{year}.{quarter}")

    return pd.Index(periods, name="smpl")


def parse_fmdata_text(text: str) -> pd.DataFrame:
    """Parse legacy `fmdata` text into a wide pandas frame."""

    return _parse_fm_numeric_file(text, block_name="fmdata")


def parse_fmdata_file(path: Path | str) -> pd.DataFrame:
    """Parse an `fmdata` file from disk."""

    text = _read_text(path)
    return parse_fmdata_text(text)


def parse_fmage_text(text: str) -> pd.DataFrame:
    """Parse legacy `fmage` text into a wide pandas frame."""

    return _parse_fm_numeric_file(text, block_name="fmage")


def parse_fmage_file(path: Path | str) -> pd.DataFrame:
    """Parse an `fmage` file from disk."""

    text = _read_text(path)
    return parse_fmage_text(text)


def parse_fmexog_text(text: str) -> pd.DataFrame:
    """Parse legacy `fmexog` CHANGEVAR instructions into a structured frame."""

    rows: list[dict[str, object]] = []
    lines = text.splitlines()

    current_window_start: str | None = None
    current_window_end: str | None = None
    current_window_size: int | None = None
    in_changevar = False

    pending_scalar = False
    pending_variable: str | None = None
    pending_method: str | None = None
    pending_values: list[float] = []
    pending_vector = False

    def _close_pending() -> None:
        nonlocal \
            pending_scalar, \
            pending_variable, \
            pending_method, \
            pending_values, \
            pending_vector, \
            rows
        if pending_variable is None:
            return
        if current_window_start is None or current_window_end is None:
            raise ValueError("CHANGEVAR instruction found outside an SMPL window.")
        if not pending_values:
            raise ValueError(f"No values parsed for instruction {pending_variable}.")
        rows.append({
            "window_start": current_window_start,
            "window_end": current_window_end,
            "variable": pending_variable,
            "method": pending_method,
            "is_vector": bool(pending_vector),
            "n_values": len(pending_values),
            "values": tuple(pending_values),
        })
        pending_scalar = False
        pending_variable = None
        pending_method = None
        pending_values = []
        pending_vector = False

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            if pending_vector:
                continue
            if pending_scalar:
                continue
            continue
        if line.startswith("@"):  # comment marker in legacy files
            continue

        line_has_semicolon = ";" in line
        tokens = _tokens(line)
        if not tokens:
            if (pending_scalar or pending_vector) and line_has_semicolon:
                _close_pending()
            continue

        first = tokens[0].upper()
        if first == "SMPL":
            if len(tokens) < 3:
                raise ValueError("SMPL statements require start and end periods.")
            _close_pending()
            current_window_start = _normalize_period_token(tokens[1])
            current_window_end = _normalize_period_token(tokens[2])
            # Require valid window ordering.
            current_window_size = len(
                generate_smpl_period_index(current_window_start, current_window_end)
            )
            in_changevar = False
            continue

        if first == "CHANGEVAR":
            if current_window_start is None:
                raise ValueError("CHANGEVAR encountered before SMPL statement.")
            _close_pending()
            in_changevar = True
            continue

        if first == "RETURN":
            _close_pending()
            break

        if not in_changevar:
            continue

        if pending_scalar or pending_vector:
            # CHANGEVAR instructions can list values on subsequent lines. In
            # modern FAIR scenario decks we also see vector blocks that omit a
            # ';' on the header line (for example `JGPHASE ADDDIFABS` followed
            # by one numeric value per period). Treat contiguous numeric-only
            # lines as values for the active instruction and promote scalar
            # instructions to vector when multiple values are observed.
            if not _is_numeric_token(tokens[0]):
                # Next instruction header encountered.
                _close_pending()
                # Fall through to parse this line as a new instruction header.
            else:
                values = _extract_numbers(line)
                if values:
                    pending_values.extend(values)
                    if pending_scalar and len(pending_values) > 1:
                        pending_scalar = False
                        pending_vector = True

                if (
                    pending_vector
                    and current_window_size is not None
                    and len(pending_values) >= current_window_size
                ):
                    if len(pending_values) > current_window_size:
                        raise ValueError(
                            "Too many values parsed for vector instruction "
                            f"{pending_variable}: expected {current_window_size}, found {len(pending_values)}."
                        )
                    _close_pending()
                    continue

                if line_has_semicolon:
                    _close_pending()
                continue

        # A new instruction header.
        if _is_legacy_end_marker(line):
            continue

        variable = tokens[0]
        method: str | None = None
        is_vector = False
        value_text = line

        if len(tokens) >= 2:
            # Method present only when the second token is non-numeric and non-delimiter.
            if not _is_numeric_token(tokens[1]):
                method = tokens[1].strip(";'\"")
                value_text = line[line.find(method) + len(method) :]
                if line_has_semicolon:
                    is_vector = True

        if not line_has_semicolon and len(tokens) == 1:
            # Scalar with method omitted or with explicit scalar value on this line.
            is_vector = False
        elif line_has_semicolon and len(tokens) == 1:
            # VAR ; vector with no method
            is_vector = True

        extracted = _extract_numbers(value_text)

        if is_vector:
            pending_scalar = False
            pending_vector = True
            pending_variable = variable
            pending_method = method
            pending_values = list(extracted)
            if pending_values and line_has_semicolon:
                _close_pending()
            continue

        pending_scalar = True
        pending_vector = False
        pending_variable = variable
        pending_method = method
        pending_values = list(extracted)

        if pending_values:
            _close_pending()

    _close_pending()

    return pd.DataFrame(
        rows,
        columns=[
            "window_start",
            "window_end",
            "variable",
            "method",
            "is_vector",
            "n_values",
            "values",
        ],
    )


def parse_fmexog_file(path: Path | str) -> pd.DataFrame:
    """Parse a legacy `fmexog` file from disk."""

    text = _read_text(path)
    return parse_fmexog_text(text)


def load_default_legacy_templates() -> LegacyTemplateBundle:
    """Load `fmdata`, `fmage`, and `fmexog` from default template paths."""

    paths = default_legacy_config().legacy
    return LegacyTemplateBundle(
        fmdata=parse_fmdata_file(paths.fmdata),
        fmage=parse_fmage_file(paths.fmage),
        fmexog=parse_fmexog_file(paths.fmexog),
    )


def _parse_fm_numeric_file(text: str, *, block_name: str) -> pd.DataFrame:
    lines = text.splitlines()
    current_periods: pd.Index | None = None
    series_by_variable: dict[str, pd.Series] = {}

    i = 0
    while i < len(lines):
        raw_line = lines[i].strip()
        i += 1

        if not raw_line or raw_line.startswith("@"):
            continue

        tokens = _tokens(raw_line)
        if not tokens:
            continue

        command = tokens[0].upper()
        if command == "SMPL":
            if len(tokens) < 3:
                raise ValueError("SMPL statement missing boundaries.")
            current_periods = generate_smpl_period_index(tokens[1], tokens[2])
            continue

        if command != "LOAD":
            continue

        if current_periods is None:
            raise ValueError(f"LOAD block for {block_name} before SMPL statement.")
        if len(tokens) < 2:
            raise ValueError(f"LOAD block in {block_name} missing variable name.")

        variable = tokens[1]
        values: list[float] = []
        closed = False

        while i < len(lines):
            current = lines[i].strip()
            if not current:
                i += 1
                continue
            if _is_end_marker(current):
                closed = True
                i += 1
                break

            numbers = _extract_numbers(current)
            if not numbers:
                raise ValueError(f"Expected numeric values in {block_name} LOAD {variable}.")
            values.extend(numbers)
            i += 1

        if not closed:
            raise ValueError(
                f"Unterminated LOAD block for {variable} in {block_name} block "
                f"{current_periods[0]}..{current_periods[-1]}."
            )

        if len(values) != len(current_periods):
            raise ValueError(
                f"Value count mismatch for {block_name} variable {variable}: "
                f"expected {len(current_periods)} values, found {len(values)}."
            )

        _set_block_values(series_by_variable, variable, current_periods, values)

    if not series_by_variable:
        return pd.DataFrame()

    frame = pd.concat(series_by_variable, axis=1).sort_index()
    frame.index.name = "smpl"
    return frame


def _set_block_values(
    series_by_variable: dict[str, pd.Series],
    variable: str,
    periods: pd.Index,
    values: list[float],
) -> None:
    block = pd.Series(values, index=periods, name=variable, dtype="float64")
    existing = series_by_variable.get(variable)

    if existing is None:
        series_by_variable[variable] = block
        return

    all_index = existing.index.union(periods)
    updated = existing.reindex(all_index)
    updated.loc[periods] = block
    series_by_variable[variable] = updated.astype("float64")


def _read_text(path: Path | str) -> str:
    return Path(path).read_text(encoding="utf-8", errors="replace")


def _tokens(line: str) -> list[str]:
    return [token.strip("'\"") for token in _WORD_RE.findall(line)]


def _extract_numbers(line: str) -> list[float]:
    return [float(match.group(0)) for match in _NUMBER_RE.finditer(line)]


def _parse_smpl_token(token: str) -> tuple[int, int]:
    cleaned = token.strip().strip("'\"").strip()
    if "." not in cleaned:
        raise ValueError(f"Invalid SMPL token {token!r}; expected <year>.<quarter>.")

    year_text, quarter_text = cleaned.split(".", 1)
    if not year_text or not quarter_text:
        raise ValueError(f"Invalid SMPL token {token!r}; expected <year>.<quarter>.")

    if not year_text.lstrip("+").replace("-", "", 1).isdigit():
        raise ValueError(f"Invalid SMPL year token {token!r}.")

    if not quarter_text.isdigit():
        raise ValueError(f"Invalid SMPL quarter token {token!r}.")

    year = int(year_text)
    quarter = int(quarter_text)

    if quarter not in (1, 2, 3, 4):
        raise ValueError(f"SMPL quarter must be in 1..4; got {quarter} from {token!r}.")

    return year, quarter


def _normalize_period_token(token: str) -> str:
    year, quarter = _parse_smpl_token(token)
    return f"{year}.{quarter}"


def _smpl_code(year: int, quarter: int) -> int:
    return year * 4 + quarter - 1


def _smpl_from_code(code: int) -> tuple[int, int]:
    year, remainder = divmod(code, 4)
    return year, remainder + 1


def _is_legacy_end_marker(line: str) -> bool:
    return bool(re.fullmatch(r"[^A-Za-z0-9]*END[^A-Za-z0-9]*", line.strip(), re.IGNORECASE))


def _is_end_marker(line: str) -> bool:
    return bool(re.fullmatch(r"[^A-Za-z0-9]*END[^A-Za-z0-9]*", line.strip(), re.IGNORECASE))


def _is_numeric_token(token: str) -> bool:
    return bool(_NUMBER_RE.fullmatch(token.strip("'\"")))
