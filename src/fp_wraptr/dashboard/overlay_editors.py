"""Scenario-card registry for overlay-backed dashboard editors."""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import streamlit as st
import yaml

from fp_wraptr.dashboard.ptcoef_tools import load_ptcoef_deck, write_ptcoef_overlay
from fp_wraptr.dashboard.scenario_tools import ScenarioInputPreflight
from fp_wraptr.data.series_pipeline.fp_targets import render_changevar_include
from fp_wraptr.hygiene import find_project_root
from fp_wraptr.io.input_parser import parse_fmexog_text
from fp_wraptr.scenarios.config import ScenarioConfig

_CARD_SPECS_ROOT = Path("projects_local") / "cards"
_CREATE_ASSIGN_RE = re.compile(
    r"^(?P<prefix>\s*CREATE\s+)(?P<name>[A-Za-z][A-Za-z0-9_]*)(?P<sep>\s*=\s*)"
    r"(?P<value>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][-+]?\d+)?)"
    r"(?P<suffix>\s*;\s*)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ScenarioCardSection:
    """Optional grouped symbols for constants cards."""

    title: str
    symbols: tuple[str, ...]


@dataclass(frozen=True)
class ScenarioCardSpec:
    """Declarative scenario-card definition for dashboard authoring surfaces."""

    card_id: str
    label: str
    description: str
    capability: str
    order: int = 0
    required_includes: tuple[str, ...] = ()
    source_file: str | None = None
    target_file: str | None = None
    symbols: tuple[str, ...] = ()
    sections: tuple[ScenarioCardSection, ...] = ()
    series_variable: str | None = None
    series_method: str | None = None
    series_mode: str | None = None
    smpl_start: str | None = None
    smpl_end: str | None = None
    spec_path: Path | None = None


@dataclass(frozen=True)
class _CreateEntry:
    name: str
    value: float
    line_index: int
    prefix: str
    sep: str
    suffix: str


@dataclass(frozen=True)
class _SeriesSnapshot:
    variable: str
    method: str
    value: float
    smpl_start: str
    smpl_end: str


_LEGACY_PTCOEF_CARD = ScenarioCardSpec(
    card_id="deck.ptcoef",
    label="Coefficient Deck",
    description="Structured overlay editor for coefficient decks referenced by the staged input tree.",
    capability="deck_table",
    order=10,
    required_includes=("ptcoef.txt",),
    source_file="ptcoef.txt",
)


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_repo_root(config: ScenarioConfig, preflight: ScenarioInputPreflight) -> Path:
    candidates: list[Path] = []
    if config.input_overlay_dir is not None:
        candidates.append(Path(config.input_overlay_dir).expanduser())
    if preflight.overlay_dir is not None:
        candidates.append(Path(preflight.overlay_dir).expanduser())
    if preflight.entry_source_path is not None:
        candidates.append(Path(preflight.entry_source_path).expanduser())

    for candidate in candidates:
        root = find_project_root(candidate)
        if root is not None:
            return root.resolve()
    return _default_repo_root()


def _coerce_str(raw: Any) -> str:
    if raw is None:
        return ""
    return str(raw).strip()


def _coerce_symbol_list(raw: Any) -> tuple[str, ...]:
    if not isinstance(raw, list):
        return ()
    out: list[str] = []
    for item in raw:
        token = _coerce_str(item).upper()
        if token:
            out.append(token)
    return tuple(out)


def _parse_sections(raw: Any) -> tuple[ScenarioCardSection, ...]:
    if not isinstance(raw, list):
        return ()
    sections: list[ScenarioCardSection] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        title = _coerce_str(item.get("title"))
        symbols = _coerce_symbol_list(item.get("symbols"))
        if not title or not symbols:
            continue
        sections.append(ScenarioCardSection(title=title, symbols=symbols))
    return tuple(sections)


def _parse_card_spec(path: Path, raw: Any) -> ScenarioCardSpec:
    if not isinstance(raw, dict):
        raise ValueError(f"Card spec {path} must be a mapping")

    card_id = _coerce_str(raw.get("card_id"))
    if not card_id:
        raise ValueError(f"Card spec {path} is missing required field 'card_id'")
    label = _coerce_str(raw.get("label")) or card_id
    description = _coerce_str(raw.get("description"))
    capability = _coerce_str(raw.get("capability"))
    if not capability:
        raise ValueError(f"Card spec {path} is missing required field 'capability'")

    required_includes = tuple(
        token.lower()
        for token in _coerce_symbol_list(raw.get("required_includes"))
        if token
    )
    sections = _parse_sections(raw.get("sections"))
    symbols = _coerce_symbol_list(raw.get("symbols") or raw.get("constants"))
    if not symbols and sections:
        flattened: list[str] = []
        for section in sections:
            for symbol in section.symbols:
                if symbol not in flattened:
                    flattened.append(symbol)
        symbols = tuple(flattened)

    series = raw.get("series") if isinstance(raw.get("series"), dict) else {}

    return ScenarioCardSpec(
        card_id=card_id,
        label=label,
        description=description,
        capability=capability,
        order=int(raw.get("order") or 0),
        required_includes=required_includes,
        source_file=_coerce_str(raw.get("source_file")) or None,
        target_file=_coerce_str(raw.get("target_file")) or None,
        symbols=symbols,
        sections=sections,
        series_variable=_coerce_str(series.get("variable")).upper() or None,
        series_method=_coerce_str(series.get("fp_method")).upper() or None,
        series_mode=_coerce_str(series.get("mode")).lower() or None,
        smpl_start=_coerce_str(series.get("smpl_start")) or None,
        smpl_end=_coerce_str(series.get("smpl_end")) or None,
        spec_path=path,
    )


def _infer_family(config: ScenarioConfig, preflight: ScenarioInputPreflight) -> str | None:
    if config.input_overlay_dir is not None:
        token = Path(config.input_overlay_dir).name.strip().lower()
        if token:
            return token
    if preflight.overlay_dir is not None:
        token = Path(preflight.overlay_dir).name.strip().lower()
        if token:
            return token
    return None


def _load_family_specs(*, family: str, repo_root: Path) -> list[ScenarioCardSpec]:
    if not family:
        return []
    spec_dir = repo_root / _CARD_SPECS_ROOT / family
    if not spec_dir.exists():
        return []

    specs: list[ScenarioCardSpec] = []
    candidates = sorted(spec_dir.glob("*.yaml")) + sorted(spec_dir.glob("*.yml"))
    for path in candidates:
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))
            spec = _parse_card_spec(path, payload)
            specs.append(spec)
        except Exception:
            continue
    return sorted(specs, key=lambda item: (item.order, item.label.lower(), item.card_id))


def available_scenario_cards(
    config: ScenarioConfig,
    preflight: ScenarioInputPreflight,
) -> list[ScenarioCardSpec]:
    if config.input_overlay_dir is None or not preflight.ok:
        return []

    include_names = {name.lower() for name in preflight.include_files}
    family = _infer_family(config, preflight)
    repo_root = _resolve_repo_root(config, preflight)

    cards: list[ScenarioCardSpec] = []
    if family is not None:
        for card in _load_family_specs(family=family, repo_root=repo_root):
            required = set(card.required_includes)
            if required and not required.issubset(include_names):
                continue
            cards.append(card)

    if not cards and "ptcoef.txt" in include_names:
        cards.append(_LEGACY_PTCOEF_CARD)

    return cards


def render_scenario_cards(
    config: ScenarioConfig,
    preflight: ScenarioInputPreflight,
) -> None:
    cards = available_scenario_cards(config, preflight)
    with st.expander("Scenario Cards", expanded=False):
        st.caption("Overlay-backed authoring cards discovered from the scenario input tree.")
        if not cards:
            st.caption("No authoring cards are available for the current scenario.")
            return
        for card in cards:
            with st.container(border=True):
                st.markdown(f"**{card.label}**")
                if card.description:
                    st.caption(card.description)
                if card.capability == "deck_table" and (card.source_file or "").lower() == "ptcoef.txt":
                    _render_ptcoef_card(config, preflight)
                elif card.capability == "create_constants":
                    _render_create_constants_card(config, card)
                elif card.capability == "series_constant":
                    _render_series_constant_card(config, card)
                else:
                    st.info(f"Card capability `{card.capability}` is not implemented yet.")
                if card.spec_path is not None:
                    st.caption(f"Spec: `{card.spec_path}`")


def _resolve_card_paths(
    *,
    config: ScenarioConfig,
    source_file: str,
    target_file: str | None = None,
) -> tuple[Path, Path, str]:
    src_name = str(source_file or "").strip()
    if not src_name:
        raise ValueError("Card is missing source_file")

    target_name = str(target_file or src_name).strip()
    fp_home = Path(config.fp_home)
    overlay = Path(config.input_overlay_dir) if config.input_overlay_dir is not None else None

    target_path = (overlay / target_name) if overlay is not None else (fp_home / target_name)
    if overlay is not None:
        overlay_source = overlay / src_name
        if overlay_source.exists():
            return overlay_source, target_path, "overlay"

    fp_source = fp_home / src_name
    if fp_source.exists():
        return fp_source, target_path, "fp_home"

    searched = [str(fp_source)]
    if overlay is not None:
        searched.insert(0, str(overlay / src_name))
    raise FileNotFoundError(f"Card source file not found (searched: {', '.join(searched)})")


def _format_number(value: float) -> str:
    return f"{float(value):.12g}"


def _timestamp_token() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _write_overlay_text(target_path: Path, text: str) -> tuple[Path | None, bool]:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        existing = target_path.read_text(encoding="utf-8", errors="replace")
        if existing == text:
            return None, False
        backup = target_path.with_name(f"{target_path.name}.bak.{_timestamp_token()}")
        shutil.copy2(target_path, backup)
    else:
        backup = None
    target_path.write_text(text, encoding="utf-8")
    return backup, True


def _parse_numeric_create_entries(text: str) -> dict[str, _CreateEntry]:
    out: dict[str, _CreateEntry] = {}
    for idx, line in enumerate(text.splitlines()):
        match = _CREATE_ASSIGN_RE.match(line)
        if match is None:
            continue
        name = match.group("name").upper()
        try:
            value = float(match.group("value"))
        except ValueError:
            continue
        out[name] = _CreateEntry(
            name=match.group("name"),
            value=value,
            line_index=idx,
            prefix=match.group("prefix"),
            sep=match.group("sep"),
            suffix=match.group("suffix"),
        )
    return out


def _render_create_updates_text(
    *,
    text: str,
    entries: dict[str, _CreateEntry],
    updates: dict[str, float],
) -> tuple[str, tuple[str, ...]]:
    lines = text.splitlines()
    changed: list[str] = []

    for raw_name, new_value in updates.items():
        name = str(raw_name).upper()
        entry = entries.get(name)
        if entry is None:
            continue
        if float(new_value) == float(entry.value):
            continue
        lines[entry.line_index] = (
            f"{entry.prefix}{entry.name}{entry.sep}{_format_number(float(new_value))}{entry.suffix}"
        )
        changed.append(name)

    rendered = "\n".join(lines)
    if text.endswith("\n"):
        rendered += "\n"
    return rendered, tuple(sorted(changed))


def _render_create_constants_card(config: ScenarioConfig, card: ScenarioCardSpec) -> None:
    source_file = card.source_file
    if not source_file:
        st.error("Card spec is missing `source_file`.")
        return

    try:
        source_path, target_path, source_kind = _resolve_card_paths(
            config=config,
            source_file=source_file,
            target_file=card.target_file,
        )
    except Exception as exc:
        st.error(f"Failed to resolve `{source_file}`: {exc}")
        return

    text = source_path.read_text(encoding="utf-8", errors="replace")
    entries = _parse_numeric_create_entries(text)
    if not entries:
        st.warning(f"No numeric `CREATE` assignments found in `{source_path}`.")
        return

    symbols = card.symbols if card.symbols else tuple(sorted(entries.keys()))
    missing_symbols = [symbol for symbol in symbols if symbol not in entries]

    st.write(f"Current source: `{source_path}` ({source_kind})")
    st.write(f"Overlay target: `{target_path}`")
    if source_kind != "overlay":
        st.warning(f"Saving will create or replace `{target_path.name}` inside `input_overlay_dir`.")

    updates: dict[str, float] = {}
    changed_lines: list[str] = []

    sections = card.sections if card.sections else (ScenarioCardSection(title="Constants", symbols=symbols),)
    for section in sections:
        st.markdown(f"**{section.title}**")
        for symbol in section.symbols:
            entry = entries.get(symbol)
            if entry is None:
                continue
            col_name, col_value = st.columns([1, 1])
            col_name.code(symbol)
            new_value = col_value.number_input(
                symbol,
                value=float(entry.value),
                format="%.8f",
                key=f"scenario_card_{card_key(config.name, card.card_id + ':' + symbol)}",
                label_visibility="collapsed",
            )
            updates[symbol] = float(new_value)
            if float(new_value) != float(entry.value):
                changed_lines.append(f"`{symbol}`: {entry.value:.8g} -> {new_value:.8g}")

    if missing_symbols:
        st.warning("Missing symbols in source file: " + ", ".join(sorted(missing_symbols)))

    if changed_lines:
        st.write("Pending changes: " + ", ".join(changed_lines))
    else:
        st.caption("No constant edits yet.")

    if config.input_overlay_dir is None:
        st.warning("Saving requires `input_overlay_dir`; this card is read-only.")
        return

    confirm_write = st.checkbox(
        f"Confirm writing `{target_path.name}` under `input_overlay_dir`",
        key=f"scenario_card_confirm_{config.name}_{card.card_id}",
    )
    if st.button("Save Overlay Changes", key=f"scenario_card_save_{config.name}_{card.card_id}"):
        if not confirm_write:
            st.error("Enable the confirmation checkbox before writing the overlay copy.")
            return

        rendered_text, changed_symbols = _render_create_updates_text(
            text=text,
            entries=entries,
            updates=updates,
        )
        if not changed_symbols:
            st.info("No constant values changed; nothing was written.")
            return

        backup_path, wrote = _write_overlay_text(target_path, rendered_text)
        if not wrote:
            st.info("Overlay already matches requested values; nothing was written.")
            return

        st.success(f"Saved `{target_path.name}` to {target_path}")
        if backup_path is not None:
            st.caption(f"Backup created: `{backup_path}`")
        st.rerun()


def _resolve_smpl_token(token: str | None, config: ScenarioConfig, fallback: str) -> str:
    raw = str(token or "").strip()
    if raw in {"${forecast_start}", "$forecast_start", "forecast_start"}:
        return str(config.forecast_start)
    if raw in {"${forecast_end}", "$forecast_end", "forecast_end"}:
        return str(config.forecast_end)
    if raw:
        return raw
    return fallback


def _load_series_snapshot(*, source_path: Path, variable: str) -> _SeriesSnapshot:
    parsed = parse_fmexog_text(source_path.read_text(encoding="utf-8", errors="replace"))
    candidates = [
        item
        for item in parsed.get("changes", [])
        if str(item.get("variable", "")).strip().upper() == variable.upper()
    ]
    if not candidates:
        raise ValueError(f"Variable {variable!r} not found in CHANGEVAR blocks")

    change = candidates[-1]
    values = change.get("values") or []
    if not values:
        raise ValueError(f"Variable {variable!r} has no values in CHANGEVAR block")

    method = str(change.get("method") or "SAMEVALUE").strip().upper()
    smpl_start = str(change.get("sample_start") or parsed.get("sample_start") or "").strip()
    smpl_end = str(change.get("sample_end") or parsed.get("sample_end") or "").strip()

    return _SeriesSnapshot(
        variable=variable.upper(),
        method=method,
        value=float(values[-1]),
        smpl_start=smpl_start,
        smpl_end=smpl_end,
    )


def _render_series_constant_card(config: ScenarioConfig, card: ScenarioCardSpec) -> None:
    source_file = card.source_file
    variable = card.series_variable
    if not source_file or not variable:
        st.error("Card spec is missing `source_file` or `series.variable`.")
        return

    try:
        source_path, target_path, source_kind = _resolve_card_paths(
            config=config,
            source_file=source_file,
            target_file=card.target_file,
        )
        snapshot = _load_series_snapshot(source_path=source_path, variable=variable)
    except Exception as exc:
        st.error(f"Failed to load `{source_file}`: {exc}")
        return

    method = card.series_method or snapshot.method
    smpl_start = _resolve_smpl_token(
        card.smpl_start,
        config,
        snapshot.smpl_start or str(config.forecast_start),
    )
    smpl_end = _resolve_smpl_token(
        card.smpl_end,
        config,
        snapshot.smpl_end or str(config.forecast_end),
    )

    st.write(f"Current source: `{source_path}` ({source_kind})")
    st.write(f"Overlay target: `{target_path}`")
    st.caption(f"Variable: `{snapshot.variable}`  Method: `{method}`  SMPL: `{smpl_start}` -> `{smpl_end}`")

    new_value = st.number_input(
        f"{snapshot.variable} value",
        value=float(snapshot.value),
        format="%.12f",
        key=f"scenario_card_{card_key(config.name, card.card_id + ':' + snapshot.variable)}",
    )

    if float(new_value) != float(snapshot.value):
        st.write(f"Pending change: `{snapshot.variable}`: {snapshot.value:.10g} -> {new_value:.10g}")
    else:
        st.caption("No series edits yet.")

    if config.input_overlay_dir is None:
        st.warning("Saving requires `input_overlay_dir`; this card is read-only.")
        return

    confirm_write = st.checkbox(
        f"Confirm writing `{target_path.name}` under `input_overlay_dir`",
        key=f"scenario_card_confirm_{config.name}_{card.card_id}",
    )
    if st.button("Save Overlay Changes", key=f"scenario_card_save_{config.name}_{card.card_id}"):
        if not confirm_write:
            st.error("Enable the confirmation checkbox before writing the overlay copy.")
            return

        rendered = render_changevar_include(
            variable=snapshot.variable,
            fp_method=method,
            smpl_start=smpl_start,
            smpl_end=smpl_end,
            values=[float(new_value)],
            mode=card.series_mode or "constant",
        )

        backup_path, wrote = _write_overlay_text(target_path, rendered)
        if not wrote:
            st.info("Overlay already matches requested values; nothing was written.")
            return

        st.success(f"Saved `{target_path.name}` to {target_path}")
        if backup_path is not None:
            st.caption(f"Backup created: `{backup_path}`")
        st.rerun()


def _render_ptcoef_card(
    config: ScenarioConfig,
    preflight: ScenarioInputPreflight,
) -> None:
    _ = preflight
    try:
        deck = load_ptcoef_deck(overlay_dir=config.input_overlay_dir, fp_home=config.fp_home)
    except Exception as exc:
        st.error(f"Failed to load `ptcoef.txt`: {exc}")
        return

    st.write(f"Current source: `{deck.source_path}` ({deck.source_kind})")
    st.write(f"Overlay target: `{deck.target_path}`")
    if deck.source_kind != "overlay":
        st.warning("Saving will create or replace `ptcoef.txt` inside `input_overlay_dir`.")

    current_section: str | None = None
    updated_values: dict[str, float] = {}
    changed_lines: list[str] = []
    for entry in deck.entries:
        if entry.section and entry.section != current_section:
            st.markdown(f"**{entry.section}**")
            current_section = entry.section
        col_name, col_value = st.columns([1, 1])
        col_name.code(entry.name)
        new_value = col_value.number_input(
            entry.name,
            value=float(entry.value),
            format="%.8f",
            key=f"scenario_card_{card_key(config.name, entry.name)}",
            label_visibility="collapsed",
        )
        updated_values[entry.name] = float(new_value)
        if float(new_value) != float(entry.value):
            changed_lines.append(f"`{entry.name}`: {entry.value:.8g} -> {new_value:.8g}")

    if changed_lines:
        st.write("Pending changes: " + ", ".join(changed_lines))
    else:
        st.caption("No coefficient edits yet.")

    confirm_write = st.checkbox(
        "Confirm writing `ptcoef.txt` under `input_overlay_dir`",
        key=f"scenario_card_confirm_{config.name}_deck.ptcoef",
    )
    if st.button("Save Overlay Changes", key=f"scenario_card_save_{config.name}_deck.ptcoef"):
        if not confirm_write:
            st.error("Enable the confirmation checkbox before writing the overlay copy.")
            return
        try:
            result = write_ptcoef_overlay(deck, updated_values)
        except Exception as exc:
            st.error(f"Failed to save `ptcoef.txt`: {exc}")
            return
        if not result.changed_names:
            st.info("No coefficient values changed; nothing was written.")
            return
        st.success(f"Saved `ptcoef.txt` to {result.target_path}")
        if result.backup_path is not None:
            st.caption(f"Backup created: `{result.backup_path}`")
        st.rerun()


def card_key(config_name: str, value: str) -> str:
    return f"{config_name}_{value}"
