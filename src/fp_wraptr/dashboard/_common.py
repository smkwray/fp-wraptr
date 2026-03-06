"""Shared utility functions for dashboard pages."""

from __future__ import annotations

import base64
from contextlib import suppress
from pathlib import Path
from typing import Final
from urllib.parse import urlencode

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CUSTOM_ARTIFACTS_LABEL: Final[str] = "Custom directory"
_SIDEBAR_LOGO_QUERY_KEY: Final[str] = "sidebar-logo"
_SIDEBAR_LOGO_DEFAULT: Final[str] = "default"
_SIDEBAR_LOGO_ALT: Final[str] = "alt"


def logo_path(name: str) -> Path:
    """Return the absolute path to a dashboard logo asset in ``logo/dashboard/``."""
    return _REPO_ROOT / "logo" / "dashboard" / name


def page_favicon() -> object:
    """Return the Raptr favicon for ``set_page_config``, falling back to emoji."""
    path = logo_path("favicon.png")
    if path.exists():
        try:
            from PIL import Image

            return Image.open(path)
        except Exception:
            pass
    return "\U0001F4CA"


def logo_html(name: str, width: int, alt: str = "") -> str:
    """Return an ``<img>`` tag with a base64-encoded PNG for inline use.

    Bypasses Streamlit's ``st.image`` processing (no JPEG compression,
    no fullscreen button, no caching artefacts).  Render with
    ``st.markdown(..., unsafe_allow_html=True)``.
    """
    path = logo_path(name)
    if not path.exists():
        return ""
    return image_html_for_path(path, width=width, alt=alt)


def image_html_for_path(
    path: Path | str,
    *,
    width: int,
    alt: str = "",
    height: int | None = None,
) -> str:
    """Return an ``<img>`` tag for any local PNG path with fixed dimensions."""
    resolved = Path(path)
    if not resolved.exists():
        return ""
    data = base64.b64encode(resolved.read_bytes()).decode()
    height_attr = f' height="{height}"' if height is not None else ""
    css_height = f"height:{height}px;" if height is not None else ""
    return (
        f'<img src="data:image/png;base64,{data}" '
        f'width="{width}"{height_attr} alt="{alt}" '
        f'style="width:{width}px;{css_height}object-fit:contain;pointer-events:none;user-select:none;">'
    )


def _sidebar_logo_alt_path() -> Path:
    return _REPO_ROOT / "logo" / "small" / "rptr-mv_rmgb.png"


def _title_logo_alt_path() -> Path:
    return _REPO_ROOT / "logo" / "small" / "arch_rmgb.png"


def _render_logo_toggle_component(
    *,
    default_path: Path,
    alt_path: Path,
    width: int,
    height: int,
    alt: str,
) -> bool:
    if not default_path.exists() or not alt_path.exists():
        return False

    default_data = base64.b64encode(default_path.read_bytes()).decode()
    alt_data = base64.b64encode(alt_path.read_bytes()).decode()
    html = f"""
<div style="display:flex;align-items:center;justify-content:flex-start;padding:0;margin:0;">
  <button type="button" aria-label="Toggle logo"
    style="border:none;background:transparent;padding:0;margin:0;cursor:pointer;line-height:0;">
    <img
      id="logo-toggle-img"
      src="data:image/png;base64,{default_data}"
      alt="{alt}"
      width="{width}"
      height="{height}"
      style="width:{width}px;height:{height}px;object-fit:contain;display:block;"
    />
  </button>
</div>
<script>
  (function() {{
    const root = document.currentScript && document.currentScript.parentElement;
    const button = root ? root.querySelector("button") : null;
    const img = root ? root.querySelector("img") : null;
    if (!button || !img) return;
    const defaultSrc = "data:image/png;base64,{default_data}";
    const altSrc = "data:image/png;base64,{alt_data}";
    let usingAlt = false;
    button.addEventListener("click", function() {{
      usingAlt = !usingAlt;
      img.src = usingAlt ? altSrc : defaultSrc;
    }});
  }})();
</script>
"""
    components.html(html, height=height + 8, width=width + 8)
    return True


def _query_href_with_overrides(overrides: dict[str, str | None]) -> str:
    payload: dict[str, str | list[str]] = {}
    for key, value in st.query_params.items():
        if isinstance(value, list):
            payload[str(key)] = [str(item) for item in value]
        else:
            payload[str(key)] = str(value)
    for key, value in overrides.items():
        token = str(key)
        if value is None:
            payload.pop(token, None)
        else:
            payload[token] = str(value)
    encoded = urlencode(payload, doseq=True)
    return f"?{encoded}" if encoded else "?"


def sidebar_logo_toggle_html(
    *,
    width: int = 56,
    height: int = 56,
    alt: str = "Raptr",
    query_key: str = _SIDEBAR_LOGO_QUERY_KEY,
) -> str:
    """Return a clickable sidebar logo that toggles between default and alt art."""
    raw = st.query_params.get(query_key, _SIDEBAR_LOGO_DEFAULT)
    if isinstance(raw, list):
        raw = raw[0] if raw else _SIDEBAR_LOGO_DEFAULT
    current = str(raw or _SIDEBAR_LOGO_DEFAULT).strip().lower()
    use_alt = current == _SIDEBAR_LOGO_ALT
    image_path = _sidebar_logo_alt_path() if use_alt else logo_path("sidebar-raptr.png")
    next_value = _SIDEBAR_LOGO_DEFAULT if use_alt else _SIDEBAR_LOGO_ALT
    href = _query_href_with_overrides({query_key: next_value})
    image = image_html_for_path(image_path, width=width, height=height, alt=alt)
    return (
        f'<a href="{href}" style="display:inline-block;line-height:0;text-decoration:none;" '
        f'title="Toggle logo">{image}</a>'
    )


def render_sidebar_logo_toggle(
    *,
    width: int = 56,
    height: int = 56,
    alt: str = "Raptr",
) -> None:
    """Render a client-side logo toggle in the sidebar without reloading the page."""
    default_path = logo_path("sidebar-raptr.png")
    alt_path = _sidebar_logo_alt_path()
    with st.sidebar:
        if _render_logo_toggle_component(
            default_path=default_path,
            alt_path=alt_path,
            width=width,
            height=height,
            alt=alt,
        ):
            return
        if default_path.exists():
            st.markdown(logo_html("sidebar-raptr.png", width, alt), unsafe_allow_html=True)


def render_title_logo_toggle(
    *,
    width: int = 72,
    height: int = 72,
    alt: str = "fp-wraptr",
) -> None:
    """Render a client-side title logo toggle without reloading the page."""
    default_path = logo_path("header-pair.png")
    alt_path = _title_logo_alt_path()
    if _render_logo_toggle_component(
        default_path=default_path,
        alt_path=alt_path,
        width=width,
        height=height,
        alt=alt,
    ):
        return
    if default_path.exists():
        st.markdown(logo_html("header-pair.png", width, alt), unsafe_allow_html=True)


def render_page_title(
    title: str,
    *,
    caption: str | None = None,
    divider: bool = True,
    logo_width: int = 72,
    logo_height: int = 72,
) -> None:
    """Render a page title with the clickable header logo on the left."""
    logo_col, title_col = st.columns([1, 11])
    with logo_col:
        render_title_logo_toggle(width=logo_width, height=logo_height)
    with title_col:
        st.title(title)
    if caption:
        st.caption(caption)
    if divider:
        st.divider()


def artifacts_dir_from_query() -> Path:
    """Read the artifacts directory from the Streamlit query string."""
    params = st.query_params.get("artifacts-dir", "artifacts")
    if isinstance(params, list):
        return Path(params[0] if params else "artifacts")
    if not params:
        return Path("artifacts")
    return Path(params if isinstance(params, str) else "artifacts")


def resolve_artifacts_dir_token(raw: str | Path, *, repo_root: Path | None = None) -> Path:
    """Resolve a typed artifacts directory token to a concrete path."""
    root = Path(repo_root).resolve() if repo_root is not None else _REPO_ROOT
    candidate = Path(str(raw or "").strip() or "artifacts").expanduser()
    if candidate.is_absolute():
        return candidate
    return (root / candidate).resolve()


def artifacts_dir_display(path: Path | str, *, repo_root: Path | None = None) -> str:
    """Format one artifacts directory path for compact UI display."""
    root = Path(repo_root).resolve() if repo_root is not None else _REPO_ROOT
    resolved = Path(path).expanduser()
    try:
        return str(resolved.relative_to(root))
    except ValueError:
        return str(resolved)


def discover_artifacts_roots(
    *,
    repo_root: Path | None = None,
    include: Path | str | None = None,
) -> tuple[Path, ...]:
    """Discover repo-local artifacts roots like ``artifacts`` and ``artifacts-2008``."""
    root = Path(repo_root).resolve() if repo_root is not None else _REPO_ROOT
    candidates: dict[str, Path] = {}
    if root.exists():
        for child in root.iterdir():
            if child.is_dir() and child.name.lower().startswith("artifacts"):
                candidates[str(child.resolve())] = child.resolve()
    if include is not None:
        extra = Path(include).expanduser()
        resolved_extra = extra.resolve() if extra.exists() else resolve_artifacts_dir_token(extra, repo_root=root)
        candidates[str(resolved_extra)] = resolved_extra
    return tuple(
        sorted(
            candidates.values(),
            key=lambda path: (0 if path.name.lower() == "artifacts" else 1, path.name.lower()),
        )
    )


def render_artifacts_dir_picker(
    *,
    repo_root: Path | None = None,
    state_key: str = "artifacts_dir",
    query_key: str = "artifacts-dir",
) -> Path:
    """Render a sidebar artifacts-root picker with discovered and custom paths."""
    root = Path(repo_root).resolve() if repo_root is not None else _REPO_ROOT
    current_raw = str(st.session_state.get(state_key) or artifacts_dir_from_query())
    current_path = resolve_artifacts_dir_token(current_raw, repo_root=root)
    discovered = discover_artifacts_roots(repo_root=root, include=current_path)
    known_labels = [artifacts_dir_display(path, repo_root=root) for path in discovered]
    label_to_path = {label: path for label, path in zip(known_labels, discovered, strict=True)}

    default_known = artifacts_dir_display(current_path, repo_root=root)
    default_option = (
        default_known if default_known in label_to_path else _CUSTOM_ARTIFACTS_LABEL
    )

    selected = st.sidebar.selectbox(
        "Known artifacts roots",
        options=[_CUSTOM_ARTIFACTS_LABEL, *known_labels],
        index=[_CUSTOM_ARTIFACTS_LABEL, *known_labels].index(default_option),
        help="Choose a discovered repo-local `artifacts*` directory or switch to a custom path below.",
    )
    effective = label_to_path[selected] if selected != _CUSTOM_ARTIFACTS_LABEL else current_path
    if selected == _CUSTOM_ARTIFACTS_LABEL:
        typed_value = st.sidebar.text_input(
            "Custom artifacts directory",
            value=artifacts_dir_display(current_path, repo_root=root),
            help="Type a relative repo path like `artifacts-2008` or any absolute path.",
        )
        effective = resolve_artifacts_dir_token(typed_value, repo_root=root)

    st.session_state[state_key] = str(effective)
    with suppress(Exception):
        st.query_params[query_key] = str(effective)
    return effective


def reset_requested_from_query() -> bool:
    """Return True if ``?reset=1`` (or similar truthy value) is in the URL."""
    raw = st.query_params.get("reset")
    if isinstance(raw, list):
        raw = raw[0] if raw else ""
    token = str(raw or "").strip().lower()
    return token in {"1", "true", "yes", "y", "on"}


def cache_key_for_path(path: Path) -> str:
    """Return a cache-invalidation key based on file size and mtime."""
    if not path.exists():
        return "missing"
    stat = path.stat()
    return f"{stat.st_size}:{stat.st_mtime_ns}"


def apply_data_editor_checkbox_edits(
    table: pd.DataFrame,
    *,
    widget_key: str,
    checkbox_column: str = "Use",
    raw_state: object | None = None,
) -> pd.DataFrame:
    """Apply checkbox edits from a Streamlit data editor onto a base table.

    Streamlit persists ``st.data_editor`` changes under the widget key in
    ``st.session_state``. Reconstructing selection from the returned DataFrame can
    miss a re-check after a prior uncheck, so apply the persisted row deltas
    directly to the current base table.
    """
    if checkbox_column not in table.columns or table.empty:
        return table

    if raw_state is None:
        raw_state = st.session_state.get(widget_key)
    if not isinstance(raw_state, dict):
        return table
    edited_rows = raw_state.get("edited_rows")
    if not isinstance(edited_rows, dict):
        return table

    out = table.copy()
    checkbox_idx = out.columns.get_loc(checkbox_column)
    for raw_row_idx, row_delta in edited_rows.items():
        if not isinstance(row_delta, dict) or checkbox_column not in row_delta:
            continue
        try:
            row_idx = int(raw_row_idx)
        except (TypeError, ValueError):
            continue
        if row_idx < 0 or row_idx >= len(out):
            continue
        out.iat[row_idx, checkbox_idx] = bool(row_delta[checkbox_column])
    return out
