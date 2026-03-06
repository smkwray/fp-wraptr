"""Small dashboard helpers for agent-first handoff panels."""

from __future__ import annotations

from pathlib import Path

import streamlit as st


def render_agent_handoff(
    *,
    title: str,
    prompt: str,
    workspace_id: str = "",
    run_dir: Path | str | None = None,
    pack_id: str = "",
) -> None:
    with st.expander(title, expanded=False):
        if workspace_id:
            st.write(f"Workspace ID: `{workspace_id}`")
        if pack_id:
            st.write(f"Pack ID: `{pack_id}`")
        if run_dir:
            st.write(f"Run directory: `{Path(run_dir)}`")
        st.caption("Suggested MCP/agent task")
        st.code(prompt)


def render_advanced_toggle(*, key: str, label: str = "Show advanced authoring tools") -> bool:
    st.caption(
        "Default workflow: use an agent to create and mutate managed workspaces, then use the dashboard to inspect and compare results."
    )
    return bool(st.toggle(label, value=False, key=key))
