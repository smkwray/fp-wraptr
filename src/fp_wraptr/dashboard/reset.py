"""Dashboard reset and restart helpers."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from contextlib import suppress
from pathlib import Path

import streamlit as st


def soft_reset_dashboard_state(
    *,
    reset_keys: tuple[str, ...],
    artifacts_dir: str = "artifacts",
) -> None:
    for key in reset_keys:
        st.session_state.pop(key, None)
    st.session_state["artifacts_dir"] = artifacts_dir
    with suppress(Exception):
        st.cache_data.clear()
    with suppress(Exception):
        st.cache_resource.clear()
    with suppress(Exception):
        st.query_params.pop("reset")
    with suppress(Exception):
        st.query_params["artifacts-dir"] = artifacts_dir


def request_dashboard_restart(
    *,
    app_path: Path,
    artifacts_dir: str = "artifacts",
    port: int = 8501,
) -> None:
    repo_root = Path(app_path).resolve().parents[2]
    current_pid = os.getpid()
    parent_pid = os.getppid()
    python = sys.executable
    log_path = repo_root / "artifacts" / ".dashboard" / "restart.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    script = f"""\
sleep 1
kill {current_pid} >/dev/null 2>&1 || true
parent_cmd="$(ps -p {parent_pid} -o command= 2>/dev/null || true)"
case "$parent_cmd" in
  *"fp dashboard"*)
    kill {parent_pid} >/dev/null 2>&1 || true
    ;;
esac
cd {shlex.quote(str(repo_root))}
exec {shlex.quote(python)} -m streamlit run {shlex.quote(str(app_path))} --server.port {int(port)} -- --artifacts-dir {shlex.quote(str(artifacts_dir))} >> {shlex.quote(str(log_path))} 2>&1
"""
    subprocess.Popen(
        ["/bin/zsh", "-lc", script],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
