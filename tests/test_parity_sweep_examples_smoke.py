from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    script_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "parity_sweep_examples_smoke.py"
    )
    spec = importlib.util.spec_from_file_location("parity_sweep_examples_smoke", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_default_sweep_workers_uses_half_logical_cores(monkeypatch) -> None:
    module = _load_module()
    monkeypatch.setattr(module.os, "cpu_count", lambda: 20)
    assert module._default_sweep_workers() == 10


def test_default_sweep_workers_falls_back_to_four_when_unknown(monkeypatch) -> None:
    module = _load_module()
    monkeypatch.setattr(module.os, "cpu_count", lambda: None)
    assert module._default_sweep_workers() == 4
