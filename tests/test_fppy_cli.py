from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from fppy.cli import (
    _apply_outside_seed_frame,
    _build_keyboard_stub_lag_probe_targets,
    _cmd_mini_run,
    _collect_assignment_records_by_lhs,
    _collect_outside_seed_stats,
    _resolve_auto_keyboard_all_assignments_flag,
    _resolve_auto_keyboard_context_first_iter_flag,
    _resolve_auto_keyboard_min_iters,
    _resolve_auto_keyboard_rho_aware_flag,
    _resolve_auto_keyboard_solve_stub_flags,
    _resolve_eq_flags_preset_label,
    _serialize_setupsolve_for_summary,
    _should_compute_residual_probe,
    _snapshot_probe_values,
)
from fppy.parser import parse_fminput
from fppy.solve_setup import SetupSolveConfig


def test_apply_outside_seed_frame_does_not_seed_present_zero_values() -> None:
    frame = pd.DataFrame(
        {"PCY": [100.0, 0.0, 0.0]},
        index=["2025.3", "2025.4", "2025.5"],
    )

    seeded_frame, seeded_cells = _apply_outside_seed_frame(
        frame,
        targets=("PCY",),
        windows=(("2025.4", "2025.5"),),
    )

    assert seeded_cells == 0
    assert seeded_frame["PCY"].tolist() == [100.0, 0.0, 0.0]


def test_apply_outside_seed_frame_preserves_zero_run_when_prior_is_zero() -> None:
    frame = pd.DataFrame(
        {"PCY": [0.0, 0.0, 0.0]},
        index=["2025.3", "2025.4", "2025.5"],
    )

    seeded_frame, seeded_cells = _apply_outside_seed_frame(
        frame,
        targets=("PCY",),
        windows=(("2025.4", "2025.5"),),
    )

    assert seeded_cells == 0
    assert seeded_frame["PCY"].tolist() == [0.0, 0.0, 0.0]


def test_apply_outside_seed_frame_still_seeds_missing_sentinel_values() -> None:
    frame = pd.DataFrame(
        {"PCY": [100.0, -99.0, -99.0]},
        index=["2025.3", "2025.4", "2025.5"],
    )

    seeded_frame, seeded_cells = _apply_outside_seed_frame(
        frame,
        targets=("PCY",),
        windows=(("2025.4", "2025.5"),),
    )

    assert seeded_cells == 2
    assert seeded_frame["PCY"].tolist() == [100.0, 100.0, 100.0]


def test_collect_outside_seed_stats_counts_candidates() -> None:
    frame = pd.DataFrame(
        {"PCY": [100.0, 0.0, -99.0]},
        index=["2025.3", "2025.4", "2025.5"],
    )
    stats = _collect_outside_seed_stats(
        frame,
        targets=("PCY",),
        windows=(("2025.4", "2025.5"),),
    )
    assert stats["resolved_target_count"] == 1
    assert stats["inspected_cells"] == 2
    assert stats["missing_like_cells"] == 1
    assert stats["prior_missing_like_cells"] == 0
    assert stats["candidate_cells"] == 1


def test_collect_outside_seed_stats_handles_unresolved_targets() -> None:
    frame = pd.DataFrame(
        {"PCY": [100.0, 0.0]},
        index=["2025.3", "2025.4"],
    )
    stats = _collect_outside_seed_stats(
        frame,
        targets=("MISSING",),
        windows=(("2025.4", "2025.4"),),
    )
    assert stats["resolved_target_count"] == 0
    assert stats["inspected_cells"] == 0
    assert stats["candidate_cells"] == 0


def test_should_compute_residual_probe_skips_before_min_iters() -> None:
    assert (
        _should_compute_residual_probe(
            iteration=39,
            max_iters=100,
            min_iters=40,
            convergence_ratio=0.0,
        )
        is False
    )


def test_should_compute_residual_probe_runs_at_min_iters_when_converged() -> None:
    assert (
        _should_compute_residual_probe(
            iteration=40,
            max_iters=100,
            min_iters=40,
            convergence_ratio=0.5,
        )
        is True
    )


def test_should_compute_residual_probe_runs_on_final_iteration_even_if_not_converged() -> None:
    assert (
        _should_compute_residual_probe(
            iteration=100,
            max_iters=100,
            min_iters=40,
            convergence_ratio=5.0,
        )
        is True
    )


def test_resolve_auto_keyboard_solve_stub_flags_enables_stub_defaults() -> None:
    (
        auto_stub_active,
        period_sequential_effective,
        period_scoped_effective,
        period_sequential_auto_enabled,
        period_scoped_auto_enabled,
    ) = _resolve_auto_keyboard_solve_stub_flags(
        auto_enable_eq_from_solve=True,
        solve_active_outside=True,
        solve_active_filevar="keyboard",
        solve_active_keyboard_targets_set=frozenset({"PCY", "PCPF"}),
        eq_use_setupsolve=False,
        eq_period_sequential=False,
        eq_period_scoped="auto",
    )

    assert auto_stub_active is True
    assert period_sequential_effective is True
    assert period_scoped_effective == "on"
    assert period_sequential_auto_enabled is True
    assert period_scoped_auto_enabled is True


def test_resolve_auto_keyboard_solve_stub_flags_keeps_explicit_flags() -> None:
    (
        auto_stub_active,
        period_sequential_effective,
        period_scoped_effective,
        period_sequential_auto_enabled,
        period_scoped_auto_enabled,
    ) = _resolve_auto_keyboard_solve_stub_flags(
        auto_enable_eq_from_solve=True,
        solve_active_outside=True,
        solve_active_filevar="KEYBOARD",
        solve_active_keyboard_targets_set=frozenset({"PCY"}),
        eq_use_setupsolve=False,
        eq_period_sequential=True,
        eq_period_scoped="off",
    )

    assert auto_stub_active is True
    assert period_sequential_effective is True
    assert period_scoped_effective == "off"
    assert period_sequential_auto_enabled is False
    assert period_scoped_auto_enabled is False


def test_resolve_auto_keyboard_all_assignments_flag_auto_enables_for_stub() -> None:
    effective, auto_enabled = _resolve_auto_keyboard_all_assignments_flag(
        auto_stub_active=True,
        eq_period_sequential_all_assignments=False,
    )
    assert effective is True
    assert auto_enabled is True


def test_resolve_auto_keyboard_all_assignments_flag_respects_explicit_on() -> None:
    effective, auto_enabled = _resolve_auto_keyboard_all_assignments_flag(
        auto_stub_active=True,
        eq_period_sequential_all_assignments=True,
    )
    assert effective is True
    assert auto_enabled is False


def test_resolve_auto_keyboard_min_iters_auto_enables_for_stub() -> None:
    min_iters, max_iters, auto_enabled = _resolve_auto_keyboard_min_iters(
        auto_stub_active=True,
        eq_use_setupsolve=False,
        eq_iters=None,
        eq_backfill_min_iters=1,
        eq_backfill_max_iters=1,
    )
    assert min_iters == 2
    assert max_iters == 2
    assert auto_enabled is True


def test_resolve_auto_keyboard_min_iters_respects_explicit_setupsolve() -> None:
    min_iters, max_iters, auto_enabled = _resolve_auto_keyboard_min_iters(
        auto_stub_active=True,
        eq_use_setupsolve=True,
        eq_iters=None,
        eq_backfill_min_iters=1,
        eq_backfill_max_iters=1,
    )
    assert min_iters == 1
    assert max_iters == 1
    assert auto_enabled is False


def test_resolve_auto_keyboard_min_iters_respects_explicit_eq_iters() -> None:
    min_iters, max_iters, auto_enabled = _resolve_auto_keyboard_min_iters(
        auto_stub_active=True,
        eq_use_setupsolve=False,
        eq_iters=1,
        eq_backfill_min_iters=1,
        eq_backfill_max_iters=1,
    )
    assert min_iters == 1
    assert max_iters == 1
    assert auto_enabled is False


def test_resolve_auto_keyboard_context_first_iter_flag_auto_enables() -> None:
    effective, auto_enabled = _resolve_auto_keyboard_context_first_iter_flag(
        auto_stub_active=True,
        eq_period_sequential_all_assignments=True,
        eq_use_setupsolve=False,
        eq_period_sequential_context_assignments_first_iter_only=False,
    )
    assert effective is True
    assert auto_enabled is True


def test_resolve_auto_keyboard_context_first_iter_flag_noop_without_all_assignments() -> None:
    effective, auto_enabled = _resolve_auto_keyboard_context_first_iter_flag(
        auto_stub_active=True,
        eq_period_sequential_all_assignments=False,
        eq_use_setupsolve=False,
        eq_period_sequential_context_assignments_first_iter_only=False,
    )
    assert effective is False
    assert auto_enabled is False


def test_resolve_auto_keyboard_context_first_iter_flag_no_auto_when_setupsolve_enabled() -> None:
    effective, auto_enabled = _resolve_auto_keyboard_context_first_iter_flag(
        auto_stub_active=True,
        eq_period_sequential_all_assignments=True,
        eq_use_setupsolve=True,
        eq_period_sequential_context_assignments_first_iter_only=False,
    )
    assert effective is False
    assert auto_enabled is False


def test_resolve_auto_keyboard_rho_aware_flag_auto_enables_for_setupsolve() -> None:
    effective, auto_enabled, auto_reason = _resolve_auto_keyboard_rho_aware_flag(
        auto_stub_active=True,
        auto_stub_min_iters_enabled=False,
        eq_use_setupsolve=True,
        eq_backfill_specs_has_rho_terms=True,
        eq_backfill_rho_aware=False,
        eq_backfill_rho_resid_ar1=False,
    )
    assert effective is True
    assert auto_enabled is True
    assert auto_reason == "setupsolve_rho_terms"


def test_resolve_auto_keyboard_rho_aware_flag_auto_enables_for_stub_min_iters() -> None:
    effective, auto_enabled, auto_reason = _resolve_auto_keyboard_rho_aware_flag(
        auto_stub_active=True,
        auto_stub_min_iters_enabled=True,
        eq_use_setupsolve=False,
        eq_backfill_specs_has_rho_terms=True,
        eq_backfill_rho_aware=False,
        eq_backfill_rho_resid_ar1=False,
    )
    assert effective is True
    assert auto_enabled is True
    assert auto_reason == "keyboard_stub_min_iters_rho_terms"


def test_resolve_auto_keyboard_rho_aware_flag_auto_enables_for_stub_without_min_iters() -> None:
    effective, auto_enabled, auto_reason = _resolve_auto_keyboard_rho_aware_flag(
        auto_stub_active=True,
        auto_stub_min_iters_enabled=False,
        eq_use_setupsolve=False,
        eq_backfill_specs_has_rho_terms=True,
        eq_backfill_rho_aware=False,
        eq_backfill_rho_resid_ar1=False,
    )
    assert effective is True
    assert auto_enabled is True
    assert auto_reason == "keyboard_stub_rho_terms"


def test_resolve_auto_keyboard_rho_aware_flag_respects_explicit_resid_ar1() -> None:
    effective, auto_enabled, auto_reason = _resolve_auto_keyboard_rho_aware_flag(
        auto_stub_active=True,
        auto_stub_min_iters_enabled=True,
        eq_use_setupsolve=True,
        eq_backfill_specs_has_rho_terms=True,
        eq_backfill_rho_aware=False,
        eq_backfill_rho_resid_ar1=True,
    )
    assert effective is False
    assert auto_enabled is False
    assert auto_reason == "none"


def test_build_keyboard_stub_lag_probe_targets_finds_transitive_dependents() -> None:
    records = parse_fminput(
        """
IDENT USROW=-INTGR+DR+PIEFRET+USOTHER;
IDENT PIEFRET=THETA4*PIEF;
GENR DR=DRQ*GDPD;
IDENT RSMRSL2=RS-RS(-2);
"""
    )

    targets = _build_keyboard_stub_lag_probe_targets(
        records,
        keyboard_targets=("PIEF",),
        max_depth=3,
        max_targets=16,
    )

    assert "PIEFRET" in targets
    assert "USROW" in targets


def test_build_keyboard_stub_lag_probe_targets_includes_z_aliases() -> None:
    records = parse_fminput(
        """
IDENT USROW=-INTGR+DR+PIEFRET+USOTHER;
IDENT PIEFRET=THETA4*PIEF;
"""
    )

    targets = _build_keyboard_stub_lag_probe_targets(
        records,
        keyboard_targets=("PIEF",),
        max_depth=3,
        max_targets=16,
    )

    assert "USROW" in targets
    assert "USROWZ" in targets


def test_collect_assignment_records_by_lhs_filters_commands_and_targets() -> None:
    records = parse_fminput(
        """
IDENT USROW=-INTGR+DR+PIEFRET+USOTHER;
GENR DR=DRQ*GDPD;
LHS WF=EXP(LWFQZ+DELTA1*LPF(-1))*LAM;
"""
    )
    selected = _collect_assignment_records_by_lhs(
        records,
        commands=frozenset({records[0].command, records[1].command}),
        lhs_targets=frozenset({"USROWZ"}),
    )

    assert len(selected) == 1
    assert selected[0].statement == records[0].statement


def test_collect_assignment_records_by_lhs_can_select_rsmrsl2_family() -> None:
    records = parse_fminput(
        """
GENR RS1=RS-RS(-1);
GENR RSMRSL2=RS-RS(-2);
GENR RSLMRSL2=RS(-1)-RS(-2);
"""
    )
    selected = _collect_assignment_records_by_lhs(
        records,
        commands=frozenset({records[0].command}),
        lhs_targets=frozenset({"RSMRSL2", "RSLMRSL2"}),
    )
    lhs_values = [item.statement.upper() for item in selected]
    assert len(selected) == 2
    assert any("RSMRSL2" in value for value in lhs_values)
    assert any("RSLMRSL2" in value for value in lhs_values)


def test_collect_assignment_records_by_lhs_can_select_eq_targets() -> None:
    records = parse_fminput(
        """
EQ 30 RS C RS(-1) PCPD UR;
EQ 31 NONE31 C;
"""
    )
    selected = _collect_assignment_records_by_lhs(
        records,
        commands=frozenset({records[0].command}),
        lhs_targets=frozenset({"RS"}),
    )
    assert len(selected) == 1
    assert "EQ 30 RS" in selected[0].statement.upper()


def test_targeted_recompute_helpers_removed() -> None:
    # Targeted recompute was removed to avoid solver-path churn and eliminate dead
    # behavior that could surprise non-parity presets.
    import fppy.cli as fppy_cli

    assert not hasattr(fppy_cli, "_apply_targeted_assignment_recompute")
    # Also guard against accidental re-introduction in the source file.
    text = Path(fppy_cli.__file__).read_text(encoding="utf-8")
    assert "_apply_targeted_assignment_recompute" not in text
    assert "USROW_RECOMPUTE_TARGETS" not in text


def test_snapshot_probe_values_reads_period_row_and_handles_missing_targets() -> None:
    frame = pd.DataFrame(
        {"USROW": [2.5], "PIEFRET": [-1.0]},
        index=["2025.4"],
    )

    values = _snapshot_probe_values(
        frame,
        period="2025.4",
        targets=("USROW", "PIEFRET", "MISSINGVAR"),
    )

    assert values["USROW"] == 2.5
    assert values["PIEFRET"] == -1.0
    assert values["MISSINGVAR"] is None


def test_snapshot_probe_values_resolves_z_suffix_alias() -> None:
    frame = pd.DataFrame(
        {"USROWZ": [1.25]},
        index=["2025.4"],
    )

    values = _snapshot_probe_values(
        frame,
        period="2025.4",
        targets=("USROW",),
    )

    assert values["USROW"] == 1.25


def test_snapshot_probe_values_resolves_period_by_string_key() -> None:
    frame = pd.DataFrame(
        {"USROW": [3.5]},
        index=[2025.4],
    )

    values = _snapshot_probe_values(
        frame,
        period="2025.4",
        targets=("USROW",),
    )

    assert values["USROW"] == 3.5


def test_resolve_auto_keyboard_solve_stub_flags_disabled_when_setupsolve_enabled() -> None:
    (
        auto_stub_active,
        period_sequential_effective,
        period_scoped_effective,
        period_sequential_auto_enabled,
        period_scoped_auto_enabled,
    ) = _resolve_auto_keyboard_solve_stub_flags(
        auto_enable_eq_from_solve=True,
        solve_active_outside=True,
        solve_active_filevar="KEYBOARD",
        solve_active_keyboard_targets_set=frozenset({"PCY"}),
        eq_use_setupsolve=True,
        eq_period_sequential=False,
        eq_period_scoped="auto",
    )

    assert auto_stub_active is False
    assert period_sequential_effective is False
    assert period_scoped_effective == "auto"
    assert period_sequential_auto_enabled is False
    assert period_scoped_auto_enabled is False


def test_resolve_eq_flags_preset_label_prefers_explicit_label() -> None:
    label = _resolve_eq_flags_preset_label(
        eq_flags_preset_label="parity",
        enable_eq=True,
        eq_use_setupsolve=True,
        eq_period_sequential=True,
        eq_period_scoped="on",
        eq_period_sequential_context_assignments_first_iter_only=False,
    )
    assert label == "parity"


def test_resolve_eq_flags_preset_label_infers_parity_when_setupsolve_active() -> None:
    label = _resolve_eq_flags_preset_label(
        eq_flags_preset_label=None,
        enable_eq=True,
        eq_use_setupsolve=True,
        eq_period_sequential=True,
        eq_period_scoped="on",
        eq_period_sequential_context_assignments_first_iter_only=False,
    )
    assert label == "parity"


def test_serialize_setupsolve_for_summary_includes_expected_fields() -> None:
    payload = _serialize_setupsolve_for_summary(
        SetupSolveConfig(
            miniters=40,
            maxiters=100,
            maxcheck=3,
            tolall=0.001,
            tolallabs=True,
            dampall=1.0,
            filedamp="DAMP.TXT",
            filetol="TOL.TXT",
            filetolabs="TOLABS.TXT",
            nomiss=True,
        )
    )

    assert payload is not None
    assert payload["miniters"] == 40
    assert payload["maxiters"] == 100
    assert payload["maxcheck"] == 3
    assert payload["nomiss"] is True
    assert payload["tolall"] == 0.001
    assert payload["tolallabs"] is True
    assert payload["dampall"] == 1.0


def test_cmd_mini_run_report_includes_setupsolve_and_unsupported_examples(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fminput_path = tmp_path / "fminput.txt"
    fminput_path.write_text(
        "SETUPSOLVE MINITERS=40 MAXITERS=100 MAXCHECK=3 NOMISS TOLALL=0.001 TOLALLABS;\n"
        "EQ 1 PCY C;\n",
        encoding="utf-8",
    )
    config = SimpleNamespace(
        legacy=SimpleNamespace(
            fminput=fminput_path,
            fmout=tmp_path / "fmout.txt",
        )
    )

    monkeypatch.setattr("fppy.cli._load_model_config", lambda *_args, **_kwargs: config)
    monkeypatch.setattr(
        "fppy.cli.load_execution_input_bundle",
        lambda **_kwargs: SimpleNamespace(legacy_base_dir=tmp_path),
    )
    monkeypatch.setattr(
        "fppy.cli.parse_fminput_file",
        lambda _path: parse_fminput(
            "SETUPSOLVE MINITERS=40 MAXITERS=100 MAXCHECK=3 NOMISS TOLALL=0.001 TOLALLABS;\n"
            "EQ 1 PCY C;\n"
        ),
    )
    monkeypatch.setattr("fppy.cli.load_eq_specs_from_fmout", lambda _path: {})
    monkeypatch.setattr("fppy.cli.build_coef_table", lambda _specs: {})
    monkeypatch.setattr(
        "fppy.cli.run_mini_run",
        lambda *_args, **_kwargs: {
            "summary": {
                "records": 2,
                "planned": 2,
                "executed": 2,
                "failed": 0,
                "unsupported": 0,
                "unsupported_counts": {},
            },
            "issues": [],
            "unsupported_examples": [{"line": 9, "command": "EQ", "statement": "EQ 1 PCY C;"}],
            "executed_line_numbers": [1, 2],
            "output": pd.DataFrame({"PCY": [1.0]}, index=["2025.4"]),
        },
    )
    monkeypatch.setattr(
        "fppy.cli.apply_eq_backfill",
        lambda *_args, **_kwargs: {"frame": pd.DataFrame({"PCY": [1.0]}, index=["2025.4"])},
    )

    report_path = tmp_path / "fppy_report.json"
    exit_code = _cmd_mini_run(
        config_path=tmp_path / "model.toml",
        enable_eq=True,
        eq_use_setupsolve=True,
        eq_flags_preset_label="parity",
        eq_structural_read_cache="numpy_columns",
        report_json=report_path,
    )
    assert exit_code == 0

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    summary = payload["summary"]
    assert summary["eq_use_setupsolve"] is True
    assert summary["eq_flags_preset"] == "parity"
    assert isinstance(summary["setupsolve"], dict)
    assert summary["setupsolve"]["miniters"] == 40
    assert summary["setupsolve"]["maxiters"] == 100
    assert summary["setupsolve"]["maxcheck"] == 3
    assert summary["setupsolve"]["nomiss"] is True
    assert isinstance(summary["unsupported_examples"], list)
    assert summary["unsupported_examples"] == payload["unsupported_examples"]
    assert summary["eq_backfill_auto_keyboard_stub_active"] is False
    assert summary["eq_structural_read_cache"] == "numpy_columns"
    assert summary["eq_backfill_structural_read_cache"] == "numpy_columns"
    assert summary["eq_backfill_structural_read_cache_column_count"] == 0
    assert summary["eq_backfill_structural_scalar_reads_cached"] == 0
    assert summary["eq_backfill_structural_scalar_reads_frame"] == 0


def test_apply_outside_seed_frame_cannot_seed_first_row_without_prior_period() -> None:
    frame = pd.DataFrame(
        {"PCY": [0.0, 0.0]},
        index=["2025.4", "2025.5"],
    )

    seeded_frame, seeded_cells = _apply_outside_seed_frame(
        frame,
        targets=("PCY",),
        windows=(("2025.4", "2025.5"),),
    )

    # The first in-scope row has no previous period to copy from.
    assert seeded_cells == 0
    assert seeded_frame["PCY"].tolist() == [0.0, 0.0]


def test_apply_outside_seed_frame_boundary_missing_vs_present_current_period() -> None:
    frame = pd.DataFrame(
        {
            "MISSING_CURR": [100.0, -99.0],
            "PRESENT_CURR": [100.0, 40.0],
        },
        index=["2025.3", "2025.4"],
    )

    seeded_frame, seeded_cells = _apply_outside_seed_frame(
        frame,
        targets=("MISSING_CURR", "PRESENT_CURR"),
        windows=(("2025.4", "2025.4"),),
    )

    assert seeded_cells == 1
    assert seeded_frame["MISSING_CURR"].tolist() == [100.0, 100.0]
    assert seeded_frame["PRESENT_CURR"].tolist() == [100.0, 40.0]
