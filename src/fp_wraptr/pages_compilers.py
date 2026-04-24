"""Optional public-bundle compilers applied during pages export."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any


class PagesCompilerError(RuntimeError):
    """Raised when an optional public-bundle compiler fails."""


def apply_public_run_compilers(
    *,
    run_payloads: dict[str, dict[str, Any]],
    childcare_regime_compiler_config_path: Path | str | None = None,
    childcare_regime_input_contract_path: Path | str | None = None,
    childcare_regime_output_contract_path: Path | str | None = None,
) -> None:
    """Apply all configured public-bundle compilers in place."""
    if not any(
        (
            childcare_regime_compiler_config_path,
            childcare_regime_input_contract_path,
            childcare_regime_output_contract_path,
        )
    ):
        return
    required = {
        "childcare_regime_compiler_config_path": childcare_regime_compiler_config_path,
        "childcare_regime_input_contract_path": childcare_regime_input_contract_path,
        "childcare_regime_output_contract_path": childcare_regime_output_contract_path,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        formatted = ", ".join(sorted(missing))
        raise PagesCompilerError(
            f"Childcare regime compiler requires all config/contract paths; missing {formatted}"
        )
    _apply_childcare_regime_compiler(
        run_payloads=run_payloads,
        config_path=Path(str(childcare_regime_compiler_config_path)),
        input_contract_path=Path(str(childcare_regime_input_contract_path)),
        output_contract_path=Path(str(childcare_regime_output_contract_path)),
    )


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise PagesCompilerError(f"Compiler JSON not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PagesCompilerError(f"Failed to parse compiler JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise PagesCompilerError(f"Expected a JSON object in {path}")
    return payload


def _coerce_series(values: Any) -> list[float | None] | None:
    if not isinstance(values, list):
        return None
    out: list[float | None] = []
    for raw in values:
        if raw is None or isinstance(raw, bool):
            out.append(None)
            continue
        if not isinstance(raw, (int, float)):
            return None
        number = float(raw)
        out.append(number if math.isfinite(number) else None)
    return out


def _resolve_series(series: dict[str, Any], *, candidates: list[str]) -> list[float | None] | None:
    for name in candidates:
        values = _coerce_series(series.get(name))
        if values is not None:
            return values
    return None


def _vector_binary(
    left: list[float | None],
    right: list[float | None],
    *,
    op,
) -> list[float | None]:
    if len(left) != len(right):
        raise PagesCompilerError("Series length mismatch")
    out: list[float | None] = []
    for lval, rval in zip(left, right, strict=False):
        if lval is None or rval is None:
            out.append(None)
            continue
        out.append(op(lval, rval))
    return out


def _vector_subtract(left: list[float | None], right: list[float | None]) -> list[float | None]:
    return _vector_binary(left, right, op=lambda left_value, right_value: left_value - right_value)


def _vector_multiply(left: list[float | None], right: list[float | None]) -> list[float | None]:
    return _vector_binary(left, right, op=lambda left_value, right_value: left_value * right_value)


def _vector_ratio(
    numerator: list[float | None],
    denominator: list[float | None],
    *,
    default: float,
) -> list[float | None]:
    if len(numerator) != len(denominator):
        raise PagesCompilerError("Series length mismatch")
    out: list[float | None] = []
    for numer, denom in zip(numerator, denominator, strict=False):
        if numer is None:
            out.append(None)
            continue
        if denom is None or denom == 0.0:
            out.append(default)
            continue
        out.append(numer / denom)
    return out


def _vector_scale(values: list[float | None], scale: float) -> list[float | None]:
    return [None if value is None else value * scale for value in values]


def _vector_positive(values: list[float | None]) -> list[float | None]:
    return [None if value is None else max(0.0, value) for value in values]


def _vector_floor(values: list[float | None], *, floor: float) -> list[float | None]:
    return [None if value is None else max(floor, value) for value in values]


def _vector_fill(values: list[float | None], *, default: float) -> list[float | None]:
    return [default if value is None else value for value in values]


def _derive_gccprice(series: dict[str, Any]) -> list[float | None] | None:
    price = _coerce_series(series.get("GCCPRICE"))
    if price is not None:
        return price
    cost = _coerce_series(series.get("GCCOST"))
    if cost is not None:
        return cost
    burden = _coerce_series(series.get("GCBUR"))
    subsidy = _coerce_series(series.get("GCSUB"))
    if burden is None or subsidy is None:
        return None
    return _vector_binary(burden, subsidy, op=lambda left_value, right_value: left_value + right_value)


def _derive_gcbur(series: dict[str, Any], *, subsidy_rate: float) -> list[float | None] | None:
    burden = _coerce_series(series.get("GCBUR"))
    if burden is not None:
        return burden
    price = _derive_gccprice(series)
    if price is None:
        return None
    subsidy = _coerce_series(series.get("GCSUB"))
    if subsidy is not None:
        return _vector_subtract(price, subsidy)
    return _vector_scale(price, 1.0 - subsidy_rate)


def _derive_gcchh(
    *,
    run_series: dict[str, Any],
    base_series: dict[str, Any],
    run_burden: list[float | None],
    base_burden: list[float | None],
    household_burden_floor: float,
) -> list[float | None]:
    existing = _coerce_series(run_series.get("GCCHH"))
    if existing is not None:
        base_household = _coerce_series(base_series.get("GCCHH")) or _vector_fill(
            base_burden, default=household_burden_floor
        )
        burden_ratio = _vector_ratio(run_burden, base_burden, default=1.0)
        return _vector_floor(
            _vector_multiply(base_household, burden_ratio),
            floor=household_burden_floor,
        )
    burden_ratio = _vector_ratio(run_burden, base_burden, default=1.0)
    household_anchor = _coerce_series(base_series.get("GCCHH")) or _vector_fill(
        base_burden, default=household_burden_floor
    )
    return _vector_floor(
        _vector_multiply(household_anchor, burden_ratio),
        floor=household_burden_floor,
    )


def _normalize_contracts(
    *,
    config: dict[str, Any],
    input_contract: dict[str, Any],
    output_contract: dict[str, Any],
) -> None:
    config_method_tag = str(config.get("method_tag") or "").strip()
    config_quantity_basis = str(config.get("quantity_basis") or "").strip()
    output_method_tag = str(
        ((output_contract.get("required_metadata") or {}).get("method_tag") or {}).get("required_value") or ""
    ).strip()
    input_quantity_basis = str(
        ((input_contract.get("required_metadata") or {}).get("quantity_basis") or {}).get("required_value") or ""
    ).strip()
    output_quantity_basis = str(
        ((output_contract.get("required_metadata") or {}).get("quantity_basis") or {}).get("required_value") or ""
    ).strip()
    if config_method_tag != output_method_tag:
        raise PagesCompilerError("Compiler config method_tag must match the output contract")
    if config_quantity_basis != input_quantity_basis:
        raise PagesCompilerError("Compiler config quantity_basis must match the input contract")
    if config_quantity_basis != output_quantity_basis:
        raise PagesCompilerError("Compiler config quantity_basis must match the output contract")
    unpriced_config = dict(config.get("unpriced_external_handoff") or {})
    if unpriced_config.get("enabled"):
        if not str(unpriced_config.get("source_env_var") or "").strip():
            raise PagesCompilerError("Unpriced external handoff requires source_env_var when enabled")


def _coerce_regime_inputs(run_payload: dict[str, Any]) -> dict[str, float]:
    raw = run_payload.get("regime_inputs")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float] = {}
    for key, value in raw.items():
        try:
            out[str(key).strip()] = float(value)
        except Exception:
            continue
    return out


def _clamp(value: float, *, low: float, high: float) -> float:
    return min(high, max(low, value))


def _safe_ratio(numerator: float, denominator: float, *, default: float = 0.0) -> float:
    if denominator == 0.0:
        return default
    return numerator / denominator


def _coerce_external_unpriced_series(raw: Any) -> list[float | None] | None:
    return _coerce_series(raw)


def _normalize_external_regime_inputs(raw: Any) -> dict[str, float]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float] = {}
    for key, value in raw.items():
        token = str(key or "").strip()
        if not token:
            continue
        try:
            out[token] = float(value)
        except Exception:
            continue
    return out


def _load_external_unpriced_handoff(*, config: dict[str, Any]) -> dict[str, Any] | None:
    external_config = dict(config.get("unpriced_external_handoff") or {})
    if not bool(external_config.get("enabled")):
        return None
    env_var = str(external_config.get("source_env_var") or "").strip()
    if not env_var:
        raise PagesCompilerError("Unpriced external handoff requires source_env_var")
    handoff_path_raw = os.environ.get(env_var, "").strip()
    if not handoff_path_raw:
        return {
            "requested": True,
            "available": False,
            "source": "bridge_fallback",
            "run_inputs": {},
        }
    handoff_path = Path(handoff_path_raw)
    payload = _load_json(handoff_path)
    required_method_tag = str(external_config.get("required_method_tag") or "").strip()
    method_tag = str(payload.get("method_tag") or "").strip()
    if required_method_tag and method_tag != required_method_tag:
        raise PagesCompilerError(
            f"External unpriced handoff method_tag mismatch: expected {required_method_tag}, got {method_tag or '<blank>'}"
        )
    required_quantity_basis = str(external_config.get("required_quantity_basis") or "").strip()
    quantity_basis = str(payload.get("quantity_basis") or "").strip()
    if required_quantity_basis and quantity_basis != required_quantity_basis:
        raise PagesCompilerError(
            f"External unpriced handoff quantity_basis mismatch: expected {required_quantity_basis}, got {quantity_basis or '<blank>'}"
        )
    raw_run_inputs = payload.get("run_inputs")
    normalized_run_inputs: dict[str, dict[str, Any]] = {}
    if isinstance(raw_run_inputs, dict):
        iterable = list(raw_run_inputs.items())
    else:
        raw_runs = payload.get("runs")
        if not isinstance(raw_runs, list):
            raise PagesCompilerError("External unpriced handoff must define run_inputs or runs")
        iterable = []
        for item in raw_runs:
            if not isinstance(item, dict):
                continue
            run_id = str(item.get("run_id") or "").strip()
            if not run_id:
                continue
            iterable.append((run_id, item))
    series_names = dict(external_config.get("series_names") or {})
    price_name = str(series_names.get("price") or "P0_t").strip() or "P0_t"
    paid_quantity_name = str(series_names.get("paid_quantity") or "Q0_t").strip() or "Q0_t"
    unpaid_quantity_name = str(series_names.get("unpaid_quantity") or "U0_t").strip() or "U0_t"
    for run_id, raw_entry in iterable:
        token = str(run_id or "").strip()
        if not token:
            continue
        if not isinstance(raw_entry, dict):
            raise PagesCompilerError(f"External unpriced handoff entry for {token} must be an object")
        source = raw_entry.get("series") if isinstance(raw_entry.get("series"), dict) else raw_entry
        if not isinstance(source, dict):
            raise PagesCompilerError(f"External unpriced handoff missing series map for run {token}")
        price_values = _coerce_external_unpriced_series(source.get(price_name))
        paid_quantity_values = _coerce_external_unpriced_series(source.get(paid_quantity_name))
        unpaid_quantity_values = _coerce_external_unpriced_series(source.get(unpaid_quantity_name))
        if price_values is None or paid_quantity_values is None or unpaid_quantity_values is None:
            raise PagesCompilerError(
                f"External unpriced handoff missing {price_name}/{paid_quantity_name}/{unpaid_quantity_name} for run {token}"
            )
        normalized_run_inputs[token] = {
            "series": {
                price_name: price_values,
                paid_quantity_name: paid_quantity_values,
                unpaid_quantity_name: unpaid_quantity_values,
            },
            "regime_inputs": _normalize_external_regime_inputs(raw_entry.get("regime_inputs")),
            "scalars": _normalize_external_regime_inputs(raw_entry.get("scalars")),
            "notes": [str(item).strip() for item in list(raw_entry.get("notes") or []) if str(item).strip()],
        }
    return {
        "requested": True,
        "available": True,
        "source": "external_handoff",
        "handoff_mode": str(payload.get("handoff_mode") or "").strip(),
        "method_tag": method_tag,
        "quantity_basis": quantity_basis,
        "source_repo": str(payload.get("source_repo") or "").strip(),
        "generated_at": str(payload.get("generated_at") or "").strip(),
        "input_filename": handoff_path.name,
        "series_names": {
            "price": price_name,
            "paid_quantity": paid_quantity_name,
            "unpaid_quantity": unpaid_quantity_name,
        },
        "run_inputs": normalized_run_inputs,
    }


def _derive_gtaxsec_series(
    *,
    gtaxwd: list[float | None],
    gu6_share: list[float | None],
    tax_exposure_floor: float,
) -> list[float | None]:
    out: list[float | None] = []
    for gtaxwd_value, gu6_share_value in zip(gtaxwd, gu6_share, strict=False):
        if gtaxwd_value is None or gu6_share_value is None:
            out.append(None)
            continue
        out.append(gtaxwd_value * max(tax_exposure_floor, gu6_share_value))
    return out


def _run_childcare_fixed_point_core(
    *,
    base_price: float,
    base_burden: float,
    base_household_burden: float,
    base_paid_quantity: float,
    labor_delta: float,
    gu6_share: float,
    gtaxwd: float,
    regime_inputs: dict[str, float],
    solver: dict[str, Any],
    assumptions: dict[str, Any],
    unpaid_pool_override: float | None = None,
) -> dict[str, float | int | bool]:
    alpha_seed = _clamp(float(regime_inputs.get("alpha_t") or 0.0), low=0.0, high=1.0)
    subsidy_rate = _clamp(float(regime_inputs.get("sub_t") or 0.0), low=0.0, high=1.0)
    public_cost_share = _clamp(
        float(regime_inputs.get("public_cost_share") or 0.0),
        low=0.0,
        high=1.0,
    )
    kappa_q = float(regime_inputs.get("kappa_q_t") or 0.0)
    kappa_c = float(regime_inputs.get("kappa_c_t") or 0.0)

    max_iterations = max(1, int(solver.get("max_iterations") or 1))
    tolerance = max(1e-9, float(solver.get("tolerance") or 1e-6))
    relaxation = _clamp(float(solver.get("relaxation") or 1.0), low=1e-6, high=1.0)

    added_paid_care_hours_per_worker = float(
        assumptions.get("added_paid_care_hours_per_worker") or 0.0
    )
    childcare_worker_hours_per_year = float(
        assumptions.get("childcare_worker_hours_per_year") or 0.0
    )
    childcare_cost_per_hour_2017usd = float(
        assumptions.get("childcare_cost_per_hour_2017usd") or 0.0
    )
    household_burden_floor = float(assumptions.get("household_burden_floor") or 0.0)
    unpaid_care_pool_multiplier = max(
        1e-9, float(assumptions.get("unpaid_care_pool_multiplier") or 0.0)
    )
    quantity_per_worker_delta = max(
        1e-9, float(assumptions.get("quantity_per_worker_delta") or 0.0)
    )
    alpha_feedback_from_l2c = float(assumptions.get("alpha_feedback_from_l2c") or 0.0)
    alpha_subsidy_feedback = float(assumptions.get("alpha_subsidy_feedback") or 0.0)
    alpha_price_penalty = float(assumptions.get("alpha_price_penalty") or 0.0)
    slot_pressure_price_scale = float(assumptions.get("slot_pressure_price_scale") or 0.0)
    kappa_c_price_scale = float(assumptions.get("kappa_c_price_scale") or 0.0)
    qcap_floor_multiplier = max(
        1e-6, float(assumptions.get("qcap_floor_multiplier") or 0.0)
    )
    qcap_alpha_accommodation = float(assumptions.get("qcap_alpha_accommodation") or 0.0)
    tax_exposure_floor = _clamp(
        float(assumptions.get("tax_exposure_floor") or 0.0),
        low=0.0,
        high=1.0,
    )

    if (
        added_paid_care_hours_per_worker <= 0.0
        or childcare_worker_hours_per_year <= 0.0
        or childcare_cost_per_hour_2017usd <= 0.0
        or quantity_per_worker_delta <= 0.0
    ):
        raise PagesCompilerError(
            "Compiler assumptions must define positive childcare fixed-point bridge parameters"
        )

    effective_base_quantity = max(base_paid_quantity, 1e-9)
    if unpaid_pool_override is not None and unpaid_pool_override > 0.0:
        unpaid_pool = max(1e-9, unpaid_pool_override)
    else:
        unpaid_pool = effective_base_quantity * unpaid_care_pool_multiplier
    demand_increment = quantity_per_worker_delta * labor_delta
    capacity_floor = max(
        effective_base_quantity,
        effective_base_quantity * qcap_floor_multiplier,
    )
    tax_exposure = max(tax_exposure_floor, gu6_share)
    gtaxsec = gtaxwd * tax_exposure
    alpha_next = _clamp(
        _safe_ratio(demand_increment, unpaid_pool, default=0.0),
        low=0.0,
        high=1.0,
    )

    alpha = alpha_seed
    converged = False
    iteration_count = 0
    price = base_price
    burden = base_burden
    qcap = capacity_floor
    added_paid_quantity = demand_increment
    gap_ratio = 0.0

    for index in range(max_iterations):
        qcap = max(
            capacity_floor,
            effective_base_quantity * (1.0 + max(0.0, kappa_q))
            + qcap_alpha_accommodation * alpha * unpaid_pool,
        )
        added_paid_quantity = alpha * unpaid_pool + demand_increment
        unmet_demand = max(0.0, added_paid_quantity - qcap)
        gap_ratio = max(
            0.0,
            _safe_ratio(
                unmet_demand,
                effective_base_quantity + added_paid_quantity,
                default=0.0,
            ),
        )
        price_multiplier = max(
            0.05,
            1.0 + kappa_c_price_scale * kappa_c + slot_pressure_price_scale * gap_ratio,
        )
        next_price = base_price * price_multiplier
        next_burden = next_price * (1.0 - subsidy_rate)
        alpha_target = _clamp(
            alpha_seed
            + alpha_feedback_from_l2c * _safe_ratio(demand_increment, unpaid_pool, default=0.0)
            + alpha_subsidy_feedback * subsidy_rate
            - alpha_price_penalty * gap_ratio,
            low=0.0,
            high=1.0,
        )
        next_alpha = alpha + relaxation * (alpha_target - alpha)
        max_delta = max(
            abs(next_alpha - alpha),
            abs(next_price - price),
            abs(next_burden - burden),
        )
        alpha = next_alpha
        price = next_price
        burden = next_burden
        iteration_count = index + 1
        if max_delta <= tolerance:
            converged = True
            break

    price_ratio = _safe_ratio(price, base_price, default=1.0)
    jobs = added_paid_quantity * added_paid_care_hours_per_worker / childcare_worker_hours_per_year
    childcare_real = (
        added_paid_quantity
        * added_paid_care_hours_per_worker
        * childcare_cost_per_hour_2017usd
        / 1000.0
    )
    childcare_gross = childcare_real * price_ratio
    government_spending = childcare_gross * public_cost_share
    household_burden = max(
        household_burden_floor,
        base_household_burden * _safe_ratio(burden, base_burden, default=1.0),
    )

    return {
        "alpha_cc": alpha,
        "gccprice": price,
        "gcbur": burden,
        "gcchh": household_burden,
        "gccjobs": jobs,
        "gccreal": childcare_real,
        "gcgov": government_spending,
        "qcap": qcap,
        "gtaxsec": gtaxsec,
        "dl2c_loop": labor_delta,
        "dqpaid_loop": demand_increment,
        "alpha_next": alpha_next,
        "u0_cc": unpaid_pool,
        "gap_ratio": gap_ratio,
        "iterations": iteration_count,
        "converged": converged,
    }


def _derive_regime_labor_seed_ratio(
    *,
    base_l2c: float,
    base_paid_quantity: float,
    base_burden: float,
    base_gtaxsec: float,
    solved: dict[str, float | int | bool],
    assumptions: dict[str, Any],
) -> float:
    if base_l2c <= 0.0:
        return 0.0
    burden_relief = _safe_ratio(
        base_burden - float(solved["gcbur"]),
        abs(base_burden),
        default=0.0,
    )
    capacity_support = _safe_ratio(
        float(solved["qcap"]) - base_paid_quantity,
        base_paid_quantity,
        default=0.0,
    )
    tax_increase = _safe_ratio(
        float(solved["gtaxsec"]) - base_gtaxsec,
        max(abs(base_gtaxsec), 1e-9),
        default=0.0,
    )
    response = (
        float(assumptions.get("burden_relief_l2c_semi_elasticity") or 0.0) * burden_relief
        + float(assumptions.get("alpha_l2c_semi_elasticity") or 0.0) * float(solved["alpha_cc"])
        + float(assumptions.get("capacity_l2c_semi_elasticity") or 0.0) * capacity_support
        - float(assumptions.get("taxsec_l2c_semi_elasticity") or 0.0) * tax_increase
    )
    return _clamp(
        response,
        low=float(assumptions.get("negative_l2c_response_cap") or -0.12),
        high=float(assumptions.get("positive_l2c_response_cap") or 0.18),
    )


def _solve_fixed_point_period(
    *,
    base_price: float,
    base_burden: float,
    base_household_burden: float,
    base_paid_quantity: float,
    base_l2c: float,
    run_l2c: float,
    gu6_share: float,
    gtaxwd: float,
    base_gtaxsec: float,
    regime_inputs: dict[str, float],
    solver: dict[str, Any],
    assumptions: dict[str, Any],
    unpaid_pool_override: float | None = None,
) -> dict[str, float | int | bool]:
    observed_labor_delta = run_l2c - base_l2c
    seed_epsilon = max(1e-9, float(assumptions.get("labor_seed_epsilon") or 1e-6))
    prefer_regime_seed = bool(assumptions.get("prefer_regime_l2c_seed"))
    solved = _run_childcare_fixed_point_core(
        base_price=base_price,
        base_burden=base_burden,
        base_household_burden=base_household_burden,
        base_paid_quantity=base_paid_quantity,
        labor_delta=observed_labor_delta,
        gu6_share=gu6_share,
        gtaxwd=gtaxwd,
        regime_inputs=regime_inputs,
        solver=solver,
        assumptions=assumptions,
        unpaid_pool_override=unpaid_pool_override,
    )
    labor_seed_source = "observed_l2c_delta"
    seed_ratio = _derive_regime_labor_seed_ratio(
        base_l2c=base_l2c,
        base_paid_quantity=base_paid_quantity,
        base_burden=base_burden,
        base_gtaxsec=base_gtaxsec,
        solved=solved,
        assumptions=assumptions,
    )
    seeded_labor_delta = base_l2c * seed_ratio
    use_regime_seed = abs(observed_labor_delta) <= seed_epsilon
    if prefer_regime_seed and abs(seeded_labor_delta) > seed_epsilon:
        use_regime_seed = True
    if use_regime_seed and abs(seeded_labor_delta) > seed_epsilon:
        solved = _run_childcare_fixed_point_core(
            base_price=base_price,
            base_burden=base_burden,
            base_household_burden=base_household_burden,
            base_paid_quantity=base_paid_quantity,
            labor_delta=seeded_labor_delta,
            gu6_share=gu6_share,
            gtaxwd=gtaxwd,
            regime_inputs=regime_inputs,
            solver=solver,
            assumptions=assumptions,
            unpaid_pool_override=unpaid_pool_override,
        )
        observed_labor_delta = seeded_labor_delta
        labor_seed_source = "regime_response_seed"
    solved["labor_seed_source"] = labor_seed_source
    solved["labor_seed_ratio"] = _safe_ratio(observed_labor_delta, base_l2c, default=0.0)
    return solved


def _apply_childcare_regime_compiler(
    *,
    run_payloads: dict[str, dict[str, Any]],
    config_path: Path,
    input_contract_path: Path,
    output_contract_path: Path,
) -> None:
    config = _load_json(config_path)
    input_contract = _load_json(input_contract_path)
    output_contract = _load_json(output_contract_path)
    _normalize_contracts(config=config, input_contract=input_contract, output_contract=output_contract)

    shared_assumptions = dict(config.get("assumptions") or {})
    solver = dict(config.get("solver") or {})
    external_unpriced_handoff = _load_external_unpriced_handoff(config=config)
    quantity_proxy_order = [
        str(item or "").strip()
        for item in list(config.get("quantity_proxy_order") or [])
        if str(item or "").strip()
    ]
    if not quantity_proxy_order:
        raise PagesCompilerError("Compiler config must include quantity_proxy_order")

    method_tag = str(config.get("method_tag") or "").strip()
    quantity_basis = str(config.get("quantity_basis") or "").strip()
    compiler_run_payloads = [
        payload
        for payload in run_payloads.values()
        if str(payload.get("method_tag") or "").strip() == method_tag
    ]
    for payload in compiler_run_payloads:
        payload_quantity_basis = str(payload.get("quantity_basis") or "").strip()
        if payload_quantity_basis != quantity_basis:
            raise PagesCompilerError(
                f"Run {payload.get('run_id')} has quantity_basis that disagrees with compiler config"
            )

    for run_payload in compiler_run_payloads:
        run_id = str(run_payload.get("run_id") or "").strip()
        base_run_id = str(run_payload.get("base_run_id") or "").strip()
        base_payload = run_payloads.get(base_run_id)
        if base_payload is None:
            raise PagesCompilerError(f"Compiler base run not found: {base_run_id}")
        _apply_childcare_regime_to_run(
            run_payload=run_payload,
            base_payload=base_payload,
            solver=solver,
            shared_assumptions=shared_assumptions,
            outer_loop_config=dict(config.get("outer_loop") or {}),
            external_unpriced_handoff=external_unpriced_handoff,
            quantity_proxy_order=quantity_proxy_order,
        )


def _apply_childcare_regime_to_run(
    *,
    run_payload: dict[str, Any],
    base_payload: dict[str, Any],
    solver: dict[str, Any],
    shared_assumptions: dict[str, Any],
    outer_loop_config: dict[str, Any],
    external_unpriced_handoff: dict[str, Any] | None,
    quantity_proxy_order: list[str],
) -> None:
    run_series = dict(run_payload.get("series") or {})
    base_series = dict(base_payload.get("series") or {})

    regime_inputs = _coerce_regime_inputs(run_payload)
    unpriced_source = "local_bridge"
    unpriced_external = False
    unpriced_method = "bridge_runtime_v1"
    unpriced_repo = ""
    unpriced_generated_at = ""
    unpriced_handoff_mode = ""
    external_unpriced_entry = None
    if external_unpriced_handoff:
        run_input_map = dict(external_unpriced_handoff.get("run_inputs") or {})
        external_unpriced_entry = run_input_map.get(str(run_payload.get("run_id") or "").strip())
    if external_unpriced_entry is not None:
        regime_inputs = {**dict(external_unpriced_entry.get("regime_inputs") or {}), **regime_inputs}
        unpriced_handoff_mode = str(external_unpriced_handoff.get("handoff_mode") or "").strip()
        unpriced_source = unpriced_handoff_mode or "external_handoff"
        unpriced_external = unpriced_handoff_mode == "external_handoff"
        unpriced_method = str(external_unpriced_handoff.get("method_tag") or "").strip() or unpriced_method
        unpriced_repo = str(external_unpriced_handoff.get("source_repo") or "").strip()
        unpriced_generated_at = str(external_unpriced_handoff.get("generated_at") or "").strip()
    elif external_unpriced_handoff:
        unpriced_source = "bridge_fallback"
    run_price = _derive_gccprice(run_series)
    base_price = _derive_gccprice(base_series)
    run_burden = _derive_gcbur(run_series, subsidy_rate=float(regime_inputs.get("sub_t") or 0.0))
    base_burden = _derive_gcbur(base_series, subsidy_rate=0.0)
    if run_price is None or base_price is None or run_burden is None or base_burden is None:
        raise PagesCompilerError(f"Missing childcare price/burden inputs for {run_payload.get('run_id')}")

    run_l2c = _resolve_series(run_series, candidates=quantity_proxy_order)
    base_l2c = _resolve_series(base_series, candidates=quantity_proxy_order)
    if run_l2c is None or base_l2c is None:
        raise PagesCompilerError(f"Missing quantity proxy for {run_payload.get('run_id')}")
    p0_cc = list(base_price)
    q0_cc = list(base_l2c)
    u0_cc_base = _vector_scale(list(base_l2c), float(shared_assumptions.get("unpaid_care_pool_multiplier") or 0.0))
    if external_unpriced_entry is not None:
        series_names = dict(external_unpriced_handoff.get("series_names") or {})
        source_series = dict(external_unpriced_entry.get("series") or {})
        external_price = _coerce_external_unpriced_series(
            source_series.get(str(series_names.get("price") or "P0_t"))
        )
        external_paid_quantity = _coerce_external_unpriced_series(
            source_series.get(str(series_names.get("paid_quantity") or "Q0_t"))
        )
        external_unpaid_quantity = _coerce_external_unpriced_series(
            source_series.get(str(series_names.get("unpaid_quantity") or "U0_t"))
        )
        if (
            external_price is None
            or external_paid_quantity is None
            or external_unpaid_quantity is None
            or len(external_price) != len(base_price)
            or len(external_paid_quantity) != len(base_l2c)
            or len(external_unpaid_quantity) != len(base_l2c)
        ):
            raise PagesCompilerError(
                f"External unpriced handoff series length mismatch for {run_payload.get('run_id')}"
            )
        p0_cc = list(external_price)
        q0_cc = list(external_paid_quantity)
        u0_cc_base = list(external_unpaid_quantity)
    run_household = _coerce_series(run_series.get("GCCHH")) or _vector_fill(
        base_burden,
        default=float(shared_assumptions.get("household_burden_floor") or 0.0),
    )
    base_household = _coerce_series(base_series.get("GCCHH")) or _vector_fill(
        base_burden,
        default=float(shared_assumptions.get("household_burden_floor") or 0.0),
    )
    gu6_share = _coerce_series(run_series.get("GU6SHR"))
    gtaxwd = _coerce_series(run_series.get("GTAXWD"))
    if gu6_share is None or gtaxwd is None:
        raise PagesCompilerError(f"Missing GU6SHR/GTAXWD inputs for {run_payload.get('run_id')}")
    gdpr = _coerce_series(run_series.get("GDPR"))

    gccprice_out: list[float | None] = []
    gcbur_out: list[float | None] = []
    gcchh_out: list[float | None] = []
    gccjobs_out: list[float | None] = []
    gccreal_out: list[float | None] = []
    gcgov_out: list[float | None] = []
    gdpr_net_out: list[float | None] = []
    alpha_cc_out: list[float | None] = []
    qcap_out: list[float | None] = []
    gtaxsec_out: list[float | None] = []
    dl2c_loop_out: list[float | None] = []
    dqpaid_loop_out: list[float | None] = []
    alpha_next_out: list[float | None] = []
    u0_cc_out: list[float | None] = []
    solver_iterations: list[int] = []
    all_converged = True

    base_gtaxwd = _coerce_series(base_series.get("GTAXWD")) or list(gtaxwd)
    base_gu6_share = _coerce_series(base_series.get("GU6SHR")) or list(gu6_share)
    base_gtaxsec_seed = _derive_gtaxsec_series(
        gtaxwd=base_gtaxwd,
        gu6_share=base_gu6_share,
        tax_exposure_floor=float(shared_assumptions.get("tax_exposure_floor") or 0.0),
    )

    labor_seed_sources: list[str] = []

    for idx in range(len(run_l2c)):
        values = (
            run_price[idx],
            p0_cc[idx],
            run_burden[idx],
            base_burden[idx],
            run_l2c[idx],
            base_l2c[idx],
            q0_cc[idx],
            base_household[idx] if idx < len(base_household) else None,
            gu6_share[idx] if idx < len(gu6_share) else None,
            gtaxwd[idx] if idx < len(gtaxwd) else None,
            base_gtaxsec_seed[idx] if idx < len(base_gtaxsec_seed) else None,
        )
        if any(value is None for value in values):
            gccprice_out.append(None)
            gcbur_out.append(None)
            gcchh_out.append(None)
            gccjobs_out.append(None)
            gccreal_out.append(None)
            gcgov_out.append(None)
            gdpr_net_out.append(None if gdpr is None else gdpr[idx])
            alpha_cc_out.append(None)
            qcap_out.append(None)
            gtaxsec_out.append(None)
            dl2c_loop_out.append(None)
            dqpaid_loop_out.append(None)
            alpha_next_out.append(None)
            u0_cc_out.append(None)
            all_converged = False
            continue
        solved = _solve_fixed_point_period(
            base_price=float(base_price[idx]),
            base_burden=float(base_burden[idx]),
            base_household_burden=float(base_household[idx]),
            base_paid_quantity=float(q0_cc[idx]),
            base_l2c=float(base_l2c[idx]),
            run_l2c=float(run_l2c[idx]),
            gu6_share=float(gu6_share[idx]),
            gtaxwd=float(gtaxwd[idx]),
            base_gtaxsec=float(base_gtaxsec_seed[idx]),
            regime_inputs=regime_inputs,
            solver=solver,
            assumptions=shared_assumptions,
            unpaid_pool_override=float(u0_cc_base[idx]) if u0_cc_base[idx] is not None else None,
        )
        gccprice_out.append(float(solved["gccprice"]))
        gcbur_out.append(float(solved["gcbur"]))
        gcchh_out.append(float(solved["gcchh"]))
        gccjobs_out.append(float(solved["gccjobs"]))
        gccreal_out.append(float(solved["gccreal"]))
        gcgov_out.append(float(solved["gcgov"]))
        alpha_cc_out.append(float(solved["alpha_cc"]))
        qcap_out.append(float(solved["qcap"]))
        gtaxsec_out.append(float(solved["gtaxsec"]))
        dl2c_loop_out.append(float(solved["dl2c_loop"]))
        dqpaid_loop_out.append(float(solved["dqpaid_loop"]))
        alpha_next_out.append(float(solved["alpha_next"]))
        u0_cc_out.append(float(solved["u0_cc"]))
        solver_iterations.append(int(solved["iterations"]))
        labor_seed_sources.append(str(solved.get("labor_seed_source") or ""))
        all_converged = all_converged and bool(solved["converged"])
        if gdpr is None or gdpr[idx] is None:
            gdpr_net_out.append(None)
        else:
            gdpr_net_out.append(float(gdpr[idx]) - float(solved["gccreal"]))

    run_series["GCCPRICE"] = gccprice_out
    run_series["GCBUR"] = gcbur_out
    run_series["GCCHH"] = gcchh_out
    run_series["GCCJOBS"] = gccjobs_out
    run_series["GCGOV"] = gcgov_out
    run_series["GCCREAL"] = gccreal_out
    run_series["GDPR_NET"] = gdpr_net_out
    run_series["ALPHA_CC"] = alpha_cc_out
    run_series["QCAP"] = qcap_out
    run_series["GTAXSEC"] = gtaxsec_out
    run_series["DL2C_LOOP"] = dl2c_loop_out
    run_series["DQPAID_LOOP"] = dqpaid_loop_out
    run_series["ALPHA_NEXT"] = alpha_next_out
    run_series["U0_CC"] = u0_cc_out
    run_series["P0_CC"] = p0_cc
    run_series["Q0_CC"] = q0_cc
    run_series["U0_CC_BASE"] = u0_cc_base
    run_payload["series"] = run_series
    max_positive_dl2c = max(
        (value for value in dl2c_loop_out if value is not None and value > 0.0),
        default=0.0,
    )
    max_negative_dl2c = min((value for value in dl2c_loop_out if value is not None), default=0.0)
    max_abs_dl2c = max((abs(value) for value in dl2c_loop_out if value is not None), default=0.0)
    max_dqpaid_loop = max((value for value in dqpaid_loop_out if value is not None), default=0.0)
    min_dqpaid_loop = min((value for value in dqpaid_loop_out if value is not None), default=0.0)
    max_alpha_next = max((value for value in alpha_next_out if value is not None), default=0.0)
    max_u0_cc = max((value for value in u0_cc_out if value is not None), default=0.0)
    rerun_trigger_epsilon = max(
        1e-9,
        float(outer_loop_config.get("rerun_trigger_epsilon") or shared_assumptions.get("labor_seed_epsilon") or 0.0),
    )
    run_payload["regime_meta"] = {
        "method_tag": str(run_payload.get("method_tag") or "").strip(),
        "quantity_basis": str(run_payload.get("quantity_basis") or "").strip(),
        "base_run_id": str(run_payload.get("base_run_id") or "").strip(),
        "scenario_kind": str(run_payload.get("scenario_kind") or "").strip(),
        "case_role": str(run_payload.get("case_role") or "").strip(),
        "financing_rule": str(run_payload.get("financing_rule") or "").strip(),
        "public_interpretation": str(run_payload.get("public_interpretation") or "").strip(),
        "regime_inputs": regime_inputs,
        "notes": [str(item).strip() for item in list(run_payload.get("details") or []) if str(item).strip()],
        "solver_mode": "conditional_fixed_point",
        "solver_iterations": max(solver_iterations) if solver_iterations else 0,
        "solver_converged": all_converged,
        "solver_tolerance": float(solver.get("tolerance") or 0.0),
        "unpriced_method": unpriced_method,
        "unpriced_ready": True,
        "unpriced_source": unpriced_source,
        "unpriced_external": unpriced_external,
        "unpriced_repo": unpriced_repo,
        "unpriced_generated_at": unpriced_generated_at,
        "unpriced_handoff_mode": unpriced_handoff_mode,
        "labor_seed_source": "regime_response_seed"
        if "regime_response_seed" in labor_seed_sources
        else "observed_l2c_delta",
    }
    run_payload["outer_loop_meta"] = {
        "handoff_mode": "childcare_to_fair_outer_loop_seed",
        "handoff_ready": all_converged,
        "requires_fair_rerun": max_abs_dl2c > rerun_trigger_epsilon,
        "base_run_id": str(run_payload.get("base_run_id") or "").strip(),
        "quantity_update_rule": "DQPAID_LOOP = quantity_per_worker_delta * DL2C_LOOP",
        "alpha_update_rule": "ALPHA_NEXT = min(1, abs(DQPAID_LOOP) / U0_CC)",
        "labor_seed_rule": "Use observed (L2C - L2C_base) when available; otherwise seed signed DL2C_LOOP from burden relief, alpha, capacity, and tax exposure changes.",
        "labor_seed_source": "regime_response_seed"
        if "regime_response_seed" in labor_seed_sources
        else "observed_l2c_delta",
        "quantity_per_worker_delta": float(shared_assumptions.get("quantity_per_worker_delta") or 0.0),
        "unpaid_care_pool_multiplier": float(
            shared_assumptions.get("unpaid_care_pool_multiplier") or 0.0
        ),
        "max_positive_dl2c": max_positive_dl2c,
        "max_negative_dl2c": max_negative_dl2c,
        "max_abs_dl2c": max_abs_dl2c,
        "max_dqpaid_loop": max_dqpaid_loop,
        "min_dqpaid_loop": min_dqpaid_loop,
        "max_alpha_next": max_alpha_next,
        "max_u0_cc": max_u0_cc,
        "unpriced_ready": True,
        "unpriced_method": unpriced_method,
        "unpriced_source": unpriced_source,
        "unpriced_external": unpriced_external,
        "unpriced_repo": unpriced_repo,
        "unpriced_generated_at": unpriced_generated_at,
        "unpriced_handoff_mode": unpriced_handoff_mode,
    }
