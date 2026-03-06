"""Tests for policy block primitives and registry."""

from __future__ import annotations

from fp_wraptr.scenarios.policies import (
    InfrastructureSpending,
    JobGuarantee,
    MonetaryRule,
    PhaseIn,
    PolicyBlock,
    PolicyRegistry,
    compile_policies,
)


def _expected_jg_net_transfer_bn(policy: JobGuarantee) -> float:
    annual_hours = 2080
    wage_bill_bn = (policy.jobs * policy.wage * annual_hours) / 1e9
    benefits_bn = wage_bill_bn * policy.benefits_rate
    admin_bn = wage_bill_bn * policy.admin_rate
    gross_cost_bn = wage_bill_bn + benefits_bn + admin_bn

    total_offset_rate = (
        abs(policy.offsets.unemployment_benefits)
        + abs(policy.offsets.medicaid_fed)
        + abs(policy.offsets.medicaid_state)
        + abs(policy.offsets.eitc_fed)
        + abs(policy.offsets.eitc_state)
    )
    net_transfer_bn = gross_cost_bn * (1 - total_offset_rate)
    return round(net_transfer_bn / 4, 2)


def test_jg_policy_compile() -> None:
    policy = JobGuarantee(
        jobs=1000000,
        wage=15.0,
        benefits_rate=0.20,
        admin_rate=0.05,
    )
    overrides = policy.compile()
    assert isinstance(overrides, dict)
    assert "TRGHQ" in overrides
    assert overrides["TRGHQ"].value == _expected_jg_net_transfer_bn(policy)


def test_policy_registry_lookup() -> None:
    cls = PolicyRegistry.get_type("job_guarantee")

    assert cls is not None
    assert issubclass(cls, PolicyBlock)


def test_policy_phase_in() -> None:
    policy = JobGuarantee(
        jobs=1000000,
        wage=15.0,
        phase_in=PhaseIn(start="2025.4", step_per_quarter=0.20),
    )
    overrides = policy.compile()

    assert isinstance(overrides, dict)
    assert policy.phase_in is not None
    assert policy.phase_in.step_per_quarter == 0.20


def test_policy_block_to_dict() -> None:
    policy = JobGuarantee(jobs=1000000, wage=15.0, benefits_rate=0.2)

    payload = policy.to_summary()
    assert payload["type"] == "job_guarantee"
    assert "jobs" in payload["parameters"]


def test_unknown_policy_type() -> None:
    try:
        PolicyRegistry.create({"type": "does_not_exist"})
    except ValueError as exc:
        assert "Unknown policy type" in str(exc)
    else:
        raise AssertionError("Expected unknown policy type to raise ValueError")


def test_infrastructure_compile() -> None:
    """InfrastructureSpending compiles to IF override at quarterly rate."""
    policy = InfrastructureSpending(annual_spending_bn=200.0)
    overrides = policy.compile()

    assert "IF" in overrides
    assert overrides["IF"].method == "CHGSAMEABS"
    assert overrides["IF"].value == 50.0  # 200 / 4


def test_monetary_rule_compile() -> None:
    """MonetaryRule compiles to RS override with level peg by default."""
    policy = MonetaryRule(rate=4.0)
    overrides = policy.compile()

    assert "RS" in overrides
    assert overrides["RS"].method == "SAMEVALUE"
    assert overrides["RS"].value == 4.0


def test_monetary_rule_shift() -> None:
    """MonetaryRule with CHGSAMEABS compiles to a shift from baseline."""
    policy = MonetaryRule(rate=0.5, method="CHGSAMEABS")
    overrides = policy.compile()

    assert overrides["RS"].method == "CHGSAMEABS"
    assert overrides["RS"].value == 0.5


def test_compile_policies_multiple() -> None:
    """compile_policies merges overrides from multiple policy blocks."""
    policies = [
        {"type": "job_guarantee", "jobs": 10_000_000, "wage": 15.0},
        {"type": "transfer_boost", "amount_bn": 50.0},
    ]
    merged = compile_policies(policies)

    # TransferBoost overrides JG's TRGHQ (later wins)
    assert "TRGHQ" in merged
    assert merged["TRGHQ"].value == round(50.0 / 4, 2)


def test_policy_registry_all_types() -> None:
    """All 5 policy types are registered."""
    types = PolicyRegistry.list_types()
    assert len(types) == 5
    assert set(types) == {
        "job_guarantee",
        "transfer_boost",
        "tax_change",
        "infrastructure",
        "monetary_rule",
    }
