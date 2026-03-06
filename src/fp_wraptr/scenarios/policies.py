"""Policy blocks — named interventions that compile into model overrides.

A policy block is a high-level representation of a policy intervention
(e.g., "job guarantee at $15/hr absorbing 15M workers") that compiles
into the low-level VariableOverride dicts that ScenarioConfig expects.

Policy blocks are:
- Human-readable (economists think in policy parameters, not FP variable names)
- Composable (multiple policies can be stacked in one scenario)
- Auditable (the compiled overrides are stored alongside the policy spec)
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from fp_wraptr.scenarios.config import VariableOverride

__all__ = [
    "InfrastructureSpending",
    "JGOffsets",
    "JobGuarantee",
    "MonetaryRule",
    "PhaseIn",
    "PolicyBlock",
    "PolicyRegistry",
    "TaxChange",
    "TransferBoost",
    "compile_policies",
]


class PhaseIn(BaseModel):
    """Gradual policy rollout schedule."""

    start: str = Field(default="2025.4", description="First period of policy")
    step_per_quarter: float = Field(
        default=0.20,
        description="Fraction of full policy added per quarter (0.2 = full by Q5)",
    )


class JGOffsets(BaseModel):
    """Automatic stabilizer offsets for a job guarantee program.

    Values represent the fraction by which existing transfer programs
    are reduced due to JG employment. Based on Levy PSE methodology.
    """

    unemployment_benefits: float = Field(
        default=-0.25,
        description="Fractional reduction in UI benefits",
    )
    medicaid_fed: float = Field(
        default=-0.05,
        description="Fractional reduction in federal Medicaid",
    )
    medicaid_state: float = Field(
        default=-0.01,
        description="Fractional reduction in state Medicaid",
    )
    eitc_fed: float = Field(
        default=-0.05,
        description="Fractional reduction in federal EITC",
    )
    eitc_state: float = Field(
        default=-0.01,
        description="Fractional reduction in state EITC",
    )


class PolicyBlock(BaseModel):
    """Base class for policy interventions.

    Subclasses implement ``compile()`` to expand policy parameters
    into low-level variable overrides for the FP model.
    """

    type: str = Field(description="Policy type identifier")
    description: str = Field(default="", description="Human-readable description")
    phase_in: PhaseIn | None = Field(default=None, description="Gradual rollout schedule")

    def compile(self) -> dict[str, VariableOverride]:
        """Expand this policy into variable overrides.

        Returns:
            Dict mapping FP variable names to VariableOverride objects.
        """
        raise NotImplementedError(f"compile() not implemented for policy type '{self.type}'")

    def to_summary(self) -> dict[str, Any]:
        """Return a human-readable summary of this policy for audit trail."""
        return {
            "type": self.type,
            "description": self.description,
            "parameters": self.model_dump(exclude={"type", "description"}),
        }


class JobGuarantee(PolicyBlock):
    """Job guarantee / public service employment intervention.

    Based on Levy Economics Institute PSE methodology:
    - Countercyclical workforce assumption
    - Wage/benefit pass-through to average wage proxy
    - Offset savings from reduced UI, Medicaid, EITC
    - Phase-in schedule (default: 20% per quarter)

    Reference: Levy Economics Institute, "Public Service Employment"
    (rpr_4_18.pdf), and Fullwiler (SSRN 2194960).
    """

    type: str = Field(default="job_guarantee")
    jobs: float = Field(description="Target JG employment (number of persons)")
    wage: float = Field(description="Hourly JG wage ($)")
    benefits_rate: float = Field(
        default=0.20,
        description="Benefits as fraction of wage bill",
    )
    admin_rate: float = Field(
        default=0.05,
        description="Admin costs as fraction of wage bill",
    )
    wage_pass_through: float = Field(
        default=0.20,
        description="Fraction of JG-to-avg-wage gap passed into average wage",
    )
    offsets: JGOffsets = Field(default_factory=JGOffsets)

    def compile(self) -> dict[str, VariableOverride]:
        """Compile JG policy into FP variable overrides.

        Computes:
        - TRGHQ: government transfer increase (wage bill + benefits + admin)
        - TRGSQ: offset savings from reduced transfers

        Dollar values are in billions (FP model convention).
        Annual hours assumed: 2080 (40hr/wk * 52wk).
        """
        annual_hours = 2080
        # Wage bill in billions of dollars per year
        wage_bill_bn = (self.jobs * self.wage * annual_hours) / 1e9
        benefits_bn = wage_bill_bn * self.benefits_rate
        admin_bn = wage_bill_bn * self.admin_rate
        gross_cost_bn = wage_bill_bn + benefits_bn + admin_bn

        # Offset savings (fraction of gross cost)
        total_offset_rate = (
            abs(self.offsets.unemployment_benefits)
            + abs(self.offsets.medicaid_fed)
            + abs(self.offsets.medicaid_state)
            + abs(self.offsets.eitc_fed)
            + abs(self.offsets.eitc_state)
        )
        offset_savings_bn = gross_cost_bn * total_offset_rate

        # Net transfer increase = gross cost - offset savings
        net_transfer_bn = gross_cost_bn - offset_savings_bn

        overrides: dict[str, VariableOverride] = {
            # Government transfers to persons (quarterly rate = annual / 4)
            "TRGHQ": VariableOverride(
                method="CHGSAMEABS",
                value=round(net_transfer_bn / 4, 2),
            ),
        }

        return overrides

    def gross_cost_bn(self) -> float:
        """Compute gross annual program cost in billions."""
        annual_hours = 2080
        wage_bill = (self.jobs * self.wage * annual_hours) / 1e9
        return wage_bill * (1 + self.benefits_rate + self.admin_rate)


class TransferBoost(PolicyBlock):
    """One-time or sustained transfer payment increase."""

    type: str = Field(default="transfer_boost")
    amount_bn: float = Field(description="Transfer increase in billions of dollars (annual rate)")
    method: str = Field(
        default="CHGSAMEABS",
        description="Override method for TRGHQ",
    )

    def compile(self) -> dict[str, VariableOverride]:
        return {
            "TRGHQ": VariableOverride(
                method=self.method,
                value=round(self.amount_bn / 4, 2),  # quarterly
            ),
        }


class TaxChange(PolicyBlock):
    """Income or payroll tax rate adjustment."""

    type: str = Field(default="tax_change")
    rate_change_pct: float = Field(
        description="Percentage point change in effective tax rate (e.g., -2.0 for a 2pp cut)"
    )
    target: str = Field(
        default="D1G",
        description="FP tax variable to adjust (D1G = federal personal tax rate)",
    )

    def compile(self) -> dict[str, VariableOverride]:
        return {
            self.target: VariableOverride(
                method="CHGSAMEABS",
                value=self.rate_change_pct,
            ),
        }


class InfrastructureSpending(PolicyBlock):
    """Government infrastructure / fixed investment spending increase.

    Maps to the FP model's IF variable (federal nondefense fixed investment).
    Specify annual spending in billions; compiled to quarterly rate.
    """

    type: str = Field(default="infrastructure")
    annual_spending_bn: float = Field(
        description="Annual infrastructure spending increase in billions of dollars"
    )
    target: str = Field(
        default="IF",
        description="FP investment variable (IF = fed nondefense fixed investment)",
    )

    def compile(self) -> dict[str, VariableOverride]:
        return {
            self.target: VariableOverride(
                method="CHGSAMEABS",
                value=round(self.annual_spending_bn / 4, 2),  # quarterly
            ),
        }


class MonetaryRule(PolicyBlock):
    """Monetary policy rule — override the short-term interest rate.

    Sets the FP model's RS variable (3-month Treasury rate) to a fixed
    level or adjusts it by a fixed amount. Use method="SAMEVALUE" for
    a level peg (e.g., hold at 4.0%) or "CHGSAMEABS" for a shift
    from baseline (e.g., +0.5pp).
    """

    type: str = Field(default="monetary_rule")
    rate: float = Field(description="Interest rate value (level or delta, depending on method)")
    method: str = Field(
        default="SAMEVALUE",
        description="Override method: SAMEVALUE for level peg, CHGSAMEABS for shift",
    )
    target: str = Field(
        default="RS",
        description="FP interest rate variable (RS = 3-month T-bill rate)",
    )

    def compile(self) -> dict[str, VariableOverride]:
        return {
            self.target: VariableOverride(
                method=self.method,
                value=self.rate,
            ),
        }


# ---------------------------------------------------------------------------
# Registry — lookup policy types by name
# ---------------------------------------------------------------------------

_POLICY_TYPES: dict[str, type[PolicyBlock]] = {
    "job_guarantee": JobGuarantee,
    "transfer_boost": TransferBoost,
    "tax_change": TaxChange,
    "infrastructure": InfrastructureSpending,
    "monetary_rule": MonetaryRule,
}


class PolicyRegistry:
    """Registry of available policy block types."""

    @staticmethod
    def list_types() -> list[str]:
        """Return names of all registered policy types."""
        return sorted(_POLICY_TYPES.keys())

    @staticmethod
    def get_type(name: str) -> type[PolicyBlock] | None:
        """Look up a policy block class by type name."""
        return _POLICY_TYPES.get(name)

    @staticmethod
    def create(data: dict[str, Any]) -> PolicyBlock:
        """Create a policy block from a dict (e.g., parsed from YAML).

        The dict must include a ``type`` key matching a registered policy type.
        """
        policy_type = data.get("type", "")
        cls = _POLICY_TYPES.get(policy_type)
        if cls is None:
            registered = ", ".join(sorted(_POLICY_TYPES.keys()))
            raise ValueError(
                f"Unknown policy type '{policy_type}'. Registered types: {registered}"
            )
        return cls(**data)

    @staticmethod
    def register(name: str, cls: type[PolicyBlock]) -> None:
        """Register a new policy block type."""
        _POLICY_TYPES[name] = cls


def compile_policies(policies: list[dict[str, Any] | PolicyBlock]) -> dict[str, VariableOverride]:
    """Compile a list of policy blocks into merged variable overrides.

    Later policies override earlier ones if they touch the same variable.

    Args:
        policies: List of policy block dicts or PolicyBlock instances.

    Returns:
        Merged dict of variable overrides.
    """
    merged: dict[str, VariableOverride] = {}
    for policy in policies:
        block = PolicyRegistry.create(policy) if isinstance(policy, dict) else policy
        overrides = block.compile()
        merged.update(overrides)
    return merged
