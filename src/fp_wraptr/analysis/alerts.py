"""Alert helpers for forecast outputs."""

from __future__ import annotations

from dataclasses import dataclass

from fp_wraptr.io.parser import FPOutputData

__all__ = ["AlertResult", "check_alerts"]


@dataclass
class AlertResult:
    variable: str
    threshold_type: str
    threshold_value: float
    actual_value: float
    period: str
    breached: bool


def check_alerts(
    alerts: dict[str, dict[str, float]],
    output: FPOutputData,
) -> list[AlertResult]:
    """Check alert thresholds against forecast output.

    Args:
        alerts: Mapping of variable name to threshold map, e.g. {"UR": {"max": 6.0}}.
        output: Parsed FP forecast output.

    Returns:
        List of breached alert results.
    """
    results: list[AlertResult] = []
    if not alerts:
        return results

    if not output.variables:
        return results

    for variable, thresholds in alerts.items():
        forecast_variable = output.variables.get(variable)
        if forecast_variable is None:
            continue

        max_threshold = thresholds.get("max")
        min_threshold = thresholds.get("min")
        if max_threshold is None and min_threshold is None:
            continue

        levels = forecast_variable.levels
        periods = output.periods

        for index, actual_value in enumerate(levels):
            period = periods[index] if index < len(periods) else str(index)

            if max_threshold is not None and actual_value > max_threshold:
                results.append(
                    AlertResult(
                        variable=variable,
                        threshold_type="max",
                        threshold_value=float(max_threshold),
                        actual_value=float(actual_value),
                        period=period,
                        breached=True,
                    )
                )

            if min_threshold is not None and actual_value < min_threshold:
                results.append(
                    AlertResult(
                        variable=variable,
                        threshold_type="min",
                        threshold_value=float(min_threshold),
                        actual_value=float(actual_value),
                        period=period,
                        breached=True,
                    )
                )

    return results
