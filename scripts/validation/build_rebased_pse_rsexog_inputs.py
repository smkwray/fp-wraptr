#!/usr/bin/env python3
from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

import yaml

from fp_wraptr.io.loadformat import read_loadformat

REPO_ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS_ROOT = REPO_ROOT / "do" / "artifacts-pse2026-rebased-stock-core"
PSE_REBASED_ROOT = REPO_ROOT / "do" / "pse_rebased_2026"
OVERLAYS_ROOT = PSE_REBASED_ROOT / "overlays"


@dataclass(frozen=True)
class ScenarioVariant:
    key: str
    standard_run_id: str
    wage_run_id: str
    source_overlay: str
    target_overlay: str
    target_scenario: str


VARIANTS: tuple[ScenarioVariant, ...] = (
    ScenarioVariant(
        key="high20",
        standard_run_id="pse2025_high_20h_3pct_stock_pf",
        wage_run_id="pse2025_20h_3pct_wageonly_stock_pf",
        source_overlay="high20",
        target_overlay="high20_rsexog",
        target_scenario="pse2025_high_20h_3pct_rsexog_stock_pf",
    ),
    ScenarioVariant(
        key="low20",
        standard_run_id="pse2025_low_20h_3pct_stock_pf",
        wage_run_id="pse2025_20h_3pct_wageonly_stock_pf",
        source_overlay="low20",
        target_overlay="low20_rsexog",
        target_scenario="pse2025_low_20h_3pct_rsexog_stock_pf",
    ),
    ScenarioVariant(
        key="high15",
        standard_run_id="pse2025_high_15h_3pct_stock_pf",
        wage_run_id="pse2025_15h_3pct_wageonly_stock_pf",
        source_overlay="high15",
        target_overlay="high15_rsexog",
        target_scenario="pse2025_high_15h_3pct_rsexog_stock_pf",
    ),
    ScenarioVariant(
        key="low15",
        standard_run_id="pse2025_low_15h_3pct_stock_pf",
        wage_run_id="pse2025_15h_3pct_wageonly_stock_pf",
        source_overlay="low15",
        target_overlay="low15_rsexog",
        target_scenario="pse2025_low_15h_3pct_rsexog_stock_pf",
    ),
)

HORIZONS = {
    "5y": {
        "suffix": "",
        "input_file": "apsreb5.txt",
        "forecast_end": "2029.4",
    },
    "10y": {
        "suffix": "_10y",
        "input_file": "apsreb10.txt",
        "forecast_end": "2034.4",
    },
}


def _latest_artifact_dir(run_id: str) -> Path:
    matches = sorted(path for path in ARTIFACTS_ROOT.glob(f"{run_id}_*") if path.is_dir())
    if not matches:
        raise FileNotFoundError(f"Missing artifact for {run_id} under {ARTIFACTS_ROOT}")
    return matches[-1]


def _rs_values_for_run(run_id: str, *, forecast_end: str) -> list[tuple[str, float]]:
    artifact_dir = _latest_artifact_dir(run_id)
    periods, series = read_loadformat(artifact_dir / "LOADFORMAT.DAT")
    rs_series = series.get("RS")
    if rs_series is None:
        raise KeyError(f"RS missing from {artifact_dir / 'LOADFORMAT.DAT'}")
    values: list[tuple[str, float]] = []
    for period, value in zip(periods, rs_series, strict=False):
        if period < "2025.4" or period > forecast_end:
            continue
        values.append((period, float(value)))
    if not values:
        raise ValueError(f"No RS values found for {run_id} through {forecast_end}")
    return values


def _rs_helper_blocks(rs_values: list[tuple[str, float]]) -> str:
    lines: list[str] = []
    for period, value in rs_values:
        lines.extend(
            [
                f"SMPL {period} {period};",
                "CHANGEVAR;",
                "RSX SAMEVALUE",
                f"{value:.10f}",
                ";",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def _patch_psereb(*, text: str, forecast_end: str) -> str:
    text = text.replace(
        "GENR INTGZ=INTG/AAG;\nGENR RQG=",
        "GENR INTGZ=INTG/AAG;\nCREATE RSX=0;\nGENR RQG=",
    )
    text = text.replace(
        "EQ 30 RS  C RS(-1) PCPD UR UR1 PCM1L1B PCM1L1A RS1(-1) RS1(-2) ;\nLHS RS=(ABS(RS-.0)+RS-.0)/2.+.0;",
        "EQ 30 RSX  C RSX(-1) PCPD UR UR1 PCM1L1B PCM1L1A RS1(-1) RS1(-2) ;\nLHS RSX=(ABS(RSX-.0)+RSX-.0)/2.+.0;",
    )
    text = text.replace("IDENT PX=(PF*(X-FA)+PFA*FA)/X;", "IDENT RS=RSX;\nIDENT PX=(PF*(X-FA)+PFA*FA)/X;")
    text = text.replace(
        "EQ 30 FSR  C RS(-1) PCPD(-1) UR(-1) UR1(-1) \nPCM1L1B PCM1L1A RS1(-1) RS1(-2) ",
        "EQ 30 FSR  C RSX(-1) PCPD(-1) UR(-1) UR1(-1) \nPCM1L1B PCM1L1A RS1(-1) RS1(-2) ",
    )
    text = text.replace("@EXOGENOUS VARIABLE=RS;", "EXOGENOUS VARIABLE=RSX;")
    text = text.replace("SMPL 2026.1 2029.4;\nEXOGENOUS VARIABLE=LUB;", f"SMPL 2026.1 {forecast_end};\nEXOGENOUS VARIABLE=LUB;")
    return text


def _patch_fmexog(text: str) -> str:
    return text.replace("JGPHASE ADDDIFABS", "JGPHASE SAMEVALUE").replace(
        "JGWPHASE ADDDIFABS", "JGWPHASE SAMEVALUE"
    )


def _scenario_payload(*, scenario_name: str, overlay_dir: Path, input_file: str, forecast_end: str) -> dict[str, object]:
    return {
        "name": scenario_name,
        "description": "Rebased PSE stock-core scenario with RS forced to the matching wage-only path.",
        "fp_home": str(REPO_ROOT / "FM"),
        "input_overlay_dir": str(overlay_dir),
        "input_file": input_file,
        "forecast_start": "2025.4",
        "forecast_end": forecast_end,
        "backend": "fpexe",
        "fppy": {
            "eq_flags_preset": None,
            "timeout_seconds": None,
            "num_threads": None,
            "eq_structural_read_cache": "off",
            "fmout_coefs_override": None,
            "eq_iter_trace": False,
            "eq_iter_trace_period": None,
            "eq_iter_trace_targets": None,
            "eq_iter_trace_max_events": None,
        },
        "fpr": {},
        "overrides": {},
        "track_variables": ["GDPR", "GDP", "PCPF", "PIEF", "SG", "RS", "UR", "E", "JGJ", "JF", "WF", "PF"],
        "input_patches": {},
        "alerts": {},
        "extra": {},
        "artifacts_root": "do/artifacts-pse2026-rebased-stock-core",
    }


def build_rebased_rsexog_inputs() -> list[Path]:
    created: list[Path] = []
    for variant in VARIANTS:
        for horizon_name, horizon_cfg in HORIZONS.items():
            suffix = str(horizon_cfg["suffix"])
            source_overlay_dir = OVERLAYS_ROOT / f"{variant.source_overlay}_{horizon_name}"
            target_overlay_dir = OVERLAYS_ROOT / f"{variant.target_overlay}_{horizon_name}"
            if not source_overlay_dir.exists():
                raise FileNotFoundError(f"Missing source overlay: {source_overlay_dir}")
            if target_overlay_dir.exists():
                shutil.rmtree(target_overlay_dir)
            shutil.copytree(source_overlay_dir, target_overlay_dir)
            fmexog_path = target_overlay_dir / "fmexog.txt"
            fmexog_path.write_text(_patch_fmexog(fmexog_path.read_text(encoding="utf-8")), encoding="utf-8")

            rs_source_run_id = f"{variant.wage_run_id}{suffix}"
            rs_values = _rs_values_for_run(
                rs_source_run_id,
                forecast_end=str(horizon_cfg["forecast_end"]),
            )
            rs_helper_text = _rs_helper_blocks(rs_values)
            (target_overlay_dir / "rshelper.txt").write_text(rs_helper_text, encoding="utf-8")
            psereb_path = target_overlay_dir / "psereb.txt"
            psereb_text = _patch_psereb(
                text=psereb_path.read_text(encoding="utf-8"),
                forecast_end=str(horizon_cfg["forecast_end"]),
            )
            psereb_path.write_text(psereb_text, encoding="utf-8")
            wrapper_path = target_overlay_dir / str(horizon_cfg["input_file"])
            wrapper_text = wrapper_path.read_text(encoding="utf-8")
            wrapper_text = wrapper_text.replace(
                "INPUT FILE=fmexog.txt;",
                "INPUT FILE=fmexog.txt;\nINPUT FILE=rshelper.txt;",
            )
            wrapper_path.write_text(wrapper_text, encoding="utf-8")

            scenario_name = f"{variant.target_scenario}{suffix}"
            scenario_path = PSE_REBASED_ROOT / f"{scenario_name}.yaml"
            scenario_payload = _scenario_payload(
                scenario_name=scenario_name,
                overlay_dir=target_overlay_dir,
                input_file=str(horizon_cfg["input_file"]),
                forecast_end=str(horizon_cfg["forecast_end"]),
            )
            scenario_path.write_text(yaml.safe_dump(scenario_payload, sort_keys=False), encoding="utf-8")
            created.append(scenario_path)
    return created


def main() -> None:
    for path in build_rebased_rsexog_inputs():
        print(path)


if __name__ == "__main__":
    main()
