from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path

import yaml


REPO = Path(__file__).resolve().parents[2]
SOURCE = REPO / "do" / "pse_rebased_2026"
STOCK_ARTIFACT_SOURCE = (
    REPO
    / "do"
    / "artifacts-pse2026-pf-ur-nocola-meeting"
    / "stock_fm_baseline_20260415_161658"
)

SCENARIOS = [
    ("pse2025_base_stock_pf", "base_5y", "apsreb5.txt", "5y", "5Y", 5),
    ("pse2025_base_stock_pf_10y", "base_10y", "apsreb10.txt", "10y", "10Y", 10),
    ("pse2025_high_20h_3pct_stock_pf", "high20_5y", "apsreb5.txt", "5y", "5Y", 5),
    ("pse2025_high_20h_3pct_stock_pf_10y", "high20_10y", "apsreb10.txt", "10y", "10Y", 10),
    ("pse2025_low_20h_3pct_stock_pf", "low20_5y", "apsreb5.txt", "5y", "5Y", 5),
    ("pse2025_low_20h_3pct_stock_pf_10y", "low20_10y", "apsreb10.txt", "10y", "10Y", 10),
    ("pse2025_high_15h_3pct_stock_pf", "high15_5y", "apsreb5.txt", "5y", "5Y", 5),
    ("pse2025_high_15h_3pct_stock_pf_10y", "high15_10y", "apsreb10.txt", "10y", "10Y", 10),
    ("pse2025_low_15h_3pct_stock_pf", "low15_5y", "apsreb5.txt", "5y", "5Y", 5),
    ("pse2025_low_15h_3pct_stock_pf_10y", "low15_10y", "apsreb10.txt", "10y", "10Y", 10),
]

LABELS = {
    "pse2025_base_stock_pf": "PSE2026.2 Base",
    "pse2025_high_20h_3pct_stock_pf": "PSE2026.2 High $20/h",
    "pse2025_low_20h_3pct_stock_pf": "PSE2026.2 Low $20/h",
    "pse2025_high_15h_3pct_stock_pf": "PSE2026.2 High $15/h",
    "pse2025_low_15h_3pct_stock_pf": "PSE2026.2 Low $15/h",
}

THREE_PERCENT_QUARTERLY = 0.007417071777732875
COMBINED_ROOT = REPO / "do" / "pse_rebased_2026_pf_comparison"
COMBINED_ARTIFACTS = REPO / "do" / "artifacts-pse2026-pf-comparison"
COMBINED_EXPORT = REPO / "do" / "model-runs-pse2026-pf-comparison"


@dataclass(frozen=True)
class Bundle:
    key: str
    title: str
    pf_mode: str
    cola: float
    root_name: str
    artifact_name: str
    export_name: str

    @property
    def root(self) -> Path:
        return REPO / "do" / self.root_name

    @property
    def artifacts(self) -> Path:
        return REPO / "do" / self.artifact_name

    @property
    def export(self) -> Path:
        return REPO / "do" / self.export_name


BUNDLES = [
    Bundle(
        key="onezur-nocola",
        title="PSE Stock PF 1/UR, No COLA",
        pf_mode="onezur",
        cola=0.0,
        root_name="pse_rebased_2026_pf_onezur_nocola",
        artifact_name="artifacts-pse2026-pf-onezur-nocola",
        export_name="model-runs-pse2026-pf-onezur-nocola",
    ),
    Bundle(
        key="ur-nocola",
        title="PSE Stock PF UR, No COLA",
        pf_mode="ur",
        cola=0.0,
        root_name="pse_rebased_2026_pf_ur_nocola",
        artifact_name="artifacts-pse2026-pf-ur-nocola",
        export_name="model-runs-pse2026-pf-ur-nocola",
    ),
    Bundle(
        key="onezur-cola3",
        title="PSE Stock PF 1/UR, 3% JG and MW COLA",
        pf_mode="onezur",
        cola=THREE_PERCENT_QUARTERLY,
        root_name="pse_rebased_2026_pf_onezur_cola3",
        artifact_name="artifacts-pse2026-pf-onezur-cola3",
        export_name="model-runs-pse2026-pf-onezur-cola3",
    ),
    Bundle(
        key="ur-cola3",
        title="PSE Stock PF UR, 3% JG and MW COLA",
        pf_mode="ur",
        cola=THREE_PERCENT_QUARTERLY,
        root_name="pse_rebased_2026_pf_ur_cola3",
        artifact_name="artifacts-pse2026-pf-ur-cola3",
        export_name="model-runs-pse2026-pf-ur-cola3",
    ),
]

GROUP_TITLES = {
    "onezur-nocola": "PSE PF 1/UR, no COLA",
    "ur-nocola": "PSE PF UR, no COLA",
    "onezur-cola3": "PSE PF 1/UR, 3% JG + MW COLA",
    "ur-cola3": "PSE PF UR, 3% JG + MW COLA",
}

KEY_PREFIXES = {
    "onezur-nocola": "onezur_nocola",
    "ur-nocola": "ur_nocola",
    "onezur-cola3": "onezur_cola3",
    "ur-cola3": "ur_cola3",
}


def periods(start: str, end: str) -> list[str]:
    y, q = [int(part) for part in start.split(".")]
    ey, eq = [int(part) for part in end.split(".")]
    out: list[str] = []
    while (y, q) <= (ey, eq):
        out.append(f"{y}.{q}")
        q += 1
        if q == 5:
            y += 1
            q = 1
    return out


def patch_psereb(text: str, *, pf_mode: str) -> str:
    if pf_mode == "onezur":
        return text
    text = text.replace(
        "EQ 10 LPF LPF(-1) LWFD5 C T LPIM  ONEZUR LCUSTZ RHO=1;",
        "EQ 10 LPF LPF(-1) LWFD5 C T LPIM UR LCUSTZ RHO=1;",
    )
    text = text.replace(
        "EQ 10 LPF LPF(-1) LWFD5 C T LPIM ONEZUR LCUSTZ RHO=1;",
        "EQ 10 LPF LPF(-1) LWFD5 C T LPIM UR LCUSTZ RHO=1;",
    )
    text = text.replace(
        "EQ 10 FSR LPF(-1) LWFD5(-1) C T LPIM(-1) ONEZUR(-1) UR(-1)",
        "EQ 10 FSR LPF(-1) LWFD5(-1) C T LPIM(-1) UR(-1)",
    )
    return text


def append_exact_controls(text: str, *, forecast_start: str, forecast_end: str, cola: float) -> str:
    text = text.replace("JGPHASE ADDDIFABS", "JGPHASE SAMEVALUE")
    text = text.replace("JGWPHASE ADDDIFABS", "JGWPHASE SAMEVALUE")
    block_lines: list[str] = ["", "@ Exact replay controls generated by build_pse_pf_comparison_bundles.py"]
    for period in periods(forecast_start, forecast_end):
        block_lines.extend(
            [
                f"SMPL {period} {period};",
                "CHANGEVAR;",
                "JGCOLA SAMEVALUE",
                f"{cola}",
                "MWCOLA SAMEVALUE",
                f"{cola}",
                ";",
            ]
        )
    block = "\n".join(block_lines) + "\n"
    marker = "RETURN;"
    idx = text.rfind(marker)
    if idx == -1:
        return text.rstrip() + "\n" + block
    return text[:idx].rstrip() + "\n" + block + text[idx:]


def patch_yaml(path: Path, *, bundle: Bundle) -> None:
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict) or "input_overlay_dir" not in data:
        return
    overlay = Path(str(data["input_overlay_dir"]))
    data["input_overlay_dir"] = str(bundle.root / "overlays" / overlay.name)
    data["artifacts_root"] = str(Path("do") / bundle.artifact_name)
    data["description"] = f'{data.get("description", "").rstrip()} [{bundle.title}]'
    path.write_text(yaml.safe_dump(data, sort_keys=False))


def scenario_family(name: str) -> str:
    return name.removesuffix("_10y")


def scenario_label(name: str, bundle: Bundle) -> str:
    base = LABELS.get(scenario_family(name), name)
    pf = "PF 1/UR" if bundle.pf_mode == "onezur" else "PF UR"
    cola = "3% COLA" if bundle.cola else "no COLA"
    return f"{base} ({pf}, {cola})"


def write_spec(bundle: Bundle) -> None:
    runs: list[dict] = [
        {
            "run_id": "stock_fm_baseline",
            "label": "Stock FAIR Model 2/2026",
            "group": "Stock FAIR Model",
            "family_id": "stock_fm_baseline",
            "horizon_id": "5y",
            "horizon_label": "5Y",
            "horizon_years": 5,
            "scenario_name": "stock_fm_baseline",
            "summary": "Untouched stock FAIR model reference run.",
            "details": ["Forecast 2025.4 to 2029.4 using the stock FM input deck."],
        }
    ]
    for scenario_name, _overlay, _input, horizon_id, horizon_label, horizon_years in SCENARIOS:
        family = scenario_family(scenario_name)
        wage = "$20/hour" if "_20h_" in scenario_name else "$15/hour" if "_15h_" in scenario_name else "base"
        takeup = "high take-up" if "_high_" in scenario_name else "low take-up" if "_low_" in scenario_name else "base"
        runs.append(
            {
                "run_id": scenario_name,
                "label": scenario_label(scenario_name, bundle),
                "group": "PSE With Stock PF Equation",
                "family_id": family,
                "horizon_id": horizon_id,
                "horizon_label": horizon_label,
                "horizon_years": horizon_years,
                "scenario_name": scenario_name,
                "summary": f"{horizon_label} {takeup} {wage} PSE run with {scenario_label(scenario_name, bundle).split('(', 1)[1].rstrip(')')}.",
                "details": [
                    "Phase paths are exact SAMEVALUE controls to avoid additive replay drift.",
                    "JGCOLA and MWCOLA are both set to the bundle COLA value.",
                ],
            }
        )
    spec = {
        "version": 1,
        "title": bundle.title,
        "site_subpath": "model-runs",
        "runs": runs,
        "default_run_ids": [
            "pse2025_base_stock_pf",
            "pse2025_high_20h_3pct_stock_pf",
            "pse2025_low_20h_3pct_stock_pf",
            "pse2025_high_15h_3pct_stock_pf",
            "pse2025_low_15h_3pct_stock_pf",
        ],
        "presets": [
            {"id": "pse-economy", "label": "Economy", "variables": ["GDPR", "GDP", "PCPF", "PIEF", "SG", "RS"]},
            {"id": "pse-employment-wages", "label": "PSE Employment", "variables": ["UR", "E", "JGJ", "JF", "WF", "WR"]},
        ],
        "default_preset_ids": ["pse-economy"],
    }
    (bundle.root / "model-runs.spec.yaml").write_text(yaml.safe_dump(spec, sort_keys=False))


def combined_scenario_name(bundle: Bundle, scenario_name: str) -> str:
    return f"{KEY_PREFIXES[bundle.key]}__{scenario_name}"


def combined_label(scenario_name: str, bundle: Bundle) -> str:
    base = LABELS.get(scenario_family(scenario_name), scenario_name)
    pf = "PF 1/UR" if bundle.pf_mode == "onezur" else "PF UR"
    cola = "3% COLA" if bundle.cola else "no COLA"
    return f"{base} ({pf}, {cola})"


def write_combined_spec() -> None:
    runs: list[dict] = [
        {
            "run_id": "stock_fm_baseline",
            "label": "Stock FAIR Model 2/2026",
            "group": "Stock FAIR Model",
            "family_id": "stock_fm_baseline",
            "horizon_id": "5y",
            "horizon_label": "5Y",
            "horizon_years": 5,
            "scenario_name": "stock_fm_baseline",
            "summary": "Untouched stock FAIR model reference run.",
            "details": ["Forecast 2025.4 to 2029.4 using the stock FM input deck."],
        }
    ]
    default_run_ids: list[str] = []
    for bundle in BUNDLES:
        group = GROUP_TITLES[bundle.key]
        prefix = KEY_PREFIXES[bundle.key]
        for scenario_name, _overlay, _input, horizon_id, horizon_label, horizon_years in SCENARIOS:
            family = scenario_family(scenario_name)
            run_id = combined_scenario_name(bundle, scenario_name)
            if bundle.key == "ur-nocola" and horizon_id == "5y":
                default_run_ids.append(run_id)
            wage = "$20/hour" if "_20h_" in scenario_name else "$15/hour" if "_15h_" in scenario_name else "base"
            takeup = "high take-up" if "_high_" in scenario_name else "low take-up" if "_low_" in scenario_name else "base"
            runs.append(
                {
                    "run_id": run_id,
                    "label": combined_label(scenario_name, bundle),
                    "group": group,
                    "family_id": f"{prefix}__{family}",
                    "horizon_id": horizon_id,
                    "horizon_label": horizon_label,
                    "horizon_years": horizon_years,
                    "scenario_name": run_id,
                    "summary": f"{horizon_label} {takeup} {wage} PSE run for {group}.",
                    "details": [
                        "Phase paths are exact SAMEVALUE controls, so the deck can read fmexog more than once without doubling the ramp.",
                        f"JGCOLA and MWCOLA are both set to {bundle.cola}.",
                    ],
                }
            )
    spec = {
        "version": 1,
        "title": "PSE PF Equation Comparison",
        "site_subpath": "model-runs",
        "runs": runs,
        "default_run_ids": default_run_ids,
        "presets": [
            {"id": "pse-economy", "label": "Economy", "variables": ["GDPR", "GDP", "PCPF", "PIEF", "SG", "RS"]},
            {"id": "pse-employment-wages", "label": "PSE Employment", "variables": ["UR", "E", "JGJ", "JF", "WF", "WR"]},
            {"id": "pse-controls", "label": "PSE Controls", "variables": ["JGWFADJ", "JGW", "MINWAGE", "JGCOLA", "MWCOLA"]},
        ],
        "default_preset_ids": ["pse-economy"],
    }
    (COMBINED_ROOT / "model-runs.spec.yaml").write_text(yaml.safe_dump(spec, sort_keys=False))


def prepare_combined_bundle() -> None:
    if COMBINED_ROOT.exists():
        shutil.rmtree(COMBINED_ROOT)
    COMBINED_ROOT.mkdir(parents=True)
    (COMBINED_ROOT / "overlays").mkdir()

    for bundle in BUNDLES:
        for scenario_name, overlay_name, _input, _horizon_id, _horizon_label, _horizon_years in SCENARIOS:
            src_overlay = bundle.root / "overlays" / overlay_name
            dst_overlay = COMBINED_ROOT / "overlays" / f"{KEY_PREFIXES[bundle.key]}__{overlay_name}"
            shutil.copytree(src_overlay, dst_overlay)

            src_yaml = bundle.root / f"{scenario_name}.yaml"
            data = yaml.safe_load(src_yaml.read_text())
            run_id = combined_scenario_name(bundle, scenario_name)
            data["name"] = run_id
            data["input_overlay_dir"] = str(dst_overlay)
            data["artifacts_root"] = str(Path("do") / COMBINED_ARTIFACTS.name)
            data["description"] = f"{data.get('description', '').rstrip()} [{GROUP_TITLES[bundle.key]}]"
            if "track_variables" in data:
                tracked = list(data["track_variables"])
                for variable in ("JGCOLA", "MWCOLA", "JGPHASE"):
                    if variable not in tracked:
                        tracked.append(variable)
                data["track_variables"] = tracked
            (COMBINED_ROOT / f"{run_id}.yaml").write_text(yaml.safe_dump(data, sort_keys=False))

    write_combined_spec()

    if COMBINED_ARTIFACTS.exists():
        shutil.rmtree(COMBINED_ARTIFACTS)
    COMBINED_ARTIFACTS.mkdir(parents=True)
    if STOCK_ARTIFACT_SOURCE.exists():
        shutil.copytree(STOCK_ARTIFACT_SOURCE, COMBINED_ARTIFACTS / STOCK_ARTIFACT_SOURCE.name)


def prepare_bundle(bundle: Bundle) -> None:
    if bundle.root.exists():
        shutil.rmtree(bundle.root)
    shutil.copytree(SOURCE, bundle.root)

    for yaml_path in bundle.root.glob("*.yaml"):
        patch_yaml(yaml_path, bundle=bundle)

    for overlay_dir in (bundle.root / "overlays").iterdir():
        if not overlay_dir.is_dir():
            continue
        input_file = "apsreb10.txt" if overlay_dir.name.endswith("_10y") else "apsreb5.txt"
        forecast_end = "2034.4" if input_file == "apsreb10.txt" else "2029.4"
        fmexog = overlay_dir / "fmexog.txt"
        if fmexog.exists():
            fmexog.write_text(
                append_exact_controls(
                    fmexog.read_text(),
                    forecast_start="2025.4",
                    forecast_end=forecast_end,
                    cola=bundle.cola,
                )
            )
        psereb = overlay_dir / "psereb.txt"
        if psereb.exists():
            psereb.write_text(patch_psereb(psereb.read_text(), pf_mode=bundle.pf_mode))
    write_spec(bundle)

    if bundle.artifacts.exists():
        shutil.rmtree(bundle.artifacts)
    bundle.artifacts.mkdir(parents=True)
    if STOCK_ARTIFACT_SOURCE.exists():
        shutil.copytree(STOCK_ARTIFACT_SOURCE, bundle.artifacts / STOCK_ARTIFACT_SOURCE.name)


def write_index() -> None:
    out = COMBINED_EXPORT
    out.mkdir(parents=True, exist_ok=True)
    links = []
    for bundle in BUNDLES:
        rel = f"../{bundle.export.name}/index.html"
        links.append(f'<li><a href="{rel}">{bundle.title}</a></li>')
    html = (
        "<!doctype html><meta charset='utf-8'><title>PSE PF Comparison Bundles</title>"
        "<body><h1>PSE PF Comparison Bundles</h1><ul>"
        + "\n".join(links)
        + "</ul></body>"
    )
    (out / "index.html").write_text(html)


def main() -> None:
    for bundle in BUNDLES:
        prepare_bundle(bundle)
        print(json.dumps({"prepared": bundle.key, "root": str(bundle.root), "artifacts": str(bundle.artifacts)}))
    prepare_combined_bundle()
    print(json.dumps({"prepared": "combined", "root": str(COMBINED_ROOT), "artifacts": str(COMBINED_ARTIFACTS)}))


if __name__ == "__main__":
    main()
