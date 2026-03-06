# Model Runs Export

The GitHub Pages run explorer is a separate, read-only product surface. It does not reuse the live Streamlit dashboard at runtime and it does not read local `artifacts/` on Pages.

## What it ships

The public bundle under `public/model-runs/` contains:

- `index.html`, `app.js`, `styles.css`
- `manifest.json`
- `dictionary.json`
- `presets.json`
- `runs/<run_id>.json`

The static app lazy-loads run payloads from `manifest.json` and renders multi-run charts with the same transform and run-comparison semantics used by Run Panels.

## Export command

Use the checked-in export spec:

```bash
scripts/uvsafe python -B -m fp_wraptr.cli export pages \
  --spec public/model-runs.spec.yaml \
  --artifacts-dir artifacts \
  --out-dir public/model-runs
```

Or via the installed CLI:

```bash
fp export pages --spec public/model-runs.spec.yaml --artifacts-dir artifacts --out-dir public/model-runs
```

## Spec contract

The default spec lives at `public/model-runs.spec.yaml`.

It declares:

- `title`
- `site_subpath` (`model-runs`)
- ordered `runs`
- `default_run_ids`
- `presets`
- `default_preset_ids`

Run ids are stable public ids. The exporter resolves each one to the latest matching artifact directory by `scenario_name` and records the artifact timestamp in the exported manifest.

## Operator workflow

1. Refresh or curate the source runs under `artifacts/`.
2. Run `fp export pages ...`.
3. Review the diff under `public/model-runs/`.
4. Commit the generated bundle together with any spec or doc changes.
5. Push to `main`; GitHub Pages publishes docs plus the run explorer under `/model-runs/`.

## Safety rules

The exporter intentionally fails if the generated JSON would leak:

- absolute filesystem paths
- home-directory tokens
- local usernames via path strings

Only portable, relative asset paths are written into the bundle.

## Current default bundle

The checked-in default bundle targets:

- `stock_fm_baseline`
- `low_15h`
- `high_15h`
- `pse2025_base`
- `pse2025_high`
- `pse2025_low`

Default selected runs:

- `pse2025_base`
- `pse2025_high`
- `pse2025_low`

Default preset:

- `PSE Economy`
