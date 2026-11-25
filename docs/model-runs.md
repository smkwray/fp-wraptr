# Model Runs Export

The Model Runs Explorer is a static, read-only site for sharing completed runs. It is published from a checked-in bundle and does not read local `artifacts/` at runtime.

## What it ships

The public bundle under `public/model-runs/` contains:

- `index.html`, `app.js`, `styles.css`
- `manifest.json`
- `dictionary.json`
- `presets.json`
- `runs/<run_id>.json`

The static app lazy-loads run payloads from `manifest.json` and renders multi-run charts with per-variable transforms (level, change, percent change, share of a denominator) and run comparisons.

## Export command

Use the checked-in export spec:

```bash
uv run fp export pages \
  --spec public/model-runs.spec.yaml \
  --artifacts-dir artifacts \
  --out-dir public/model-runs
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

Runs whose `fp.exe` solve reports an error cause the export to fail unless you pass `--allow-solve-errors`.

## Exported paths

The exporter intentionally fails if the generated JSON would leak:

- absolute filesystem paths
- home-directory tokens
- local usernames via path strings

Only portable, relative asset paths are written into the bundle.

## Published bundle

The live explorer is built from `public/model-runs.spec.yaml`. The exported
`public/model-runs/manifest.json` lists every published run, the runs selected by default
(`default_run_ids`), and the default chart preset (`default_preset_ids`).
