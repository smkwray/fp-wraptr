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

## Publishing workflow

1. Refresh or curate the source runs under `artifacts/`.
2. Run `fp export pages ...`. It refuses runs whose `fp.exe` solve hit errors such as the iteration limit; re-solve them or pass `--allow-solve-errors`.
3. Review the diff under `public/model-runs/`.
4. Commit the generated bundle together with any spec or doc changes.
5. Push to `main`; the Pages workflow checks that every run listed in `manifest.json` has its payload, then publishes the docs plus the run explorer under `/model-runs/`.

## Safety rules

The exporter intentionally fails if the generated JSON would leak:

- absolute filesystem paths
- home-directory tokens
- local usernames via path strings

Only portable, relative asset paths are written into the bundle.

## Published bundle

The live explorer is built from `public/model-runs.spec.yaml`. The exported
`public/model-runs/manifest.json` lists every published run, the runs selected by default
(`default_run_ids`), and the default chart preset (`default_preset_ids`).
