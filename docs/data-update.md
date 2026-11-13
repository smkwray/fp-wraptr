# Model Data Update (FRED/BEA/BLS)

`fp data update-fred` refreshes the Fair-Parke model's historical dataset (`fmdata.txt`)
from FRED, BEA, and BLS and writes a **new, runnable model bundle** (a directory like `FM/`).
Point a scenario's `fp_home` at that bundle to run against the refreshed data. The base
`FM/` directory is never modified.

## Workflow

1. Generate an updated model bundle:

```bash
fp data update-fred \
  --model-dir FM \
  --out-dir artifacts/model_updates/2026-03-02 \
  --end 2025.4 \
  --sources fred --sources bea --sources bls
```

2. Run scenarios against the updated bundle:

```yaml
# examples/baseline_updated_data.yaml
name: baseline_updated_data
fp_home: artifacts/model_updates/2026-03-02/FM
forecast_start: "2026.1"  # next quarter after `sample_end_after` in the update report
forecast_end: "2029.4"
backend: both
```

`fp data update-fred` also writes ready-to-run scenario templates under `<out_dir>/scenarios/`
and prints the `recommended_forecast_start` it computed from the updated `fmdata.txt`.

For new quarterly vintages, prefer starting from the official Fair bundle
(`fp data fetch-fair-bundle`, then `fp data update-fred --from-official-bundle`); see the
[Quickstart](quickstart.md#official-fair-bundle-workflow-recommended-for-new-quarters).

## Key options

- `--end YYYY.Q` (required): last quarter to update.
- `--sources`: repeatable; `fred` (default), `bea`, `bls`.
- `--variables`: repeatable; limit the update to specific model variables.
- `--replace-history`: also overwrite existing periods (for data revisions). By default only
  periods after the current `fmdata.txt` end are filled.
- `--extend-sample`: extend `sample_end` in the copied `fmdata.txt` to `--end`.
- `--allow-carry-forward`: when extending, fill series with no new data by carrying the last
  value forward.
- `--patch-fminput-smpl-endpoints` (experimental): update the `LOADDATA` `SMPL` endpoint in
  `fminput.txt` so `fp.exe` reads the newly extended history.
- `--keyboard-augment`: repeatable; extra variables to append to the `KEYBOARD` list in
  `fminput.txt`. With `--extend-sample` and no explicit list it defaults to `RM`, `RMA`,
  `RMMRSL2`, `RMACDZ`.
- `--cache-dir`: override the cache root (default `~/.fp-wraptr/fred-cache`; BEA and BLS use
  sibling caches).

The full option list is in the [CLI reference](cli.md#fp-data-update-fred).

## API keys

Set these as environment variables:

- `FRED_API_KEY` (required when `--sources fred` is enabled)
- `BEA_API_KEY` (required when `--sources bea` is enabled)
- `BLS_API_KEY` (optional but recommended for `--sources bls`)

Keep keys out of repository files and scenario YAML. Prefer shell-profile exports or an OS
keychain/secret manager, and avoid echoing raw keys in command logs. Use
`fp bea fetch-nipa ...` and `fp bls fetch ...` to check connectivity before a full update.

## How series are converted

Mappings live in `src/fp_wraptr/data/source_map.yaml` (inspect them with the
`fp dictionary source-*` commands).

- **Frequency**: quarterly series are used as-is; monthly series are aggregated to the
  quarterly mean; annual series are rejected.
- **Annual rates**: flows marked `annual_rate: true` are de-annualized to per-quarter values
  (`value / 4` for quarterly SAAR; `sum(value / 12)` over the quarter for monthly), matching
  the stock `fmdata.txt` convention.
- **Scale/offset**: applied after aggregation (for example `UR` uses `scale: 0.01` to convert
  percent to a fraction).
- **BEA**: only NIPA table-line sources (for example `T10106`) are supported. Other BEA
  entries are reported under `skipped_unsupported_bea` and left unchanged.

Use `fp data check-fred-mapping` to compare recent `fmdata.txt` values with normalized FRED
values when calibrating a mapping.

## Output

```
artifacts/model_updates/2026-03-02/
  FM/
    fmdata.txt   # updated
    fmage.txt    # copied from base
    fmexog.txt   # copied from base
    fminput.txt  # copied from base (patched only with --patch-fminput-smpl-endpoints)
  scenarios/     # ready-to-run scenario templates
  data_update_report.json
```

`data_update_report.json` records the update range, the source map used, fetched series and
their last observation dates, the transforms applied, and skipped variables.

## Known limitations

- If `sample_end_after` differs from `fminput_fmdata_load_end` in `data_update_report.json`,
  `fp.exe` is still loading the older history endpoint from `fminput.txt`. Expect parity
  mismatches until `fminput.txt` loads the extra quarter.
- Extended-sample bundles can show `status=gate_failed` with `hard_fail_cell_count=0` when
  small mismatches concentrate in `RM/RMA/RMMRSL2/RMACDZ` at the first forecast quarter.
  Gate parity to the pre-extension window with `--gate-pabev-end`, or run `backend=fpexe`,
  before treating this as a model divergence.
- Units are taken from `source_map.yaml`; a naive FRED mapping may not match the model's
  units, so check new mappings with `fp data check-fred-mapping`.
