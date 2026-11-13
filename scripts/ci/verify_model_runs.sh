#!/usr/bin/env bash
set -euo pipefail

# Verify that the checked-in Model Runs Explorer payloads are complete:
# public/model-runs/manifest.json exists and every run's data_path is present.

workspace="${GITHUB_WORKSPACE:-$(pwd)}"
site_dir="${workspace}/public/model-runs"
manifest_path="${site_dir}/manifest.json"

if [[ ! -f "${manifest_path}" ]]; then
  echo "ERROR: Missing manifest at ${manifest_path}" >&2
  exit 1
fi

python3 - "${manifest_path}" "${site_dir}" <<'PY'
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
site_dir = Path(sys.argv[2])

runs = json.loads(manifest_path.read_text(encoding="utf-8")).get("runs", [])
if not runs:
    raise SystemExit(f"ERROR: {manifest_path} lists no runs")
missing = [
    rel_path
    for rel_path in (str(run.get("data_path", "")).strip() for run in runs)
    if not rel_path or not (site_dir / rel_path).is_file()
]
for rel_path in missing:
    print(f"ERROR: Missing model-runs payload: {rel_path or '<empty data_path>'}", file=sys.stderr)
if missing:
    raise SystemExit(1)
print(f"Model-runs manifest complete: {len(runs)} run payloads present.")
PY
