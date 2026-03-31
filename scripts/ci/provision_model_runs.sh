#!/usr/bin/env bash
set -euo pipefail

# Provision tracked model-runs JSON payloads into public/model-runs/runs.
#
# Supported sources:
#   1) MODEL_RUNS_SOURCE_DIR (directory already present on runner)
#   2) MODEL_RUNS_ARCHIVE_URL (+ optional MODEL_RUNS_BEARER_TOKEN)
#   3) Fallback fetch from MODEL_RUNS_BASE_URL using the checked-in manifest
#
# Optional integrity check:
#   - MODEL_RUNS_ARCHIVE_SHA256
#
# Destination:
#   - ${GITHUB_WORKSPACE}/public/model-runs/runs

workspace="${GITHUB_WORKSPACE:-$(pwd)}"
site_dir="${workspace}/public/model-runs"
dest_dir="${site_dir}/runs"
tmp_archive="/tmp/model-runs-archive"
base_url="${MODEL_RUNS_BASE_URL:-https://smkwray.github.io/fp-wraptr/model-runs}"
manifest_path="${site_dir}/manifest.json"
tracked_source_dir="${workspace}/public/model-runs-pse2026/runs"

echo "Provisioning model run payloads into: ${dest_dir}"
mkdir -p "${dest_dir}"

if [[ ! -f "${manifest_path}" ]]; then
  echo "ERROR: Missing manifest at ${manifest_path}" >&2
  exit 1
fi

manifest_complete() {
  python3 - "${manifest_path}" "${dest_dir}" <<'PY'
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
dest_dir = Path(sys.argv[2])

payload = json.loads(manifest_path.read_text(encoding="utf-8"))
missing = []
for run in payload.get("runs", []):
    rel_path = str(run.get("data_path", "")).strip()
    if rel_path and not (dest_dir.parent / rel_path).is_file():
        missing.append(rel_path)

if missing:
    raise SystemExit(1)
PY
}

if manifest_complete; then
  echo "Using checked-in model-runs payloads already present in ${dest_dir}"
elif [[ -d "${tracked_source_dir}" ]]; then
  find "${dest_dir}" -mindepth 1 -maxdepth 1 ! -name '.gitkeep' -exec rm -rf {} +
  echo "Using tracked repo payloads from ${tracked_source_dir}"
  cp -R "${tracked_source_dir}/." "${dest_dir}/"
elif [[ -n "${MODEL_RUNS_SOURCE_DIR:-}" ]]; then
  find "${dest_dir}" -mindepth 1 -maxdepth 1 ! -name '.gitkeep' -exec rm -rf {} +

  echo "Using MODEL_RUNS_SOURCE_DIR=${MODEL_RUNS_SOURCE_DIR}"
  if [[ ! -d "${MODEL_RUNS_SOURCE_DIR}" ]]; then
    echo "ERROR: MODEL_RUNS_SOURCE_DIR does not exist: ${MODEL_RUNS_SOURCE_DIR}" >&2
    exit 1
  fi
  cp -R "${MODEL_RUNS_SOURCE_DIR}/." "${dest_dir}/"
elif [[ -n "${MODEL_RUNS_ARCHIVE_URL:-}" ]]; then
  find "${dest_dir}" -mindepth 1 -maxdepth 1 ! -name '.gitkeep' -exec rm -rf {} +
  echo "Downloading model-runs archive from configured URL"
  curl_args=(--fail --location --silent --show-error "${MODEL_RUNS_ARCHIVE_URL}" -o "${tmp_archive}")
  if [[ -n "${MODEL_RUNS_BEARER_TOKEN:-}" ]]; then
    curl_args=(
      --fail
      --location
      --silent
      --show-error
      -H
      "Authorization: Bearer ${MODEL_RUNS_BEARER_TOKEN}"
      "${MODEL_RUNS_ARCHIVE_URL}"
      -o
      "${tmp_archive}"
    )
  fi
  curl "${curl_args[@]}"

  if [[ -n "${MODEL_RUNS_ARCHIVE_SHA256:-}" ]]; then
    echo "Validating archive SHA256"
    echo "${MODEL_RUNS_ARCHIVE_SHA256}  ${tmp_archive}" | sha256sum -c -
  fi

  archive_type="${MODEL_RUNS_ARCHIVE_TYPE:-auto}"
  if [[ "${archive_type}" == "auto" ]]; then
    case "${MODEL_RUNS_ARCHIVE_URL}" in
      *.zip) archive_type="zip" ;;
      *.tar|*.tar.gz|*.tgz) archive_type="tar" ;;
      *)
        echo "ERROR: Could not infer archive type from URL. Set MODEL_RUNS_ARCHIVE_TYPE=zip|tar." >&2
        exit 1
        ;;
    esac
  fi

  if [[ "${archive_type}" == "zip" ]]; then
    unzip -q "${tmp_archive}" -d "${dest_dir}"
  elif [[ "${archive_type}" == "tar" ]]; then
    tar -xf "${tmp_archive}" -C "${dest_dir}"
  else
    echo "ERROR: Unsupported MODEL_RUNS_ARCHIVE_TYPE=${archive_type}" >&2
    exit 1
  fi

  if [[ -d "${dest_dir}/runs" ]]; then
    cp -R "${dest_dir}/runs/." "${dest_dir}/"
    rm -rf "${dest_dir}/runs"
  fi
else
  find "${dest_dir}" -mindepth 1 -maxdepth 1 ! -name '.gitkeep' -exec rm -rf {} +
  echo "No dedicated model-runs source configured; fetching run payloads from ${base_url}"
  python3 - "${manifest_path}" "${base_url}" "${dest_dir}" <<'PY'
import json
import sys
from pathlib import Path
from urllib.request import urlopen

manifest_path = Path(sys.argv[1])
base_url = sys.argv[2].rstrip("/")
dest_dir = Path(sys.argv[3])

payload = json.loads(manifest_path.read_text(encoding="utf-8"))
for run in payload.get("runs", []):
    rel_path = str(run.get("data_path", "")).strip()
    if not rel_path:
        continue
    target = dest_dir.parent / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    url = f"{base_url}/{rel_path}"
    with urlopen(url) as response:
        target.write_bytes(response.read())
PY
fi

python3 - "${manifest_path}" "${dest_dir}" <<'PY'
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
dest_dir = Path(sys.argv[2])

payload = json.loads(manifest_path.read_text(encoding="utf-8"))
missing = []
for run in payload.get("runs", []):
    rel_path = str(run.get("data_path", "")).strip()
    if rel_path and not (dest_dir.parent / rel_path).is_file():
        missing.append(rel_path)

if missing:
    for rel_path in missing:
        print(f"ERROR: Missing required model-runs payload: {rel_path}", file=sys.stderr)
    raise SystemExit(1)
PY

echo "Model run payloads provisioned successfully."
