#!/usr/bin/env bash
set -euo pipefail

# Provision FM assets for fp.exe integration tests.
#
# Supported sources:
#   1) FP_ASSETS_SOURCE_DIR (directory already present on runner)
#   2) FP_ASSETS_ARCHIVE_URL (+ optional FP_ASSETS_BEARER_TOKEN)
#
# Optional integrity check:
#   - FP_ASSETS_ARCHIVE_SHA256
#
# Destination:
#   - ${GITHUB_WORKSPACE}/FM

workspace="${GITHUB_WORKSPACE:-$(pwd)}"
dest_dir="${workspace}/FM"
tmp_archive="/tmp/fp-assets-archive"

required_files=(
  "fp.exe"
  "fminput.txt"
  "fmdata.txt"
  "fmage.txt"
  "fmexog.txt"
)

echo "Provisioning FP assets into: ${dest_dir}"
mkdir -p "${dest_dir}"

if [[ -n "${FP_ASSETS_SOURCE_DIR:-}" ]]; then
  echo "Using FP_ASSETS_SOURCE_DIR=${FP_ASSETS_SOURCE_DIR}"
  if [[ ! -d "${FP_ASSETS_SOURCE_DIR}" ]]; then
    echo "ERROR: FP_ASSETS_SOURCE_DIR does not exist: ${FP_ASSETS_SOURCE_DIR}" >&2
    exit 1
  fi
  cp -R "${FP_ASSETS_SOURCE_DIR}/." "${dest_dir}/"
elif [[ -n "${FP_ASSETS_ARCHIVE_URL:-}" ]]; then
  echo "Downloading FP assets archive from configured URL"
  curl_args=(--fail --location --silent --show-error "${FP_ASSETS_ARCHIVE_URL}" -o "${tmp_archive}")
  if [[ -n "${FP_ASSETS_BEARER_TOKEN:-}" ]]; then
    curl_args=(--fail --location --silent --show-error -H "Authorization: Bearer ${FP_ASSETS_BEARER_TOKEN}" "${FP_ASSETS_ARCHIVE_URL}" -o "${tmp_archive}")
  fi
  curl "${curl_args[@]}"

  if [[ -n "${FP_ASSETS_ARCHIVE_SHA256:-}" ]]; then
    echo "Validating archive SHA256"
    echo "${FP_ASSETS_ARCHIVE_SHA256}  ${tmp_archive}" | sha256sum -c -
  fi

  archive_type="${FP_ASSETS_ARCHIVE_TYPE:-auto}"
  if [[ "${archive_type}" == "auto" ]]; then
    case "${FP_ASSETS_ARCHIVE_URL}" in
      *.zip) archive_type="zip" ;;
      *.tar|*.tar.gz|*.tgz) archive_type="tar" ;;
      *)
        echo "ERROR: Could not infer archive type from URL. Set FP_ASSETS_ARCHIVE_TYPE=zip|tar." >&2
        exit 1
        ;;
    esac
  fi

  if [[ "${archive_type}" == "zip" ]]; then
    unzip -q "${tmp_archive}" -d "${dest_dir}"
  elif [[ "${archive_type}" == "tar" ]]; then
    tar -xf "${tmp_archive}" -C "${dest_dir}"
  else
    echo "ERROR: Unsupported FP_ASSETS_ARCHIVE_TYPE=${archive_type}" >&2
    exit 1
  fi
else
  cat >&2 <<'EOF'
ERROR: No FP asset source configured.
Set one of:
  - FP_ASSETS_SOURCE_DIR (runner-local directory containing fp.exe + FM files), or
  - FP_ASSETS_ARCHIVE_URL (downloadable archive URL) with optional FP_ASSETS_BEARER_TOKEN.
EOF
  exit 1
fi

# Some archives may extract into an extra top-level FM/ directory.
if [[ -d "${dest_dir}/FM" ]] && [[ ! -f "${dest_dir}/fp.exe" ]]; then
  echo "Flattening nested FM/ directory from archive"
  cp -R "${dest_dir}/FM/." "${dest_dir}/"
fi

missing=0
for fname in "${required_files[@]}"; do
  if [[ ! -f "${dest_dir}/${fname}" ]]; then
    echo "ERROR: Missing required asset file: ${dest_dir}/${fname}" >&2
    missing=1
  fi
done

if [[ "${missing}" -ne 0 ]]; then
  exit 1
fi

echo "FP assets provisioned successfully."
