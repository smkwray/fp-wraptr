#!/usr/bin/env bash

# Shared helpers for resolving a per-project external virtualenv/config.

project_env_load_files() {
  local root="$1"
  local env_file=""
  local line=""
  local key=""
  local value=""
  for env_file in "$root/.env" "$root/.env.local"; do
    if [[ -f "$env_file" ]]; then
      while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line#"${line%%[![:space:]]*}"}"
        if [[ -z "$line" || "${line:0:1}" == "#" ]]; then
          continue
        fi
        if [[ "$line" != *=* ]]; then
          continue
        fi
        key="${line%%=*}"
        value="${line#*=}"
        key="${key%"${key##*[![:space:]]}"}"
        if [[ ! "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
          continue
        fi
        if [[ "$value" == \"*\" && "$value" == *\" ]]; then
          value="${value:1:${#value}-2}"
        elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
          value="${value:1:${#value}-2}"
        fi
        export "$key=$value"
      done < "$env_file"
    fi
  done
}

project_env_normalize_path() {
  local root="$1"
  local raw="$2"
  if [[ -z "$raw" ]]; then
    return 1
  fi
  raw="${raw/#\~/$HOME}"
  case "$raw" in
    /*) printf '%s\n' "$raw" ;;
    *) printf '%s\n' "$root/$raw" ;;
  esac
}

project_env_resolve_venv_root() {
  local root="$1"
  local candidate=""
  for candidate in "${PROJECT_VENV_ROOT:-}" "${UV_PROJECT_ENVIRONMENT:-}" "${VIRTUAL_ENV:-}"; do
    if [[ -n "$candidate" ]]; then
      project_env_normalize_path "$root" "$candidate"
      return 0
    fi
  done
  return 1
}

project_env_assert_outside_repo() {
  local root="$1"
  local candidate="$2"
  local label="$3"
  case "$candidate" in
    "$root"/*)
      echo "Error: $label points inside the repo: $candidate" >&2
      echo "Set $label to an external path via environment or $root/.env" >&2
      return 2
      ;;
  esac
}

project_env_activate_exports() {
  local root="$1"
  local venv_root="$2"

  export VIRTUAL_ENV="$venv_root"
  export UV_PROJECT_ENVIRONMENT="$venv_root"
  export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache-$(basename "$root")}"
  export RUFF_CACHE_DIR="${RUFF_CACHE_DIR:-/tmp/ruff-cache-$(basename "$root")}"
  export PYTHONDONTWRITEBYTECODE="${PYTHONDONTWRITEBYTECODE:-1}"
  export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/$(basename "$root")-pycache}"
  export WINEPREFIX="${WINEPREFIX:-$HOME/.wine-$(basename "$root")}"
  case ":$PATH:" in
    *":$venv_root/bin:"*) ;;
    *) export PATH="$venv_root/bin:$PATH" ;;
  esac
}
