"""Helpers for fetching and unpacking official Fair quarterly bundles."""

from __future__ import annotations

import hashlib
import json
import shutil
import urllib.error
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_FAIR_BUNDLE_URL = "https://fairmodel.econ.yale.edu/fp/FMFP.ZIP"


class FairBundleError(RuntimeError):
    """Raised when fetching or unpacking the Fair bundle fails."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_fp_exe_source(path: Path) -> Path:
    candidate = Path(path)
    if candidate.is_dir():
        candidate = candidate / "fp.exe"
    if not candidate.exists():
        raise FairBundleError(f"fp.exe source not found: {candidate}")
    if not candidate.is_file():
        raise FairBundleError(f"fp.exe source is not a file: {candidate}")
    return candidate


def ensure_fp_exe_in_model_dir(
    *, model_dir: Path, fp_exe_from: Path | None = None
) -> dict[str, str | bool]:
    """Ensure `model_dir/fp.exe` exists, optionally copying from `fp_exe_from`."""
    target_dir = Path(model_dir)
    target = target_dir / "fp.exe"
    if target.exists():
        return {
            "present": True,
            "copied": False,
            "target": str(target),
            "source": "",
        }
    if fp_exe_from is None:
        return {
            "present": False,
            "copied": False,
            "target": str(target),
            "source": "",
        }

    source = _resolve_fp_exe_source(Path(fp_exe_from))
    target_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return {
        "present": True,
        "copied": True,
        "target": str(target),
        "source": str(source),
    }


def fetch_and_unpack_fair_bundle(
    *,
    out_dir: Path,
    url: str = DEFAULT_FAIR_BUNDLE_URL,
    timeout_seconds: int = 60,
    zip_path: Path | None = None,
) -> dict[str, Any]:
    """Download and unpack the official Fair FP bundle into `out_dir`."""
    target_dir = Path(out_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    target_zip_path = target_dir / "FMFP.ZIP"
    source_kind = "http_download"
    source_url = str(url)
    last_modified = ""
    if zip_path is not None:
        source_kind = "local_zip"
        source_zip = Path(zip_path)
        if not source_zip.exists():
            raise FairBundleError(f"Local Fair bundle ZIP not found: {source_zip}")
        if not source_zip.is_file():
            raise FairBundleError(f"Local Fair bundle ZIP is not a file: {source_zip}")
        source_url = f"local_zip:{source_zip}"
        try:
            if source_zip.resolve() != target_zip_path.resolve():
                shutil.copy2(source_zip, target_zip_path)
        except Exception as exc:
            raise FairBundleError(
                f"Failed to prepare local Fair bundle ZIP from {source_zip}: {exc}"
            ) from exc
    else:
        request = urllib.request.Request(str(url), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=int(timeout_seconds)) as response:
                payload = response.read()
                headers = response.headers
        except urllib.error.HTTPError as exc:
            raise FairBundleError(
                f"HTTP {exc.code} while downloading Fair bundle from {url}"
            ) from exc
        except Exception as exc:
            raise FairBundleError(f"Failed to download Fair bundle from {url}: {exc}") from exc

        target_zip_path.write_bytes(payload)
        if headers is not None:
            try:
                last_modified = str(headers.get("Last-Modified") or "")
            except Exception:
                last_modified = ""

    zip_sha256 = _sha256(target_zip_path)

    try:
        with zipfile.ZipFile(target_zip_path) as archive:
            archive.extractall(target_dir)
    except zipfile.BadZipFile as exc:
        raise FairBundleError(
            f"Downloaded file is not a valid zip archive: {target_zip_path}"
        ) from exc

    model_dir = target_dir / "FM"
    if not model_dir.exists():
        model_dir = target_dir

    fm_hashes: dict[str, str] = {}
    for path in sorted(model_dir.glob("fm*.txt")):
        if path.is_file():
            fm_hashes[path.name] = _sha256(path)

    manifest = {
        "source_url": source_url,
        "source_kind": source_kind,
        "downloaded_at_utc": datetime.now(UTC).isoformat(),
        "last_modified": last_modified,
        "zip_path": str(target_zip_path),
        "zip_sha256": zip_sha256,
        "output_dir": str(target_dir),
        "model_dir": str(model_dir),
        "fm_txt_sha256": fm_hashes,
    }
    manifest_path = target_dir / "fair_bundle_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    return {
        "output_dir": str(target_dir),
        "model_dir": str(model_dir),
        "manifest_path": str(manifest_path),
        "zip_path": str(target_zip_path),
        **manifest,
    }
