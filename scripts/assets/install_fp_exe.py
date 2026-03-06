#!/usr/bin/env python3
"""Install fp.exe into an FM directory from a user-provided URL."""

from __future__ import annotations

import argparse
import hashlib
import shutil
import tempfile
import urllib.request
from pathlib import Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Download and install fp.exe into an FM directory.")
    p.add_argument("--url", required=True, help="Direct download URL for fp.exe")
    p.add_argument(
        "--dest-dir",
        type=Path,
        default=Path("FM"),
        help="Destination directory (default: FM)",
    )
    p.add_argument(
        "--sha256",
        default="",
        help="Optional expected sha256 hash; installation aborts on mismatch.",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacing an existing fp.exe at the destination.",
    )
    return p


def main() -> int:
    args = _build_parser().parse_args()
    dest_dir = Path(args.dest_dir).expanduser().resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / "fp.exe"
    if dest_path.exists() and not bool(args.overwrite):
        raise SystemExit(f"Destination exists: {dest_path} (use --overwrite to replace)")

    with tempfile.TemporaryDirectory(prefix="fp-wraptr-install-") as tmp:
        tmp_path = Path(tmp) / "fp.exe"
        with urllib.request.urlopen(str(args.url)) as response:  # nosec B310
            tmp_path.write_bytes(response.read())

        observed = _sha256(tmp_path)
        expected = str(args.sha256 or "").strip().lower()
        if expected and observed.lower() != expected:
            raise SystemExit(
                f"sha256 mismatch for downloaded fp.exe: expected={expected} observed={observed}"
            )

        shutil.copy2(tmp_path, dest_path)

    print(str(dest_path))
    print(f"sha256={_sha256(dest_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
