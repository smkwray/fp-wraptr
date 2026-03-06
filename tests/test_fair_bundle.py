from __future__ import annotations

import io
import zipfile
from pathlib import Path

from fp_wraptr.data.fair_bundle import ensure_fp_exe_in_model_dir, fetch_and_unpack_fair_bundle


def _build_zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("FM/fmdata.txt", "SMPL 2025.4 2025.4;\n")
        archive.writestr("FM/fmage.txt", "dummy\n")
        archive.writestr("FM/fmexog.txt", "dummy\n")
        archive.writestr("FM/fminput.txt", "dummy\n")
    return buffer.getvalue()


def test_fetch_and_unpack_fair_bundle_writes_manifest(monkeypatch, tmp_path):
    payload = _build_zip_bytes()

    class _FakeResponse:
        def __init__(self, body: bytes):
            self._body = body
            self.headers = {"Last-Modified": "Tue, 03 Mar 2026 13:40:00 GMT"}

        def read(self) -> bytes:
            return self._body

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_urlopen(_request, timeout=60):
        _ = timeout
        return _FakeResponse(payload)

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    out_dir = tmp_path / "official"
    result = fetch_and_unpack_fair_bundle(out_dir=out_dir, url="https://example.test/FMFP.ZIP")

    assert Path(result["manifest_path"]).exists()
    assert Path(result["zip_path"]).exists()
    assert Path(result["model_dir"]).exists()
    assert (Path(result["model_dir"]) / "fmdata.txt").exists()
    fm_hashes = result.get("fm_txt_sha256", {})
    assert isinstance(fm_hashes, dict)
    assert "fmdata.txt" in fm_hashes


def test_fetch_and_unpack_fair_bundle_from_local_zip_skips_network(monkeypatch, tmp_path):
    source_zip = tmp_path / "source" / "FMFP.ZIP"
    source_zip.parent.mkdir(parents=True, exist_ok=True)
    source_zip.write_bytes(_build_zip_bytes())

    def _fail_urlopen(*_args, **_kwargs):
        raise AssertionError("network should not be used for local zip path")

    monkeypatch.setattr("urllib.request.urlopen", _fail_urlopen)

    out_dir = tmp_path / "offline_bundle"
    result = fetch_and_unpack_fair_bundle(out_dir=out_dir, zip_path=source_zip)

    assert Path(result["manifest_path"]).exists()
    assert Path(result["zip_path"]).exists()
    assert Path(result["model_dir"]).exists()
    assert (Path(result["model_dir"]) / "fmdata.txt").exists()
    assert result.get("source_kind") == "local_zip"
    assert str(result.get("source_url", "")).startswith("local_zip:")


def test_ensure_fp_exe_in_model_dir_copies_from_dir(tmp_path):
    model_dir = tmp_path / "bundle" / "FM"
    model_dir.mkdir(parents=True, exist_ok=True)
    source_dir = tmp_path / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "fp.exe").write_text("binary", encoding="utf-8")

    status = ensure_fp_exe_in_model_dir(model_dir=model_dir, fp_exe_from=source_dir)

    assert status["present"] is True
    assert status["copied"] is True
    assert (model_dir / "fp.exe").exists()


def test_ensure_fp_exe_in_model_dir_reports_missing_without_source(tmp_path):
    model_dir = tmp_path / "bundle" / "FM"
    model_dir.mkdir(parents=True, exist_ok=True)

    status = ensure_fp_exe_in_model_dir(model_dir=model_dir)

    assert status["present"] is False
    assert status["copied"] is False
