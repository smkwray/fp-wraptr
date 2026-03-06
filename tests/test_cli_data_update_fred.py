from __future__ import annotations

from typer.testing import CliRunner

from fp_wraptr.cli import app
from fp_wraptr.data.update_fred import DataUpdateResult

runner = CliRunner()


def test_data_update_fred_requires_fredapi(monkeypatch):
    monkeypatch.setattr("fp_wraptr.cli.importlib.util.find_spec", lambda *_args, **_kwargs: None)

    result = runner.invoke(
        app,
        [
            "data",
            "update-fred",
            "--model-dir",
            "FM",
            "--out-dir",
            "artifacts/model_updates/demo",
            "--end",
            "2025.4",
        ],
    )

    assert result.exit_code == 1
    output = (result.stdout + result.stderr).lower()
    assert "fredapi is required" in output


def test_data_update_fred_cli_success(monkeypatch, tmp_path):
    original_find_spec = __import__("importlib").util.find_spec

    def _fake_find_spec(name, *args, **kwargs):
        if name == "fredapi":
            return object()
        return original_find_spec(name, *args, **kwargs)

    monkeypatch.setattr("fp_wraptr.cli.importlib.util.find_spec", _fake_find_spec)

    out_dir = tmp_path / "artifacts" / "model_updates" / "demo"
    bundle_dir = out_dir / "FM"
    fmdata_path = bundle_dir / "fmdata.txt"
    report_path = out_dir / "data_update_report.json"

    def _fake_update_model_from_fred(**_kwargs):
        return DataUpdateResult(
            out_dir=out_dir,
            model_bundle_dir=bundle_dir,
            fmdata_path=fmdata_path,
            report_path=report_path,
            report={
                "selected_variable_count": 1,
                "normalized_variable_count": 1,
                "fmdata_merge": {"updated_cells": 4, "carried_cells": 0},
            },
        )

    monkeypatch.setattr(
        "fp_wraptr.data.update_fred.update_model_from_fred", _fake_update_model_from_fred
    )

    result = runner.invoke(
        app,
        [
            "data",
            "update-fred",
            "--model-dir",
            "FM",
            "--out-dir",
            str(out_dir),
            "--end",
            "2025.4",
        ],
    )

    assert result.exit_code == 0
    output = result.stdout + result.stderr
    assert "Updated bundle:" in output
    assert "Report:" in output
    assert "variables_selected=1" in output


def test_data_update_fred_cli_warns_on_fminput_mismatch(monkeypatch, tmp_path):
    original_find_spec = __import__("importlib").util.find_spec

    def _fake_find_spec(name, *args, **kwargs):
        if name == "fredapi":
            return object()
        return original_find_spec(name, *args, **kwargs)

    monkeypatch.setattr("fp_wraptr.cli.importlib.util.find_spec", _fake_find_spec)

    out_dir = tmp_path / "artifacts" / "model_updates" / "demo_warn"
    bundle_dir = out_dir / "FM"
    fmdata_path = bundle_dir / "fmdata.txt"
    report_path = out_dir / "data_update_report.json"

    def _fake_update_model_from_fred(**_kwargs):
        return DataUpdateResult(
            out_dir=out_dir,
            model_bundle_dir=bundle_dir,
            fmdata_path=fmdata_path,
            report_path=report_path,
            report={
                "selected_variable_count": 2,
                "normalized_variable_count": 2,
                "sample_end_after": "2025.4",
                "fminput_fmdata_load_end": "2025.3",
                "fmdata_merge": {"updated_cells": 8, "carried_cells": 0},
            },
        )

    monkeypatch.setattr(
        "fp_wraptr.data.update_fred.update_model_from_fred", _fake_update_model_from_fred
    )

    result = runner.invoke(
        app,
        [
            "data",
            "update-fred",
            "--model-dir",
            "FM",
            "--out-dir",
            str(out_dir),
            "--end",
            "2025.4",
        ],
    )

    assert result.exit_code == 0
    output = result.stdout + result.stderr
    assert "Warning:" in output
    assert "sample_end_after=2025.4" in output
    assert "fminput_fmdata_load_end=2025.3" in output


def test_data_update_fred_cli_infers_end_from_fmdata(monkeypatch, tmp_path):
    original_find_spec = __import__("importlib").util.find_spec

    def _fake_find_spec(name, *args, **kwargs):
        if name == "fredapi":
            return object()
        return original_find_spec(name, *args, **kwargs)

    monkeypatch.setattr("fp_wraptr.cli.importlib.util.find_spec", _fake_find_spec)

    out_dir = tmp_path / "artifacts" / "model_updates" / "demo_infer"
    bundle_dir = out_dir / "FM"
    fmdata_path = bundle_dir / "fmdata.txt"
    report_path = out_dir / "data_update_report.json"
    observed = {"end_period": None}

    def _fake_parse_fm_data(_path):
        return {"sample_end": "2025.3"}

    def _fake_update_model_from_fred(**kwargs):
        observed["end_period"] = kwargs.get("end_period")
        return DataUpdateResult(
            out_dir=out_dir,
            model_bundle_dir=bundle_dir,
            fmdata_path=fmdata_path,
            report_path=report_path,
            report={
                "selected_variable_count": 1,
                "normalized_variable_count": 1,
                "fmdata_merge": {"updated_cells": 1, "carried_cells": 0},
            },
        )

    monkeypatch.setattr("fp_wraptr.io.input_parser.parse_fm_data", _fake_parse_fm_data)
    monkeypatch.setattr(
        "fp_wraptr.data.update_fred.update_model_from_fred", _fake_update_model_from_fred
    )

    result = runner.invoke(
        app,
        [
            "data",
            "update-fred",
            "--model-dir",
            "FM",
            "--out-dir",
            str(out_dir),
        ],
    )

    assert result.exit_code == 0
    output = result.stdout + result.stderr
    assert "defaulting end=2025.3" in output
    assert observed["end_period"] == "2025.3"


def test_data_update_fred_cli_from_official_bundle(monkeypatch, tmp_path):
    original_find_spec = __import__("importlib").util.find_spec

    def _fake_find_spec(name, *args, **kwargs):
        if name == "fredapi":
            return object()
        return original_find_spec(name, *args, **kwargs)

    monkeypatch.setattr("fp_wraptr.cli.importlib.util.find_spec", _fake_find_spec)

    base_dir = tmp_path / "base_bundle"
    base_model_dir = base_dir / "FM"
    base_model_dir.mkdir(parents=True, exist_ok=True)
    original_model_dir = tmp_path / "local_model"
    original_model_dir.mkdir(parents=True, exist_ok=True)
    (original_model_dir / "fp.exe").write_text("fake-exe", encoding="utf-8")

    out_dir = tmp_path / "artifacts" / "model_updates" / "demo_official"
    bundle_dir = out_dir / "FM"
    fmdata_path = bundle_dir / "fmdata.txt"
    report_path = out_dir / "data_update_report.json"
    observed: dict[str, str | None] = {"model_dir": None, "end_period": None}

    def _fake_fetch_and_unpack_fair_bundle(*, out_dir, url, timeout_seconds=60, zip_path=None):
        _ = out_dir, url, timeout_seconds, zip_path
        return {
            "output_dir": str(base_dir),
            "model_dir": str(base_model_dir),
            "manifest_path": str(base_dir / "fair_bundle_manifest.json"),
        }

    def _fake_parse_fm_data(path):
        assert str(path).endswith("/base_bundle/FM/fmdata.txt")
        return {"sample_end": "2025.3"}

    def _fake_update_model_from_fred(**kwargs):
        observed["model_dir"] = str(kwargs.get("model_dir"))
        observed["end_period"] = str(kwargs.get("end_period"))
        bundle_dir.mkdir(parents=True, exist_ok=True)
        return DataUpdateResult(
            out_dir=out_dir,
            model_bundle_dir=bundle_dir,
            fmdata_path=fmdata_path,
            report_path=report_path,
            report={
                "selected_variable_count": 1,
                "normalized_variable_count": 1,
                "fmdata_merge": {"updated_cells": 1, "carried_cells": 0},
            },
        )

    monkeypatch.setattr(
        "fp_wraptr.data.fair_bundle.fetch_and_unpack_fair_bundle",
        _fake_fetch_and_unpack_fair_bundle,
    )
    monkeypatch.setattr("fp_wraptr.io.input_parser.parse_fm_data", _fake_parse_fm_data)
    monkeypatch.setattr(
        "fp_wraptr.data.update_fred.update_model_from_fred", _fake_update_model_from_fred
    )

    result = runner.invoke(
        app,
        [
            "data",
            "update-fred",
            "--out-dir",
            str(out_dir),
            "--model-dir",
            str(original_model_dir),
            "--from-official-bundle",
            "--base-dir",
            str(base_dir),
        ],
    )

    assert result.exit_code == 0
    assert observed["model_dir"] == str(base_model_dir)
    assert observed["end_period"] == "2025.3"
    assert (bundle_dir / "fp.exe").exists()
    output = result.stdout + result.stderr
    assert "Copied fp.exe:" in output


def test_data_update_fred_cli_from_official_bundle_uses_local_zip(monkeypatch, tmp_path):
    original_find_spec = __import__("importlib").util.find_spec

    def _fake_find_spec(name, *args, **kwargs):
        if name == "fredapi":
            return object()
        return original_find_spec(name, *args, **kwargs)

    monkeypatch.setattr("fp_wraptr.cli.importlib.util.find_spec", _fake_find_spec)

    base_dir = tmp_path / "base_bundle"
    base_model_dir = base_dir / "FM"
    base_model_dir.mkdir(parents=True, exist_ok=True)
    local_zip = tmp_path / "artifacts" / "FMFP.ZIP"
    local_zip.parent.mkdir(parents=True, exist_ok=True)
    local_zip.write_bytes(b"fake-zip")

    out_dir = tmp_path / "artifacts" / "model_updates" / "demo_official_local_zip"
    bundle_dir = out_dir / "FM"
    fmdata_path = bundle_dir / "fmdata.txt"
    report_path = out_dir / "data_update_report.json"
    observed: dict[str, str | None] = {"zip_path": None}

    def _fake_fetch_and_unpack_fair_bundle(*, out_dir, url, timeout_seconds=60, zip_path=None):
        _ = out_dir, url, timeout_seconds
        observed["zip_path"] = str(zip_path) if zip_path is not None else None
        return {
            "output_dir": str(base_dir),
            "model_dir": str(base_model_dir),
            "manifest_path": str(base_dir / "fair_bundle_manifest.json"),
        }

    def _fake_parse_fm_data(path):
        assert str(path).endswith("/base_bundle/FM/fmdata.txt")
        return {"sample_end": "2025.3"}

    def _fake_update_model_from_fred(**_kwargs):
        bundle_dir.mkdir(parents=True, exist_ok=True)
        return DataUpdateResult(
            out_dir=out_dir,
            model_bundle_dir=bundle_dir,
            fmdata_path=fmdata_path,
            report_path=report_path,
            report={
                "selected_variable_count": 1,
                "normalized_variable_count": 1,
                "fmdata_merge": {"updated_cells": 1, "carried_cells": 0},
            },
        )

    monkeypatch.setattr(
        "fp_wraptr.data.fair_bundle.fetch_and_unpack_fair_bundle",
        _fake_fetch_and_unpack_fair_bundle,
    )
    monkeypatch.setattr("fp_wraptr.io.input_parser.parse_fm_data", _fake_parse_fm_data)
    monkeypatch.setattr(
        "fp_wraptr.data.update_fred.update_model_from_fred", _fake_update_model_from_fred
    )

    result = runner.invoke(
        app,
        [
            "data",
            "update-fred",
            "--out-dir",
            str(out_dir),
            "--from-official-bundle",
            "--base-dir",
            str(base_dir),
            "--official-bundle-zip-path",
            str(local_zip),
        ],
    )

    assert result.exit_code == 0
    assert observed["zip_path"] == str(local_zip)
