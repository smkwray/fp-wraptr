"""Smoke tests for fp-wraptr.

Tests that don't require fp.exe or FM/ data files should always pass.
Tests that need FM/ files are auto-skipped when files aren't present.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

# Determine if FM/ data is available for integration tests
FM_DIR = Path(__file__).parent.parent / "FM"
HAS_FM = FM_DIR.exists() and (FM_DIR / "fmout.txt").exists()
requires_fm = pytest.mark.skipif(not HAS_FM, reason="FM/ data files not available")


class TestImports:
    """Verify all package modules import without error."""

    def test_import_package(self):
        import fp_wraptr

        assert fp_wraptr.__version__

    def test_import_cli(self):
        from fp_wraptr.cli import app

        assert app is not None

    def test_import_parser(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        assert callable(parse_fp_output_text)

    def test_import_input_parser(self):
        from fp_wraptr.io.input_parser import parse_fp_input_text

        assert callable(parse_fp_input_text)

    def test_import_writer(self):
        from fp_wraptr.io.writer import write_exogenous_file

        assert callable(write_exogenous_file)

    def test_import_config(self):
        from fp_wraptr.scenarios.config import ScenarioConfig

        assert ScenarioConfig is not None

    def test_import_runner(self):
        from fp_wraptr.scenarios.runner import run_scenario

        assert callable(run_scenario)

    def test_import_diff(self):
        from fp_wraptr.analysis.diff import diff_outputs

        assert callable(diff_outputs)

    def test_import_fp_exe(self):
        from fp_wraptr.runtime.fp_exe import FPExecutable

        assert FPExecutable is not None


class TestParser:
    """Test FP output parser with synthetic data."""

    SYNTHETIC_OUTPUT = """\
Some header text
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;

    Variable   Periods forecast are  2025.4  TO   2027.4

                   2025.3      2025.4      2026.1      2026.2      2026.3
                               2026.4      2027.1      2027.2      2027.3
                               2027.4

   1 GDP      P lv   100.0      102.5      104.0      105.5      107.0
                                108.5      110.0      111.5      113.0
                                114.5
             P ch   2.0000      2.5000      1.5000      1.5000      1.5000
                                1.5000      1.5000      1.5000      1.5000
                                1.5000
             P %ch  2.0408      2.5000      1.4634      1.4423      1.4218
                                1.4019      1.3825      1.3453      1.3274
                                1.3100

   2 UR       P lv  0.45000E-01 0.44000E-01 0.43500E-01 0.43000E-01 0.42500E-01
                               0.42000E-01 0.41500E-01 0.41000E-01 0.40500E-01
                               0.40000E-01
             P ch -0.10000E-02-0.10000E-02-0.50000E-03-0.50000E-03-0.50000E-03
                              -0.50000E-03-0.50000E-03-0.50000E-03-0.50000E-03
                              -0.50000E-03
             P %ch -2.1739     -2.2222     -1.1364     -1.1494     -1.1628
                               -1.1765     -1.1905     -1.2048     -1.2195
                               -1.2346
"""

    def test_parse_synthetic_output(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text(self.SYNTHETIC_OUTPUT)

        assert result.forecast_start == "2025.4"
        assert result.forecast_end == "2027.4"
        assert "GDP" in result.variables
        assert "UR" in result.variables

    def test_parse_periods(self):
        from fp_wraptr.io.parser import _generate_periods

        periods = _generate_periods("2025.4", "2027.4")
        assert periods[0] == "2025.4"
        assert periods[-1] == "2027.4"
        assert len(periods) == 9  # 2025.4 through 2027.4

    def test_parse_gdp_levels(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text(self.SYNTHETIC_OUTPUT)
        gdp = result.variables["GDP"]

        assert len(gdp.levels) >= 2
        assert gdp.levels[0] == pytest.approx(100.0)
        assert gdp.levels[1] == pytest.approx(102.5)

    def test_parse_ur_scientific_notation(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text(self.SYNTHETIC_OUTPUT)
        ur = result.variables["UR"]

        assert len(ur.levels) >= 2
        assert ur.levels[0] == pytest.approx(0.045)
        assert ur.levels[1] == pytest.approx(0.044)

    def test_to_dataframe(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text(self.SYNTHETIC_OUTPUT)
        df = result.to_dataframe()

        assert "GDP" in df.columns
        assert "UR" in df.columns
        assert len(df) > 0

    def test_to_dict(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text(self.SYNTHETIC_OUTPUT)
        d = result.to_dict()

        assert "variables" in d
        assert "GDP" in d["variables"]
        assert "levels" in d["variables"]["GDP"]

    def test_empty_output(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text("no forecast data here")
        assert result.variables == {}
        assert result.forecast_start == ""


class TestInputParser:
    """Test FP input parser with synthetic data."""

    SYNTHETIC_INPUT = """\
US MODEL TEST
SPACE MAXVAR=100 MAXS=10 MAXCOEF=10 FIRSTPER=2020.1 LASTPER=2030.4;
@
@ Test equation
@
SMPL 2020.1 2025.3;
GENR CSZ=CS/POP;
GENR LY=LOG(Y);
EQ 1 LCSZ C LCSZ(-1) LYDZ RSA;
LHS CS=EXP(LCSZ)*POP;
IDENT PX=(PF*(X-FA)+PFA*FA)/X;
IDENT KD=(1-DELD)*KD(-1)+CD;
"""

    def test_parse_title(self):
        from fp_wraptr.io.input_parser import parse_fp_input_text

        result = parse_fp_input_text(self.SYNTHETIC_INPUT)
        assert result["title"] == "US MODEL TEST"

    def test_parse_space(self):
        from fp_wraptr.io.input_parser import parse_fp_input_text

        result = parse_fp_input_text(self.SYNTHETIC_INPUT)
        assert result["space"]["maxvar"] == "100"
        assert result["space"]["firstper"] == "2020.1"

    def test_parse_equations(self):
        from fp_wraptr.io.input_parser import parse_fp_input_text

        result = parse_fp_input_text(self.SYNTHETIC_INPUT)
        assert len(result["equations"]) >= 1
        assert result["equations"][0]["number"] == 1

    def test_parse_identities(self):
        from fp_wraptr.io.input_parser import parse_fp_input_text

        result = parse_fp_input_text(self.SYNTHETIC_INPUT)
        assert len(result["identities"]) >= 2

    def test_parse_genr(self):
        from fp_wraptr.io.input_parser import parse_fp_input_text

        result = parse_fp_input_text(self.SYNTHETIC_INPUT)
        assert len(result["generated_vars"]) >= 2

    def test_setupect_normalized_keys_no_alias(self):
        from fp_wraptr.io.input_parser import parse_fp_input_text

        text = """
SPACE MAXVAR=100 MAXS=10 FIRSTPER=2020.1;
SETUPEST MAXIT=50;
LOADDATA FILE=fminput.txt;
SOLVE DYNAMIC FILEVAR=KEYBOARD NORESET;
PRINTVAR FILEOUT=FCST.VAR PCY GDP;
"""
        result = parse_fp_input_text(text)

        assert "setupect" in result
        assert "setupecst" not in result
        assert "setupeat" not in result
        assert len(result["setupect"]) == 1
        assert result["setupect"][0]["maxit"] == "50"
        assert result["space"]["maxvar"] == "100"
        assert result["space"]["maxs"] == "10"
        assert result["space"]["firstper"] == "2020.1"
        assert result["load_data"] == ["fminput.txt"]
        assert result["solve"]["params"]["filevar"] == "KEYBOARD"
        assert result["printvar"][0]["file"] == "FCST.VAR"

    def test_commands_by_type_normalized(self):
        from fp_wraptr.io.input_parser import parse_fp_input_text

        result = parse_fp_input_text(
            "SMPL 2020.1 2025.3;"
            "SPACE MAXVAR=100;"
            "SETUPEST MAXIT=10;"
            "SETUPSOLVE MAXITER=8;"
            "SOLVE DYNAMIC FILEVAR=KEYBOARD;"
            "PRINTVAR FILEOUT=FCST.VAR GDPC1;"
            "QUIT;"
        )

        keys = set(result["commands_by_type"].keys())
        assert "setupest" in keys
        assert all(key == key.lower() for key in keys)

    def test_unknown_command_stays_lowercase_key(self):
        from fp_wraptr.io.input_parser import parse_fp_input_text

        result = parse_fp_input_text(
            "US MODEL TEST\nSPACE MAXVAR=100;\nMALFORMED CMD=ONE;\nSETUPEST MAXIT=30;"
        )

        assert "malformed" in result["commands_by_type"]
        assert len(result["unhandled_commands"]) == 1
        assert result["unhandled_commands"][0]["name"] == "MALFORMED"

    def test_malformed_command_body_is_non_fatal(self):
        from fp_wraptr.io.input_parser import parse_fp_input_text

        result = parse_fp_input_text(
            "SPACE MAXVAR;\nSETUPSOLVE ;\nSMPL 2020.1;\nPRINTVAR FILEOUT=;\n"
        )

        keys = set(result["commands_by_type"])
        assert all(key == key.lower() for key in keys)
        assert result["space"] == {}
        assert result["setupsolve"] == [{}]
        assert result["samples"] == []
        assert result["printvar"][0]["file"] is None
        assert result["printvar"][0]["variables"] == []


class TestMCPServerTools:
    """Test MCP tools directly without running transport."""

    SYNTHETIC_OUTPUT = """\
Some header text
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
    Variable   Periods forecast are  2025.4  TO   2025.5

                   2025.4      2025.5
                               2026.1      2026.2

   1 GDP      P lv   1.0      2.0
                                3.0      4.0
             P ch   0.1      0.1
                                0.1      0.1
             P %ch  10.0     10.0
                                10.0     10.0
"""

    def test_list_output_variables_tool(self, tmp_path):
        from fp_wraptr.mcp_server import list_output_variables

        output_path = tmp_path / "fmout.txt"
        output_path.write_text(self.SYNTHETIC_OUTPUT, encoding="utf-8")

        payload = json.loads(list_output_variables(str(output_path)))
        assert payload["path"] == str(output_path)
        assert len(payload["variables"]) == 1
        assert payload["variables"][0]["name"] == "GDP"
        assert payload["variables"][0]["level_count"] == 4

    def test_list_output_equations_tool(self, tmp_path):
        from fp_wraptr.mcp_server import list_output_equations

        output_path = tmp_path / "fmout.txt"
        output_path.write_text(self.SYNTHETIC_OUTPUT, encoding="utf-8")

        payload = json.loads(list_output_equations(str(output_path)))
        assert payload["path"] == str(output_path)
        assert payload["equations"] == []

    def test_parse_fp_output_tool_json(self, tmp_path):
        from fp_wraptr.mcp_server import parse_fp_output

        output_path = tmp_path / "fmout.txt"
        output_path.write_text(self.SYNTHETIC_OUTPUT, encoding="utf-8")

        payload = json.loads(parse_fp_output(str(output_path)))
        assert payload["forecast_start"] == "2025.4"
        assert payload["forecast_end"] == "2025.5"
        assert "GDP" in payload["variables"]
        assert payload["variables"]["GDP"]["levels"] == [1.0, 2.0, 3.0, 4.0]

    def test_mcp_resources_guarded(self):
        import fp_wraptr.mcp_server as server

        if hasattr(server.mcp, "resource"):
            assert hasattr(server, "output_variables_resource")
            assert hasattr(server, "output_equations_resource")

    def test_validate_scenario_tool(self, tmp_path):
        from fp_wraptr.mcp_server import validate_scenario

        payload = yaml.safe_dump({
            "name": "mcp_test",
            "description": "scenario for MCP validation",
        })
        parsed = json.loads(validate_scenario(payload))
        assert parsed["valid"] is True
        assert parsed["name"] == "mcp_test"

    def test_list_scenarios_tool(self, tmp_path):
        from fp_wraptr.mcp_server import list_scenarios

        examples = tmp_path / "examples"
        examples.mkdir()
        (examples / "sample.yaml").write_text(
            yaml.safe_dump({
                "name": "sample",
                "description": "sample scenario",
            }),
            encoding="utf-8",
        )

        payload = json.loads(list_scenarios(str(examples)))
        assert isinstance(payload, list)
        assert any(entry.get("name") == "sample" for entry in payload)

    def test_get_run_history_tool(self, tmp_path):
        from fp_wraptr.mcp_server import get_run_history
        from fp_wraptr.scenarios.config import ScenarioConfig

        run_dir = tmp_path / "history_20260223_120000"
        run_dir.mkdir()
        ScenarioConfig(name="mcp_history").to_yaml(run_dir / "scenario.yaml")
        (run_dir / "fmout.txt").write_text("SOLVE DYNAMIC;\n", encoding="utf-8")

        payload = json.loads(get_run_history(str(tmp_path)))
        assert payload["count"] == 1
        assert payload["runs"][0]["scenario_name"] == "mcp_history"

    def test_run_batch_scenarios_tool(self, tmp_path, monkeypatch):
        from fp_wraptr.mcp_server import run_batch_scenarios
        from fp_wraptr.runtime.fp_exe import FPRunResult
        from fp_wraptr.scenarios.runner import ScenarioResult

        examples = tmp_path / "examples"
        examples.mkdir()
        (examples / "batch_a.yaml").write_text(
            yaml.safe_dump({"name": "batch_a"}),
            encoding="utf-8",
        )
        (examples / "batch_b.yaml").write_text(
            yaml.safe_dump({"name": "batch_b"}),
            encoding="utf-8",
        )

        def fake_run_scenario(config, output_dir=None, backend=None):
            run_dir = Path(output_dir) / config.name
            run_dir.mkdir(parents=True, exist_ok=True)
            output_path = run_dir / "fmout.txt"
            output_path.write_text(
                "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;\n",
                encoding="utf-8",
            )
            return ScenarioResult(
                config=config,
                output_dir=run_dir,
                run_result=FPRunResult(
                    return_code=0,
                    stdout="ok",
                    stderr="",
                    working_dir=run_dir,
                    input_file=run_dir / "fminput.txt",
                    output_file=output_path,
                    duration_seconds=0.01,
                ),
            )

        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("fp_wraptr.scenarios.runner.run_scenario", fake_run_scenario)

        payload = json.loads(
            run_batch_scenarios(
                ["batch_a", "batch_b"],
                output_dir=str(tmp_path / "artifacts"),
            )
        )

        assert payload["total"] == 2
        assert payload["succeeded"] == 2
        assert payload["failed"] == 0
        assert all(item["success"] for item in payload["results"])
        names = {item["name"] for item in payload["results"]}
        assert names == {"batch_a", "batch_b"}

    def test_run_batch_scenarios_tool_handles_missing_scenario(self, tmp_path, monkeypatch):
        from fp_wraptr.mcp_server import run_batch_scenarios

        monkeypatch.chdir(tmp_path)

        payload = json.loads(
            run_batch_scenarios(["missing"], output_dir=str(tmp_path / "artifacts"))
        )
        assert payload["total"] == 1
        assert payload["succeeded"] == 0
        assert payload["failed"] == 1
        assert payload["results"][0]["name"] == "missing"
        assert payload["results"][0]["success"] is False

    def test_create_scenario_valid(self, tmp_path):
        from fp_wraptr.mcp_server import create_scenario

        examples_dir = tmp_path / "examples"
        payload = json.loads(
            create_scenario(
                yaml.safe_dump({"name": "mcp_created", "description": "created by MCP test"}),
                filename="mcp_created.yaml",
                examples_dir=str(examples_dir),
            )
        )

        target = examples_dir / "mcp_created.yaml"
        assert payload["created"] is True
        assert payload["name"] == "mcp_created"
        assert payload["path"] == str(target)
        assert target.exists()

    def test_create_scenario_invalid(self, tmp_path):
        from fp_wraptr.mcp_server import create_scenario

        payload = json.loads(
            create_scenario("{{{{", filename="bad.yaml", examples_dir=str(tmp_path))
        )
        assert "error" in payload

    def test_update_scenario_valid(self, tmp_path):
        from fp_wraptr.mcp_server import update_scenario

        path = tmp_path / "mcp_update.yaml"
        path.write_text(yaml.safe_dump({"name": "old_name"}), encoding="utf-8")

        payload = json.loads(
            update_scenario(
                str(path),
                yaml.safe_dump({"name": "mcp_updated", "description": "updated by MCP test"}),
            )
        )

        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert payload["updated"] is True
        assert payload["name"] == "mcp_updated"
        assert loaded["name"] == "mcp_updated"

    def test_update_scenario_missing(self, tmp_path):
        from fp_wraptr.mcp_server import update_scenario

        payload = json.loads(update_scenario(str(tmp_path / "does-not-exist.yaml"), "name: nope"))
        assert "error" in payload


class TestFmDataParser:
    """Test fmdata parser."""

    SYNTHETIC_FMDATA = """\
SMPL 1950.1 1950.4;
LOAD X;
1.0 2.0 3.0 4.0
'END'
LOAD Y;
5.0
6.0
7.0
8.0
'END'
END;
"""

    @requires_fm
    def test_parse_real_fmdata(self):
        from fp_wraptr.io.input_parser import parse_fm_data

        data = parse_fm_data(FM_DIR / "fmdata.txt")
        assert data["sample_start"] == "1952.1"
        assert data["sample_end"] == "2025.3"
        assert "CS" in data["series"]
        assert len(data["series"]["CS"]) >= 1
        assert len(data["series"]["CS"][0]["values"]) > 200

    def test_parse_synthetic_fmdata(self):
        from fp_wraptr.io.input_parser import parse_fm_data_text

        data = parse_fm_data_text(self.SYNTHETIC_FMDATA)
        assert data["series"]
        assert data["series"]["X"][0]["values"] == [1.0, 2.0, 3.0, 4.0]
        assert len(data["series"]["Y"][0]["values"]) == 4


class TestFmExogParser:
    """Test fmexog parser."""

    SYNTHETIC_FMEXOG = """\
SMPL 2025.4 2029.4;
CHANGEVAR;
CCGQ CHGSAMEPCT
0.007417072
CCHQ SAMEVALUE
3
ISZQ
0.004962932
INTS ;
40.0
41.0
42.0
RETURN;
"""

    def test_parse_synthetic_fmexog(self):
        from fp_wraptr.io.input_parser import parse_fmexog_text

        data = parse_fmexog_text(self.SYNTHETIC_FMEXOG)
        assert data["blocks"]
        assert data["blocks"][0]["sample_start"] == "2025.4"
        assert data["blocks"][0]["sample_end"] == "2029.4"

        changes = {c["variable"]: c for c in data["changes"]}
        assert changes["CCGQ"]["method"] == "CHGSAMEPCT"
        assert changes["CCHQ"]["method"] == "SAMEVALUE"
        assert changes["ISZQ"]["method"] is None
        assert len(changes["INTS"]["values"]) == 3

    @requires_fm
    def test_parse_real_fmexog(self):
        from fp_wraptr.io.input_parser import parse_fmexog

        data = parse_fmexog(FM_DIR / "fmexog.txt")
        assert len(data["blocks"]) == 3
        assert data["blocks"][0]["sample_start"] == "2025.4"
        assert "CCGQ" in {c["variable"] for c in data["blocks"][0]["changes"]}
        assert any(c["variable"] == "INTS" for c in data["blocks"][0]["changes"])


class TestScenarioConfig:
    """Test scenario configuration model."""

    def test_create_config(self):
        from fp_wraptr.scenarios.config import ScenarioConfig

        config = ScenarioConfig(name="test", description="A test scenario")
        assert config.name == "test"
        assert config.fp_home == Path("FM")

    def test_config_with_overrides(self):
        from fp_wraptr.scenarios.config import ScenarioConfig, VariableOverride

        config = ScenarioConfig(
            name="high_growth",
            overrides={
                "YS": VariableOverride(method="CHGSAMEPCT", value=0.008),
            },
        )
        assert "YS" in config.overrides
        assert config.overrides["YS"].value == 0.008

    def test_config_yaml_roundtrip(self, tmp_path):
        from fp_wraptr.scenarios.config import ScenarioConfig, VariableOverride

        config = ScenarioConfig(
            name="roundtrip_test",
            description="Test YAML round-trip",
            overrides={
                "YS": VariableOverride(method="CHGSAMEPCT", value=0.008),
            },
            track_variables=["PCY", "UR"],
        )

        yaml_path = tmp_path / "test_scenario.yaml"
        config.to_yaml(yaml_path)

        loaded = ScenarioConfig.from_yaml(yaml_path)
        assert loaded.name == "roundtrip_test"
        assert "YS" in loaded.overrides
        assert loaded.overrides["YS"].value == 0.008


class TestScenarioRunner:
    """Test scenario execution orchestration."""

    def test_run_scenario_writes_fmexog_override(self, tmp_path, monkeypatch):
        from fp_wraptr.runtime.fp_exe import FPRunResult
        from fp_wraptr.scenarios.config import ScenarioConfig, VariableOverride
        from fp_wraptr.scenarios.runner import run_scenario

        fp_home = tmp_path / "FM"
        fp_home.mkdir()
        (fp_home / "fminput.txt").write_text(
            "TITLE TEST\n"
            "INPUT FILE=fmexog.txt;\n"
            "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;\n",
            encoding="utf-8",
        )

        def fake_run(self, input_file, work_dir, extra_env=None):
            output_path = work_dir / "fmout.txt"
            output_path.write_text(
                "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;\n",
                encoding="utf-8",
            )
            return FPRunResult(
                return_code=0,
                stdout="simulated",
                stderr="",
                working_dir=work_dir,
                input_file=input_file,
                output_file=output_path,
                duration_seconds=0.01,
            )

        monkeypatch.setattr(
            "fp_wraptr.scenarios.runner.FPExecutable.check_available", lambda _self: True
        )
        monkeypatch.setattr("fp_wraptr.scenarios.runner.FPExecutable.run", fake_run)

        config = ScenarioConfig(
            name="override_test",
            fp_home=fp_home,
            forecast_start="2025.4",
            forecast_end="2029.4",
            overrides={
                "YS": VariableOverride(method="CHGSAMEPCT", value=0.008),
            },
        )

        result = run_scenario(config=config, output_dir=tmp_path / "artifacts")

        patched_input = result.output_dir / "work" / "fminput.txt"
        merged_exog = result.output_dir / "work" / "fmexog.txt"
        override_file = result.output_dir / "work" / "fmexog_override.txt"

        assert override_file.exists()
        assert merged_exog.exists()
        assert patched_input.exists()
        patched_text = patched_input.read_text(encoding="utf-8")
        assert "INPUT FILE=fmexog.txt;" in patched_text
        assert "fmexog_override.txt" not in patched_text
        assert "YS CHGSAMEPCT" in merged_exog.read_text(encoding="utf-8")
        assert "YS CHGSAMEPCT" in override_file.read_text(encoding="utf-8")
        assert result.run_result is not None
        assert result.run_result.success
        assert (result.output_dir / "fmout.txt").exists()


class TestScenarioOverridePipeline:
    """Test override-only scenario execution when fp.exe is unavailable."""

    def test_fallback_writes_fmexog_override_file(self, tmp_path, monkeypatch):
        from fp_wraptr.runtime.fp_exe import FPExecutable
        from fp_wraptr.scenarios.config import ScenarioConfig, VariableOverride
        from fp_wraptr.scenarios.runner import run_scenario

        fp_home = tmp_path / "FM"
        fp_home.mkdir()
        (fp_home / "fminput.txt").write_text(
            "TITLE TEST\n"
            "INPUT FILE=fmexog.txt;\n"
            "SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;\n",
            encoding="utf-8",
        )

        monkeypatch.setattr(
            FPExecutable,
            "check_available",
            lambda _self: False,
        )

        config = ScenarioConfig(
            name="override_fallback",
            fp_home=fp_home,
            forecast_start="2025.4",
            forecast_end="2029.4",
            overrides={
                "YS": VariableOverride(method="CHGSAMEPCT", value=0.008),
            },
        )

        result = run_scenario(config=config, output_dir=tmp_path)

        override_file = result.output_dir / "work" / "fmexog_override.txt"
        merged_exog = result.output_dir / "work" / "fmexog.txt"
        patched_input = result.output_dir / "work" / "fminput.txt"

        assert override_file.exists()
        assert merged_exog.exists()
        override_text = override_file.read_text(encoding="utf-8")
        assert "CHANGEVAR" in override_text
        assert "YS" in override_text

        assert patched_input.exists()
        patched_text = patched_input.read_text(encoding="utf-8")
        assert "INPUT FILE=fmexog.txt;" in patched_text


class TestWriter:
    """Test FP file writers."""

    def test_write_exogenous(self, tmp_path):
        from fp_wraptr.io.writer import write_exogenous_file

        output = tmp_path / "fmexog_test.txt"
        write_exogenous_file(
            variables={
                "YS": {"method": "CHGSAMEPCT", "value": 0.008},
                "RS": {"method": "SAMEVALUE", "value": 3.5},
            },
            sample_start="2025.4",
            sample_end="2029.4",
            output_path=output,
        )

        assert output.exists()
        text = output.read_text()
        assert "CHANGEVAR" in text
        assert "YS CHGSAMEPCT" in text
        assert "RS SAMEVALUE" in text
        assert "RETURN" in text

    def test_patch_input_file(self, tmp_path):
        from fp_wraptr.io.writer import patch_input_file

        base = tmp_path / "base.txt"
        base.write_text("SETUPSOLVE MINITERS=3 MAXCHECK=30 ;")

        patched = tmp_path / "patched.txt"
        patch_input_file(base, {"MINITERS=3": "MINITERS=40"}, patched)

        assert "MINITERS=40" in patched.read_text()

    def test_patch_fmexog_reference(self, tmp_path):
        from fp_wraptr.io.writer import patch_fmexog_reference

        base = tmp_path / "base.txt"
        base.write_text("INPUT FILE=FMEXOG.TXT;\nSMPL 2025.4 2029.4;\n")
        output = tmp_path / "patched.txt"
        patched = patch_fmexog_reference(base, tmp_path / "fmexog_override.txt", output)

        assert "INPUT FILE=fmexog_override.txt;" in patched.read_text()

    def test_patch_fmexog_reference_robust_cases(self, tmp_path):
        from fp_wraptr.io.writer import patch_fmexog_reference

        override = tmp_path / "fmexog_override.txt"
        cases = [
            ("INPUT FILE=fmexog.txt;\n", "INPUT FILE=fmexog_override.txt;"),
            ("  input file = FMEXOG.TXT ;  @ exogenous input\n", "@ exogenous input"),
            ("INPUT   FILE=./fmexog.txt;\n", "INPUT   FILE=fmexog_override.txt;"),
            ("input   file=FMEXOG.TXT;\n", "input   file=fmexog_override.txt;"),
            (
                "INPUT FILE=FMEGEXOG.TXT;   @ should preserve inline suffix\n",
                "@ should preserve inline suffix",
            ),
            ("INPUT FILE=FMEXOG.TXT; -- keep this suffix\n", "; -- keep this suffix"),
        ]

        for idx, (content, expected) in enumerate(cases):
            base = tmp_path / f"base_{idx}.txt"
            out = tmp_path / f"patched_{idx}.txt"
            base.write_text(content, encoding="utf-8")
            patched = patch_fmexog_reference(base, override, out)
            text = patched.read_text(encoding="utf-8")
            assert "fmexog_override.txt" in text
            assert expected in text


class TestDiff:
    """Test run comparison."""

    def test_diff_identical_outputs(self):
        from fp_wraptr.analysis.diff import diff_outputs
        from fp_wraptr.io.parser import ForecastVariable, FPOutputData

        data = FPOutputData(
            variables={
                "GDP": ForecastVariable(var_id=1, name="GDP", levels=[100.0, 105.0, 110.0]),
            }
        )
        result = diff_outputs(data, data)
        assert result["deltas"]["GDP"]["abs_delta"] == 0.0

    def test_diff_different_outputs(self):
        from fp_wraptr.analysis.diff import diff_outputs
        from fp_wraptr.io.parser import ForecastVariable, FPOutputData

        baseline = FPOutputData(
            variables={
                "GDP": ForecastVariable(var_id=1, name="GDP", levels=[100.0, 105.0, 110.0]),
            }
        )
        scenario = FPOutputData(
            variables={
                "GDP": ForecastVariable(var_id=1, name="GDP", levels=[100.0, 107.0, 115.0]),
            }
        )
        result = diff_outputs(baseline, scenario)
        assert result["deltas"]["GDP"]["abs_delta"] == pytest.approx(5.0)
        assert result["deltas"]["GDP"]["pct_delta"] == pytest.approx(5.0 / 110.0 * 100)


class TestFPExecutable:
    """Test FP executable wrapper."""

    def test_exe_not_available(self, tmp_path):
        from fp_wraptr.runtime.fp_exe import FPExecutable

        fp = FPExecutable(fp_home=tmp_path)
        assert not fp.check_available()

    def test_exe_path(self):
        from fp_wraptr.runtime.fp_exe import FPExecutable

        fp = FPExecutable(fp_home=Path("FM"))
        assert fp.exe_path == Path("FM/fp.exe")

    def test_auto_detect_wine(self):
        import platform

        from fp_wraptr.runtime.fp_exe import FPExecutable

        fp = FPExecutable(fp_home=Path("FM"))
        if platform.system() != "Windows":
            assert fp.use_wine is True
        else:
            assert fp.use_wine is False


class TestEstimation:
    """Test estimation result parsing with synthetic data."""

    SYNTHETIC_ESTIMATION = """\
SPACE MAXVAR=500 MAXS=30 MAXCOEF=30 FIRSTPER=1952.1 LASTPER=2029.4;
US MODEL TEST RUN

Equation number =   1
**********************

Dependent variable = LGDP

 1960.1 -   2025.3,    0 missing obs.,  263 obs. used.

First stage regressors

  10 LGDP     ( -1)
  15 C        (  0)

Iter   Rhos:  1
   1     0.550000000
   2     0.580000000
   3     0.581000000

Number of iterations for RHO =    3


Mean of dependent variable        8.123456789

                     Coef est             SE           T statistic      Mean
   10 LGDP    ( -1)       0.950000000       0.010000000  95.00000     8.000000
   15 C       (  0)       0.400000000       0.050000000   8.00000     1.000000
    0 RHO     ( -1)       0.581000000       0.060000000   9.68333     0.000000

SE of equation            =     0.005000000
Sum of squared residuals  = 0.650000000E-02   0.100000000E-06
Average absolute error    =     0.003500000
Sum of ABS residuals      = 0.920000000
R squared                 =   0.99950
Durbin-Watson statistic   =   2.05000
Overid test: p-value      = 0.01234         (df =   3)
SMPL and No. Obs.         = 1960.1   2025.3    263

Some other text
"""

    def test_parse_estimation_count(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text(self.SYNTHETIC_ESTIMATION)
        assert len(result.estimations) == 1

    def test_parse_estimation_metadata(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text(self.SYNTHETIC_ESTIMATION)
        eq = result.estimations[0]
        assert eq.equation_number == 1
        assert eq.dependent_var == "LGDP"
        assert eq.sample_start == "1960.1"
        assert eq.sample_end == "2025.3"
        assert eq.n_obs == 263

    def test_parse_estimation_stats(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text(self.SYNTHETIC_ESTIMATION)
        eq = result.estimations[0]
        assert eq.r_squared == pytest.approx(0.99950)
        assert eq.durbin_watson == pytest.approx(2.05)
        assert eq.se_equation == pytest.approx(0.005)
        assert eq.rho_iterations == 3
        assert eq.mean_dep_var == pytest.approx(8.123456789)
        assert eq.overid_pvalue == pytest.approx(0.01234)
        assert eq.overid_df == 3

    def test_parse_coefficients(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text(self.SYNTHETIC_ESTIMATION)
        coefs = result.estimations[0].coefficients
        assert len(coefs) == 3

        lgdp = coefs[0]
        assert lgdp.var_name == "LGDP"
        assert lgdp.var_id == 10
        assert lgdp.lag == -1
        assert lgdp.estimate == pytest.approx(0.95)
        assert lgdp.std_error == pytest.approx(0.01)
        assert lgdp.t_statistic == pytest.approx(95.0)

    def test_parse_model_title(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text(self.SYNTHETIC_ESTIMATION)
        assert result.model_title == "US MODEL TEST RUN"


class TestSolveIterations:
    """Test solve iteration parsing."""

    SYNTHETIC_SOLVE = """\
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
ITERS=    40  2025.4
ITERS=    40  2026.1
ITERS=    25  2026.2
"""

    def test_parse_iterations(self):
        from fp_wraptr.io.parser import parse_fp_output_text

        result = parse_fp_output_text(self.SYNTHETIC_SOLVE)
        assert len(result.solve_iterations) == 3
        assert result.solve_iterations[0].period == "2025.4"
        assert result.solve_iterations[0].iterations == 40
        assert result.solve_iterations[2].period == "2026.2"
        assert result.solve_iterations[2].iterations == 25


class TestPeriodHeaders:
    """Test period header parsing from actual column headers."""

    def test_parse_period_headers(self):
        from fp_wraptr.io.parser import _parse_period_headers

        text = """\

                   2025.3      2025.4      2026.1      2026.2      2026.3
                               2026.4      2027.1      2027.2      2027.3
                               2027.4

 427 PCY     P lv   5.0353
"""
        periods = _parse_period_headers(text)
        assert periods == [
            "2025.3",
            "2025.4",
            "2026.1",
            "2026.2",
            "2026.3",
            "2026.4",
            "2027.1",
            "2027.2",
            "2027.3",
            "2027.4",
        ]


class TestPackedValues:
    """Test parsing of packed scientific notation values without spaces."""

    def test_packed_negative_sci(self):
        from fp_wraptr.io.parser import _extract_floats

        # Real example from fmout.txt P ch row
        vals = _extract_floats("-0.45280E-01-0.54776E-01")
        assert len(vals) == 2
        assert vals[0] == pytest.approx(-0.04528)
        assert vals[1] == pytest.approx(-0.054776)

    def test_packed_positive_negative(self):
        from fp_wraptr.io.parser import _extract_floats

        vals = _extract_floats("0.15432E-02-0.15838E-02")
        assert len(vals) == 2
        assert vals[0] == pytest.approx(0.0015432)
        assert vals[1] == pytest.approx(-0.0015838)

    def test_trailing_dot(self):
        from fp_wraptr.io.parser import _extract_floats

        # Large numbers sometimes have trailing dot: "10059."
        vals = _extract_floats("10059.")
        assert len(vals) == 1
        assert vals[0] == pytest.approx(10059.0)


class TestToDict:
    """Test to_dict includes new fields."""

    def test_to_dict_has_estimations(self):
        from fp_wraptr.io.parser import EstimationResult, FPOutputData, SolveIteration

        data = FPOutputData(
            estimations=[
                EstimationResult(equation_number=1, dependent_var="GDP"),
            ],
            solve_iterations=[
                SolveIteration(period="2025.4", iterations=40),
            ],
        )
        d = data.to_dict()
        assert len(d["estimations"]) == 1
        assert d["estimations"][0]["equation_number"] == 1
        assert len(d["solve_iterations"]) == 1
        assert d["solve_iterations"][0]["period"] == "2025.4"


@requires_fm
class TestIntegration:
    """Integration tests requiring FM/ data files."""

    def test_parse_real_output_forecast(self):
        from fp_wraptr.io.parser import parse_fp_output

        result = parse_fp_output(FM_DIR / "fmout.txt")
        assert result.forecast_start == "2025.4"
        assert result.forecast_end == "2029.4"
        assert result.base_period == "2025.3"
        assert len(result.periods) == 18
        assert result.periods[0] == "2025.3"
        assert result.periods[-1] == "2029.4"

    def test_parse_real_output_variables(self):
        from fp_wraptr.io.parser import parse_fp_output

        result = parse_fp_output(FM_DIR / "fmout.txt")
        assert len(result.variables) == 5
        assert set(result.variables.keys()) == {"PCY", "PCPF", "UR", "PIEF", "GDPR"}

        # Check exact values for PCY (first variable)
        pcy = result.variables["PCY"]
        assert pcy.var_id == 427
        assert len(pcy.levels) == 18
        assert pcy.levels[0] == pytest.approx(5.0353)  # base period
        assert pcy.levels[1] == pytest.approx(4.2968)  # first forecast
        assert pcy.levels[-1] == pytest.approx(2.5923)  # last forecast

        assert len(pcy.changes) == 18
        assert pcy.changes[0] == pytest.approx(-0.58871)
        assert pcy.changes[3] == pytest.approx(-0.04528, rel=1e-3)  # packed sci notation

        assert len(pcy.pct_changes) == 18
        assert pcy.pct_changes[0] == pytest.approx(-35.744)

    def test_parse_real_output_ur(self):
        """UR uses scientific notation for all level values."""
        from fp_wraptr.io.parser import parse_fp_output

        result = parse_fp_output(FM_DIR / "fmout.txt")
        ur = result.variables["UR"]
        assert ur.var_id == 87
        assert ur.levels[0] == pytest.approx(0.043373, rel=1e-4)
        assert ur.levels[1] == pytest.approx(0.041789, rel=1e-4)

    def test_parse_real_output_estimations(self):
        from fp_wraptr.io.parser import parse_fp_output

        result = parse_fp_output(FM_DIR / "fmout.txt")
        assert len(result.estimations) == 31

        # Check first estimation (eq 10, LPF)
        eq10 = result.estimations[0]
        assert eq10.equation_number == 10
        assert eq10.dependent_var == "LPF"
        assert eq10.n_obs == 287
        assert eq10.r_squared == pytest.approx(0.99997)
        assert eq10.durbin_watson == pytest.approx(2.11699)
        assert eq10.rho_iterations == 5
        assert eq10.overid_pvalue == pytest.approx(0.0)
        assert eq10.overid_df == 6

        # Verify coefficient count and first coefficient
        assert len(eq10.coefficients) == 16
        first_coef = eq10.coefficients[0]
        assert first_coef.var_name == "LPF"
        assert first_coef.estimate == pytest.approx(0.886852027)

    def test_parse_real_output_solve_iterations(self):
        from fp_wraptr.io.parser import parse_fp_output

        result = parse_fp_output(FM_DIR / "fmout.txt")
        assert len(result.solve_iterations) == 17
        assert result.solve_iterations[0].period == "2025.4"
        assert result.solve_iterations[0].iterations == 40
        assert result.solve_iterations[-1].period == "2029.4"

    def test_parse_real_output_model_title(self):
        from fp_wraptr.io.parser import parse_fp_output

        result = parse_fp_output(FM_DIR / "fmout.txt")
        assert "US MODEL" in result.model_title

    def test_parse_real_output_dataframe(self):
        from fp_wraptr.io.parser import parse_fp_output

        result = parse_fp_output(FM_DIR / "fmout.txt")
        df = result.to_dataframe()
        assert len(df) == 18
        assert list(df.columns) == ["PCY", "PCPF", "UR", "PIEF", "GDPR"]
        assert df.index[0] == "2025.3"
        assert df.index[-1] == "2029.4"

    def test_parse_real_input(self):
        from fp_wraptr.io.input_parser import parse_fp_input

        result = parse_fp_input(FM_DIR / "fminput.txt")
        assert result["title"]
        assert len(result["equations"]) > 0
        assert len(result["identities"]) > 0
