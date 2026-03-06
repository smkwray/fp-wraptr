# Scenario DSL

`fp-wraptr` includes a lightweight human-readable scenario DSL that compiles to
standard `ScenarioConfig` YAML/JSON.

Use it when you want concise scenario authoring without writing full YAML.

## Compile Command

```bash
fp dsl compile path/to/scenario.dsl
fp dsl compile path/to/scenario.dsl --format json --output compiled.json
```

## Supported Commands (MVP)

- `scenario <name>`
- `description <text...>`
- `fp_home <path>`
- `input_file <filename>`
- `forecast <start> <end>` or `forecast <start> to <end>`
- `track <VAR1,VAR2,...>`
- `set <VAR> <METHOD> <VALUE>`
- `alert <VAR> <min|max> <VALUE>`
- `patch <KEY> <VALUE...>`
- `policy <type> key=value key=value ...`
- `extra key=value key=value ...`

Comments use `#` and blank lines are ignored.

## Example

```text
scenario jg_soft_landing
description "JG + lower rates"
forecast 2025.4 to 2029.4
track PCY,UR,GDPR

policy job_guarantee jobs=15000000 wage=15.0 benefits_rate=0.2
set RS SAMEVALUE 4.0
alert UR max 6.0
patch cmd:SETUPSOLVE.MAXCHECK 80
extra owner=macro-team active=true
```

Compile to YAML:

```bash
fp dsl compile examples/jg_soft_landing.dsl --format yaml --output examples/jg_soft_landing.yaml
```

## Notes

- Policy lines are compiled through the existing policy registry.
- `set` overrides are applied in DSL order and can override earlier policy-derived values.
- `patch` supports both literal replacement keys and command-aware keys
  such as `cmd:SETUPSOLVE.MAXCHECK`.
