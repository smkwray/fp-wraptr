# PSE2025 Scenario Cards (Curated)

This folder holds curated, operator-facing card definitions for PSE2025 scenario authoring.

## Managed Workspace Pattern

- Treat `projects_local/authoring/pse2025/.../bundle_draft.yaml` as the maintained source of truth for research variants.
- Use the shared `pse2025.jg_constants` card for family-wide `CREATE` defaults that should apply across maintained PSE2025 variants.
- Use variant-local `pse2025.jg_constants` instances for per-variant `CREATE` overrides such as alternate `JGW` / `JGCOLA` combinations.
- For bundle workspaces, inspect shared cards with `fp workspace cards <workspace_id>` and variant-local cards with `fp workspace cards <workspace_id> --variant-id <variant_id>`.
- When a variant needs an exact dashboard/run label, set its `scenario_name` in the managed bundle workspace instead of renaming artifact folders by hand.

## Include Chain Validation

Observed runtime include chain:

- `psebase.txt` / `pselow.txt` / `psehigh.txt`
  - `INPUT FILE=pse_common.txt;`
  - `SMPL 2025.4 2029.4;`
  - `INPUT FILE=intgadj.txt;`
- `pse_common.txt`
  - `INPUT FILE=ptcoef.txt;`
  - `INPUT FILE=fmexog.txt;`

Observed outputs from wrappers:

- `psebase.txt` -> `PRINTVAR FILEOUT=OUT_PSEBASE.DAT LOADFORMAT;`
- `pselow.txt` -> `PRINTVAR FILEOUT=OUT_PSELOW.DAT LOADFORMAT;`
- `psehigh.txt` -> `PRINTVAR FILEOUT=OUT_PSEHIGH.DAT LOADFORMAT;`

## Coefficient File Semantics

- `ptcoef.txt` is the active runtime include for JG take-up coefficients.
- `pse_takeup_coeffs.txt` is a readable long-name source copy; it is not the loaded runtime file.
- Reason: Fair-Parke filename/identifier constraints. `ptcoef.txt` is kept short for reliable `INPUT FILE=...;` resolution.

## Safe Override Patterns

1. Overlay deck edits (default/safest):
   - Write updated values to `input_overlay_dir/ptcoef.txt`.
   - Keeps base model files unchanged and is scenario-scoped.
2. Wrapper `CHANGEVAR` overrides (quick experiments):
   - Add coefficient-level overrides in scenario wrappers after `INPUT FILE=pse_common.txt;`.
   - Best for temporary runs; less transparent than explicit deck copies for long-lived scenarios.
3. Include-swap strategy (controlled deck variants):
   - Copy `pse_common.txt` into overlay and point `INPUT FILE=` at a variant coefficient file.
   - Keep filenames within Fair-Parke limits.
4. `INTGADJ` series overrides:
   - Update `input_overlay_dir/intgadj.txt` as a forecast-window `CHANGEVAR` include.
   - Card output remains attachable through the normal staged overlay tree.
