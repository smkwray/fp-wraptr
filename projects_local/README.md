# `projects_local/` (local-first, with curated tracked exceptions)

Most of this directory is for **repo-local, operator-specific overlays** that should not be committed.

Tracked exceptions exist for curated scenario families that need raw overlay inputs in-repo:

- `projects_local/pse2008/`
- `projects_local/pse2025/`
- `projects_local/scenario_catalog.yaml`

## PSE2025 overlay

Create: `projects_local/pse2025/` and copy the PSE2025 input scripts into it (keep filenames identical):

- `psebase.txt`
- `pselow.txt`
- `psehigh.txt`
- `pse_common.txt`
- `ptcoef.txt`
- `intgadj.txt`
- `pse_takeup_coeffs.txt` (reference-only; FP has filename limits for INPUT scripts)

The tracked bundle config `bundles/pse2025.yaml` expects these files to exist under `projects_local/pse2025/`.

## PSE2008 overlay

`projects_local/pse2008/` is the tracked historical-counterfactual PSE2008 family overlay. It launches the JG layer in `2008.4`, runs the short horizon through `2012.4`, and carries its own `pse_common.txt`, `ptcoef.txt`, and `intgadj.txt` content.
