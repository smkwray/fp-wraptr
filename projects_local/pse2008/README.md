# PSE2008 Overlay Family

This folder holds the raw PSE2008 overlay inputs imported from the external FAIR worktree.

Phase 1 intent:

- keep the source overlay files here as the runnable family input
- reuse baseline FM files from `FM/` (`fmdata.txt`, `fmage.txt`, `fmexog.txt`)
- keep authoring workspace output separate under `projects_local/authoring/` if Phase 2 adds cards later

## Include Chain

- `psebase.txt` / `pselow.txt` / `psehigh.txt`
  - `INPUT FILE=pse_common.txt;`
  - `INPUT FILE=intgadj.txt;`
- `pse_common.txt`
  - `LOADDATA FILE=fmdata.txt;`
  - `LOADDATA FILE=fmage.txt;`
  - `INPUT FILE=ptcoef.txt;`

Wrapper outputs:

- `psebase.txt` -> `OUT_PSEBASE.DAT`
- `pselow.txt` -> `OUT_PSELOW.DAT`
- `psehigh.txt` -> `OUT_PSEHIGH.DAT`

## Historical-Counterfactual Semantics

- Launch window starts at `2008.4`, not `2025.4`.
- `intgadj.txt` carries historical `INTGADJ` values from `2008.4` through `2012.4`.
- `pse_common.txt` keeps the PSE2008-specific JG defaults and state calibration for the 2008Q3 pool snapshot.
- No long-horizon `fmexog.txt` forecast extension is used in this short-horizon version.

## Coefficient Files

- `ptcoef.txt` is the active runtime include used by FP.
- `pse_takeup_coeffs.txt` is a readable reference copy only; it is not loaded by the wrappers.
- Reason: the short `ptcoef.txt` filename is the stable include target for FP runtime resolution.
