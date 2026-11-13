# Pack Authoring

Packs are agent-facing manifests for scenario families.

## Purpose

A pack tells agents and tools how to treat a local or public family such as `pse2025`:

- which catalog entries belong to the family
- which cards should be exposed
- which named recipes are supported
- which visualization presets should be offered

This keeps project-specific knowledge out of the core product surface.

## Location

- Public/core packs may live under `src/fp_wraptr/packs/`
- Local or personal packs live under `projects_local/packs/<pack_id>/pack.yaml`

## Minimal manifest

```yaml
pack_id: pse2025
label: PSE2025 Working Pack
family: pse2025
visibility: local
cards_family: pse2025
catalog_entry_ids: [pse2025-base, pse2025-bundle]
recipes:
  - recipe_id: change-coefficients
    label: Change coefficients safely
visualizations:
  - view_id: pse-main
    label: Main tracks
    chart_type: forecast_overlay
    variables: [GDPR, PCPF, UR]
```

## Local pack guidance

Use local packs for personal scenario families, overlays, and experiments. The core package ships the generic workflow; family-specific recipes and visualization presets live in the local pack manifest.
