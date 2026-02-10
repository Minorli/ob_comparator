# Change: Extra object detection hardening and cleanup candidate framework

## Why
Current extra-object reporting has three practical gaps for migration operators:
1) print-only `MATERIALIZED VIEW` objects are skipped from target-extra detection,
2) early count-stage warnings for INDEX/CONSTRAINT are based on rough metadata totals and can be noisy,
3) there is no built-in framework to emit cleanup candidates for target-side extra objects.

These gaps reduce report clarity during repeated Oracle->OceanBase cutover checks.

## What Changes
- Include print-only `MATERIALIZED VIEW` in `extra_targets` detection while still keeping MVIEW as print-only for fixup generation.
- Align INDEX/CONSTRAINT count signaling with semantic comparison results (avoid rough pre-warning noise in early count stage).
- Add opt-in cleanup-candidate framework:
  - new switch: `generate_extra_cleanup` (default `false`),
  - when enabled, export commented cleanup candidates under `fixup_scripts/cleanup_candidates/`.

## Impact
- Affected specs: `compare-objects`, `generate-fixup`, `configuration-control`, `export-reports`
- Affected code: `schema_diff_reconciler.py`, `config.ini.template`, `readme_config.txt`, tests
- Behavior: default runtime behavior remains unchanged except improved extra detection/report accuracy for MVIEW and reduced noisy early warnings.
