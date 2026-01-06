# Change: Update trigger/view remap behavior and trigger grants

## Why
Current remap inference moves triggers and views to follow table-derived schema mapping, which conflicts with the new requirement to preserve original schemas by default and can cause missing grants for trigger creation. The check scope flags (`check_primary_types`/`check_extra_types`) also do not fully gate inference and validation, leading to unexpected schema inference for disabled types.

## What Changes
- Keep TRIGGER objects in their source schema unless an explicit remap rule exists.
- Keep VIEW/MATERIALIZED VIEW objects in their source schema unless explicitly remapped.
- Generate required cross-schema GRANT statements together with TRIGGER fixup scripts (prefer same file).
- Ensure trigger DDL rewrites ON clause/table references to remapped tables while preserving trigger schema.
- Ensure `check_primary_types`/`check_extra_types` scope all inference, dependency checks, and validations.
- Treat MATERIALIZED VIEW as print-only (no OB validation/fixup) and PACKAGE/PACKAGE BODY as print-only by default.

## Impact
- Affected specs: remap-fixup
- Affected code: `schema_diff_reconciler.py`, `test_schema_diff_reconciler.py`
