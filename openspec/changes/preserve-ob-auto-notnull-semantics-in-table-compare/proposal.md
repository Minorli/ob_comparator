# Proposal: Preserve OB Auto NOT NULL Semantics During Table Compare

## Why

When Oracle source columns are represented as `NOT NULL ENABLE NOVALIDATE`, the table-level compare relies on target-side enabled single-column `IS NOT NULL` checks to suppress redundant `table_alter` DDL.

Current OceanBase metadata loading prunes `*_OBNOTNULL_*` / `*_OBCHECK_*` constraints too early in `dump_ob_metadata()`. As a result, `check_primary_objects()` cannot see target-side equivalent semantics and may still generate:

- `ADD CONSTRAINT ... CHECK (<col> IS NOT NULL) ENABLE NOVALIDATE`

even when OceanBase already has an enabled equivalent auto-generated check.

## What Changes

- Keep OceanBase auto-generated NOT NULL style constraints available for table semantic compare.
- Continue suppressing those constraints from ordinary constraint diff noise.
- Add regression coverage for the full `dump_ob_metadata() -> check_primary_objects()` path.

## Impact

- Fixes redundant `nullability_novalidate_tighten` DDL generation when target already has equivalent enabled OB auto check semantics.
- Does not change ordinary constraint diff reporting rules beyond preserving semantic suppress accuracy.
