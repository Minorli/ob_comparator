## Context
Column order mismatches are requested on demand, but metadata ordering and system-generated columns
can create noisy reports. The check must be optional and safe by default.

## Goals / Non-Goals
- Goals:
  - Add an optional column order check for TABLE objects.
  - Filter noise columns (OMS helpers, auto-generated, SYS_NC, hidden/invisible).
  - Report order mismatches without generating fixup DDL.
- Non-Goals:
  - Automatic column reordering DDL.
  - Changing existing column set/type/length comparison rules.

## Decisions
- Fetch column order metadata only when `check_column_order` is enabled; skip comparison when
  order metadata is unavailable, NULL, or ambiguous (e.g., duplicate order positions).
- Derive order from the filtered column sequence; use `COLUMN_ID` only as a sorting key and never
  as a direct equality check. Extra/non-compared columns are removed before ordering.
- Compare order only when the filtered column sets match to avoid duplicate noise with missing/extra
  column mismatches.
- Report mismatches via a dedicated detail export and a summary count; keep fixups unchanged.

## Risks / Trade-offs
- Some target versions may lack `COLUMN_ID` in `DBA_TAB_COLUMNS` / `DBA_TAB_COLS`; the check will be
  skipped with a recorded reason to avoid false positives.

## Migration Plan
None. Default behavior remains unchanged with `check_column_order=false`.

## Open Questions
- Confirm `COLUMN_ID` availability across supported OceanBase versions in Oracle mode.
