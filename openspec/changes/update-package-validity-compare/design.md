## Context
PACKAGE and PACKAGE BODY were previously print-only. Users now require explicit comparison of existence and compile status, plus error visibility for invalid objects.

## Goals / Non-Goals
- Goals: Compare package/package body existence and VALID/INVALID status; separate source-invalid packages from mismatch counts; expose error summary for invalid objects; show package differences in the main report.
- Non-Goals: Auto-fix or compile packages; generate fixup SQL for package bodies; choose between dbcat vs metadata for DDL generation (future work).

## Decisions
- Decision: Use DBA_OBJECTS.STATUS as the primary validity signal on both source and target.
- Decision: Use DBA_ERRORS to capture compile error details when STATUS is INVALID and DBA_ERRORS is available.
- Decision: Classify source-invalid packages as SOURCE_INVALID and exclude them from mismatch counts.
- Decision: Emit a dedicated report file for package comparison results and a summarized package section in the main report.

## Output Format
The package comparison report will include columns:
- SRC_OWNER, OBJECT_NAME, OBJECT_TYPE, SRC_STATUS
- TGT_OWNER, TGT_STATUS
- RESULT (OK/MISSING_TARGET/MISSING_SOURCE/TARGET_INVALID/STATUS_MISMATCH/SOURCE_INVALID)
- ERROR_COUNT, FIRST_ERROR (when available)

The main report will include a package section with:
- Counts for SOURCE_INVALID, MISSING_TARGET, TARGET_INVALID, STATUS_MISMATCH
- A list of source-invalid objects and target-side issues (missing or invalid) only; full details stay in the package comparison report

## Risks / Trade-offs
- DBA_ERRORS may be unavailable or restricted; in that case, error detail is omitted with a reason recorded.
- OceanBase may not expose full error detail for invalid objects; treat as best-effort.

## Migration Plan
- Add metadata extraction and reporting paths without changing existing comparison outputs.
- Keep MATERIALIZED VIEW as print-only; only PACKAGE/PACKAGE BODY behavior changes.

## Open Questions
- None
