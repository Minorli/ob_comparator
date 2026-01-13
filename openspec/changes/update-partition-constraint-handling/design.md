## Context
OceanBase requires partition keys to be part of PRIMARY KEY definitions for partitioned tables. Oracle allows PKs that do not include the partition key. OMS aligns with OceanBase by downgrading those PKs to UNIQUE. The comparator should match this behavior in both comparison and fixup generation.

## Goals / Non-Goals
- Goals:
  - Detect partitioned tables and their partition key columns.
  - Treat non-inclusive PKs as UNIQUE for comparison and fixup.
  - Avoid reporting missing PKs in non-inclusive cases when a UNIQUE constraint exists.
- Non-Goals:
  - Rewriting partition definitions or table DDL.
  - Changing partitioning on target tables.

## Decisions
- Use Oracle and OceanBase dictionary views to load partition key columns:
  - Oracle: DBA_PART_KEY_COLUMNS (OWNER, NAME, COLUMN_NAME, COLUMN_POSITION).
  - OceanBase: DBA_PART_KEY_COLUMNS (same fields when available).
- Classification logic is based on source partition key columns and source PK columns.
- For partitioned tables where PK does not include all partition key columns:
  - Comparison treats a target UNIQUE constraint on the same columns as correct.
  - Missing PK is not reported; missing UNIQUE is reported instead.
  - Fixup emits UNIQUE DDL in place of PRIMARY KEY.

## Risks / Trade-offs
- Partition metadata may be unavailable due to permissions; fallback should avoid false positives.
- Target tables without partitioning (or with different partition keys) may complicate interpretation; the rule is based on source definitions only.

## Migration Plan
- Add metadata fields to OracleMetadata and ObMetadata for partition keys.
- Update constraint comparison and fixup generation code paths.
- Validate with representative partitioned tables in Oracle and OceanBase.

## Open Questions
- Should a target PRIMARY KEY on non-inclusive columns be treated as satisfying the UNIQUE expectation, or reported as extra? (default to treat as satisfying UNIQUE)
