## Context
Oracle NUMBER(*,0) represents a zero-scale numeric with unspecified precision. Oracle's maximum precision is 38, and migration tools typically normalize NUMBER(*,0) to NUMBER(38,0) in OceanBase. The current comparison logic treats NULL precision on the source and 38 on the target as a mismatch, leading to false positives and unnecessary fixup DDL.

## Goals / Non-Goals
- Goals:
  - Treat NUMBER(*,0) (precision NULL, scale=0) and NUMBER(38,0) as equivalent.
  - Avoid generating fixup DDL for equivalent columns.
- Non-Goals:
  - Change comparison semantics for other NUMBER precision/scale combinations.
  - Alter how NUMBER without scale (plain NUMBER) is handled.

## Decision
Introduce a targeted compatibility rule:
- If source precision is NULL and source scale is 0 (or NULL treated as 0), and target precision is 38 with scale 0, the columns are treated as matching.
- All other precision/scale comparisons remain unchanged.

## Risks / Trade-offs
- Risk: Some environments may prefer treating NUMBER(*,0) as unbounded. Mitigation: keep rule narrowly scoped to 38,0 equivalence only.

## Test Plan
- Case 1: src=(NULL,0), tgt=(38,0) -> match.
- Case 2: src=(NULL,0), tgt=(37,0) -> mismatch.
- Case 3: src=(NULL,2), tgt=(38,0) -> mismatch.
