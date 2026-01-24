## ADDED Requirements

### Requirement: CHECK expression parentheses normalization
The system SHALL treat CHECK constraint expressions as matching when they differ only by redundant parentheses and casing, while preserving string literal contents.

#### Scenario: Parentheses-normalized CHECK
- **WHEN** the source CHECK expression is `A > 0 AND B IN (1,2,3)`
- **AND** the target CHECK expression is `(("A" > 0) and ("B" in (1,2,3)))`
- **THEN** the CHECK constraint is treated as matching

### Requirement: Derived UNIQUE constraint filtering
The system SHALL ignore target-side UNIQUE constraints that are derived from UNIQUE indexes when the source has an equivalent UNIQUE index on the same columns or expression.

#### Scenario: UNIQUE index appears as UNIQUE constraint in target
- **WHEN** the source has a UNIQUE index on columns (A,B)
- **AND** the target exposes a UNIQUE constraint on (A,B) with the same index
- **THEN** the constraint comparison does not report an extra constraint

#### Scenario: Expression UNIQUE index appears as UNIQUE constraint
- **WHEN** the source has a UNIQUE index on an expression
- **AND** the target exposes a UNIQUE constraint with empty or SYS_NC columns but a matching index expression
- **THEN** the constraint comparison does not report an extra constraint

### Requirement: Deferrable constraints unsupported
The system SHALL classify DEFERRABLE/DEFERRED PK/UK/FK/CHECK constraints as unsupported for OceanBase and exclude them from missing comparisons.

#### Scenario: DEFERRABLE PK
- **WHEN** the source has a DEFERRABLE primary key
- **THEN** the PK is recorded as unsupported and not reported as missing

### Requirement: Descending index unsupported
The system SHALL treat indexes containing DESC columns as unsupported in OceanBase and exclude them from missing index comparisons.

#### Scenario: DESC index in source
- **WHEN** the source index uses DESC columns
- **THEN** the index is recorded as unsupported and not reported as missing
