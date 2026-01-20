## MODIFIED Requirements

### Requirement: Table ALTER scripts for column differences
The system SHALL generate ALTER TABLE statements for missing columns and length mismatches, and comment DROP suggestions for extra columns.

#### Scenario: Missing column
- **WHEN** a source column is missing in the target
- **THEN** an ALTER TABLE ADD statement is generated

#### Scenario: Virtual column missing
- **WHEN** a source virtual column is missing in the target
- **THEN** an ALTER TABLE ADD statement is generated with GENERATED ALWAYS AS (expression)

#### Scenario: VARCHAR length too short
- **WHEN** a VARCHAR/VARCHAR2 BYTE column is shorter than the minimum required length
- **THEN** an ALTER TABLE MODIFY statement inflates the target length

#### Scenario: NUMBER precision too small
- **WHEN** a NUMBER/DECIMAL/NUMERIC column in the target has smaller precision or incompatible scale
- **THEN** an ALTER TABLE MODIFY statement widens precision/scale to match the source

#### Scenario: Extra column
- **WHEN** a column exists only in the target
- **THEN** a commented DROP COLUMN suggestion is emitted

#### Scenario: VARCHAR length oversize
- **WHEN** a VARCHAR/VARCHAR2 BYTE column exceeds the oversize threshold
- **THEN** the fixup emits a warning comment rather than a MODIFY statement

## ADDED Requirements

### Requirement: CHECK and FK rule fixup
The system SHALL generate missing CHECK constraint DDL using the source SEARCH_CONDITION and include FK DELETE_RULE clauses when present.

#### Scenario: Missing CHECK constraint
- **WHEN** a user-defined CHECK constraint is missing in the target
- **THEN** an ALTER TABLE ADD CONSTRAINT ... CHECK (...) statement is emitted

#### Scenario: NOT NULL checks excluded
- **WHEN** a CHECK constraint is system-generated for NOT NULL semantics
- **THEN** it is excluded from CHECK constraint fixup generation

#### Scenario: FK delete rule preserved
- **WHEN** a missing FK has a DELETE_RULE in the source
- **THEN** the generated FK DDL includes the ON DELETE clause

### Requirement: Function-based index fallback
The system SHALL build fallback CREATE INDEX statements using expression metadata when dbcat extraction is unavailable.

#### Scenario: Function-based index rebuild
- **WHEN** a function-based index is missing and dbcat extraction fails
- **THEN** the system uses DBA_IND_EXPRESSIONS metadata to build a CREATE INDEX statement
