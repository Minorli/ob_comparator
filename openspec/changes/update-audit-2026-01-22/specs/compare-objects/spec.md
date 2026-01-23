## ADDED Requirements

### Requirement: Target deferrable metadata handling
The system SHALL read DEFERRABLE/DEFERRED from OceanBase when the columns exist and treat missing target metadata as UNKNOWN rather than a mismatch by itself.

#### Scenario: Target exposes deferrable metadata
- **WHEN** DBA_CONSTRAINTS exposes DEFERRABLE and DEFERRED columns in OceanBase
- **THEN** the system includes those flags in constraint comparison

#### Scenario: Target lacks deferrable metadata
- **WHEN** OceanBase does not expose DEFERRABLE/DEFERRED metadata
- **THEN** the system treats target deferrable/deferred as UNKNOWN and records a metadata-gap note

### Requirement: Dependency grant status beyond VIEW
The system SHALL evaluate cross-schema dependency grant status for non-VIEW objects using the same privilege inference logic used for VIEW chains.

#### Scenario: Procedure depends on table across schema
- **WHEN** a PROCEDURE references a TABLE in another schema without the required privilege
- **THEN** the dependency report marks the reference as GRANT_MISSING

#### Scenario: Unmapped dependency types
- **WHEN** a dependency reference type has no privilege mapping
- **THEN** the dependency report marks the grant status as GRANT_UNKNOWN

## MODIFIED Requirements

### Requirement: Extra object checks
The system SHALL validate INDEX, CONSTRAINT, SEQUENCE, and TRIGGER objects against source metadata when enabled.

#### Scenario: Index comparison
- **WHEN** a TABLE has indexes in the source
- **THEN** the system compares index column sequences and uniqueness with target indexes

#### Scenario: Source index metadata missing
- **WHEN** index metadata for a source TABLE is unavailable
- **THEN** the system treats the source index set as empty and reports only target extras

#### Scenario: OMS-generated index filtering
- **WHEN** an OceanBase index matches OMS-generated patterns
- **THEN** the index is excluded from comparison

#### Scenario: SYS_NC index column normalization
- **WHEN** a source and target index align on column sets but SYS_NC column names differ
- **THEN** the index is treated as matching and excluded from missing/extra results regardless of index name

#### Scenario: Unique index backed by constraint
- **WHEN** a source index is NONUNIQUE and the target is UNIQUE for the same column set
- **AND** the target has a PK/UK constraint on that column set
- **THEN** the uniqueness difference is treated as acceptable

#### Scenario: Constraint comparison
- **WHEN** a TABLE has PK/UK/FK/CHECK constraints in the source
- **THEN** the system compares constraint column sets, referenced table information, and CHECK expressions

#### Scenario: Check constraint expression match
- **WHEN** a source CHECK constraint expression exists in the target under a different name
- **THEN** the constraint is treated as matched

#### Scenario: Check constraint same-name mismatch
- **WHEN** a target CHECK constraint shares the same name but has a different expression
- **THEN** the system reports a mismatch for the same-name constraint

#### Scenario: Constraint reference info unavailable
- **WHEN** target constraint reference fields cannot be loaded
- **THEN** the system falls back to basic constraint metadata for comparison

#### Scenario: OceanBase auto NOT NULL constraints
- **WHEN** an OceanBase constraint name matches the OBNOTNULL pattern
- **THEN** the constraint is excluded from comparison

#### Scenario: Sequence comparison
- **WHEN** a schema contains SEQUENCE objects in the source
- **THEN** the system checks whether corresponding sequences exist in the target schema

#### Scenario: Trigger comparison
- **WHEN** a TABLE has TRIGGER objects in the source
- **THEN** the system checks whether corresponding triggers exist in the target schema
