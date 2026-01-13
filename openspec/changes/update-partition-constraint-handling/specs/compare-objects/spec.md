## ADDED Requirements
### Requirement: Partition key metadata
The system SHALL load partition key columns for partitioned tables from Oracle and OceanBase metadata to support constraint comparison rules.

#### Scenario: Partition key metadata loaded
- **WHEN** a table is partitioned in the source
- **THEN** its partition key column list is available for constraint comparison

## MODIFIED Requirements
### Requirement: Constraint comparison
The system SHALL compare PK/UK/FK constraints between source and target tables, with partition-aware rules for primary keys.

#### Scenario: Constraint comparison
- **WHEN** a TABLE has PK/UK/FK constraints in the source
- **THEN** the system compares constraint column sets and referenced table information

#### Scenario: Partitioned table with inclusive PK
- **WHEN** a partitioned table has a PRIMARY KEY that includes all partition key columns
- **THEN** a missing target PRIMARY KEY is reported as missing

#### Scenario: Partitioned table with non-inclusive PK
- **WHEN** a partitioned table has a PRIMARY KEY that does not include all partition key columns
- **AND** the target has a UNIQUE constraint on the same columns
- **THEN** the system treats the constraint as matching and does not report a missing PRIMARY KEY

#### Scenario: Partitioned table with non-inclusive PK and missing UNIQUE
- **WHEN** a partitioned table has a PRIMARY KEY that does not include all partition key columns
- **AND** the target lacks both a matching PRIMARY KEY and UNIQUE constraint
- **THEN** the system reports a missing UNIQUE constraint (downgraded from the PRIMARY KEY)
