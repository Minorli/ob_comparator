## ADDED Requirements
### Requirement: Partitioned PK downgrade fixup
The system SHALL generate UNIQUE constraint DDL for partitioned tables where the source PRIMARY KEY does not include all partition key columns.

#### Scenario: Downgrade missing PK to UNIQUE
- **WHEN** a partitioned table has a non-inclusive PRIMARY KEY in the source
- **AND** the target lacks a matching UNIQUE constraint
- **THEN** the fixup output generates a UNIQUE constraint instead of a PRIMARY KEY

#### Scenario: UNIQUE already present
- **WHEN** a partitioned table has a non-inclusive PRIMARY KEY in the source
- **AND** the target already has a matching UNIQUE constraint
- **THEN** no PRIMARY KEY fixup is generated
