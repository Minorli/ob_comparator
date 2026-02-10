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
- **WHEN** a source and target index have the same name but SYS_NC column names differ
- **THEN** the index is treated as matching and excluded from missing/extra results

#### Scenario: GTT index session discriminator normalization
- **WHEN** an OceanBase GTT index column list starts with `SYS_SESSION_ID`
- **THEN** the comparison ignores the leading `SYS_SESSION_ID` and matches by remaining business/index-expression columns

#### Scenario: GTT internal index filtering
- **WHEN** OceanBase reports internal helper index names matching `IDX_FOR_HEAP_GTT_*`
- **THEN** those indexes are excluded from missing/extra mismatch results

#### Scenario: Unique index backed by constraint
- **WHEN** a source index is NONUNIQUE and the target is UNIQUE for the same column set
- **AND** the target has a PK/UK constraint on that column set
- **THEN** the uniqueness difference is treated as acceptable

#### Scenario: Constraint comparison
- **WHEN** a TABLE has PK/UK/FK constraints in the source
- **THEN** the system compares constraint column sets and referenced table information

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
