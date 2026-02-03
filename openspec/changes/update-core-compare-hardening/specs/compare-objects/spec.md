## MODIFIED Requirements

### Requirement: Constraint comparison
The system SHALL compare constraint column sets, referenced table information, and referential action rules (delete_rule/update_rule) when available.

#### Scenario: Constraint comparison
- **WHEN** a TABLE has PK/UK/FK constraints in the source
- **THEN** the system compares constraint column sets and referenced table information

#### Scenario: FK update_rule comparison
- **WHEN** a foreign key has an UPDATE_RULE in the source and target
- **THEN** the system reports a mismatch if the UPDATE_RULE values differ

#### Scenario: Constraint reference info unavailable
- **WHEN** target constraint reference fields cannot be loaded
- **THEN** the system falls back to basic constraint metadata for comparison

#### Scenario: OceanBase auto NOT NULL constraints
- **WHEN** an OceanBase constraint name matches the OBNOTNULL pattern
- **THEN** the constraint is excluded from comparison

### Requirement: PUBLIC synonym inclusion
The system SHALL include PUBLIC synonyms that reference configured source schemas even when PUBLIC is not listed in source_schemas, and treat OceanBase __public owner as PUBLIC for comparison.

#### Scenario: PUBLIC synonym points to configured schema
- **WHEN** a PUBLIC synonym references a TABLE_OWNER in source_schemas
- **THEN** the synonym is included in the comparison scope

#### Scenario: PUBLIC synonym points to other schema
- **WHEN** a PUBLIC synonym references a TABLE_OWNER not in source_schemas
- **THEN** the synonym is excluded as a system synonym

#### Scenario: OceanBase __public owner
- **WHEN** OceanBase reports a synonym owner as __public
- **THEN** the system treats it as PUBLIC for comparison and reporting
