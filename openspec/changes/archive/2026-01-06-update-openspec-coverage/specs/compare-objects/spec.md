# compare-objects

## ADDED Requirements

### Requirement: PUBLIC synonym inclusion
The system SHALL include PUBLIC synonyms that reference configured source schemas even when PUBLIC is not listed in source_schemas.

#### Scenario: PUBLIC synonym points to configured schema
- **WHEN** a PUBLIC synonym references a TABLE_OWNER in source_schemas
- **THEN** the synonym is included in the comparison scope

#### Scenario: PUBLIC synonym points to other schema
- **WHEN** a PUBLIC synonym references a TABLE_OWNER not in source_schemas
- **THEN** the synonym is excluded as a system synonym
