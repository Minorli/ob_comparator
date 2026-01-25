## ADDED Requirements

### Requirement: Constraint-backed expression index filtering
The system SHALL treat target-side PK/UK constraints as covering indexes using the referenced
index definition (including expressions) when `INDEX_NAME` metadata is available.

#### Scenario: Expression UNIQUE index covered by constraint index
- **WHEN** the source has a UNIQUE index on an expression
- **AND** the target has a UNIQUE constraint with SYS_NC columns that references that index by name
- **THEN** the index comparison does not report the source index as missing
