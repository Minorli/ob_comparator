## ADDED Requirements

### Requirement: Strip USING INDEX clause
The system SHALL remove `USING INDEX <index_name>` from constraint DDL during cleanup to ensure OceanBase compatibility.

#### Scenario: Constraint DDL includes USING INDEX
- **WHEN** generated constraint DDL contains `USING INDEX IDX_NAME`
- **THEN** the emitted fixup DDL omits the USING INDEX clause
