## ADDED Requirements

### Requirement: DOUBLE PRECISION cleanup
The system SHALL replace `DOUBLE PRECISION` with `BINARY_DOUBLE` in generated DDL to ensure OceanBase compatibility.

#### Scenario: TABLE DDL contains DOUBLE PRECISION
- **WHEN** generated DDL includes a column defined as `DOUBLE PRECISION`
- **THEN** the DDL emitted for fixup uses `BINARY_DOUBLE` instead
