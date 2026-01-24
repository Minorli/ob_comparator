## ADDED Requirements

### Requirement: NUMBER(*,0) precision compatibility
The system SHALL treat Oracle NUMBER(*,0) (precision NULL, scale 0) as compatible with target NUMBER(38,0).

#### Scenario: NUMBER(*,0) vs NUMBER(38,0)
- **WHEN** the source column precision is NULL and scale is 0
- **AND** the target column precision is 38 and scale is 0
- **THEN** the column is treated as matching and no fixup is generated
