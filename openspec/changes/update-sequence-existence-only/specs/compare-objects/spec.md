## MODIFIED Requirements

### Requirement: Sequence comparison
The system SHALL validate sequence existence in the target schema when sequence checks are enabled.

#### Scenario: Sequence exists
- **WHEN** a SEQUENCE exists in the source and target
- **THEN** the system records the sequence as OK and does not compare attributes

#### Scenario: Sequence missing
- **WHEN** a SEQUENCE exists in the source but not in the target
- **THEN** the system records the sequence as missing

#### Scenario: Sequence extra
- **WHEN** a SEQUENCE exists in the target but not in the source
- **THEN** the system records the sequence as extra
