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

## ADDED Requirements

### Requirement: LONG blacklist dependency handling
The system SHALL treat LONG/LONG RAW blacklist tables as non-blocking for dependency analysis.

#### Scenario: LONG table exists in target
- **WHEN** a table is blacklisted with LONG/LONG RAW and the target table exists
- **THEN** the table is NOT used as a dependency block source

#### Scenario: LONG table missing in target
- **WHEN** a table is blacklisted with LONG/LONG RAW and the target table is missing
- **THEN** dependent objects are NOT marked blocked solely because of this LONG table
