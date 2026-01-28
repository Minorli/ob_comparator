## ADDED Requirements

### Requirement: LONG blacklist dependency handling
The system SHALL treat LONG/LONG RAW blacklist tables as non-blocking for dependency analysis regardless of target table existence.

#### Scenario: LONG table exists in target
- **WHEN** a table is blacklisted with LONG/LONG RAW and the target table exists
- **THEN** the table is NOT used as a dependency block source

#### Scenario: LONG table missing in target
- **WHEN** a table is blacklisted with LONG/LONG RAW and the target table is missing
- **THEN** the table is NOT used as a dependency block source
