## ADDED Requirements

### Requirement: Column order mismatches are report-only
The system SHALL not generate fixup DDL for column order mismatches.

#### Scenario: Column order mismatch only
- **WHEN** a table differs only by column order
- **THEN** no fixup scripts are produced for column order changes

#### Scenario: Mixed-signal table
- **WHEN** a table has column order mismatches and other column differences
- **THEN** fixup scripts include only missing/length/type changes, not reordering
