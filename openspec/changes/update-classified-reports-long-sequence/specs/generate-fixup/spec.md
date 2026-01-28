## ADDED Requirements

### Requirement: LONG blacklist tables generate fixup with conversion
The system SHALL generate fixup DDL for missing tables that are blacklisted due to LONG/LONG RAW by converting LONG/LONG RAW columns to CLOB/BLOB, while allowing dependent objects to generate fixups.

#### Scenario: Missing LONG table
- **WHEN** a TABLE is missing and is blacklisted with LONG/LONG RAW
- **THEN** the system emits CREATE TABLE fixup and converts LONG/LONG RAW columns to CLOB/BLOB

#### Scenario: Dependent object on LONG table
- **WHEN** a VIEW/TRIGGER/SYNONYM depends on a LONG/LONG RAW table
- **THEN** the dependent object is eligible for fixup generation (not blocked by the LONG table)
