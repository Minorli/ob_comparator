## ADDED Requirements

### Requirement: Noise-suppressed mismatches excluded from fixups
The system SHALL exclude noise-suppressed mismatches from fixup script generation while preserving
fixups for high-signal mismatches.

#### Scenario: Suppressed-only mismatch
- **WHEN** a mismatch is classified as noise-suppressed
- **THEN** no fixup DDL is generated for that mismatch

#### Scenario: Mixed-signal table
- **WHEN** a table has both noise-suppressed and high-signal differences
- **THEN** fixup scripts include only the high-signal differences for that table
