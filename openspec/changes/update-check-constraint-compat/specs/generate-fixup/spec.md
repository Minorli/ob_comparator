## MODIFIED Requirements

### Requirement: Missing object DDL generation
The system SHALL generate CREATE/ALTER DDL for missing objects by type and store them in fixup_scripts subdirectories. CHECK constraints marked as UNSUPPORTED SHALL be excluded from fixup generation.

#### Scenario: Unsupported CHECK constraint
- **WHEN** a missing CHECK constraint is classified as UNSUPPORTED
- **THEN** no fixup DDL is generated for that constraint
