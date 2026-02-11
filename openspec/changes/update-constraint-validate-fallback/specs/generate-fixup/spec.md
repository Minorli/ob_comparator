## MODIFIED Requirements

### Requirement: Constraint fixup generation
The system SHALL apply `constraint_missing_fixup_validate_mode` when generating missing constraint DDL.

#### Scenario: Missing FK with safe mode
- **WHEN** a missing FK is generated and mode is `safe_novalidate`
- **THEN** generated DDL appends `ENABLE NOVALIDATE`

#### Scenario: Missing CHECK with source mode and VALIDATED source
- **WHEN** a missing CHECK is generated and mode is `source` and source state is `VALIDATED`
- **THEN** generated DDL appends `ENABLE VALIDATE`

## ADDED Requirements

### Requirement: Deferred validate script generation
The system SHALL generate deferred validation scripts for constraints created with NOVALIDATE.

#### Scenario: NOVALIDATE constraints exist
- **WHEN** one or more missing constraints are emitted as NOVALIDATE
- **THEN** the system writes `ENABLE VALIDATE CONSTRAINT` SQL under `fixup_scripts/constraint_validate_later`
