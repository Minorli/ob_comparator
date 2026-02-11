## ADDED Requirements

### Requirement: Missing-constraint validate mode switch
The system SHALL provide `constraint_missing_fixup_validate_mode` to control VALIDATE behavior when generating missing constraint fixup DDL.

#### Scenario: safe_novalidate mode
- **WHEN** `constraint_missing_fixup_validate_mode=safe_novalidate`
- **THEN** missing constraint fixup DDL uses `ENABLE NOVALIDATE`

#### Scenario: source mode
- **WHEN** `constraint_missing_fixup_validate_mode=source`
- **THEN** missing constraint fixup follows source `VALIDATED` state, and falls back to `ENABLE NOVALIDATE` if source state is unavailable

#### Scenario: force_validate mode
- **WHEN** `constraint_missing_fixup_validate_mode=force_validate`
- **THEN** missing constraint fixup DDL uses `ENABLE VALIDATE`
