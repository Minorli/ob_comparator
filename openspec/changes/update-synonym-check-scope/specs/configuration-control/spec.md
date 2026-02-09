## ADDED Requirements
### Requirement: Synonym check scope configuration
The system SHALL support `synonym_check_scope` to control SYNONYM comparison scope.

#### Scenario: Default scope
- **WHEN** `synonym_check_scope` is missing or empty
- **THEN** the system defaults to `public_only`

#### Scenario: Invalid scope value
- **WHEN** `synonym_check_scope` is set to an unsupported value
- **THEN** the system logs a warning and falls back to `public_only`
