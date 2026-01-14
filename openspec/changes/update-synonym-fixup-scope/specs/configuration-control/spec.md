## ADDED Requirements
### Requirement: Synonym fixup scope configuration
The system SHALL support synonym_fixup_scope to control SYNONYM fixup generation scope.

#### Scenario: Default scope
- **WHEN** synonym_fixup_scope is missing or empty
- **THEN** the system defaults to all

#### Scenario: Invalid scope value
- **WHEN** synonym_fixup_scope is set to an unsupported value
- **THEN** the system logs a warning and falls back to all
