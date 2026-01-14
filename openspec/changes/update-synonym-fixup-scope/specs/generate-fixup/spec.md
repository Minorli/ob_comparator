## ADDED Requirements
### Requirement: Synonym fixup scope filter
The system SHALL filter SYNONYM fixup output based on synonym_fixup_scope.

#### Scenario: Public-only synonyms
- **WHEN** synonym_fixup_scope is set to public_only
- **THEN** only PUBLIC synonyms are emitted under fixup_scripts/synonym

#### Scenario: All in-scope synonyms
- **WHEN** synonym_fixup_scope is set to all or left empty
- **THEN** SYNONYM fixup generation includes PUBLIC and non-PUBLIC synonyms within the configured source schema scope
