## ADDED Requirements
### Requirement: Synonym comparison scope filter
The system SHALL keep source and target SYNONYM comparison scope aligned with `synonym_check_scope`.

#### Scenario: Public-only comparison
- **WHEN** `synonym_check_scope` is `public_only`
- **THEN** only PUBLIC synonyms are included in SYNONYM missing/extra comparison and summary counts

#### Scenario: Full synonym comparison
- **WHEN** `synonym_check_scope` is `all`
- **THEN** both PUBLIC and non-PUBLIC in-scope synonyms are included in SYNONYM missing/extra comparison and summary counts
