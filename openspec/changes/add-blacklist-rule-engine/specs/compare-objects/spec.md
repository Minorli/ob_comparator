## MODIFIED Requirements
### Requirement: Blacklist table detection
The system SHALL determine blacklisted TABLEs using `blacklist_mode` with sources from `OMS_USER.TMP_BLACK_TABLE` and/or the configured blacklist rule set.

#### Scenario: Blacklist disabled
- **WHEN** `blacklist_mode` is set to `disabled`
- **THEN** the system skips blacklist discovery and proceeds without blacklist filtering

#### Scenario: Auto mode uses TMP_BLACK_TABLE and rules
- **WHEN** `blacklist_mode` is `auto` and `TMP_BLACK_TABLE` exists
- **THEN** the system loads blacklist entries from the table and merges them with rule-derived entries, de-duplicating by `(OWNER, TABLE_NAME, BLACK_TYPE, DATA_TYPE)`

#### Scenario: Auto mode falls back to rules
- **WHEN** `blacklist_mode` is `auto` and `TMP_BLACK_TABLE` is missing
- **THEN** the system logs a warning and uses rule-derived entries only

#### Scenario: Table-only mode
- **WHEN** `blacklist_mode` is `table_only`
- **THEN** the system uses `TMP_BLACK_TABLE` only and ignores rule-derived entries

#### Scenario: Rules-only mode with invalid rules
- **WHEN** `blacklist_mode` is `rules_only` and the rules file cannot be loaded
- **THEN** the system logs a warning and proceeds without blacklist filtering

## ADDED Requirements
### Requirement: Blacklist rule evaluation
The system SHALL evaluate blacklist rules against Oracle metadata for the configured source schemas, honoring version gates and per-rule enable/disable controls.

#### Scenario: Owner-scoped rule execution
- **WHEN** a rule is executed
- **THEN** its SQL is filtered to `source_schemas` and executed in chunks to avoid oversized IN clauses

#### Scenario: Version-gated rule
- **WHEN** a rule specifies `min_ob_version` or `max_ob_version`
- **THEN** the rule is skipped if the configured OceanBase version is outside the allowed range

#### Scenario: Rule query failure
- **WHEN** a rule query fails due to missing privileges or missing views
- **THEN** the system logs the failure and continues with remaining rules
