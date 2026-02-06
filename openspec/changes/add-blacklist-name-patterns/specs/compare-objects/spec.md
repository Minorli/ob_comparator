# compare-objects

## ADDED Requirements

### Requirement: Blacklist rule enable tags
The system SHALL honor a per-rule `enabled` tag in `blacklist_rules.json` to decide whether a rule is executed.

#### Scenario: Rule enabled
- **WHEN** a blacklist rule has `enabled=true` or the tag is missing
- **THEN** the rule is executed as part of blacklist detection

#### Scenario: Rule disabled
- **WHEN** a blacklist rule has `enabled=false`
- **THEN** the rule is skipped and a log note records the rule id and skip reason

### Requirement: Name-pattern blacklist rules
The system SHALL support name-pattern blacklist rules rendered from configured keywords.

#### Scenario: Pattern clause rendered
- **WHEN** `blacklist_name_patterns` or `blacklist_name_patterns_file` provides keywords
- **THEN** the system renders `{{name_pattern_clause}}` using literal substring matching

#### Scenario: Pattern clause empty
- **WHEN** no keywords are provided
- **THEN** rules containing `{{name_pattern_clause}}` are skipped with a warning

### Requirement: Built-in RENAME blacklist rule
The system SHALL include a built-in `RENAME_TABLES` blacklist rule to catch table names containing `_RENAME`.

#### Scenario: RENAME table detected
- **WHEN** a table name matches the RENAME rule
- **THEN** the table is recorded as blacklisted with reason code `BLACKLIST_NAME_PATTERN`

### Requirement: Name-pattern reason mapping
The system SHALL recognize NAME_PATTERN and RENAME blacklist types as supported reasons.

#### Scenario: Reason mapping
- **WHEN** a blacklist row has black_type of NAME_PATTERN or RENAME
- **THEN** reports use the mapped reason rather than “未知黑名单类型”
