## ADDED Requirements
### Requirement: Blacklist rule configuration
The system SHALL support configuration settings that control blacklist rule evaluation, including `blacklist_mode`, `blacklist_rules_path`, `blacklist_rules_enable`, `blacklist_rules_disable`, and `blacklist_lob_max_mb`.

#### Scenario: Default blacklist mode
- **WHEN** `blacklist_mode` is missing
- **THEN** the system defaults to `auto`

#### Scenario: Missing rules path
- **WHEN** `blacklist_rules_path` is missing
- **THEN** the system loads the default bundled rules file

#### Scenario: Invalid enable/disable list
- **WHEN** `blacklist_rules_enable` or `blacklist_rules_disable` contains unknown rule ids
- **THEN** the system logs a warning and ignores unknown ids

#### Scenario: Invalid LOB threshold
- **WHEN** `blacklist_lob_max_mb` is missing or invalid
- **THEN** the system defaults to 512 MB
