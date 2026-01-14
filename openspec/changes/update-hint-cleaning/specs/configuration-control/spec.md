## ADDED Requirements
### Requirement: Hint cleanup policy settings
The system SHALL support ddl_hint_policy, ddl_hint_allowlist, ddl_hint_denylist, and ddl_hint_allowlist_file to control hint preservation during DDL cleanup.

#### Scenario: Default hint policy
- **WHEN** ddl_hint_policy is missing
- **THEN** the system uses keep_supported and logs a summary of removed hints

#### Scenario: Allowlist overrides
- **WHEN** ddl_hint_allowlist includes additional hint names
- **THEN** those hints are preserved even if they are not in the built-in supported list

#### Scenario: Denylist overrides
- **WHEN** ddl_hint_denylist includes a hint name
- **THEN** that hint is removed regardless of allowlist or built-in support

#### Scenario: Allowlist file loading
- **WHEN** ddl_hint_allowlist_file points to a readable file
- **THEN** the system loads additional allowed hints from one-per-line entries, ignoring blank lines and comments
