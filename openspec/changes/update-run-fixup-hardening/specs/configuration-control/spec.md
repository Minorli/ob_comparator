## MODIFIED Requirements

### Requirement: Fixup configuration defaults
The system SHALL expose additional fixup safety settings with safe defaults.

#### Scenario: fixup_dir boundary control
- **WHEN** `fixup_dir_allow_outside_repo` is false and fixup_dir resolves outside repo_root
- **THEN** run_fixup SHALL refuse to run with a configuration error

#### Scenario: fixup SQL size limit
- **WHEN** `fixup_max_sql_file_mb` is configured
- **THEN** run_fixup SHALL skip any SQL file exceeding the limit

#### Scenario: auto-grant cache limit
- **WHEN** `fixup_auto_grant_cache_limit` is configured
- **THEN** run_fixup SHALL cap auto-grant caches using the configured limit
