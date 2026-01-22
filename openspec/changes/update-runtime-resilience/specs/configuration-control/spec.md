## ADDED Requirements

### Requirement: Retry and failure policy controls
The system SHALL honor retry and failure policy settings for obclient metadata queries, dbcat extraction, and Oracle DDL fetches when configured, and fall back to safe defaults when they are missing or invalid.

#### Scenario: Invalid obclient retry settings
- **WHEN** obclient_retry_count or backoff settings are invalid
- **THEN** the system logs a warning and uses safe defaults

#### Scenario: Unknown dbcat failure policy
- **WHEN** dbcat_failure_policy is not one of abort/fallback/continue
- **THEN** the system logs a warning and uses a safe default

#### Scenario: Invalid Oracle DDL retry limits
- **WHEN** oracle_ddl_batch_retry_limit or oracle_ddl_single_retry_limit is invalid
- **THEN** the system logs a warning and uses safe defaults
