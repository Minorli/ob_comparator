## ADDED Requirements

### Requirement: Synonym remap policy configuration
The system SHALL honor synonym_remap_policy to control synonym remap inference.

#### Scenario: Default policy auto
- **WHEN** synonym_remap_policy is missing
- **THEN** the system defaults to auto

#### Scenario: Invalid policy value
- **WHEN** synonym_remap_policy is not in the supported set
- **THEN** the system logs a warning and falls back to auto

### Requirement: obclient retry controls
The system SHALL support obclient_error_policy, obclient_retry_max, and obclient_retry_backoff_ms to control retry behavior.

#### Scenario: Default retry values
- **WHEN** obclient_retry_max or obclient_retry_backoff_ms is missing
- **THEN** the system defaults to retry_max=3 and retry_backoff_ms=1000

#### Scenario: Auto retry on transient failure
- **WHEN** obclient_error_policy is auto and a transient error occurs
- **THEN** the system retries up to obclient_retry_max with backoff

#### Scenario: Abort on fatal failure
- **WHEN** a fatal obclient error is detected
- **THEN** the system stops retries and reports an error regardless of policy

### Requirement: Oracle DDL batch retry limit
The system SHALL honor oracle_ddl_batch_retry_limit to cap per-object fallback retries after a batch failure.

#### Scenario: Retry limit reached
- **WHEN** an object exceeds oracle_ddl_batch_retry_limit attempts
- **THEN** the system records the failure and continues without further retries
