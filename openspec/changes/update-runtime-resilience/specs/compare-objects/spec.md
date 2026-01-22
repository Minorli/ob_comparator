## ADDED Requirements

### Requirement: Metadata query retries
The system SHALL retry transient metadata query failures for OceanBase and Oracle when retry settings are configured, and fail the run if retries are exhausted.

#### Scenario: Transient obclient failure retried
- **WHEN** an OceanBase metadata query fails with a transient error
- **AND** obclient_retry_count > 1
- **THEN** the query is retried with backoff before the run fails

#### Scenario: Non-transient metadata failure
- **WHEN** a metadata query fails with a non-transient error
- **THEN** the system does not retry and fails the run
