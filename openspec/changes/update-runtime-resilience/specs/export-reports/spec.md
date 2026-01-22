## ADDED Requirements

### Requirement: DDL and metadata failure export
The system SHALL export a failure summary report when metadata queries or DDL extraction fail after retries or fallbacks.

#### Scenario: Failure summary export
- **WHEN** any metadata or DDL extraction failures occur
- **THEN** the system writes main_reports/ddl_fetch_failures_<timestamp>.txt with object, stage, error, and action columns
