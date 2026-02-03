## ADDED Requirements

### Requirement: Optional report database storage
The system SHALL optionally persist report summaries and details into OceanBase using obclient when report_to_db is enabled, without affecting file report generation.

#### Scenario: Report DB disabled
- **WHEN** report_to_db is false
- **THEN** the system performs no database report writes and continues to export file reports only

#### Scenario: Report DB enabled
- **WHEN** report_to_db is true and target schema is available
- **THEN** the system writes report summary and configured detail entries into diff_ report tables

#### Scenario: Report DB failure (tolerant)
- **WHEN** a database write fails and report_db_fail_abort is false
- **THEN** the system logs the failure and continues without aborting the run

#### Scenario: Retention cleanup
- **WHEN** report_retention_days is greater than 0
- **THEN** the system deletes report rows older than the retention window after a successful write

### Requirement: Detail storage scope control
The system SHALL honor report_db_detail_mode and report_db_detail_max_rows when storing detail rows.

#### Scenario: Detail scope limited
- **WHEN** report_db_detail_mode excludes OK or SKIPPED
- **THEN** only missing/mismatched/unsupported rows are stored

#### Scenario: Detail cap enforced
- **WHEN** report_db_detail_max_rows is exceeded
- **THEN** detail writes are truncated and the truncation is recorded in the summary metadata

### Requirement: Per-type count storage
The system SHALL persist the main report “检查汇总” counts by object type into a dedicated diff_ counts table when report_to_db is enabled.

#### Scenario: Count table write
- **WHEN** report_to_db is true and object count summary is available
- **THEN** the system writes per-type oracle/ob/missing/unsupported/extra counts into DIFF_REPORT_COUNTS
