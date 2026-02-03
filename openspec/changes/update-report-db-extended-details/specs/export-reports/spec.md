## ADDED Requirements

### Requirement: Report DB usability export
The system SHALL persist object usability check results to DIFF_REPORT_USABILITY when report_to_db is enabled.

#### Scenario: Usability results available
- **WHEN** report_to_db=true and usability checks run
- **THEN** the system writes per-object usability rows with reason and detail JSON

### Requirement: Report DB package compare export
The system SHALL persist package comparison summaries to DIFF_REPORT_PACKAGE_COMPARE when report_to_db is enabled.

#### Scenario: Package compare generated
- **WHEN** package_compare_<timestamp>.txt is produced
- **THEN** the system writes summary, diff hash, and diff file path for each PACKAGE / PACKAGE BODY

### Requirement: Report DB trigger status export
The system SHALL persist trigger status differences to DIFF_REPORT_TRIGGER_STATUS when report_to_db is enabled.

#### Scenario: Trigger status report generated
- **WHEN** trigger_status_report.txt contains differences
- **THEN** the system writes per-trigger status rows with enabled/valid states

### Requirement: Report DB retention for extended tables
The system SHALL apply report_retention_days cleanup to new report DB tables.

#### Scenario: Retention cleanup runs
- **WHEN** report_retention_days is set
- **THEN** rows older than the retention window are removed from DIFF_REPORT_USABILITY, DIFF_REPORT_PACKAGE_COMPARE, and DIFF_REPORT_TRIGGER_STATUS
