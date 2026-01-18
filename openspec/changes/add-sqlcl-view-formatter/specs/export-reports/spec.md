## ADDED Requirements
### Requirement: DDL formatter report export
The system SHALL export a formatter summary report when ddl_format_enable is true and any DDL is processed for formatting.

#### Scenario: Formatter enabled
- **WHEN** ddl_format_enable is true and any object is eligible for formatting
- **THEN** the system writes main_reports/ddl_format_report_<timestamp>.txt with per-type counts and failure reasons

#### Scenario: Formatter disabled
- **WHEN** ddl_format_enable is false
- **THEN** no formatter report is written
