## ADDED Requirements

### Requirement: Report DB analytic views
The system SHALL create read-only analytic views in the report database when report_to_db is enabled and required base tables exist.

#### Scenario: Report DB views created
- **WHEN** report_to_db is true and report_db_store_scope is core or full
- **THEN** the system creates analytic views (actions, object profile, trends) and records their status in report artifacts

#### Scenario: Report DB views skipped
- **WHEN** report_to_db is false or required base tables are missing
- **THEN** the system skips analytic view creation and records a skip note in report artifacts

### Requirement: Report SQL template export
The system SHALL export a report_sql_<timestamp>.txt file containing pre-filled report_id queries when report_to_db is enabled.

#### Scenario: SQL template created
- **WHEN** report_to_db is true
- **THEN** report_sql_<timestamp>.txt is written to the run directory and recorded in report artifacts

#### Scenario: SQL template skipped
- **WHEN** report_to_db is false
- **THEN** no SQL template file is created
