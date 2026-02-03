## ADDED Requirements

### Requirement: Report DB configuration
The system SHALL support configuration of report database storage via SETTINGS keys, applying defaults when unset.

#### Scenario: Defaults applied
- **WHEN** report_to_db, report_db_schema, report_retention_days, report_db_fail_abort, report_db_detail_mode, report_db_detail_max_rows, report_db_insert_batch, or report_db_save_full_json are missing
- **THEN** the system applies safe defaults and continues

#### Scenario: Invalid detail mode
- **WHEN** report_db_detail_mode contains unknown tokens
- **THEN** the system ignores unknown values and logs a warning

#### Scenario: Invalid batch size
- **WHEN** report_db_insert_batch is missing or <= 0
- **THEN** the system defaults to a safe batch size (e.g., 200)
