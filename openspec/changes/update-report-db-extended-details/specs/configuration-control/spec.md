## ADDED Requirements

### Requirement: Report DB enabled by default
The system SHALL default report_to_db to true when the config key is missing.

#### Scenario: report_to_db missing
- **WHEN** SETTINGS.report_to_db is not configured
- **THEN** the system enables report_to_db by default
