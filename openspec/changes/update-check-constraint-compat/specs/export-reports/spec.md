## ADDED Requirements

### Requirement: Unsupported CHECK constraint report
The system SHALL export unsupported CHECK constraints to main_reports/run_<timestamp>/constraints_unsupported_detail_<timestamp>.txt when any unsupported CHECK constraints exist.

#### Scenario: Unsupported CHECK constraints exist
- **WHEN** unsupported CHECK constraints are detected
- **THEN** constraints_unsupported_detail_<timestamp>.txt is written with | delimited fields and a header row
