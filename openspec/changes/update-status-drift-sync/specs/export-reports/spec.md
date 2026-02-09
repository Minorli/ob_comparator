## MODIFIED Requirements
### Requirement: Trigger status report export
The system SHALL export trigger_list validation results and trigger status differences to main_reports/trigger_status_report.txt when trigger_list is configured or trigger status differences exist.

#### Scenario: Status drift detail export
- **WHEN** trigger/constraint status drift exists
- **THEN** the system writes `status_drift_detail_<timestamp>.txt`
- **AND** each row includes object_type, schema/object identity, source status, target status, and action hint

#### Scenario: No status drift
- **WHEN** no trigger or constraint status drift is detected
- **THEN** `status_drift_detail_<timestamp>.txt` is not generated
