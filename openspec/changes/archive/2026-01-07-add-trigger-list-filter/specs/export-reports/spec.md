## ADDED Requirements
### Requirement: Trigger list mismatch report
The system SHALL export a trigger list mismatch report to main_reports/trigger_miss.txt when trigger_list is configured.

#### Scenario: Listed trigger not missing
- **WHEN** trigger_list includes a trigger that already exists in the target
- **THEN** trigger_miss.txt records it as not missing

#### Scenario: Listed trigger not found in source
- **WHEN** trigger_list includes a trigger that does not exist in the source metadata
- **THEN** trigger_miss.txt records it as not found

#### Scenario: Malformed list entries
- **WHEN** trigger_list includes an invalid line
- **THEN** trigger_miss.txt records the invalid entry with a reason
