## ADDED Requirements

### Requirement: Trigger status report excludes unsupported tables
The system SHALL omit trigger status report rows when the trigger’s parent table is unsupported or blacklisted.

#### Scenario: Trigger on unsupported table
- **WHEN** a trigger belongs to an unsupported or blacklisted table
- **THEN** trigger_status_report.txt does not include the trigger row or its status diff counts
