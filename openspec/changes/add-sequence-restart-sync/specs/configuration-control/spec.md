## ADDED Requirements
### Requirement: Sequence sync mode configuration
The system SHALL provide a sequence_sync_mode setting to control whether sequence restart fixups are generated.

#### Scenario: Default off
- **WHEN** sequence_sync_mode is missing
- **THEN** the system uses off and does not generate sequence restart fixups

#### Scenario: Invalid value
- **WHEN** sequence_sync_mode is not one of off|last_number
- **THEN** the system logs a warning and falls back to off
