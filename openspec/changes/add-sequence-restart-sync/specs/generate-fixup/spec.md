## ADDED Requirements
### Requirement: Optional sequence restart fixups
The system SHALL generate sequence restart scripts when sequence_sync_mode is enabled, using Oracle LAST_NUMBER as the restart value.

#### Scenario: Sequence sync off
- **WHEN** sequence_sync_mode is off
- **THEN** no sequence_restart scripts are generated

#### Scenario: Missing sequence with sync enabled
- **WHEN** a SEQUENCE is missing in the target and sequence_sync_mode is last_number
- **THEN** the system emits an ALTER SEQUENCE ... RESTART WITH <last_number> script under fixup_scripts/sequence_restart

#### Scenario: LAST_NUMBER unavailable
- **WHEN** LAST_NUMBER is missing for a sequence
- **THEN** the system logs a warning and skips sequence_restart generation for that sequence
