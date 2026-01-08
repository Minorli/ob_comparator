## ADDED Requirements
### Requirement: Trigger fixup list filter
The system SHALL support a trigger_list configuration that limits TRIGGER fixup generation to the listed triggers.

#### Scenario: Trigger list configured
- **WHEN** trigger_list is set and includes `APP.TR_X`
- **THEN** only the listed triggers are generated under fixup_scripts/trigger
- **AND** missing triggers not in the list are skipped

#### Scenario: Trigger list not configured
- **WHEN** trigger_list is empty or not set
- **THEN** all missing triggers are generated as before

#### Scenario: Invalid list entries
- **WHEN** trigger_list contains malformed lines or unknown trigger names
- **THEN** those entries are reported without generating DDL
