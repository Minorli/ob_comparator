## MODIFIED Requirements
### Requirement: Trigger fixup list filter
The system SHALL support trigger_list to limit TRIGGER fixup generation to a configured list, and SHALL fall back to full trigger generation when the list is missing, unreadable, or contains no valid entries.

#### Scenario: Trigger list configured
- **WHEN** trigger_list is set and includes a trigger name
- **THEN** only listed triggers are generated under fixup_scripts/trigger

#### Scenario: Trigger list not configured or empty
- **WHEN** trigger_list is empty or not set
- **THEN** all missing triggers are generated as before

#### Scenario: Trigger list unreadable
- **WHEN** trigger_list is configured but cannot be read or yields no valid entries
- **THEN** the system logs a warning and falls back to generating all missing triggers
