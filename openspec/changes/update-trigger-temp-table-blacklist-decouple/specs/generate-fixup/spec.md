## MODIFIED Requirements

### Requirement: Trigger fixup list filter
The system SHALL support trigger_list to limit TRIGGER fixup generation when the list is readable and TRIGGER checks are enabled, and fall back to full trigger generation when the list is missing, empty, or unreadable.

#### Scenario: Temporary-table trigger is unsupported
- **WHEN** a missing TRIGGER is classified as `TRIGGER_ON_TEMP_TABLE_UNSUPPORTED`
- **THEN** no script is generated under `fixup_scripts/trigger/`
- **AND** an informational DDL reference is generated under `fixup_scripts/unsupported/trigger/`
- **AND** the file comments state that OceanBase does not support this temporary-table DML trigger and user refactoring is required

