## MODIFIED Requirements

### Requirement: Temporary-table trigger unsupported classification
The system SHALL classify missing TRIGGER objects as unsupported with a dedicated reason when their parent table is a temporary table, regardless of blacklist mode.

#### Scenario: Blacklist disabled but parent table is temporary
- **WHEN** `blacklist_mode=disabled`
- **AND** a missing TRIGGER depends on a source TABLE that is identified as temporary from metadata
- **THEN** the TRIGGER support row uses `reason_code=TRIGGER_ON_TEMP_TABLE_UNSUPPORTED`
- **AND** the reason explains OceanBase does not support simple DML triggers on temporary tables (`ORA-00600/-4007`)
- **AND** the TRIGGER is not treated as fixable missing

#### Scenario: Non-trigger dependents of temporary tables
- **WHEN** `blacklist_mode=disabled`
- **AND** a non-TRIGGER object depends on a temporary table
- **THEN** that object continues through the existing comparison path
- **AND** it is not automatically reclassified by the temporary-table trigger rule

