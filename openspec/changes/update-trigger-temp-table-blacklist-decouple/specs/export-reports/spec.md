## MODIFIED Requirements

### Requirement: Dedicated temp-trigger unsupported detail export
The system SHALL export a dedicated detail file for trigger unsupported rows caused by temporary-table limitations.

#### Scenario: Temporary-table trigger unsupported exists with blacklist disabled
- **WHEN** `blacklist_mode=disabled`
- **AND** unsupported TRIGGER rows include `reason_code=TRIGGER_ON_TEMP_TABLE_UNSUPPORTED`
- **THEN** the system writes `triggers_temp_table_unsupported_detail_<timestamp>.txt`
- **AND** the row also appears in unsupported summary/manual-action outputs
- **AND** it does not appear in fixable-missing trigger counts

