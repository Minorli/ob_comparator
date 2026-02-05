## ADDED Requirements

### Requirement: Detail item storage toggle
The system SHALL support report_db_detail_item_enable to control whether DIFF_REPORT_DETAIL_ITEM rows are written.

#### Scenario: Detail item enabled
- **WHEN** report_db_detail_item_enable is true and report_db_store_scope=full
- **THEN** the system writes DIFF_REPORT_DETAIL_ITEM rows

#### Scenario: Detail item disabled
- **WHEN** report_db_detail_item_enable is false
- **THEN** no DIFF_REPORT_DETAIL_ITEM rows are written

### Requirement: Detail item row cap
The system SHALL honor report_db_detail_item_max_rows to cap DIFF_REPORT_DETAIL_ITEM row volume.

#### Scenario: Detail item row cap applied
- **WHEN** report_db_detail_item_max_rows is set to a positive integer
- **THEN** DIFF_REPORT_DETAIL_ITEM writes are truncated to that limit and the summary marks truncation
