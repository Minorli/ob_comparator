## ADDED Requirements

### Requirement: Detail item row storage
When report_to_db is enabled and report_db_store_scope is full, the system SHALL store flattened detail items so that JSON-only details are queryable as rows.

#### Scenario: Table column mismatch items
- **WHEN** a TABLE has missing/extra/length/type mismatches
- **THEN** DIFF_REPORT_DETAIL_ITEM contains one row per column mismatch item

#### Scenario: Sequence missing items
- **WHEN** sequences are missing or extra in OceanBase
- **THEN** DIFF_REPORT_DETAIL_ITEM contains one row per missing/extra sequence

#### Scenario: Unsupported reason details
- **WHEN** an object is UNSUPPORTED or BLOCKED
- **THEN** DIFF_REPORT_DETAIL_ITEM records reason_code/dependency/root_cause as rows

#### Scenario: Store scope gating
- **WHEN** report_db_store_scope is summary or core
- **THEN** DIFF_REPORT_DETAIL_ITEM rows are not written
