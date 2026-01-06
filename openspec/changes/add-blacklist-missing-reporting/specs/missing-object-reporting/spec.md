## ADDED Requirements
### Requirement: Blacklist ingestion
The system SHALL load `OMS_USER.TMP_BLACK_TABLE` (OWNER, TABLE_NAME, DATA_TYPE, BLACK_TYPE) and mark any listed table as blacklisted for reporting.

#### Scenario: Table is listed in TMP_BLACK_TABLE
- **WHEN** `TMP_BLACK_TABLE` contains a row for `HR.EMP`
- **THEN** `HR.EMP` is treated as blacklisted in missing-table reporting

### Requirement: OMS-ready missing mapping excludes blacklisted tables
The system SHALL exclude blacklisted missing TABLE entries from `tables_views_miss` output while preserving supported TABLE and VIEW mappings.

#### Scenario: Missing TABLE is blacklisted
- **WHEN** a TABLE is missing in the target and is blacklisted
- **THEN** it does not appear in `tables_views_miss`

#### Scenario: Missing TABLE is supported
- **WHEN** a TABLE is missing in the target and is not blacklisted
- **THEN** it appears in `tables_views_miss` with the existing remap format

### Requirement: Blacklist report output
The system SHALL generate `main_reports/blacklist_tables.txt` grouped by schema and sorted, listing blacklisted missing TABLEs with `BLACK_TYPE`, `DATA_TYPE`, and a human-readable reason.

#### Scenario: Blacklisted missing tables exist
- **WHEN** at least one missing TABLE is blacklisted
- **THEN** `blacklist_tables.txt` is written with schema sections and sorted entries

### Requirement: Summary counts split for blacklisted tables
The system SHALL report missing TABLE counts excluding blacklisted tables and add a separate summary line for blacklisted missing tables.

#### Scenario: Mixed missing tables
- **WHEN** missing TABLEs include both supported and blacklisted entries
- **THEN** the TABLE count excludes blacklisted entries
- **AND** a separate blacklist count reports the excluded entries

### Requirement: Black type normalization and reason mapping
The system SHALL interpret `BLACK_TYPE` case-insensitively, map known categories to reason text, and preserve unknown categories in the blacklist report.

#### Scenario: Known and unknown BLACK_TYPE values
- **WHEN** a row has `BLACK_TYPE` = `spe`
- **THEN** the report shows the normalized category with the defined reason
- **WHEN** a row has an unknown `BLACK_TYPE`
- **THEN** the report still lists the entry with the raw category and `DATA_TYPE`
