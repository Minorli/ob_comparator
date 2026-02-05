## ADDED Requirements

### Requirement: Report DB coverage expansion
When report_to_db is enabled, the system SHALL persist additional report datasets in OceanBase so that most diagnostics can be queried without reading TXT files.

#### Scenario: Dependency chains stored
- **WHEN** dependency chains are generated
- **THEN** the system stores dependency edges in a DIFF_REPORT_DEPENDENCY table keyed by report_id

#### Scenario: VIEW chains stored
- **WHEN** VIEW fixup chains are generated
- **THEN** the system stores view-chain nodes and status in a DIFF_REPORT_VIEW_CHAIN table

#### Scenario: Remap conflicts stored
- **WHEN** remap conflicts are detected
- **THEN** the system stores conflicts with reasons and candidates in a DIFF_REPORT_REMAP_CONFLICT table

#### Scenario: Object mapping stored
- **WHEN** full_object_mapping is available
- **THEN** the system stores SRC/TGT mapping rows in DIFF_REPORT_OBJECT_MAPPING

#### Scenario: Blacklist tables stored
- **WHEN** blacklist tables are identified
- **THEN** the system stores table, black_type, and conversion status in DIFF_REPORT_BLACKLIST

#### Scenario: Fixup skip summary stored
- **WHEN** fixup skip summaries are generated
- **THEN** the system stores per-type skip reasons and counts in DIFF_REPORT_FIXUP_SKIP

#### Scenario: OMS missing mapping stored
- **WHEN** OMS missing TABLE/VIEW mappings are exported
- **THEN** the system stores the mappings in DIFF_REPORT_OMS_MISSING

#### Scenario: Store scope limits write coverage
- **WHEN** report_db_store_scope is set to summary
- **THEN** only summary and counts datasets are stored in the database

#### Scenario: Store scope core
- **WHEN** report_db_store_scope is set to core
- **THEN** summary, counts, detail, and grants datasets are stored, while extended datasets are skipped

### Requirement: Report artifact catalog
The system SHALL store a catalog of generated TXT reports in the database to clarify coverage and provide file paths when details are not stored.

#### Scenario: Artifact catalog entries
- **WHEN** a report run completes
- **THEN** DIFF_REPORT_ARTIFACT records report_type, file path, row count, hash, and coverage status

#### Scenario: Detail truncation
- **WHEN** detail rows exceed report_db_detail_max_rows
- **THEN** the system marks the summary as truncated and records the full TXT path in DIFF_REPORT_ARTIFACT
