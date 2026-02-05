## ADDED Requirements

### Requirement: Report DB store scope
The system SHALL support report_db_store_scope to control how much report content is persisted to OceanBase.

#### Scenario: Default scope
- **WHEN** report_db_store_scope is missing
- **THEN** the system uses scope "full"

#### Scenario: Summary scope
- **WHEN** report_db_store_scope is set to summary
- **THEN** only summary/counts (and artifact catalog) are written to the database

#### Scenario: Core scope
- **WHEN** report_db_store_scope is set to core
- **THEN** summary/counts/detail/grants/usability/package_compare/trigger_status (and artifact catalog) are written, while extended datasets are skipped

#### Scenario: Full scope
- **WHEN** report_db_store_scope is set to full
- **THEN** all supported report datasets (including dependency chains, view chains, remap conflicts, mapping, blacklist, fixup skip, OMS missing, artifact catalog) are written
