## MODIFIED Requirements

### Requirement: OMS-ready missing TABLE/VIEW export
The system SHALL export missing TABLE and VIEW mappings grouped by target schema under main_reports/tables_views_miss, using separate per-schema files for TABLE and VIEW.

#### Scenario: Missing table mapping
- **WHEN** a TABLE is missing and not blacklisted
- **THEN** the schema file `schema_T.txt` includes SRC=TARGET or SRC when names match

#### Scenario: Missing view mapping
- **WHEN** a VIEW is missing
- **THEN** the schema file `schema_V.txt` includes the missing view mapping

## ADDED Requirements

### Requirement: Filtered grant export
The system SHALL export filtered/unsupported GRANT privileges to `main_reports/filtered_grants.txt` when any privileges are skipped.

#### Scenario: Filtered grant entries exist
- **WHEN** unsupported GRANT privileges are filtered
- **THEN** `filtered_grants.txt` lists the filtered entries with category, grantee, privilege, object, and reason
