# export-reports

## Purpose
Define report outputs, auxiliary export files, and summary visibility.

## Requirements

### Requirement: Rich report output
The system SHALL render a summary report to the console and export a plain-text report to main_reports/report_<timestamp>.txt that includes endpoint environment info (when available) and an execution summary.

#### Scenario: Report export
- **WHEN** a comparison run completes
- **THEN** a report file is written under main_reports with the run timestamp

#### Scenario: Report export with endpoint info
- **WHEN** a comparison run completes and endpoint info is available
- **THEN** the report includes source/target environment details and an execution summary of actions

#### Scenario: Empty master list
- **WHEN** the primary check list is empty
- **THEN** the system still writes a report with zero counts and skip reasons

### Requirement: Grant output suppressed
The system SHALL omit GRANT details from report output and keep grant scripts in fixup outputs only.

#### Scenario: Grant output suppressed
- **WHEN** a comparison run completes
- **THEN** the report does not display GRANT statements or counts

### Requirement: Object mapping export
The system SHALL export full object mappings to main_reports/object_mapping_<timestamp>.txt.

#### Scenario: Mapping export
- **WHEN** full_object_mapping is available
- **THEN** the system writes SRC_FULL, OBJECT_TYPE, and TGT_FULL per line

### Requirement: Remap conflict export
The system SHALL export unresolved remap conflicts to main_reports/remap_conflicts_<timestamp>.txt.

#### Scenario: Remap conflicts found
- **WHEN** remap inference is ambiguous
- **THEN** the system writes the conflicts with reasons to the remap_conflicts file

### Requirement: Dependency chain export
The system SHALL export dependency chains when print_dependency_chains is enabled.

#### Scenario: Dependency chains enabled
- **WHEN** print_dependency_chains is true and dependencies are available
- **THEN** the system writes main_reports/dependency_chains_<timestamp>.txt

### Requirement: Schema coverage summary
The system SHALL report missing source schemas and missing or extra target schemas in the summary.

#### Scenario: Target schema missing
- **WHEN** a target schema expected by remap does not exist in OceanBase
- **THEN** the report includes a hint about missing target schemas

### Requirement: OMS-ready missing TABLE/VIEW export
The system SHALL export missing TABLE and VIEW mappings grouped by target schema under main_reports/tables_views_miss.

#### Scenario: Missing table mapping
- **WHEN** a TABLE is missing and not blacklisted
- **THEN** the schema file includes SRC=TARGET or SRC when names match

#### Scenario: Missing view mapping
- **WHEN** a VIEW is missing
- **THEN** the schema file includes the missing view mapping

### Requirement: Blacklist report export
The system SHALL export blacklisted tables to main_reports/blacklist_tables.txt grouped by schema with reasons and status details.

#### Scenario: Blacklisted tables listed
- **WHEN** tables match TMP_BLACK_TABLE entries
- **THEN** blacklist_tables.txt lists TABLE, BLACK_TYPE, DATA_TYPE, STATUS, DETAIL, and reason

#### Scenario: Unknown blacklist category
- **WHEN** a blacklisted table has an unrecognized BLACK_TYPE
- **THEN** the report still lists the entry with an unknown reason

#### Scenario: LONG conversion status
- **WHEN** a blacklisted table is marked LONG/LONG RAW
- **THEN** the report includes conversion status such as VERIFIED, MISSING_TABLE, MISSING_COLUMN, or TYPE_MISMATCH

### Requirement: Trigger list mismatch export
The system SHALL export trigger_list validation results to main_reports/trigger_miss.txt when trigger_list is configured, including fallback notes when the list is unusable.

#### Scenario: Trigger list entries validated
- **WHEN** trigger_list is configured
- **THEN** trigger_miss.txt includes invalid entries, missing/selected entries, and non-missing entries

#### Scenario: Trigger list unreadable
- **WHEN** trigger_list cannot be read or has no valid entries
- **THEN** trigger_miss.txt records the fallback note and summary counts

### Requirement: Missing count adjustment for blacklist
The system SHALL exclude blacklisted tables from TABLE missing counts and report a separate blacklist count.

#### Scenario: Mixed missing tables
- **WHEN** missing tables include both supported and blacklisted entries
- **THEN** the summary shows TABLE missing without blacklisted entries and adds TABLE (BLACKLIST)

### Requirement: Extra target objects listing
The system SHALL list objects that exist in the target but are not expected by the mapping.

#### Scenario: Extra target object detected
- **WHEN** an object exists in OceanBase but is not in the expected mapping
- **THEN** the report includes it under extra target objects

### Requirement: Run summary section
The system SHALL append a run summary to the end of the report and log the same summary on completion.

#### Scenario: Run completes
- **WHEN** the run finishes successfully
- **THEN** the summary includes start/end time, total duration, per-phase timing, actions done/skipped, key findings, attention items, and next-step recommendations
