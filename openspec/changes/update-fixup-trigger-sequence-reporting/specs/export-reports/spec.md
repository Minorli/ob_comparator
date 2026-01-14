## MODIFIED Requirements

### Requirement: Rich report output
The system SHALL render a summary report to the console and export a plain-text report to report_dir, honoring report_dir_layout for per-run subdirectories.

#### Scenario: Per-run report directory
- **WHEN** report_dir_layout is per_run
- **THEN** all report artifacts are written under report_dir/run_<timestamp>/

### Requirement: OMS-ready missing TABLE/VIEW export
The system SHALL export missing TABLE and VIEW mappings grouped by target schema under report_dir/missed_tables_views_for_OMS, using separate per-schema files for TABLE and VIEW.

#### Scenario: Missing table mapping
- **WHEN** a TABLE is missing and not blacklisted
- **THEN** the schema_T.txt file is written under missed_tables_views_for_OMS

## ADDED Requirements

### Requirement: Fixup skip reason export
The system SHALL export a fixup skip summary file when missing objects are detected but fixup scripts are skipped by filters or missing DDL.

#### Scenario: Index skip summary
- **WHEN** missing indexes exist and some are skipped from fixup generation
- **THEN** the report includes an index skip summary with reason counts
