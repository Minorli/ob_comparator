## MODIFIED Requirements

### Requirement: Trigger list mismatch export
The system SHALL export trigger list validation and trigger status mismatches to a dedicated report file when trigger_list is configured.

#### Scenario: Trigger list report includes status mismatches
- **WHEN** trigger_list is configured and triggers have enabled/valid differences
- **THEN** the report lists missing triggers and status mismatches in the same file

#### Scenario: Trigger list unreadable
- **WHEN** trigger_list cannot be read or contains no valid entries
- **THEN** the report records the fallback note and summary counts

### Requirement: Missing count adjustment for unsupported objects
The system SHALL exclude unsupported objects from supported missing counts and expose unsupported/blocked counts separately in the summary.

#### Scenario: Missing tables include unsupported
- **WHEN** missing TABLEs include blacklisted or unsupported entries
- **THEN** the summary shows supported missing count and a separate unsupported count

#### Scenario: Missing views blocked by unsupported tables
- **WHEN** missing VIEWs are blocked by unsupported dependencies
- **THEN** the summary includes a blocked count and a dependency reason entry in detail reports

### Requirement: OMS-ready missing TABLE/VIEW export
The system SHALL export missing TABLE and VIEW mappings for OMS consumption, excluding unsupported or blocked objects.

#### Scenario: Unsupported table excluded
- **WHEN** a TABLE is classified as unsupported
- **THEN** it is excluded from missed_tables_views_for_OMS output

## ADDED Requirements

### Requirement: Unsupported object detail export
The system SHALL export a pipe-delimited unsupported object report with reason and dependency details.

#### Scenario: Unsupported object listed
- **WHEN** an object is classified as unsupported or blocked
- **THEN** it appears in unsupported_objects_detail_<timestamp>.txt with reason code and dependency info

### Requirement: Report splitting for large runs
The system SHALL support a split report layout where the main report is concise and detailed listings are written to separate files.

#### Scenario: Split report mode
- **WHEN** report_detail_mode is split known or default
- **THEN** the main report lists summary counts and references to detail files
