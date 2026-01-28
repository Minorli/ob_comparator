## ADDED Requirements

### Requirement: Per-type missing/unsupported exports
The system SHALL export per-type detail reports that separate supported-missing objects from unsupported/blocked objects when report_detail_mode=split.

#### Scenario: Missing supported per type
- **WHEN** missing objects include supported entries and report_detail_mode=split
- **THEN** the system writes `missing_<TYPE>_detail_<timestamp>.txt` for each affected TYPE

#### Scenario: Unsupported per type
- **WHEN** missing objects include unsupported or blocked entries and report_detail_mode=split
- **THEN** the system writes `unsupported_<TYPE>_detail_<timestamp>.txt` for each affected TYPE

#### Scenario: Pipe-delimited headers
- **WHEN** a per-type file is generated
- **THEN** the file starts with a `#` header line using `|`-delimited columns

### Requirement: Root-cause column in unsupported per-type
The system SHALL include a ROOT_CAUSE column in per-type unsupported/blocked reports.

#### Scenario: Unsupported per-type contains root cause
- **WHEN** an object is blocked by unsupported dependencies
- **THEN** the unsupported per-type report includes ROOT_CAUSE

### Requirement: Per-type report indexing
The system SHALL include per-type missing/unsupported files in `report_index_<timestamp>.txt`.

#### Scenario: Per-type files indexed
- **WHEN** per-type reports are generated
- **THEN** report_index lists each file with category, path, row count, and description
