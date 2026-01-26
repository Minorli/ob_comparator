## ADDED Requirements

### Requirement: Report index export
The system SHALL export a per‑run report index file listing all report/detail outputs with path, type, short description, and row counts when available.

#### Scenario: Index written on completion
- **WHEN** a comparison run completes
- **THEN** `main_reports/run_<ts>/report_index_<ts>.txt` is written and contains every generated report/detail file for that run

#### Scenario: Split detail mode
- **WHEN** `report_detail_mode=split`
- **THEN** the index lists the split detail files (e.g., `missing_objects_detail_<ts>.txt`) with their roles

#### Scenario: Summary detail mode
- **WHEN** `report_detail_mode=summary`
- **THEN** the index notes that detail exports are suppressed and points to the main report only

### Requirement: Execution conclusion block
The system SHALL render a concise “执行结论” block near the top of the report, summarizing actionable counts and next‑step guidance, without removing any existing report information.

#### Scenario: Conclusion always present
- **WHEN** a report is generated
- **THEN** the report includes an “执行结论” block before detailed sections

### Requirement: Sequential section numbering and consistent terminology
The system SHALL keep report section numbering sequential and apply consistent terminology for missing/mismatch/extra/unsupported/blocked/print‑only labels.

#### Scenario: Numbering is sequential
- **WHEN** the report is generated
- **THEN** section numbering does not skip or reorder numeric labels

#### Scenario: Terminology normalized
- **WHEN** the report is generated
- **THEN** headings and summary labels use the standardized terms defined in documentation

### Requirement: Report completeness preserved
The system SHALL preserve existing report outputs; the index is additive and no existing detail file is removed or replaced.

#### Scenario: Existing detail outputs preserved
- **WHEN** the index is added
- **THEN** all previously generated detail files remain unchanged in content and location
