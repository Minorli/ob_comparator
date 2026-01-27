## ADDED Requirements

### Requirement: Migration focus report
The system SHALL export a migration focus report that lists only missing-supported objects and unsupported/blocked objects.

#### Scenario: Focus report generated
- **WHEN** a comparison run completes
- **THEN** the system writes migration_focus_<timestamp>.txt under the run report directory
- **AND** the report includes two sections: MISSING_SUPPORTED and UNSUPPORTED_OR_BLOCKED

#### Scenario: Missing supported entries
- **WHEN** missing objects are classified as SUPPORTED
- **THEN** those entries appear under the MISSING_SUPPORTED section with recommended actions

#### Scenario: Unsupported or blocked entries
- **WHEN** missing objects are classified as UNSUPPORTED or BLOCKED
- **THEN** those entries appear under the UNSUPPORTED_OR_BLOCKED section with reason codes and dependencies

## MODIFIED Requirements

### Requirement: Rich report output
The system SHALL highlight missing-supported vs unsupported/blocked counts in the summary
and point to the migration focus report for quick review.

#### Scenario: Focus report available
- **WHEN** migration_focus_<timestamp>.txt is generated
- **THEN** the main report includes a hint to review it for migration action items

### Requirement: Report index export
The system SHALL include the migration focus report in the report index with a clear description.

#### Scenario: Report index entries generated
- **WHEN** report_index_<timestamp>.txt is generated
- **THEN** it includes migration_focus_<timestamp>.txt with description "迁移聚焦清单"
