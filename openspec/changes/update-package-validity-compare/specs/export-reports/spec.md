## ADDED Requirements
### Requirement: Package comparison export
The system SHALL export a detailed package comparison report that lists per-object source/target status and error summaries.

#### Scenario: Package comparison export
- **WHEN** package comparison results are available
- **THEN** the system writes main_reports/package_compare_<timestamp>.txt with per-object status, result, and error summary

### Requirement: Package comparison in main report
The system SHALL include package comparison differences in the main report output.

#### Scenario: Package differences present
- **WHEN** package comparison results are available
- **THEN** the main report includes a package section listing source-invalid objects and target status mismatches
