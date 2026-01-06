# export-reports

## MODIFIED Requirements

### Requirement: Rich report output
The system SHALL render a summary report to the console and export a plain-text report to main_reports/report_<timestamp>.txt without including GRANT details.

#### Scenario: Grant output suppressed
- **WHEN** a comparison run completes
- **THEN** the report does not display grant statements or grant counts and directs grant output to fixup scripts only
