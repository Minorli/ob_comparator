# export-reports

## MODIFIED Requirements

### Requirement: Rich report output
The system SHALL render a summary report to the console and export a plain-text report to main_reports/report_<timestamp>.txt that includes endpoint environment info (when available) and an execution summary.

#### Scenario: Report export with endpoint info
- **WHEN** a comparison run completes and endpoint info is available
- **THEN** the report includes source/target environment details and an execution summary of actions

### Requirement: Trigger list mismatch export
The system SHALL export trigger_list validation results to main_reports/trigger_miss.txt when trigger_list is configured, including fallback notes when the list is unusable.

#### Scenario: Trigger list unreadable
- **WHEN** trigger_list cannot be read or has no valid entries
- **THEN** trigger_miss.txt records the fallback note and summary counts

## ADDED Requirements

### Requirement: Run summary section
The system SHALL append a run summary to the end of the report and log the same summary on completion.

#### Scenario: Run completes
- **WHEN** the run finishes successfully
- **THEN** the summary includes start/end time, total duration, per-phase timing, actions done/skipped, key findings, attention items, and next-step recommendations
