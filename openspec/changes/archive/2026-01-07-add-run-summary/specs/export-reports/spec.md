## ADDED Requirements
### Requirement: Run summary section
The system SHALL append a run summary section to the end of the report that includes total runtime, per-phase durations, and a narrative summary of completed versus skipped actions, key findings, attention items, and next-step suggestions derived from the run configuration and results.

#### Scenario: Full comparison run
- **WHEN** a comparison run completes with primary and extra checks enabled
- **THEN** the report includes total runtime, per-phase durations, actions completed, and key findings

#### Scenario: Partial run or disabled checks
- **WHEN** checks or fixup generation are disabled by config
- **THEN** the report lists skipped actions and the reason (config disabled or missing data)

### Requirement: End-of-run log summary
The system SHALL emit a structured run summary to the runtime log after report generation.

#### Scenario: Run completion
- **WHEN** the run finishes and the report is exported
- **THEN** the log prints total runtime, phase durations, actions completed/skipped, key findings, and attention items
