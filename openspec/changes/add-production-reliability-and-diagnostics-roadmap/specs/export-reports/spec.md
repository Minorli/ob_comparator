## ADDED Requirements

### Requirement: Difference explanation export
The report exporter SHALL serialize existing compare/fixup explanation records in both text and machine-readable form without re-deriving compare decisions.

#### Scenario: Split report mode
- **WHEN** report_detail_mode is split
- **THEN** detail files include reason code, rule id, source evidence summary, target evidence summary, decision, and action columns where applicable

#### Scenario: Machine-readable explanations
- **WHEN** a run completes
- **THEN** the run directory includes a machine-readable explanation artifact for mismatches, suppressions, manual actions, and generated fixups

#### Scenario: Compatible VARCHAR window
- **WHEN** a VARCHAR/VARCHAR2 difference is accepted as compatible by the compare stage
- **THEN** split detail files, report_db, and machine-readable mismatch artifacts do not include it as a mismatch or generated fixup

### Requirement: Compatibility matrix export
The system SHALL export compatibility matrix decisions for the run.

#### Scenario: Run completes
- **WHEN** a comparison run completes
- **THEN** the run directory contains a compatibility matrix artifact listing source mode, target version, object family, operation, decision, and reason

#### Scenario: Manual or degraded decision
- **WHEN** an object is routed to manual or degraded handling by compatibility matrix
- **THEN** the main report and manual actions include the matrix reason

### Requirement: Heartbeat and phase summary export
The system SHALL summarize long-operation progress in run artifacts.

#### Scenario: Run completes
- **WHEN** a run completes
- **THEN** the report includes phase durations and slow-operation warnings

#### Scenario: Run interrupted
- **WHEN** a run is interrupted after heartbeat state was written
- **THEN** the latest heartbeat state remains available for diagnostic package generation

### Requirement: Recovery manifest export
The system SHALL export recovery information for completed phases and replayable objects.

#### Scenario: Checkpoints exist
- **WHEN** a run writes checkpoint metadata
- **THEN** the run directory includes a recovery manifest listing phases, object checkpoints, hashes, and resume eligibility

### Requirement: Diagnostic package index
The system SHALL include diagnostic package entry points in reports.

#### Scenario: Run completes
- **WHEN** diagnostic package generation is available
- **THEN** the main report and report index include a concrete `python3 diagnostic_bundle.py --run-dir <run_dir> --config <config.ini>` command or artifact path needed to generate a support bundle
