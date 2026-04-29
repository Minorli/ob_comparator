## ADDED Requirements

### Requirement: Effective timeout summary
The system SHALL print and export an effective timeout summary for every run.

#### Scenario: Main program starts
- **WHEN** the main comparator starts
- **THEN** it logs effective values for `cli_timeout`, `obclient_timeout`, `ob_session_query_timeout_us`, table presence timeout, and long-operation warning thresholds

#### Scenario: Fixup executor starts
- **WHEN** `run_fixup.py` starts
- **THEN** it logs effective values for `fixup_cli_timeout`, `ob_session_query_timeout_us`, long SQL warning threshold, execution mode, and selected safety tiers

### Requirement: Timeout interaction warnings
The system SHALL warn when configured timeout values can create a misleading hang-like experience.

#### Scenario: Session timeout greatly exceeds process timeout
- **WHEN** `ob_session_query_timeout_us` is greater than the relevant obclient process timeout converted to microseconds
- **THEN** the system warns that process timeout will fire before the database session timeout

#### Scenario: Fixup timeout is long
- **WHEN** `fixup_cli_timeout` is greater than the configured long-operation warning threshold
- **THEN** `run_fixup.py` warns that a single SQL statement may appear idle until heartbeat or timeout output is emitted

### Requirement: Long operation heartbeat configuration
The system SHALL provide configuration for long operation heartbeat and slow-operation warning thresholds.

#### Scenario: Heartbeat interval configured
- **WHEN** `progress_log_interval` or the dedicated heartbeat interval is configured
- **THEN** long phases use that interval for progress logging with a safe minimum value

#### Scenario: Slow phase threshold configured
- **WHEN** a phase exceeds `slow_phase_warning_sec`
- **THEN** the system logs a warning containing phase name, elapsed time, and current operation context

### Requirement: Recovery configuration
The system SHALL expose configuration and CLI controls for checkpoint and resume behavior.

#### Scenario: Checkpoint enabled
- **WHEN** checkpointing is enabled
- **THEN** the system writes checkpoint metadata with `decision_config_hash`, `runtime_config_hash`, tool version, input artifact hash, phase, and timestamp

#### Scenario: Resume requested after harmless runtime config change
- **WHEN** resume is requested and only runtime/display settings such as report path, log level, heartbeat interval, diagnostic output, or log path changed
- **THEN** the system allows resume, records the runtime config hash mismatch, and includes changed key names in the recovery manifest

#### Scenario: Resume requested with mismatched decision inputs
- **WHEN** resume is requested but `decision_config_hash` or input artifact hash does not match
- **THEN** the system refuses resume by default and reports the changed decision keys or artifact mismatch

#### Scenario: Forced resume requested
- **WHEN** the operator passes `--force-resume` with a non-empty `--resume-override-reason`
- **THEN** the system records the override reason, changed keys, and operator intent in the recovery manifest before continuing

### Requirement: Diagnostic package configuration
The system SHALL expose diagnostic package controls in config, docs, and CLI help.

#### Scenario: Diagnostic package enabled
- **WHEN** diagnostic package generation is enabled
- **THEN** `diagnostic_bundle.py` applies configured redaction, artifact inclusion, maximum per-file size, maximum bundle size, SQL-content opt-in, and identifier redaction settings

### Requirement: Compatibility registry configuration
The system SHALL load compatibility decisions from a shipped registry with an optional config override.

#### Scenario: Default registry loaded
- **WHEN** no registry override is configured
- **THEN** the system loads the shipped `compatibility_registry.json` and records its version or hash in the run artifacts

#### Scenario: Registry override configured
- **WHEN** a registry override path is configured
- **THEN** the system validates the file schema before compare starts and fails fast with a clear error if it is malformed
