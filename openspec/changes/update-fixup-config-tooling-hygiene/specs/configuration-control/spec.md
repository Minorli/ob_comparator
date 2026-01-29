## ADDED Requirements

### Requirement: Config template integrity
The project SHALL keep `config.ini.template` free of duplicate keys to avoid ambiguity for users and tooling.

#### Scenario: Duplicate keys in template
- **WHEN** `config.ini.template` contains duplicate keys
- **THEN** the validation test fails and reports the duplicate key names

## MODIFIED Requirements

### Requirement: Logging configuration
The system SHALL write a run log file to log_dir and honor log_level for console output, and it SHALL not silently suppress exceptions in runtime helpers.

#### Scenario: Log directory available
- **WHEN** log_dir is set and writable
- **THEN** a run_<timestamp>.log is created with DEBUG-level detail and console logs use log_level

#### Scenario: Log directory unavailable
- **WHEN** log_dir cannot be created
- **THEN** the system logs a warning and continues with console-only output

#### Scenario: Exception visibility
- **WHEN** a runtime helper encounters an unexpected exception
- **THEN** the error is logged with context rather than silently ignored
