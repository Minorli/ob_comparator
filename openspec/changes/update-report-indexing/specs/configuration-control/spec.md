## MODIFIED Requirements

### Requirement: Logging configuration
The system SHALL honor `log_level` for console output and accept `auto` to choose a safe default based on interactivity.

#### Scenario: Default log_level
- **WHEN** `log_level` is missing
- **THEN** the system treats it as `auto`

#### Scenario: Auto in TTY
- **WHEN** `log_level=auto` and stdout is a TTY
- **THEN** console logging uses INFO level

#### Scenario: Auto in non‑TTY
- **WHEN** `log_level=auto` and stdout is not a TTY
- **THEN** console logging uses WARNING level

#### Scenario: Explicit log level
- **WHEN** `log_level` is explicitly set (DEBUG/INFO/WARNING/ERROR/CRITICAL)
- **THEN** the system uses the specified level for console output
