## ADDED Requirements

### Requirement: Suite Layout Consolidation
The system SHALL store compatibility suite assets under `compatibility_suite/` with `runner/`, `cases/`, `docs/`, and `sql/` subdirectories.

#### Scenario: Default layout exists
- **WHEN** a developer inspects the repository
- **THEN** `compatibility_suite/runner/compatibility_runner.py` exists
- **AND** `compatibility_suite/cases/cases.json` exists

### Requirement: Suite Runner Isolation
The compatibility suite runner SHALL rely on CLI arguments for suite scope (schemas, tags, case scope) and SHALL not require suite-specific keys in user configuration.

#### Scenario: Run with default config
- **WHEN** the runner is invoked with only a `config.ini`
- **THEN** it uses config data for connection settings only
- **AND** suite behavior defaults to the CLI options and built-in defaults
