# execute-fixup

## MODIFIED Requirements

### Requirement: Script discovery and filtering
The fixup executor SHALL collect SQL scripts from fixup_scripts subdirectories and apply include/exclude filters.

#### Scenario: Exclude directory filter
- **WHEN** --exclude-dirs is specified
- **THEN** scripts under those subdirectories are skipped

## ADDED Requirements

### Requirement: Log level configuration
The fixup executor SHALL honor log_level from config.ini for console output.

#### Scenario: Unknown log_level
- **WHEN** log_level is not a valid logging level
- **THEN** the executor logs a warning and defaults to INFO
