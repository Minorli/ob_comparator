## ADDED Requirements
### Requirement: Numeric interval cutoff setting
The system SHALL support interval_partition_cutoff_numeric to control numeric interval partition expansion.

#### Scenario: Missing numeric cutoff
- **WHEN** interval_partition_cutoff_numeric is missing or empty
- **THEN** numeric interval partition expansion is skipped

#### Scenario: Invalid numeric cutoff
- **WHEN** interval_partition_cutoff_numeric is configured but cannot be parsed as a positive number
- **THEN** the system logs a warning and skips numeric interval partition expansion

#### Scenario: Valid numeric cutoff
- **WHEN** interval_partition_cutoff_numeric is configured with a positive number
- **THEN** the system uses it as the upper bound for numeric interval expansion

## MODIFIED Requirements
### Requirement: Dependency and fixup toggles
The system SHALL honor check_dependencies and generate_fixup toggles.

#### Scenario: Dependencies disabled
- **WHEN** check_dependencies is false
- **THEN** dependency checks and grant calculations are skipped

#### Scenario: Fixup disabled
- **WHEN** generate_fixup is false
- **THEN** no fixup scripts are generated

#### Scenario: Fixup enabled with grants disabled
- **WHEN** generate_fixup is true and generate_grants is false
- **THEN** fixup scripts are generated and grant SQL is skipped
