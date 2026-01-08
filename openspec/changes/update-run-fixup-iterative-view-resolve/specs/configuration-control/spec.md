## ADDED Requirements

### Requirement: Fixup execution timeout setting
The system SHALL support fixup_cli_timeout to control run_fixup obclient execution timeouts, defaulting to 3600 seconds when missing or invalid.

#### Scenario: Long-running fixup execution
- **WHEN** fixup_cli_timeout is set to 7200
- **THEN** run_fixup uses a 7200-second timeout for obclient execution

#### Scenario: Default timeout
- **WHEN** fixup_cli_timeout is missing or invalid
- **THEN** run_fixup uses a 3600-second timeout for obclient execution

#### Scenario: Disable fixup timeout
- **WHEN** fixup_cli_timeout is set to 0
- **THEN** run_fixup executes without a timeout
