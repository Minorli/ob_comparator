## MODIFIED Requirements

### Requirement: Fixup execution safety
The system SHALL guard fixup execution with robust error handling and safe file moves.

#### Scenario: obclient invocation failure
- **WHEN** obclient cannot be executed due to missing binary or OS errors
- **THEN** run_fixup SHALL terminate with a clear configuration error

#### Scenario: done/ overwrite prevention
- **WHEN** a successful script is moved into done/ and the target file already exists
- **THEN** run_fixup SHALL preserve the prior file (rename/timestamp) before moving the new file

### Requirement: Iterative execution accounting
The system SHALL report cumulative failures as unique scripts across rounds.

#### Scenario: repeated failures across rounds
- **WHEN** the same script fails in multiple rounds
- **THEN** cumulative_failed SHALL count it once

### Requirement: View chain autofix cycle handling
The system SHALL block autofix execution when dependency cycles are detected.

#### Scenario: view chain contains cycles
- **WHEN** view chains contain cyclic dependencies
- **THEN** run_fixup SHALL mark the view as blocked and SHALL NOT emit executable SQL for that chain

### Requirement: SQL size guard
The system SHALL skip executing oversized SQL files.

#### Scenario: SQL file exceeds limit
- **WHEN** a SQL file is larger than `fixup_max_sql_file_mb`
- **THEN** run_fixup SHALL skip it with a warning and record the skip reason

### Requirement: Error classification coverage
The system SHALL classify common Oracle/OB errors for reporting and retry logic.

#### Scenario: additional error codes
- **WHEN** errors include ORA-00054/01017/12170/04031/01555/00060
- **THEN** run_fixup SHALL classify them into distinct failure types
