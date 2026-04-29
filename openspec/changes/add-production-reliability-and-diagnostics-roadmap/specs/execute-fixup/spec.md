## ADDED Requirements

### Requirement: Safety-tier execution filter
The fixup executor SHALL support selecting scripts by safety tier.

#### Scenario: Default execution
- **WHEN** run_fixup is executed without an explicit safety-tier override
- **THEN** destructive and manual tiers are skipped and reported

#### Scenario: Explicit destructive execution
- **WHEN** the operator explicitly includes the destructive tier
- **THEN** run_fixup requires a confirmation flag and records the selection in the execution summary

#### Scenario: Manual-only family selected
- **WHEN** a manual tier directory or file is selected
- **THEN** run_fixup prints the manual action context before execution and records that the operator explicitly selected it

### Requirement: Fixup execution heartbeat
The fixup executor SHALL emit progress before and during long-running file or statement execution.

#### Scenario: File execution starts
- **WHEN** run_fixup starts executing a SQL file
- **THEN** it logs file path, safety tier, object identity when known, statement count, timeout, and execution mode before invoking obclient

#### Scenario: File execution mode cannot observe statement progress
- **WHEN** run_fixup uses file mode and passes the whole SQL file to an obclient subprocess
- **THEN** heartbeat is file-level/process-level only, statement index may be unknown, and the executor MUST NOT claim statement-level progress for that file

#### Scenario: Statement execution starts
- **WHEN** run_fixup starts executing a statement in statement mode
- **THEN** it logs statement index, total statements, file path, timeout, and a redacted SQL preview

#### Scenario: Statement runs long
- **WHEN** a statement in statement mode exceeds `slow_sql_warning_sec`
- **THEN** run_fixup emits a heartbeat warning with elapsed time and the same execution context

#### Scenario: SQL file runs long
- **WHEN** a file-mode obclient subprocess exceeds `slow_sql_warning_sec`
- **THEN** run_fixup emits a file-level heartbeat warning with elapsed time, file path, safety tier, timeout, process id when available, and last known context

### Requirement: Fixup execution recovery
The fixup executor SHALL support resuming execution from ledger and fixup plan metadata.

#### Scenario: Resume execution
- **WHEN** run_fixup is started with resume enabled
- **THEN** completed files or statements with matching fingerprints are skipped and reported

#### Scenario: Fingerprint mismatch
- **WHEN** a previously completed file or statement has changed
- **THEN** run_fixup refuses to treat it as completed and records the mismatch

### Requirement: Timeout result clarity
The fixup executor SHALL convert timeout events into clear execution results.

#### Scenario: SQL timeout
- **WHEN** obclient execution exceeds the configured timeout
- **THEN** run_fixup records timeout as the failure category, keeps the script in place, writes an error report entry, and prints the timeout value that fired
