## MODIFIED Requirements

### Requirement: Result handling
The fixup executor SHALL move successfully executed scripts to fixup_scripts/done and keep failed scripts in place. When a script contains multiple SQL statements, the executor SHALL continue executing remaining statements after a statement failure and log the failures.

#### Scenario: Successful execution
- **WHEN** all statements in a script execute without errors
- **THEN** the script is moved into the done subdirectory

#### Scenario: Statement failure
- **WHEN** a statement in a script fails
- **THEN** the executor logs the error and continues with the next statement
- **AND** the script remains in place for retry

## ADDED Requirements

### Requirement: Fixup execution timeout
The fixup executor SHALL honor fixup_cli_timeout when running obclient commands.

#### Scenario: Custom timeout
- **WHEN** fixup_cli_timeout is configured
- **THEN** obclient commands use the configured timeout for run_fixup

#### Scenario: Timeout disabled
- **WHEN** fixup_cli_timeout is set to 0
- **THEN** run_fixup executes without an obclient timeout

### Requirement: Iterative VIEW dependency resolution
The fixup executor SHALL, during --iterative runs, attempt to resolve VIEW creation failures caused by missing referenced objects using existing fixup scripts.

#### Scenario: Missing dependency resolved
- **WHEN** a VIEW fails with a missing object error and a corresponding fixup script exists
- **THEN** the executor runs the dependency fixup and retries the VIEW

### Requirement: On-demand grants for VIEW creation
The fixup executor SHALL, during --iterative runs, execute only the GRANT statements needed by a failing VIEW when the error indicates insufficient privileges.

#### Scenario: Insufficient privilege on view creation
- **WHEN** a VIEW fails with an insufficient privilege error
- **THEN** the executor applies only relevant GRANT statements for the VIEW dependencies and retries

### Requirement: Fixup dependency scope
The fixup executor SHALL only attempt to create missing dependencies that are present in fixup_scripts.

#### Scenario: Dependency not in fixup scripts
- **WHEN** a VIEW fails due to a missing dependency that has no fixup script
- **THEN** the executor logs the missing dependency and continues without creating it
