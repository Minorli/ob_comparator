# execute-fixup

## Purpose
Define how fixup scripts are selected, ordered, and executed.

## Requirements

### Requirement: Config-driven execution
The fixup executor SHALL read OceanBase connection info and fixup directory from config.ini.

#### Scenario: Missing configuration
- **WHEN** required OCEANBASE_TARGET fields are missing
- **THEN** the executor exits with a configuration error

#### Scenario: Fixup directory missing
- **WHEN** the configured fixup directory does not exist
- **THEN** the executor exits with a configuration error

### Requirement: Script discovery and filtering
The fixup executor SHALL collect SQL scripts from fixup_scripts subdirectories and apply include/exclude filters.

#### Scenario: Directory filter
- **WHEN** --only-dirs is specified
- **THEN** only SQL files from those subdirectories are executed

#### Scenario: Exclude directory filter
- **WHEN** --exclude-dirs is specified
- **THEN** scripts under those subdirectories are skipped

#### Scenario: Type filter
- **WHEN** --only-types is specified
- **THEN** the executor maps object types to directories and executes only those directories

#### Scenario: Filename glob filter
- **WHEN** --glob is specified
- **THEN** only files matching the glob patterns are executed

### Requirement: Execution order
The fixup executor SHALL support a default priority order and an optional dependency-aware order.

#### Scenario: Default order
- **WHEN** --smart-order is not provided
- **THEN** scripts execute in the legacy priority order

#### Scenario: Dependency-aware order
- **WHEN** --smart-order is enabled
- **THEN** scripts execute by dependency layers with grants before dependent objects

#### Scenario: Unknown directory in smart order
- **WHEN** a subdirectory is not part of the predefined dependency layers
- **THEN** its scripts are executed after the known layers

### Requirement: Result handling
The fixup executor SHALL move successfully executed scripts to fixup_scripts/done and keep failed scripts in place.

#### Scenario: Successful execution
- **WHEN** a script executes without errors
- **THEN** it is moved into the done subdirectory

#### Scenario: Failed execution
- **WHEN** a script execution fails
- **THEN** it remains in place for retry

### Requirement: Statement-level execution
The fixup executor SHALL execute SQL scripts statement-by-statement and continue after statement failures.

#### Scenario: Partial failures in a script
- **WHEN** a script contains multiple statements and one fails
- **THEN** remaining statements are still executed and the script is marked as failed with a failure count

### Requirement: Grant pruning and error report
The fixup executor SHALL execute GRANT scripts statement-by-statement, remove successful GRANT statements from the source file, and move the file to done when no statements remain. The executor SHALL write a capped error report under fixup_scripts/errors.

#### Scenario: Grant statements succeed
- **WHEN** a GRANT file executes and some statements succeed
- **THEN** successful GRANT statements are removed from the file and only failed GRANTs remain

#### Scenario: All grant statements succeed
- **WHEN** all GRANT statements in a file succeed
- **THEN** the file is moved into fixup_scripts/done

#### Scenario: Error report generated
- **WHEN** statement failures occur during execution
- **THEN** an error report file is written under fixup_scripts/errors with capped entries

### Requirement: Iterative retry mode
The fixup executor SHALL support iterative retry rounds when --iterative is enabled.

#### Scenario: Iterative mode enabled
- **WHEN** --iterative is set
- **THEN** the executor repeats execution rounds until progress stops or max rounds is reached

### Requirement: View-chain autofix
The fixup executor SHALL support --view-chain-autofix to generate per-view plans and SQL from VIEWs_chain and auto-execute them.

#### Scenario: View already exists
- **WHEN** a VIEW is present in the target
- **THEN** the per-view plan/SQL is generated and marked as skipped without execution

#### Scenario: View-chain grants missing
- **WHEN** a view-chain plan needs grants and no matching entries are found
- **THEN** the executor auto-generates object-level GRANT statements

### Requirement: Recompile retries
The fixup executor SHALL optionally recompile INVALID objects when --recompile is enabled.

#### Scenario: Recompile enabled
- **WHEN** --recompile is used
- **THEN** the executor attempts recompilation for INVALID objects up to the max retry count

#### Scenario: Recompile limit reached
- **WHEN** INVALID objects remain after the maximum retries
- **THEN** the executor reports the remaining INVALID objects

### Requirement: Log level configuration
The fixup executor SHALL honor log_level from config.ini for console output.

#### Scenario: Unknown log_level
- **WHEN** log_level is not a valid logging level
- **THEN** the executor logs a warning and defaults to INFO
