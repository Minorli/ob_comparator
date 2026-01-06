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
