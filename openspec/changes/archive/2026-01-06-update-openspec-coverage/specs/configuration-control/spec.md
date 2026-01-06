# configuration-control

## MODIFIED Requirements

### Requirement: Output directories
The system SHALL write reports, fixups, dbcat caches, and logs under report_dir, fixup_dir, dbcat_output_dir, and log_dir, creating directories when needed.

#### Scenario: Missing output directories
- **WHEN** any of report_dir, fixup_dir, dbcat_output_dir, or log_dir does not exist
- **THEN** the system attempts to create the directory and continues if successful

### Requirement: External tool validation
The system SHALL validate runtime dependencies and critical paths before execution.

#### Scenario: Missing Oracle client
- **WHEN** oracle_client_lib_dir is missing or invalid
- **THEN** the system terminates with a configuration error before connecting to Oracle

#### Scenario: Missing obclient executable
- **WHEN** OCEANBASE_TARGET.executable does not exist
- **THEN** the system terminates with a configuration error before metadata loading

#### Scenario: Missing dbcat binary
- **WHEN** generate_fixup is enabled and dbcat_bin is empty
- **THEN** the system logs a warning and continues with limited DDL sources

#### Scenario: Invalid dbcat path
- **WHEN** generate_fixup is enabled and dbcat_bin points to a non-existent path
- **THEN** the system terminates with a configuration error

#### Scenario: Missing JAVA_HOME
- **WHEN** generate_fixup is enabled and java_home/JAVA_HOME is missing
- **THEN** the system logs a warning that dbcat may fail

## ADDED Requirements

### Requirement: Logging configuration
The system SHALL write a run log file to log_dir and honor log_level for console output.

#### Scenario: Log directory available
- **WHEN** log_dir is set and writable
- **THEN** a run_<timestamp>.log is created with DEBUG-level detail and console logs use log_level

#### Scenario: Log directory unavailable
- **WHEN** log_dir cannot be created
- **THEN** the system logs a warning and continues with console-only output

### Requirement: Timeout controls
The system SHALL honor cli_timeout for dbcat execution and obclient_timeout for OceanBase CLI calls.

#### Scenario: Custom timeouts
- **WHEN** cli_timeout or obclient_timeout is configured
- **THEN** external commands use those timeouts during execution

### Requirement: Fixup tuning parameters
The system SHALL honor fixup_workers and progress_log_interval for fixup generation, applying safe defaults when invalid.

#### Scenario: Invalid fixup_workers
- **WHEN** fixup_workers is missing, non-numeric, or <= 0
- **THEN** the system defaults to min(12, CPU) workers

#### Scenario: Invalid progress interval
- **WHEN** progress_log_interval is missing or < 1
- **THEN** the system defaults to 10 seconds and enforces a minimum of 1 second

### Requirement: dbcat tuning and cache controls
The system SHALL honor dbcat_chunk_size and dbcat_parallel_workers, and use cache_parallel_workers and dbcat_cleanup_run_dirs when configured.

#### Scenario: dbcat parallel workers cap
- **WHEN** dbcat_parallel_workers is greater than 8
- **THEN** the system caps it at 8

#### Scenario: dbcat cache cleanup
- **WHEN** dbcat_cleanup_run_dirs is true
- **THEN** the system removes per-run dbcat output directories after cache normalization

### Requirement: Report width configuration
The system SHALL use report_width to control report rendering width.

#### Scenario: Invalid report width
- **WHEN** report_width is missing or invalid
- **THEN** the system defaults to 160 columns

### Requirement: Trigger list path validation
The system SHALL validate trigger_list paths during initialization and warn when the list is missing.

#### Scenario: Trigger list file missing
- **WHEN** trigger_list is set but the file does not exist
- **THEN** the system logs a warning and proceeds without list-based filtering
