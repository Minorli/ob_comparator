# configuration-control

## Purpose
Define configuration-driven behavior, feature gates, and runtime validation.
## Requirements
### Requirement: Config file loading
The system SHALL read configuration from a config.ini file, using the CLI argument when provided or the default path otherwise.

#### Scenario: Default config
- **WHEN** no config path argument is provided
- **THEN** config.ini in the current working directory is used

#### Scenario: Missing config file
- **WHEN** the specified config.ini path does not exist
- **THEN** the system terminates with a configuration error

### Requirement: Default value fallback
The system SHALL apply internal defaults when optional config keys are missing.

#### Scenario: Missing optional key
- **WHEN** a non-required SETTINGS key is not present
- **THEN** the system uses its internal default value

### Requirement: Required source schemas
The system SHALL require a non-empty source_schemas list.

#### Scenario: Missing source_schemas
- **WHEN** source_schemas is empty or missing
- **THEN** the system terminates with an error before running comparisons

### Requirement: Type scope gating
The system SHALL use check_primary_types and check_extra_types to gate metadata loading, remap inference, validation, and fixup scope.

#### Scenario: TABLE-only scope
- **WHEN** check_primary_types is TABLE and check_extra_types is empty
- **THEN** only TABLE-related metadata and checks are performed

#### Scenario: Unknown type in check list
- **WHEN** check_primary_types or check_extra_types contains an unknown type
- **THEN** the system logs a warning and ignores the unknown type

#### Scenario: Empty type lists default to all
- **WHEN** check_primary_types or check_extra_types is empty
- **THEN** the system enables all allowed types for that category

### Requirement: Dependency and fixup toggles
The system SHALL honor check_dependencies and generate_fixup toggles.

#### Scenario: Dependencies disabled
- **WHEN** check_dependencies is false
- **THEN** dependency checks and grant calculations are skipped

#### Scenario: Fixup disabled
- **WHEN** generate_fixup is false
- **THEN** no fixup scripts are generated

### Requirement: Output directories
The system SHALL write reports, fixups, dbcat caches, and logs under report_dir, fixup_dir, dbcat_output_dir, and log_dir, creating directories when needed.

#### Scenario: Missing output directories
- **WHEN** any of report_dir, fixup_dir, dbcat_output_dir, or log_dir does not exist
- **THEN** the system attempts to create the directory and continues if successful

### Requirement: Boolean parsing
The system SHALL treat common boolean string values (true/false/1/0/yes/no) as flags in configuration.

#### Scenario: Boolean false string
- **WHEN** a boolean setting is set to "false"
- **THEN** the corresponding feature is disabled

### Requirement: Config wizard
The system SHALL provide an interactive wizard to fill missing config values when --wizard is specified.

#### Scenario: No TTY
- **WHEN** --wizard is used in a non-interactive environment
- **THEN** the system exits to avoid blocking

### Requirement: External tool validation
The system SHALL validate runtime dependencies and critical paths before execution.

#### Scenario: Missing Oracle client
- **WHEN** oracle_client_lib_dir is missing or invalid
- **THEN** the system terminates with a configuration error before connecting to Oracle

#### Scenario: Missing obclient executable
- **WHEN** OCEANBASE_TARGET.executable does not exist
- **THEN** the system terminates with a configuration error before metadata loading

#### Scenario: Missing dbcat path
- **WHEN** generate_fixup is enabled and dbcat_bin is empty
- **THEN** the system logs a warning and continues with limited DDL sources

#### Scenario: Invalid dbcat path
- **WHEN** generate_fixup is enabled and dbcat_bin points to a non-existent path
- **THEN** the system terminates with a configuration error

#### Scenario: Missing JAVA_HOME
- **WHEN** generate_fixup is enabled and java_home/JAVA_HOME is missing
- **THEN** the system logs a warning that dbcat may fail

### Requirement: Logging configuration
The system SHALL write a run log file to log_dir and honor log_level for console output.

#### Scenario: Log directory available
- **WHEN** log_dir is set and writable
- **THEN** a run_<timestamp>.log is created with DEBUG-level detail and console logs use log_level

#### Scenario: Log directory unavailable
- **WHEN** log_dir cannot be created
- **THEN** the system logs a warning and continues with console-only output

### Requirement: Project reference logging
The system SHALL log the project homepage and issue tracker URLs during startup.

#### Scenario: Startup logging
- **WHEN** a comparison run starts
- **THEN** the runtime log prints the project homepage and issue tracker URLs

### Requirement: Timeout controls
The system SHALL honor cli_timeout for dbcat execution, obclient_timeout for OceanBase CLI calls, and fixup_cli_timeout for run_fixup execution.

#### Scenario: Custom timeouts
- **WHEN** cli_timeout, obclient_timeout, or fixup_cli_timeout is configured
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

### Requirement: Grant generation toggle
The system SHALL provide a generate_grants setting to control grant DDL generation and injection into fixup scripts.

#### Scenario: Grant generation disabled
- **WHEN** generate_grants is false
- **THEN** no grant SQL is generated and no grant statements are injected into fixup DDL

#### Scenario: Grant generation enabled
- **WHEN** generate_grants is true
- **THEN** the system loads privilege metadata and generates grant SQL under fixup_scripts/grants

### Requirement: Oracle privilege metadata sources
The system SHALL read Oracle privilege metadata from DBA_TAB_PRIVS, DBA_SYS_PRIVS, DBA_ROLE_PRIVS, and DBA_ROLES when grant generation is enabled.

#### Scenario: Privilege metadata load
- **WHEN** generate_grants is true
- **THEN** privilege and role metadata are loaded and cached for grant generation

### Requirement: Grant extraction scope
The system SHALL support grant_tab_privs_scope to control how DBA_TAB_PRIVS is filtered.

#### Scenario: Owner-only scope
- **WHEN** grant_tab_privs_scope is set to owner
- **THEN** object privileges are loaded only for objects owned by the configured source schemas

#### Scenario: Owner-or-grantee scope
- **WHEN** grant_tab_privs_scope is set to owner_or_grantee
- **THEN** object privileges are loaded for objects owned by the source schemas and for grants where the grantee is in scope

### Requirement: Grant statement merging toggles
The system SHALL support grant_merge_privileges and grant_merge_grantees to control GRANT statement compaction.

#### Scenario: Merge privileges enabled
- **WHEN** grant_merge_privileges is true
- **THEN** multiple privileges for the same grantee/object/grantable are merged into one GRANT statement

#### Scenario: Merge grantees enabled
- **WHEN** grant_merge_grantees is true
- **THEN** multiple grantees for the same object/privilege/grantable are merged into one GRANT statement

### Requirement: Grant compatibility settings
The system SHALL allow configuration overrides for supported system privileges, supported object privileges, and Oracle-maintained role inclusion.

#### Scenario: Default supported privileges
- **WHEN** no override is provided
- **THEN** system privileges are derived from the target OceanBase catalog and object privileges use the built-in allowlist

#### Scenario: Supported privilege override
- **WHEN** grant_supported_sys_privs or grant_supported_object_privs is configured
- **THEN** the system uses the configured lists instead of defaults

#### Scenario: Oracle-maintained roles toggle
- **WHEN** grant_include_oracle_maintained_roles is false
- **THEN** roles marked ORACLE_MAINTAINED are skipped in CREATE ROLE generation
