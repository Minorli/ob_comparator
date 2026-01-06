# generate-fixup

## MODIFIED Requirements

### Requirement: Trigger fixup list filter
The system SHALL support trigger_list to limit TRIGGER fixup generation when the list is readable and TRIGGER checks are enabled, and fall back to full trigger generation when the list is missing, empty, or unreadable.

#### Scenario: Trigger list configured and valid
- **WHEN** trigger_list is configured and TRIGGER checks are enabled
- **THEN** only listed missing triggers are generated under fixup_scripts/trigger

#### Scenario: Trigger list unreadable or empty
- **WHEN** trigger_list cannot be read or contains no valid entries
- **THEN** the system logs a warning and generates all missing triggers

#### Scenario: Trigger checks disabled
- **WHEN** TRIGGER is not enabled in check_extra_types
- **THEN** trigger_list is only format-validated and does not filter fixup generation

## ADDED Requirements

### Requirement: Fixup directory hygiene
The system SHALL clean fixup_dir contents before generation when the directory is relative or within the run root, and SHALL skip auto-clean when fixup_dir is outside the run root.

#### Scenario: Safe cleanup
- **WHEN** fixup_dir resolves inside the current working directory or is relative
- **THEN** existing fixup_dir contents are removed before new scripts are generated

#### Scenario: Unsafe cleanup
- **WHEN** fixup_dir resolves outside the current working directory
- **THEN** the system skips auto-clean and logs a warning to avoid accidental deletion

### Requirement: Fixup generation concurrency
The system SHALL honor fixup_workers and progress_log_interval for parallel fixup generation and periodic progress logging.

#### Scenario: Fixup workers default
- **WHEN** fixup_workers is missing, non-numeric, or <= 0
- **THEN** the system uses min(12, CPU) workers

#### Scenario: Progress interval clamp
- **WHEN** progress_log_interval is missing or < 1
- **THEN** the system defaults to 10 seconds and enforces a minimum of 1 second

### Requirement: dbcat export caching and parallelism
The system SHALL load DDL from dbcat_output_dir cache when available, export missing DDL via dbcat using dbcat_parallel_workers, dbcat_chunk_size, and cli_timeout, and normalize results back into the cache.

#### Scenario: Cached DDL available
- **WHEN** a requested object DDL exists in dbcat_output_dir cache
- **THEN** the system uses the cached DDL without running dbcat

#### Scenario: dbcat parallel export
- **WHEN** dbcat export is required
- **THEN** the system runs per-schema exports in parallel up to the configured worker cap (1-8)

#### Scenario: MATERIALIZED VIEW export
- **WHEN** a MATERIALIZED VIEW is requested for dbcat export
- **THEN** the system skips automatic export and logs that dbcat does not support MVIEW

### Requirement: Print-only object fixup behavior
The system SHALL not generate fixup DDL for print-only primary types (MATERIALIZED VIEW, PACKAGE, PACKAGE BODY).

#### Scenario: Missing package
- **WHEN** a PACKAGE or PACKAGE BODY is missing in the target
- **THEN** it is reported as print-only and no fixup script is generated
