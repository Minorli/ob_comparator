# generate-fixup

## Purpose
Define fixup DDL generation outputs, ordering, and grant script production.

## Requirements

### Requirement: Fixup generation toggle
The system SHALL generate fixup scripts only when generate_fixup is enabled.

#### Scenario: Fixup disabled
- **WHEN** generate_fixup is false
- **THEN** no fixup scripts are written

### Requirement: Fixup scope filters
The system SHALL filter fixup output by fixup_schemas and fixup_types when configured.

#### Scenario: Schema filter
- **WHEN** fixup_schemas is set to a subset of target schemas
- **THEN** fixup scripts are generated only for objects in those target schemas

#### Scenario: Type filter
- **WHEN** fixup_types is set to a subset of object types
- **THEN** fixup scripts are generated only for those types

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

### Requirement: Missing object DDL generation
The system SHALL generate CREATE DDL scripts for missing objects by type and store them in fixup_scripts subdirectories.

#### Scenario: Missing view
- **WHEN** a VIEW is missing in the target
- **THEN** a CREATE VIEW script is emitted under fixup_scripts/view

#### Scenario: Missing object without DDL
- **WHEN** DDL cannot be obtained for a missing object
- **THEN** the system logs a warning and skips script generation for that object

#### Scenario: Table CREATE length inflation
- **WHEN** a missing TABLE has VARCHAR/VARCHAR2 BYTE columns below the minimum length rule
- **THEN** the CREATE TABLE DDL inflates those column lengths to the minimum threshold

### Requirement: Table ALTER scripts for column differences
The system SHALL generate ALTER TABLE statements for missing columns and length mismatches, and comment DROP suggestions for extra columns.

#### Scenario: Missing column
- **WHEN** a source column is missing in the target
- **THEN** an ALTER TABLE ADD statement is generated

#### Scenario: VARCHAR length too short
- **WHEN** a VARCHAR/VARCHAR2 BYTE column is shorter than the minimum required length
- **THEN** an ALTER TABLE MODIFY statement inflates the target length

#### Scenario: Extra column
- **WHEN** a column exists only in the target
- **THEN** a commented DROP COLUMN suggestion is emitted

#### Scenario: VARCHAR length oversize
- **WHEN** a VARCHAR/VARCHAR2 BYTE column exceeds the oversize threshold
- **THEN** the fixup emits a warning comment rather than a MODIFY statement

### Requirement: LONG type mapping in fixups
The system SHALL map LONG to CLOB and LONG RAW to BLOB when generating missing-column ADD statements.

#### Scenario: Missing LONG column
- **WHEN** a missing column is LONG in the source
- **THEN** the ADD COLUMN uses CLOB in the target DDL

#### Scenario: Missing LONG RAW column
- **WHEN** a missing column is LONG RAW in the source
- **THEN** the ADD COLUMN uses BLOB in the target DDL

#### Scenario: LONG type mismatch
- **WHEN** a source column is LONG or LONG RAW but the target column type differs
- **THEN** an ALTER TABLE MODIFY statement is generated to use CLOB or BLOB

### Requirement: DDL extraction fallback
The system SHALL prefer dbcat DDL output and fall back to DBMS_METADATA for TABLE and VIEW DDL when dbcat output is missing or unsupported.

#### Scenario: dbcat returns unsupported table DDL
- **WHEN** dbcat output indicates unsupported TABLE DDL
- **THEN** the system attempts to fetch TABLE DDL via DBMS_METADATA

#### Scenario: dbcat provides view DDL
- **WHEN** dbcat returns VIEW DDL for a missing VIEW
- **THEN** the system uses the dbcat DDL for fixup generation

#### Scenario: dbcat missing view DDL
- **WHEN** dbcat does not return VIEW DDL for a missing VIEW
- **THEN** the system attempts to fetch VIEW DDL via DBMS_METADATA

#### Scenario: dbcat not configured
- **WHEN** generate_fixup is enabled but dbcat_bin is missing
- **THEN** the system logs a warning and continues with limited DDL sources

### Requirement: Trigger DDL remap and grants
The system SHALL preserve trigger schema, rewrite ON clause table references to remapped targets, and append required GRANT statements.

#### Scenario: Trigger depends on remapped table
- **WHEN** a TRIGGER targets a TABLE remapped to another schema
- **THEN** the trigger DDL references the remapped table and includes necessary GRANT statements

### Requirement: DDL cleanup for OceanBase
The system SHALL remove Oracle-only clauses from generated DDL to improve OceanBase compatibility.

#### Scenario: VIEW cleanup uses OceanBase version
- **WHEN** a VIEW DDL contains Oracle-only clauses such as EDITIONABLE or WITH CHECK OPTION
- **THEN** the clauses are removed based on the detected OceanBase version

### Requirement: SQL rewrite safety
The system SHALL avoid rewriting object references inside string literals and comments.

#### Scenario: View DDL contains table name in a string
- **WHEN** a view DDL includes a table name inside a string literal or comment
- **THEN** that occurrence is not rewritten during remap

### Requirement: Synonym DDL from metadata
The system SHALL generate synonym DDL using cached synonym metadata when available.

#### Scenario: PUBLIC synonym
- **WHEN** a PUBLIC synonym has cached metadata
- **THEN** the generated DDL uses CREATE OR REPLACE PUBLIC SYNONYM with the resolved target

#### Scenario: PUBLIC synonyms filtered by schema list
- **WHEN** PUBLIC synonyms point to schemas outside the configured source schema list
- **THEN** those PUBLIC synonyms are ignored in cached metadata

### Requirement: Compile and grant scripts
The system SHALL generate compile scripts for missing dependencies and GRANT scripts derived from Oracle privileges and dependency-based grants when generate_grants is enabled.

#### Scenario: Missing dependency
- **WHEN** a dependent object exists but required dependencies are missing in the target
- **THEN** an ALTER ... COMPILE script is produced in fixup_scripts/compile

#### Scenario: Grant generation enabled
- **WHEN** generate_grants is true
- **THEN** fixup_scripts/grants contains object, role, and system GRANT statements

#### Scenario: Grant generation disabled
- **WHEN** generate_grants is false
- **THEN** no GRANT statements are emitted in fixup outputs

### Requirement: Source privilege remap
The system SHALL remap Oracle object privileges to target objects and schemas, preserving WITH GRANT OPTION when present.

#### Scenario: Remapped object grant
- **WHEN** a source object privilege targets a remapped object
- **THEN** the generated GRANT references the remapped object and remapped grantee schema

#### Scenario: WITH GRANT OPTION preserved
- **WHEN** a source privilege is marked GRANTABLE
- **THEN** the GRANT statement includes WITH GRANT OPTION

### Requirement: Role and system grants
The system SHALL emit GRANT statements for DBA_ROLE_PRIVS and DBA_SYS_PRIVS entries, preserving ADMIN OPTION when present.

#### Scenario: Role grant preserved
- **WHEN** a role is granted to a schema in Oracle
- **THEN** the system emits GRANT <role> TO <grantee> [WITH ADMIN OPTION]

#### Scenario: System privilege preserved
- **WHEN** a system privilege exists in Oracle
- **THEN** the system emits GRANT <privilege> TO <grantee> [WITH ADMIN OPTION]

### Requirement: Dependency-derived grants
The system SHALL add cross-schema grants required by remapped dependency edges and deep view dependency chains.

#### Scenario: Direct dependency edge
- **WHEN** a dependent object references a target object in a different schema
- **THEN** the system adds the required privilege for that referenced object

#### Scenario: View transitive dependency
- **WHEN** a VIEW or MATERIALIZED VIEW depends on a chain of objects across schemas
- **THEN** the system adds grants for referenced objects along the chain

### Requirement: Grant DDL injection
The system SHALL append object-level GRANT statements to per-object fixup DDL when those grants target the created object.

#### Scenario: Object DDL includes grants
- **WHEN** an object has remapped privileges targeting its target name
- **THEN** its fixup DDL includes those GRANT statements after the CREATE/ALTER

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
