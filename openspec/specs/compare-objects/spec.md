# compare-objects

## Purpose
Define object comparison scope, metadata handling, and comparison rules.

## Requirements

### Requirement: Primary object coverage
The system SHALL compare source and target objects for the primary object set and track print-only types.

#### Scenario: Standard primary types
- **WHEN** the source contains TABLE, VIEW, PROCEDURE, FUNCTION, SYNONYM, JOB, SCHEDULE, TYPE, and TYPE BODY
- **THEN** the system includes them in the primary comparison scope

#### Scenario: Print-only primary types
- **WHEN** the source contains MATERIALIZED VIEW, PACKAGE, or PACKAGE BODY
- **THEN** the system records them as print-only and skips OceanBase validation

### Requirement: One-time metadata dump
The system SHALL dump Oracle and OceanBase metadata once per run and use in-memory data for comparisons.

#### Scenario: Oracle metadata load succeeds
- **WHEN** the run starts with valid Oracle connectivity and permissions
- **THEN** DBA_OBJECTS/DBA_TAB_COLUMNS/DBA_INDEXES/DBA_CONSTRAINTS/DBA_TRIGGERS/DBA_SEQUENCES are loaded in batch

#### Scenario: OceanBase metadata load succeeds
- **WHEN** the run starts with valid OceanBase connectivity
- **THEN** DBA_OBJECTS and related metadata are loaded in batch via obclient

#### Scenario: Metadata load failure
- **WHEN** a required metadata query fails
- **THEN** the system terminates the run with an error

### Requirement: Blacklist table detection
The system SHALL detect whether OMS_USER.TMP_BLACK_TABLE exists and whether it has records for the configured schemas.

#### Scenario: Blacklist table missing
- **WHEN** OMS_USER.TMP_BLACK_TABLE is not available in the source
- **THEN** the system logs a warning and proceeds without blacklist filtering

#### Scenario: Blacklist table has records
- **WHEN** OMS_USER.TMP_BLACK_TABLE has rows for the configured schemas
- **THEN** the system logs the record count and enables blacklist filtering for reporting

### Requirement: Table comparison rules
The system SHALL detect missing tables and compare table columns with OMS and hidden columns excluded.

#### Scenario: Missing table
- **WHEN** a TABLE exists in the source but not in the target
- **THEN** the result records the TABLE as missing

#### Scenario: Materialized view not treated as table
- **WHEN** a MATERIALIZED VIEW appears in DBA_TABLES and DBA_MVIEWS
- **THEN** the TABLE check excludes the MVIEW entry and treats it as a print-only object

#### Scenario: OMS and hidden columns are ignored
- **WHEN** a column is an OMS_* column in OceanBase or a hidden column in Oracle
- **THEN** the column is ignored for column set comparison

#### Scenario: VARCHAR length rules
- **WHEN** a common column is VARCHAR/VARCHAR2 with BYTE semantics
- **THEN** the target length MUST be within [ceil(src*1.5), ceil(src*2.5)]

#### Scenario: CHAR semantics length rules
- **WHEN** a common column is VARCHAR/VARCHAR2 with CHAR semantics
- **THEN** the target length MUST match the source length exactly

#### Scenario: LONG type conversion validation
- **WHEN** a common column is LONG or LONG RAW in the source
- **THEN** the target column type MUST be CLOB or BLOB and the table is marked mismatched if not

#### Scenario: Source column metadata missing
- **WHEN** the source column metadata for a TABLE cannot be loaded
- **THEN** the TABLE is recorded as mismatched with a missing metadata note

### Requirement: Existence-only checks for non-table primary objects
The system SHALL validate existence for non-table primary objects (excluding print-only types) without column-level checks.

#### Scenario: View existence
- **WHEN** a VIEW is present in the source
- **THEN** the system marks it OK if the target VIEW exists or missing if it does not

### Requirement: Extra object checks
The system SHALL validate INDEX, CONSTRAINT, SEQUENCE, and TRIGGER objects against source metadata when enabled.

#### Scenario: Index comparison
- **WHEN** a TABLE has indexes in the source
- **THEN** the system compares index column sequences and uniqueness with target indexes

#### Scenario: Source index metadata missing
- **WHEN** index metadata for a source TABLE is unavailable
- **THEN** the system treats the source index set as empty and reports only target extras

#### Scenario: OMS-generated index filtering
- **WHEN** an OceanBase index matches OMS-generated patterns
- **THEN** the index is excluded from comparison

#### Scenario: SYS_NC index column normalization
- **WHEN** a source and target index have the same name but SYS_NC column names differ
- **THEN** the index is treated as matching and excluded from missing/extra results

#### Scenario: Unique index backed by constraint
- **WHEN** a source index is NONUNIQUE and the target is UNIQUE for the same column set
- **AND** the target has a PK/UK constraint on that column set
- **THEN** the uniqueness difference is treated as acceptable

#### Scenario: Constraint comparison
- **WHEN** a TABLE has PK/UK/FK constraints in the source
- **THEN** the system compares constraint column sets and referenced table information

#### Scenario: Constraint reference info unavailable
- **WHEN** target constraint reference fields cannot be loaded
- **THEN** the system falls back to basic constraint metadata for comparison

#### Scenario: OceanBase auto NOT NULL constraints
- **WHEN** an OceanBase constraint name matches the OBNOTNULL pattern
- **THEN** the constraint is excluded from comparison

#### Scenario: Sequence comparison
- **WHEN** a schema contains SEQUENCE objects in the source
- **THEN** the system checks whether corresponding sequences exist in the target schema

#### Scenario: Trigger comparison
- **WHEN** a TABLE has TRIGGER objects in the source
- **THEN** the system checks whether corresponding triggers exist in the target schema

### Requirement: Comment comparison
The system SHALL compare table and column comments when comment checking is enabled and metadata is available.

#### Scenario: Comments enabled
- **WHEN** check_comments is enabled and comment metadata is loaded
- **THEN** the system reports mismatched table or column comments

#### Scenario: Comment whitespace normalization
- **WHEN** comments differ only by whitespace
- **THEN** the system treats the comments as equivalent

#### Scenario: Comments metadata unavailable
- **WHEN** comment metadata cannot be loaded from either side
- **THEN** the comment comparison is skipped with a recorded reason

### Requirement: Dependency comparison
The system SHALL compare expected dependencies derived from source metadata with target dependencies.

#### Scenario: Missing dependency
- **WHEN** an expected dependency is not found in the target
- **THEN** the system records a missing dependency with a reason

### Requirement: PUBLIC synonym inclusion
The system SHALL include PUBLIC synonyms that reference configured source schemas even when PUBLIC is not listed in source_schemas.

#### Scenario: PUBLIC synonym points to configured schema
- **WHEN** a PUBLIC synonym references a TABLE_OWNER in source_schemas
- **THEN** the synonym is included in the comparison scope

#### Scenario: PUBLIC synonym points to other schema
- **WHEN** a PUBLIC synonym references a TABLE_OWNER not in source_schemas
- **THEN** the synonym is excluded as a system synonym
