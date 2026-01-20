## MODIFIED Requirements

### Requirement: One-time metadata dump
The system SHALL dump Oracle and OceanBase metadata once per run and use in-memory data for comparisons.

#### Scenario: Oracle metadata load succeeds
- **WHEN** the run starts with valid Oracle connectivity and permissions
- **THEN** DBA_OBJECTS/DBA_TAB_COLUMNS/DBA_INDEXES/DBA_CONSTRAINTS/DBA_TRIGGERS/DBA_SEQUENCES are loaded in batch

#### Scenario: Oracle metadata includes extended column fields
- **WHEN** Oracle column metadata is loaded
- **THEN** the system captures CHAR_USED, CHAR_LENGTH, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, HIDDEN_COLUMN, and VIRTUAL_COLUMN

#### Scenario: Oracle metadata includes check constraints and delete rules
- **WHEN** Oracle constraint metadata is loaded
- **THEN** the system captures CONSTRAINT_TYPE (including CHECK), DELETE_RULE, and SEARCH_CONDITION where available

#### Scenario: Oracle metadata includes function-based index expressions
- **WHEN** Oracle index metadata is loaded
- **THEN** the system loads DBA_IND_EXPRESSIONS and associates COLUMN_EXPRESSION with index column positions

#### Scenario: OceanBase metadata load succeeds
- **WHEN** the run starts with valid OceanBase connectivity
- **THEN** DBA_OBJECTS and related metadata are loaded in batch via obclient

#### Scenario: OceanBase metadata includes CHAR semantics when available
- **WHEN** OceanBase column metadata is loaded
- **THEN** the system captures CHAR_USED and DATA_LENGTH when the fields are available, otherwise records a safe fallback

#### Scenario: Metadata load failure
- **WHEN** a required metadata query fails
- **THEN** the system terminates the run with an error

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

#### Scenario: CHAR semantics mismatch reported
- **WHEN** the source uses CHAR semantics and the target does not
- **THEN** the column is recorded as a length/semantics mismatch

#### Scenario: NUMBER precision/scale mismatch
- **WHEN** a common column is NUMBER/DECIMAL/NUMERIC with precision or scale differences
- **THEN** the column is recorded as a type mismatch with precision/scale details

#### Scenario: Virtual column missing
- **WHEN** a source virtual column is missing in the target
- **THEN** the TABLE is recorded as mismatched with the missing virtual column noted

#### Scenario: Virtual column expression mismatch
- **WHEN** a virtual column exists on both sides but the generation expression differs
- **THEN** the TABLE is recorded as mismatched with the expression difference noted

#### Scenario: LONG type conversion validation
- **WHEN** a common column is LONG or LONG RAW in the source
- **THEN** the target column type MUST be CLOB or BLOB and the table is marked mismatched if not

#### Scenario: LONG mapped to CLOB
- **WHEN** the source column type is LONG and the target column type is CLOB
- **THEN** the column is treated as matching for comparison

#### Scenario: LONG RAW mapped to BLOB
- **WHEN** the source column type is LONG RAW and the target column type is BLOB
- **THEN** the column is treated as matching for comparison

#### Scenario: Source column metadata missing
- **WHEN** the source column metadata for a TABLE cannot be loaded
- **THEN** the TABLE is recorded as mismatched with a missing metadata note

### Requirement: Extra object checks
The system SHALL validate INDEX, CONSTRAINT, SEQUENCE, and TRIGGER objects against source metadata when enabled.

#### Scenario: Index comparison
- **WHEN** a TABLE has indexes in the source
- **THEN** the system compares index column sequences and uniqueness with target indexes

#### Scenario: Function-based index comparison
- **WHEN** an index column is defined by an expression in the source
- **THEN** the system compares target index expressions to the source expression instead of SYS_NC column names

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
- **WHEN** a TABLE has PK/UK/FK/CK constraints in the source
- **THEN** the system compares constraint column sets, referenced table information, and CHECK expressions

#### Scenario: Foreign key delete rule mismatch
- **WHEN** a source FK has a DELETE_RULE that differs from the target FK
- **THEN** the constraint is recorded as mismatched with the delete rule difference

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
