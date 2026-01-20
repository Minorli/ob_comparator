## Context
The current comparison/fixup flow does not fully capture Oracle constraint semantics, numeric precision/scale, and function-based index details. In large migrations this leads to silent drift (CHECK constraints, FK delete rules), false positives/negatives (CHAR semantics), and incomplete fixups (virtual columns, function indexes).

## Goals / Non-Goals
- Goals:
  - Enforce parity for CHECK constraints and FK delete rules.
  - Compare NUMBER precision/scale to prevent data loss.
  - Use target CHAR_USED/DATA_LENGTH where possible for correct BYTE/CHAR semantics.
  - Detect virtual columns and compare generation expressions.
  - Compare function-based indexes by expression and enable fallback DDL generation.
- Non-Goals:
  - No change to remap semantics or schema mapping rules.
  - No change to dbcat extraction strategy outside fallback paths.
  - No new user-facing configuration switches.

## Decisions
1. Constraint handling
   - Include constraint type C (CHECK) in metadata collection and comparison.
   - Compare CHECK constraints by normalized SEARCH_CONDITION.
   - Exclude system-generated NOT NULL checks by:
     - Constraint name pattern (SYS_C%) and
     - Expression matching '<col> IS NOT NULL' normalization.
   - Capture DELETE_RULE for FK and report mismatch when rules differ.

2. Virtual columns
   - Capture VIRTUAL_COLUMN and expression (from DATA_DEFAULT) in Oracle metadata.
   - Compare virtual column presence and normalized expressions.
   - For fixup, generate ALTER TABLE ADD with GENERATED ALWAYS AS (expr).

3. NUMBER precision/scale
   - Treat mismatch when precision or scale differs.
   - Allow fixup only to widen precision/scale (no shrink).
   - Report as a type mismatch with precision/scale details.

4. CHAR semantics
   - When OB provides CHAR_USED/DATA_LENGTH, use it to determine BYTE vs CHAR semantics.
   - If OB CHAR_USED is NULL/unavailable, assume BYTE semantics and record a warning tag in mismatch details.

5. Function-based indexes
   - Capture DBA_IND_EXPRESSIONS and map expressions by COLUMN_POSITION.
   - Build index signature using expressions when present; fall back to column names otherwise.
   - In fallback DDL generation, prefer expressions when building CREATE INDEX statements.

## Risks / Trade-offs
- Expression normalization for CHECK and function-based indexes may not perfectly match across dialects.
  - Mitigation: Normalize whitespace, case, and quote usage conservatively; log when normalization is uncertain.
- Additional metadata queries increase dump time.
  - Mitigation: Batch queries with existing chunking; reuse existing connection and fallback paths.
- OB dictionary differences across versions.
  - Mitigation: Feature-detect columns (CHAR_USED, IND_EXPRESSIONS) and fall back to previous behavior.

## Migration Plan
1. Extend Oracle and OB metadata queries.
2. Update in-memory metadata models.
3. Implement comparison updates for columns, constraints, and indexes.
4. Update fixup generation for CHECK/FK rules, virtual columns, precision/scale, and function indexes.
5. Add unit + integration tests and validate reports.

Rollback: revert to prior metadata fields and comparison rules; fixup generation reverts to PK/UK/FK-only and column-length-only behavior.

## Test Plan (detailed cases)

### TC-P0-01 CHECK constraints
- Oracle setup:
  - CREATE TABLE T_CHECK (
    ID NUMBER PRIMARY KEY,
    AMOUNT NUMBER CHECK (AMOUNT > 0),
    STATUS VARCHAR2(10) CHECK (STATUS IN ('A','B')),
    NOTE VARCHAR2(10) NOT NULL
  );
- OB setup:
  - CREATE TABLE T_CHECK (ID NUMBER PRIMARY KEY, AMOUNT NUMBER, STATUS VARCHAR2(10), NOTE VARCHAR2(10));
- Expected comparison:
  - Missing CHECK constraints for AMOUNT and STATUS.
  - NOT NULL check for NOTE is ignored (covered by column nullability).
- Expected fixup:
  - ALTER TABLE ... ADD CONSTRAINT ... CHECK (AMOUNT > 0)
  - ALTER TABLE ... ADD CONSTRAINT ... CHECK (STATUS IN ('A','B'))

### TC-P0-02 FK delete rule
- Oracle setup:
  - CREATE TABLE PARENT (ID NUMBER PRIMARY KEY);
  - CREATE TABLE CHILD (
      ID NUMBER PRIMARY KEY,
      PID NUMBER,
      CONSTRAINT FK_CHILD FOREIGN KEY (PID) REFERENCES PARENT(ID) ON DELETE CASCADE
    );
- OB setup:
  - CREATE TABLE PARENT (ID NUMBER PRIMARY KEY);
  - CREATE TABLE CHILD (ID NUMBER PRIMARY KEY, PID NUMBER);
- Expected comparison:
  - FK missing or delete rule mismatch reported.
- Expected fixup:
  - ALTER TABLE CHILD ADD CONSTRAINT FK_CHILD FOREIGN KEY (PID) REFERENCES PARENT(ID) ON DELETE CASCADE;

### TC-P0-03 NUMBER precision/scale
- Oracle setup:
  - CREATE TABLE T_NUM (A NUMBER(10,2), B NUMBER(8));
- OB setup:
  - CREATE TABLE T_NUM (A NUMBER(8,2), B NUMBER(8,2));
- Expected comparison:
  - A precision too small; B scale mismatch.
- Expected fixup:
  - ALTER TABLE T_NUM MODIFY (A NUMBER(10,2));
  - ALTER TABLE T_NUM MODIFY (B NUMBER(8));

### TC-P0-04 CHAR semantics
- Oracle setup:
  - CREATE TABLE T_CHAR (C1 VARCHAR2(10 CHAR), C2 VARCHAR2(10 BYTE));
- OB setup:
  - CREATE TABLE T_CHAR (C1 VARCHAR2(10 BYTE), C2 VARCHAR2(15 BYTE));
- Expected comparison:
  - C1 semantics mismatch; C2 length within range.
- Expected fixup:
  - ALTER TABLE T_CHAR MODIFY (C1 VARCHAR2(10 CHAR));

### TC-P1-01 Virtual columns
- Oracle setup:
  - CREATE TABLE T_VIRT (AMT NUMBER, TAX NUMBER GENERATED ALWAYS AS (AMT * 0.1) VIRTUAL);
- OB setup:
  - CREATE TABLE T_VIRT (AMT NUMBER);
- Expected comparison:
  - TAX virtual column missing or expression mismatch.
- Expected fixup:
  - ALTER TABLE T_VIRT ADD (TAX NUMBER GENERATED ALWAYS AS (AMT * 0.1) VIRTUAL);

### TC-P1-02 Function-based indexes
- Oracle setup:
  - CREATE TABLE T_IDX (NAME VARCHAR2(30));
  - CREATE INDEX IDX_UPPER_NAME ON T_IDX (UPPER(NAME));
- OB setup:
  - CREATE TABLE T_IDX (NAME VARCHAR2(30));
- Expected comparison:
  - IDX_UPPER_NAME missing by expression-based signature.
- Expected fixup:
  - CREATE INDEX IDX_UPPER_NAME ON T_IDX (UPPER(NAME));

### TC-P1-03 OB metadata fallback
- Unit test only:
  - Simulate missing CHAR_USED or IND_EXPRESSIONS columns by toggling feature flags in the metadata loader.
- Expected comparison:
  - Fallback to BYTE semantics and SYS_NC normalization with warning tags.

### TC-P1-04 Partitioned PK compatibility regression
- Ensure partitioned PK downgrade logic remains unchanged and still produces expected UNIQUE fallback.

## Open Questions
- Should CHECK constraint comparison ignore DISABLED constraints or compare enabled-only (current behavior)?
- Should NUMBER precision/scale allow target scale increase when precision also increases (to preserve integer digits)?
