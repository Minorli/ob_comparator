## MODIFIED Requirements

### Requirement: Statement-level execution
The fixup executor SHALL execute SQL scripts statement-by-statement and continue after statement failures. When a script contains ALTER SESSION SET CURRENT_SCHEMA, the executor SHALL apply that directive to subsequent statements executed in separate sessions.

#### Scenario: Current schema directive applied
- **WHEN** a script begins with ALTER SESSION SET CURRENT_SCHEMA and contains unqualified DDL
- **THEN** each subsequent statement is executed with that directive so the intended schema is used

### Requirement: Recompile retries
The fixup executor SHALL optionally recompile INVALID objects when --recompile is enabled, and SHALL skip recompilation for object types not supported by OceanBase compile syntax (VIEW, MATERIALIZED VIEW, TYPE BODY).

#### Scenario: Recompile enabled
- **WHEN** --recompile is used
- **THEN** the executor attempts recompilation for INVALID objects up to the max retry count

#### Scenario: Unsupported compile type
- **WHEN** an INVALID object is VIEW, MATERIALIZED VIEW, or TYPE BODY
- **THEN** the executor skips recompilation for that object and records a skip message
