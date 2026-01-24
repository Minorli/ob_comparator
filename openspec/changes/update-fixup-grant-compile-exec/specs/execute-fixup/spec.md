## MODIFIED Requirements

### Requirement: Statement-level execution
The fixup executor SHALL execute SQL scripts statement-by-statement and continue after statement failures. When a script contains ALTER SESSION SET CURRENT_SCHEMA, the executor SHALL apply that directive to subsequent statements executed in separate sessions.

#### Scenario: Current schema directive applied
- **WHEN** a script begins with ALTER SESSION SET CURRENT_SCHEMA and contains unqualified DDL
- **THEN** each subsequent statement is executed with that directive so the intended schema is used
