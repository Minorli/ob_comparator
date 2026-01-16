## ADDED Requirements

### Requirement: Unsupported DDL segregation
The system SHALL route DDL for unsupported or blocked objects to dedicated directories and exclude them from fixup execution.

#### Scenario: Unsupported table DDL
- **WHEN** a TABLE is classified as unsupported
- **THEN** its CREATE DDL is written under fixup_scripts/tables_unsupported and not queued for fixup

#### Scenario: Blocked dependent object
- **WHEN** a VIEW/SYNONYM/TRIGGER/PLSQL object is blocked by unsupported dependencies
- **THEN** its DDL is written under fixup_scripts/unsupported/<type> with a blocking reason

### Requirement: Temporary table directory split
The system SHALL separate temporary table DDL output from regular table DDL output.

#### Scenario: Temporary table detected
- **WHEN** a TABLE is temporary
- **THEN** its DDL is written under a temporary table subdirectory and excluded from supported fixups
