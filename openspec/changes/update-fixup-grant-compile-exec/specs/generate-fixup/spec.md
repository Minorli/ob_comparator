## MODIFIED Requirements

### Requirement: Compile and grant scripts
The system SHALL generate compile scripts for missing dependencies and GRANT scripts derived from Oracle privileges and dependency-based grants when generate_grants is enabled, emitting both missing-grant and full-audit outputs. Unsupported target types (VIEW/MATERIALIZED VIEW) SHALL be excluded from compile outputs.

#### Scenario: View compile omitted
- **WHEN** a missing dependency is a VIEW or MATERIALIZED VIEW
- **THEN** no ALTER ... COMPILE script is generated for that object

#### Scenario: Grant generation enabled
- **WHEN** generate_grants is true
- **THEN** fixup_scripts/grants_all contains object, role, and system GRANT statements, and fixup_scripts/grants_miss contains missing grants only

### Requirement: Grant script schema scope
The system SHALL emit GRANT statements only for objects owned by configured source schemas or their remapped target schemas, and SHALL not rely on ALTER SESSION SET CURRENT_SCHEMA in grant scripts.

#### Scenario: System owner skipped
- **WHEN** a dependency-derived grant references an object owned by SYS or PUBLIC
- **THEN** the GRANT statement is omitted from fixup outputs

#### Scenario: Fully-qualified grant output
- **WHEN** GRANT statements are generated
- **THEN** object names are schema-qualified and no ALTER SESSION SET CURRENT_SCHEMA header is emitted
