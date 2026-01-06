# generate-fixup

## MODIFIED Requirements

### Requirement: Compile and grant scripts
The system SHALL generate compile scripts for missing dependencies and GRANT scripts derived from source privileges and dependency-based grants when generate_grants is enabled.

#### Scenario: Grant generation enabled
- **WHEN** generate_grants is true
- **THEN** fixup_scripts/grants contains grant SQL for object, role, and system privileges

#### Scenario: Grant generation disabled
- **WHEN** generate_grants is false
- **THEN** no grant scripts are generated and no grant statements are appended to object DDL

## ADDED Requirements

### Requirement: Source privilege remap
The system SHALL remap Oracle object privileges from DBA_TAB_PRIVS to target objects and schemas using explicit and inferred remap rules.

#### Scenario: Remapped object grant
- **WHEN** a source object has a privilege and its target object is remapped
- **THEN** the generated GRANT targets the remapped object and remapped grantee schema

#### Scenario: WITH GRANT OPTION
- **WHEN** GRANTABLE is YES in DBA_TAB_PRIVS
- **THEN** the GRANT statement includes WITH GRANT OPTION

### Requirement: Role and system grant preservation
The system SHALL emit GRANT statements for DBA_ROLE_PRIVS and DBA_SYS_PRIVS entries, preserving ADMIN OPTION, and remapping grantee schemas when applicable.

#### Scenario: Role grant
- **WHEN** a role is granted to a schema in Oracle
- **THEN** the system emits GRANT <role> TO <grantee> [WITH ADMIN OPTION]

#### Scenario: System privilege
- **WHEN** a system privilege exists in Oracle
- **THEN** the system emits GRANT <privilege> TO <grantee> [WITH ADMIN OPTION]

### Requirement: Dependency-derived grants
The system SHALL add cross-schema grants required by remapped dependency edges and deep view dependency chains.

#### Scenario: Direct dependency edge
- **WHEN** a dependent object references a target object in a different schema
- **THEN** the system adds the required privilege for that referenced object

#### Scenario: View transitive dependency
- **WHEN** a VIEW or MATERIALIZED VIEW depends on a chain of objects across schemas
- **THEN** the system adds grants for all referenced objects along the chain (cross-schema only)

### Requirement: Grant DDL injection
The system SHALL append object-level GRANT statements to per-object fixup DDL when those grants target the created object.

#### Scenario: Object DDL includes grants
- **WHEN** an object has remapped source privileges for the target object
- **THEN** its fixup DDL includes those GRANT statements after the CREATE/ALTER statement
