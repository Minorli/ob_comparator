## MODIFIED Requirements

### Requirement: Role and system grants
The system SHALL emit GRANT statements for DBA_ROLE_PRIVS entries and for DBA_SYS_PRIVS entries that are supported by the target OceanBase privilege catalog, preserving ADMIN OPTION when present. Unsupported system privileges SHALL be skipped with a warning summary.

#### Scenario: Role grant preserved
- **WHEN** a role is granted to a schema in Oracle
- **THEN** the system emits GRANT <role> TO <grantee> [WITH ADMIN OPTION]

#### Scenario: Supported system privilege preserved
- **WHEN** a system privilege exists in Oracle and is supported by the target OB catalog
- **THEN** the system emits GRANT <privilege> TO <grantee> [WITH ADMIN OPTION]

#### Scenario: Unsupported system privilege skipped
- **WHEN** a system privilege exists in Oracle but is not supported by the target OB catalog
- **THEN** the system skips the GRANT and logs a warning summary

## ADDED Requirements

### Requirement: Object privilege compatibility filtering
The system SHALL filter object-level GRANT statements to a supported privilege allowlist and skip unsupported object privileges with a warning summary.

#### Scenario: Unsupported object privilege skipped
- **WHEN** a table privilege is not in the supported allowlist
- **THEN** the system omits the GRANT statement and records it in the warning summary

#### Scenario: MERGE VIEW privilege filtered
- **WHEN** a GRANT statement contains MERGE VIEW
- **THEN** the system skips it as unsupported in OceanBase

### Requirement: Role DDL generation
The system SHALL generate CREATE ROLE statements for user-defined roles referenced by grants and emit them before any GRANT statements that reference those roles.

#### Scenario: Custom role created
- **WHEN** a role referenced in grants is user-defined
- **THEN** a CREATE ROLE statement is emitted before the GRANTs

#### Scenario: Oracle-maintained role skipped
- **WHEN** a role is marked ORACLE_MAINTAINED and the include switch is false
- **THEN** no CREATE ROLE statement is emitted

#### Scenario: Role authentication type unknown
- **WHEN** a role requires a password or external authentication
- **THEN** the system emits CREATE ROLE with NOT IDENTIFIED and logs a warning for manual follow-up
