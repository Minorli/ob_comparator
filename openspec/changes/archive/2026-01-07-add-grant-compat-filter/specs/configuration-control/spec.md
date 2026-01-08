## MODIFIED Requirements

### Requirement: Oracle privilege metadata sources
The system SHALL read Oracle privilege metadata from DBA_TAB_PRIVS, DBA_SYS_PRIVS, DBA_ROLE_PRIVS, and DBA_ROLES when grant generation is enabled.

#### Scenario: Privilege metadata load
- **WHEN** generate_grants is true
- **THEN** privilege and role metadata are loaded and cached for grant generation

## ADDED Requirements

### Requirement: Grant compatibility settings
The system SHALL allow optional configuration overrides for supported system privileges, supported object privileges, and Oracle-maintained role inclusion.

#### Scenario: Default supported privileges
- **WHEN** no override is provided
- **THEN** system privileges are derived from the target OB catalog and object privileges use the built-in allowlist

#### Scenario: Supported privilege override
- **WHEN** grant_supported_sys_privs or grant_supported_object_privs is configured
- **THEN** the system uses the configured lists instead of defaults

#### Scenario: Oracle-maintained roles toggle
- **WHEN** grant_include_oracle_maintained_roles is false
- **THEN** roles marked ORACLE_MAINTAINED are skipped in CREATE ROLE generation
