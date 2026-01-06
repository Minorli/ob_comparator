# configuration-control

## ADDED Requirements

### Requirement: Grant generation toggle
The system SHALL provide a generate_grants setting to control grant DDL generation and injection into fixup scripts.

#### Scenario: Grant generation disabled
- **WHEN** generate_grants is false
- **THEN** no grant SQL is generated and no grant statements are injected into fixup DDL

#### Scenario: Grant generation enabled
- **WHEN** generate_grants is true
- **THEN** the system loads privilege metadata and generates grant SQL under fixup_scripts/grants

### Requirement: Oracle privilege metadata sources
The system SHALL read Oracle privilege metadata from DBA_TAB_PRIVS, DBA_SYS_PRIVS, and DBA_ROLE_PRIVS when grant generation is enabled.

#### Scenario: Privilege metadata load
- **WHEN** generate_grants is true
- **THEN** privilege data is loaded and cached for grant generation
