## ADDED Requirements

### Requirement: Grant grantee existence filtering
The system SHALL filter GRANT statements so that only existing OceanBase users or roles are targeted (PUBLIC is always allowed).

#### Scenario: Missing grantee
- **WHEN** a GRANT statement targets a user or role that does not exist in OceanBase
- **THEN** the system skips that GRANT statement
- **AND** logs a warning identifying the missing grantee
