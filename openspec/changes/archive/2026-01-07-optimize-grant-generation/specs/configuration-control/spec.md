# configuration-control

## ADDED Requirements

### Requirement: Grant extraction scope
The system SHALL support grant_tab_privs_scope to control how DBA_TAB_PRIVS is filtered.

#### Scenario: Owner-only scope
- **WHEN** grant_tab_privs_scope is set to `owner`
- **THEN** object privileges are loaded only for objects owned by the configured source schemas

#### Scenario: Owner-or-grantee scope
- **WHEN** grant_tab_privs_scope is set to `owner_or_grantee`
- **THEN** object privileges are loaded for objects owned by the source schemas and for grants where grantee is in scope

### Requirement: Grant statement merging toggles
The system SHALL support grant_merge_privileges and grant_merge_grantees to control GRANT statement compaction.

#### Scenario: Merge privileges enabled
- **WHEN** grant_merge_privileges is true
- **THEN** multiple privileges for the same grantee/object/grantable are merged into one GRANT statement

#### Scenario: Merge grantees enabled
- **WHEN** grant_merge_grantees is true
- **THEN** multiple grantees for the same object/privilege/grantable are merged into one GRANT statement
