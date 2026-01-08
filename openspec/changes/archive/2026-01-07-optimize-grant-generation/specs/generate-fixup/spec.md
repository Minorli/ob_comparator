# generate-fixup

## ADDED Requirements

### Requirement: GRANT statement compaction
The system SHALL optionally compact object GRANT statements when grant_merge_privileges and/or grant_merge_grantees are enabled.

#### Scenario: Merge privileges for a grantee
- **WHEN** multiple privileges exist for the same grantee/object/grantable and grant_merge_privileges is true
- **THEN** a single GRANT statement with multiple privileges is emitted

#### Scenario: Merge grantees for a privilege
- **WHEN** multiple grantees share the same object/privilege/grantable and grant_merge_grantees is true
- **THEN** a single GRANT statement with multiple grantees is emitted
