## MODIFIED Requirements
### Requirement: Grant plan generation compatibility
The system SHALL generate executable grant plans for OceanBase-compatible role semantics and SHALL remap Oracle-only catalog role grants to the configured OB-compatible catalog role.

#### Scenario: Role grant remap
- **WHEN** source role grant contains `SELECT_CATALOG_ROLE`
- **THEN** generated role grant uses `OB_CATALOG_ROLE`

#### Scenario: Object grants granted to catalog role
- **WHEN** source object privilege grantee is `SELECT_CATALOG_ROLE`
- **THEN** generated object grant grantee is `OB_CATALOG_ROLE`
