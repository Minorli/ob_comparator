## MODIFIED Requirements

### Requirement: Compile and grant scripts
The system SHALL generate compile scripts for missing dependencies and GRANT scripts derived from Oracle privileges and dependency-based grants when generate_grants is enabled. It SHALL output both full and missing-only grant scripts, where missing-only is computed by comparing against OceanBase privilege catalogs.

#### Scenario: Missing dependency
- **WHEN** a dependent object exists but required dependencies are missing in the target
- **THEN** an ALTER ... COMPILE script is produced in fixup_scripts/compile

#### Scenario: Grant generation enabled
- **WHEN** generate_grants is true
- **THEN** fixup_scripts/grants_all contains full expected grants
- **AND** fixup_scripts/grants_miss contains only grants missing in OceanBase

#### Scenario: Grant generation disabled
- **WHEN** generate_grants is false
- **THEN** no GRANT statements are emitted in fixup outputs
