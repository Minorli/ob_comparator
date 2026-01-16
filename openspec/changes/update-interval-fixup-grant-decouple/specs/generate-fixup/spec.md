## ADDED Requirements
### Requirement: Numeric interval partition expansion scripts
The system SHALL generate ADD PARTITION scripts for numeric interval-partitioned tables when numeric cutoff is configured.

#### Scenario: Numeric interval table detected
- **WHEN** a source TABLE is interval-partitioned on a numeric key
- **AND** generate_interval_partition_fixup is true
- **AND** interval_partition_cutoff_numeric is a valid positive number
- **THEN** the system generates ALTER TABLE ... ADD PARTITION statements up to the numeric cutoff

#### Scenario: Numeric cutoff missing
- **WHEN** a numeric interval table is detected but interval_partition_cutoff_numeric is missing or invalid
- **THEN** the system logs a warning and skips numeric interval expansion for that table

#### Scenario: Unsupported numeric interval expression
- **WHEN** the numeric interval expression cannot be parsed
- **THEN** the system logs a warning and skips that table's numeric interval expansion

## MODIFIED Requirements
### Requirement: Compile and grant scripts
The system SHALL generate compile scripts for missing dependencies and GRANT scripts derived from Oracle privileges and dependency-based grants when generate_grants is enabled, emitting both missing-grant and full-audit outputs.

#### Scenario: Missing dependency
- **WHEN** a dependent object exists but required dependencies are missing in the target
- **THEN** an ALTER ... COMPILE script is produced in fixup_scripts/compile

#### Scenario: Grant generation enabled
- **WHEN** generate_grants is true
- **THEN** fixup_scripts/grants_all contains object, role, and system GRANT statements, and fixup_scripts/grants_miss contains missing grants only

#### Scenario: Grant generation disabled
- **WHEN** generate_grants is false
- **THEN** no GRANT statements are emitted in fixup outputs

#### Scenario: Fixup generation remains enabled
- **WHEN** generate_fixup is true and generate_grants is false
- **THEN** non-grant fixup scripts are still generated
