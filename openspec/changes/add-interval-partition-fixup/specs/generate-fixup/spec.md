## ADDED Requirements
### Requirement: Interval partition DDL cleanup
The system SHALL remove INTERVAL partition clauses from generated CREATE TABLE DDL to ensure OceanBase compatibility.

#### Scenario: Interval clause removed
- **WHEN** a generated CREATE TABLE DDL contains an `INTERVAL (...)` clause
- **THEN** the INTERVAL clause is removed before writing the fixup script

### Requirement: Interval partition expansion scripts
The system SHALL generate ADD PARTITION scripts for interval-partitioned tables when interval fixup is enabled.

#### Scenario: Interval table detected
- **WHEN** a source TABLE is identified as interval-partitioned
- **AND** `generate_interval_partition_fixup` is true with a valid cutoff date
- **THEN** the system generates `ALTER TABLE ... ADD PARTITION` statements up to the cutoff date

#### Scenario: Output directory
- **WHEN** interval partition scripts are generated
- **THEN** they are written under `fixup_scripts/table_alter/interval_add_<cutoff>/` with one file per table

#### Scenario: Unsupported interval expression
- **WHEN** the interval expression cannot be parsed
- **THEN** the system logs a warning and skips that table's interval expansion script
