## ADDED Requirements
### Requirement: Interval partition fixup settings
The system SHALL support `generate_interval_partition_fixup` and `interval_partition_cutoff` to control interval partition DDL generation.

#### Scenario: Default interval fixup disabled
- **WHEN** `generate_interval_partition_fixup` is missing
- **THEN** the system defaults it to false and does not generate interval partition DDL

#### Scenario: Default cutoff date
- **WHEN** `interval_partition_cutoff` is missing
- **THEN** the system defaults it to 20280301

#### Scenario: Invalid cutoff date
- **WHEN** `generate_interval_partition_fixup` is true and `interval_partition_cutoff` is invalid
- **THEN** the system logs a warning and skips interval partition DDL generation

#### Scenario: Valid cutoff date
- **WHEN** `generate_interval_partition_fixup` is true and `interval_partition_cutoff` is a valid YYYYMMDD value
- **THEN** the system uses it as the upper bound for partition expansion
