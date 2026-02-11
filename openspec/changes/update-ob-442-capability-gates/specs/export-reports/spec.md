## ADDED Requirements
### Requirement: Feature gate visibility in reports
The system SHALL expose OceanBase version parsing results and effective gate decisions in run reports.

#### Scenario: Version-aware gate summary
- **WHEN** a comparison run completes
- **THEN** the report includes:
- configured values for `generate_interval_partition_fixup` and `mview_check_fixup_mode`
- parsed OceanBase version (or unknown)
- effective decisions for interval fixup and MATERIALIZED VIEW handling

#### Scenario: Fallback warning in report
- **WHEN** auto mode is used and OceanBase version is unknown
- **THEN** the report includes a warning that legacy-safe fallback was applied

