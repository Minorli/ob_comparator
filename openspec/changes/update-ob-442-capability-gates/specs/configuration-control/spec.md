## ADDED Requirements
### Requirement: OB version-aware feature gates
The system SHALL support version-aware feature gating for interval partition fixup and MATERIALIZED VIEW check/fixup behaviors.

#### Scenario: Auto gate on OB >= 4.4.2
- **WHEN** OceanBase version is parsed as `>= 4.4.2`
- **AND** `generate_interval_partition_fixup=auto`
- **AND** `mview_check_fixup_mode=auto`
- **THEN** interval partition fixup generation is disabled by default
- **AND** MATERIALIZED VIEW check/fixup is enabled by default

#### Scenario: Auto gate on OB < 4.4.2
- **WHEN** OceanBase version is parsed as `< 4.4.2`
- **AND** `generate_interval_partition_fixup=auto`
- **AND** `mview_check_fixup_mode=auto`
- **THEN** interval partition fixup generation remains enabled by default
- **AND** MATERIALIZED VIEW remains print-only by default

#### Scenario: Version unknown fallback
- **WHEN** OceanBase version cannot be parsed
- **AND** either gate is set to `auto`
- **THEN** the system falls back to legacy-safe behavior (interval enabled, MATERIALIZED VIEW print-only)
- **AND** logs the fallback decision explicitly

#### Scenario: Manual override
- **WHEN** `generate_interval_partition_fixup` is explicitly set to `true` or `false`
- **THEN** the configured value overrides auto gate decisions
- **AND** the same applies to `mview_check_fixup_mode=on|off`

