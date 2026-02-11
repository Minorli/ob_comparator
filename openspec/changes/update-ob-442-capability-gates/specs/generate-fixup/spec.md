## MODIFIED Requirements
### Requirement: Missing object DDL generation
The system SHALL generate CREATE DDL scripts for missing objects by type and store them in fixup_scripts subdirectories, with MATERIALIZED VIEW generation controlled by the effective feature gate.

#### Scenario: Missing materialized view in enabled mode
- **WHEN** a MATERIALIZED VIEW is missing in the target
- **AND** `effective_mview_enabled=true`
- **THEN** a CREATE MATERIALIZED VIEW script is emitted under `fixup_scripts/materialized_view`

#### Scenario: Missing materialized view in print-only mode
- **WHEN** a MATERIALIZED VIEW is missing in the target
- **AND** `effective_mview_enabled=false`
- **THEN** it is reported as print-only and no fixup script is generated

### Requirement: dbcat export caching and parallelism
The system SHALL load DDL from dbcat_output_dir cache when available, export missing DDL via dbcat for supported types, and use metadata fallback for unsupported types including MATERIALIZED VIEW.

#### Scenario: MATERIALIZED VIEW export fallback
- **WHEN** MATERIALIZED VIEW DDL is required
- **THEN** the system does not rely on dbcat `--mview`
- **AND** uses metadata extraction fallback to build fixup DDL

### Requirement: Print-only object fixup behavior
The system SHALL not generate fixup DDL for primary types that are marked print-only by effective runtime gates.

#### Scenario: Runtime print-only type
- **WHEN** a primary type is marked print-only by gate evaluation
- **THEN** missing objects of that type are reported but skipped for fixup output

## ADDED Requirements
### Requirement: Interval partition fixup gate
The system SHALL decide interval partition fixup generation by the effective interval gate value.

#### Scenario: Interval auto disabled on OB >= 4.4.2
- **WHEN** `generate_interval_partition_fixup=auto`
- **AND** OceanBase version is `>= 4.4.2`
- **THEN** no interval partition fixup scripts are generated

#### Scenario: Interval auto enabled on OB < 4.4.2
- **WHEN** `generate_interval_partition_fixup=auto`
- **AND** OceanBase version is `< 4.4.2`
- **THEN** interval partition fixup scripts are generated when candidate tables exist

