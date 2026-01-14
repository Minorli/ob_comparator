## MODIFIED Requirements
### Requirement: Print-only object fixup behavior
The system SHALL not generate fixup DDL for print-only primary types (MATERIALIZED VIEW).

#### Scenario: Missing materialized view
- **WHEN** a MATERIALIZED VIEW is missing in the target
- **THEN** it is reported as print-only and no fixup script is generated

## ADDED Requirements
### Requirement: Package fixup DDL generation
The system SHALL generate PACKAGE and PACKAGE BODY DDL scripts for missing objects when fixup generation is enabled and filters allow the types.

#### Scenario: Missing package
- **WHEN** a PACKAGE is missing in the target and fixup_types allows PACKAGE
- **THEN** a CREATE PACKAGE script is emitted under fixup_scripts/package

#### Scenario: Missing package body
- **WHEN** a PACKAGE BODY is missing in the target and fixup_types allows PACKAGE BODY
- **THEN** a CREATE PACKAGE BODY script is emitted under fixup_scripts/package_body

#### Scenario: Package DDL source fallback
- **WHEN** dbcat does not provide PACKAGE/PACKAGE BODY DDL for a missing object
- **THEN** the system falls back to DBMS_METADATA for that package DDL
