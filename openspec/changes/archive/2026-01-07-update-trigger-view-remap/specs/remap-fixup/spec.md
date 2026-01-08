## ADDED Requirements
### Requirement: Trigger Schema Preservation
The system SHALL keep TRIGGER objects in their source schema unless an explicit remap rule exists.

#### Scenario: Trigger on remapped table without explicit trigger remap
- **WHEN** a TRIGGER belongs to a table that is remapped to another schema
- **AND** there is no explicit TRIGGER remap rule
- **THEN** the TRIGGER target schema remains the source schema

### Requirement: View Schema Preservation
The system SHALL keep VIEW and MATERIALIZED VIEW objects in their source schema unless explicitly remapped.

#### Scenario: View without explicit remap
- **WHEN** a VIEW or MATERIALIZED VIEW has no explicit remap rule
- **THEN** the VIEW target schema remains the source schema

### Requirement: Trigger Fixup Grants
The system SHALL include required cross-schema GRANT statements alongside TRIGGER fixup scripts.

#### Scenario: Trigger depends on remapped table
- **WHEN** a TRIGGER depends on a table in another schema after remap
- **THEN** the TRIGGER fixup output includes the needed GRANT statements for that dependency

### Requirement: Trigger DDL Table Remap
The system SHALL rewrite TRIGGER DDL table references (including the ON clause) to use remapped table names while preserving the TRIGGER schema.

#### Scenario: Trigger DDL rewrite
- **WHEN** a TRIGGER is generated for a remapped table
- **THEN** the ON clause references the remapped table schema and name

### Requirement: Type Scope Enforcement
The system SHALL respect `check_primary_types` and `check_extra_types` for remap inference, dependency checks, and validation scope.

#### Scenario: Only tables enabled
- **WHEN** `check_primary_types` is configured to include only TABLE
- **THEN** schema inference and validation for PACKAGE/PACKAGE BODY are skipped

### Requirement: Print-Only Types
The system SHALL treat MATERIALIZED VIEW and PACKAGE/PACKAGE BODY as print-only by default.

#### Scenario: Materialized view unsupported
- **WHEN** a MATERIALIZED VIEW is present in the source schema
- **THEN** the report lists the object but no OceanBase validation or fixup is performed

#### Scenario: Package print-only
- **WHEN** a PACKAGE or PACKAGE BODY is present in the source schema
- **THEN** the report lists the object but no OceanBase validation or fixup is performed
