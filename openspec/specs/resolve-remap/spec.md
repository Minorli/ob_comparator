# resolve-remap

## Purpose
Define remap resolution rules, inference boundaries, and conflict handling.

## Requirements

### Requirement: Explicit remap precedence
The system SHALL apply explicit remap rules with highest priority and bypass inference when a rule exists.

#### Scenario: Explicit remap present
- **WHEN** remap_rules.txt maps SRC.A to TGT.A
- **THEN** the target for SRC.A is TGT.A regardless of inferred schema mapping

#### Scenario: Explicit TYPE BODY rule
- **WHEN** remap_rules.txt includes a TYPE BODY rule for SRC.TY BODY
- **THEN** the TYPE BODY target is derived from that explicit rule and applied to the TYPE BODY mapping

### Requirement: Explicit rule validation
The system SHALL record remap rules that reference missing source objects as extraneous and exclude them from the active mapping.

#### Scenario: Remap rule references missing object
- **WHEN** remap_rules.txt contains a source object that does not exist in Oracle metadata
- **THEN** the rule is recorded as extraneous and not applied

#### Scenario: Invalid rule export
- **WHEN** extraneous remap rules are detected and a remap file path is provided
- **THEN** the system writes the invalid rules to a remap_rules_invalid file without modifying the original

#### Scenario: BODY alias support
- **WHEN** a remap rule uses the BODY suffix for PACKAGE BODY or TYPE BODY
- **THEN** the rule is treated as a valid alias for the underlying object

### Requirement: Multi-to-one remap protection
The system SHALL detect multiple source objects mapping to the same target and fall back the later object to a 1:1 mapping.

#### Scenario: Duplicate target mapping
- **WHEN** two different source objects map to the same target name and type
- **THEN** the latter mapping is reset to source=target and a warning is logged

### Requirement: No-infer schema types
The system SHALL keep VIEW, MATERIALIZED VIEW, TRIGGER, PACKAGE, and PACKAGE BODY in the source schema unless explicitly remapped.

#### Scenario: Trigger on remapped table without explicit trigger remap
- **WHEN** a TRIGGER depends on a remapped TABLE but has no explicit TRIGGER remap
- **THEN** the TRIGGER target schema remains the source schema

#### Scenario: View without explicit remap
- **WHEN** a VIEW or MATERIALIZED VIEW has no explicit remap rule
- **THEN** the target schema remains the source schema

### Requirement: Paired object schema consistency
The system SHALL keep PACKAGE/PACKAGE BODY and TYPE/TYPE BODY mappings aligned to the same target schema.

#### Scenario: Package and body mismatch
- **WHEN** PACKAGE and PACKAGE BODY inference result in different target schemas
- **THEN** the system aligns both to a single target based on explicit rules or BODY inference

### Requirement: Parent-follow remap for dependent objects
The system SHALL map INDEX and CONSTRAINT objects to the target schema of their parent TABLE.

#### Scenario: Index follows remapped table
- **WHEN** TABLE SRC.T1 is remapped to TGT.T1
- **THEN** SRC.IDX1 is mapped to TGT.IDX1

### Requirement: Constraint follows remapped table
The system SHALL map constraints to the target schema of their parent TABLE.

#### Scenario: Constraint follows remapped table
- **WHEN** TABLE SRC.T1 is remapped to TGT.T1
- **THEN** SRC.PK_T1 is mapped to TGT.PK_T1

### Requirement: Sequence remap via dependencies
The system SHALL infer SEQUENCE targets based on dependent object remaps when no explicit rule exists.

#### Scenario: Trigger uses sequence
- **WHEN** a TRIGGER depends on a SEQUENCE and the TRIGGER target schema is inferred
- **THEN** the SEQUENCE is inferred to the same target schema

#### Scenario: Sequence inference conflict
- **WHEN** dependent objects map the SEQUENCE to multiple target schemas
- **THEN** the SEQUENCE is recorded as a remap conflict

### Requirement: PUBLIC synonym preservation
The system SHALL preserve the PUBLIC owner for PUBLIC synonyms unless an explicit remap rule exists.

#### Scenario: PUBLIC synonym default behavior
- **WHEN** a synonym is owned by PUBLIC and no explicit remap is provided
- **THEN** the synonym remains in the PUBLIC schema

### Requirement: Synonym dependency inference
The system SHALL infer non-PUBLIC synonym targets based on direct dependencies when no explicit rule exists.

#### Scenario: Synonym depends on remapped table
- **WHEN** a non-PUBLIC synonym references a TABLE remapped to another schema
- **THEN** the synonym is inferred to that target schema

### Requirement: Dependency-driven remap for code objects
The system SHALL infer target schemas for PROCEDURE, FUNCTION, TYPE, TYPE BODY, and SYNONYM based on dependencies when no explicit rule exists.

#### Scenario: Procedure depends on remapped view
- **WHEN** a PROCEDURE references a VIEW remapped to schema X
- **THEN** the PROCEDURE is inferred to schema X

### Requirement: Schema mapping fallback
The system SHALL derive a schema mapping from TABLE remaps and apply it as a fallback for other objects when inference is enabled.

#### Scenario: Many-to-one table remap
- **WHEN** multiple source schemas map to a single target schema via TABLE remaps
- **THEN** other objects can inherit that target schema by fallback mapping

### Requirement: Inference toggle
The system SHALL disable schema inference when infer_schema_mapping is false.

#### Scenario: Inference disabled
- **WHEN** infer_schema_mapping is false and no explicit rule exists
- **THEN** objects remain in their source schema unless handled by parent-follow rules

### Requirement: Remap conflict handling
The system SHALL detect ambiguous remap inference and record conflicts for explicit user resolution.

#### Scenario: One-to-many remap conflict
- **WHEN** an object depends on tables in multiple target schemas and no explicit rule exists
- **THEN** the object is recorded in remap_conflicts and excluded from mapping

#### Scenario: Conflict exclusion from master list
- **WHEN** an object is recorded in remap_conflicts
- **THEN** it is excluded from the primary check list and fixup mapping

### Requirement: Type gating for remap inference
The system SHALL restrict remap inference and mapping to the enabled object types defined by check_primary_types and check_extra_types.

#### Scenario: TABLE-only primary types
- **WHEN** check_primary_types is configured to only include TABLE
- **THEN** non-TABLE types are excluded from remap inference and mapping
