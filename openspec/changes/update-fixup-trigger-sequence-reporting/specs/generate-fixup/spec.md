## MODIFIED Requirements

### Requirement: Trigger DDL remap and grants
The system SHALL qualify the CREATE TRIGGER name with the remapped target schema and rewrite ON 子句与触发器体内的对象引用为 remap 后的 schema.对象 形式，并在需要时追加授权语句。该行为受 trigger_qualify_schema 开关控制。

#### Scenario: Trigger on remapped table
- **WHEN** a TRIGGER targets a TABLE remapped to another schema
- **THEN** the trigger DDL uses the remapped schema in CREATE TRIGGER and ON 子句

#### Scenario: Trigger body DML references
- **WHEN** a TRIGGER body contains INSERT INTO/UPDATE/DELETE/FROM/JOIN references
- **THEN** those object references are schema-qualified using the remapped targets

#### Scenario: Trigger qualification disabled
- **WHEN** trigger_qualify_schema is false
- **THEN** the trigger DDL preserves legacy behavior (only minimal remap)

## ADDED Requirements

### Requirement: Sequence fixup respects remap policy
The system SHALL generate SEQUENCE fixup DDL using the target schema derived from sequence_remap_policy.

#### Scenario: Sequence remap policy = source_only
- **WHEN** sequence_remap_policy is source_only
- **THEN** SEQUENCE fixup DDL uses the source schema regardless of inferred mappings

#### Scenario: Sequence remap policy = infer
- **WHEN** sequence_remap_policy is infer
- **THEN** SEQUENCE fixup DDL uses dependency-based schema inference where available

### Requirement: Index fixup skip reporting
The system SHALL report missing index fixup skip reasons and counts when missing indexes are detected but scripts are not generated.

#### Scenario: Index fixup filtered
- **WHEN** fixup_types excludes INDEX
- **THEN** the report records INDEX skip counts with reason=type_filter

#### Scenario: Index DDL missing
- **WHEN** index DDL cannot be derived from dbcat or metadata
- **THEN** the report records skip reason=ddl_missing for those indexes
