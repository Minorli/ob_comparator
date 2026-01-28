## MODIFIED Requirements

### Requirement: SQL rewrite safety
The system SHALL avoid rewriting object references inside string literals, comments, **and table aliases**.

#### Scenario: Alias should not be remapped
- **WHEN** a VIEW DDL contains `FROM SCHEMA_A.TABLE1 T` and an object `SCHEMA_X.T` is remapped
- **THEN** the alias `T` is preserved and not rewritten to a qualified name

#### Scenario: Qualified reference still remapped
- **WHEN** a VIEW DDL contains `FROM SCHEMA_A.TABLE1` and SCHEMA_A.TABLE1 is remapped
- **THEN** the qualified reference is rewritten to the remapped target

#### Scenario: Derived table alias preserved
- **WHEN** a VIEW DDL contains `FROM (SELECT ...) T`
- **THEN** the alias `T` is preserved and not rewritten

### Requirement: View schema remains source unless explicitly remapped
The system SHALL keep VIEW target schema in the source unless an explicit remap rule is provided, while still remapping referenced objects.

#### Scenario: View stays in source schema but dependencies remapped
- **WHEN** a VIEW has no explicit remap rule
- **AND** referenced objects are remapped to a new schema
- **THEN** the VIEW target schema remains the source schema
- **AND** the VIEW SQL references the remapped target objects (including synonym resolution to base objects)
