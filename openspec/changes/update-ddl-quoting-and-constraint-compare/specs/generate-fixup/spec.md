## ADDED Requirements
### Requirement: Identifier quoting in generated DDL
The system SHALL emit schema-qualified identifiers in generated DDL using the form `"SCHEMA"."OBJECT"` when it inserts or rewrites qualified names, and SHALL avoid emitting `"SCHEMA.OBJECT"` or double-quoting already-quoted identifiers.

#### Scenario: Trigger header quoting
- **WHEN** trigger fixup rewrites CREATE TRIGGER and the ON clause
- **THEN** the output uses `"SCHEMA"."TRIGGER"` and `ON "SCHEMA"."TABLE"`

#### Scenario: Trigger body remap quoting
- **WHEN** trigger body DML or sequence references are remapped to qualified names
- **THEN** the inserted qualified references use `"SCHEMA"."OBJECT"` form

#### Scenario: View and synonym DDL quoting
- **WHEN** a VIEW or SYNONYM DDL is generated
- **THEN** the object name is quoted as `"SCHEMA"."OBJECT"` (PUBLIC synonym quotes the name only)

#### Scenario: FK REFERENCES quoting
- **WHEN** a CONSTRAINT REFERENCES clause is generated
- **THEN** the referenced table uses `"SCHEMA"."TABLE"`

#### Scenario: No double quoting
- **WHEN** source DDL already includes quoted identifiers
- **THEN** the output does not add an extra layer of quotes
