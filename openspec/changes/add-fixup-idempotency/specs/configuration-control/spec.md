## ADDED Requirements

### Requirement: Fixup idempotency configuration
The system SHALL honor fixup_idempotent_mode and fixup_idempotent_types to control idempotent fixup DDL generation.

#### Scenario: Unknown idempotent mode
- **WHEN** fixup_idempotent_mode is not one of off/guard/replace/drop_create
- **THEN** the system logs a warning and defaults to off

#### Scenario: Unknown type in idempotent types
- **WHEN** fixup_idempotent_types contains an unknown object type
- **THEN** the unknown type is ignored with a warning
