## ADDED Requirements

### Requirement: Idempotent fixup DDL modes
The system SHALL support idempotent DDL generation modes for fixup scripts.

#### Scenario: Idempotency disabled
- **WHEN** fixup_idempotent_mode is off or unset
- **THEN** fixup DDL is emitted without idempotent wrappers

#### Scenario: Replace-capable object types
- **WHEN** fixup_idempotent_mode is replace
- **AND** the object type supports CREATE OR REPLACE
- **THEN** the emitted DDL uses CREATE OR REPLACE

#### Scenario: Guarded non-replaceable objects
- **WHEN** fixup_idempotent_mode is guard
- **AND** the object type does not support CREATE OR REPLACE
- **THEN** the emitted DDL is wrapped in a guard block that checks existence and skips creation when present

#### Scenario: Drop-create mode
- **WHEN** fixup_idempotent_mode is drop_create
- **AND** the object type is in fixup_idempotent_types
- **THEN** the emitted DDL includes a guarded DROP followed by CREATE

#### Scenario: Idempotent type filter
- **WHEN** fixup_idempotent_types is provided
- **THEN** idempotent wrapping applies only to those object types
