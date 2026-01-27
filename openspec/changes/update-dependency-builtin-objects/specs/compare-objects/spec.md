## MODIFIED Requirements

### Requirement: Dependency comparison
The system SHALL compare expected dependencies derived from source metadata with target dependencies, and skip built-in dependencies that do not require remap.

#### Scenario: Missing dependency
- **WHEN** an expected dependency is not found in the target
- **THEN** the system records a missing dependency with a reason

#### Scenario: Built-in DUAL dependency
- **WHEN** the referenced object is PUBLIC.DUAL or SYS.DUAL
- **AND** no target mapping exists for that referenced object
- **THEN** the system marks the dependency as skipped with a reason indicating the built-in dependency requires no remap
- **AND** the dependency is not reported as missing

#### Scenario: Non-builtin missing dependency
- **WHEN** the referenced object is not a built-in object
- **AND** no target mapping exists for that referenced object
- **THEN** the system reports the dependency as skipped with the standard remap-missing reason
