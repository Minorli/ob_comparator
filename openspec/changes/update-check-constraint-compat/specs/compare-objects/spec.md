## ADDED Requirements

### Requirement: CHECK constraint expression extraction
The system SHALL extract CHECK constraint expressions using SEARCH_CONDITION_VC when available and fall back to SEARCH_CONDITION when not available.

#### Scenario: SEARCH_CONDITION_VC available
- **WHEN** DBA_CONSTRAINTS exposes SEARCH_CONDITION_VC
- **THEN** the system uses SEARCH_CONDITION_VC for CHECK constraint comparison

#### Scenario: SEARCH_CONDITION_VC missing
- **WHEN** SEARCH_CONDITION_VC is unavailable but SEARCH_CONDITION exists
- **THEN** the system uses SEARCH_CONDITION for CHECK constraint comparison

#### Scenario: CHECK expression unavailable
- **WHEN** neither SEARCH_CONDITION_VC nor SEARCH_CONDITION is available
- **THEN** the system records the CHECK expression as unknown and continues comparison with a recorded reason

### Requirement: CHECK constraint compatibility classification
The system SHALL classify CHECK constraints that are incompatible with OceanBase as UNSUPPORTED and record a reason code.

#### Scenario: SYS_CONTEXT in CHECK constraint
- **WHEN** a CHECK constraint expression contains SYS_CONTEXT('USERENV', ...)
- **THEN** the constraint is classified as UNSUPPORTED with a SYS_CONTEXT reason

#### Scenario: DEFERRABLE CHECK constraint
- **WHEN** a CHECK constraint is DEFERRABLE or INITIALLY DEFERRED
- **THEN** the constraint is classified as UNSUPPORTED with a DEFERRABLE reason

## MODIFIED Requirements

### Requirement: Constraint comparison
The system SHALL compare PK/UK/FK/CHECK constraints between source and target when constraint checks are enabled. CHECK constraints are compared using normalized expressions and deferrable flags.

#### Scenario: CHECK constraint match
- **WHEN** a CHECK constraint exists in both source and target with equivalent normalized expressions
- **THEN** the constraint is treated as matching

#### Scenario: CHECK constraint unsupported
- **WHEN** a CHECK constraint is classified as UNSUPPORTED
- **THEN** it is counted as unsupported rather than missing
