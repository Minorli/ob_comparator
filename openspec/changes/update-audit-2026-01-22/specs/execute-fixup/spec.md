## MODIFIED Requirements

### Requirement: Execution order
The fixup executor SHALL support a default priority order and an optional dependency-aware order, ensuring TYPE/TYPE BODY precede PROCEDURE/FUNCTION and package/type bodies execute after their specs.

#### Scenario: Default order
- **WHEN** --smart-order is not provided
- **THEN** scripts execute in the legacy priority order

#### Scenario: Dependency-aware order
- **WHEN** --smart-order is enabled
- **THEN** scripts execute by dependency layers with types before routines and specs before bodies

#### Scenario: Unknown directory in smart order
- **WHEN** a subdirectory is not part of the predefined dependency layers
- **THEN** its scripts are executed after the known layers
