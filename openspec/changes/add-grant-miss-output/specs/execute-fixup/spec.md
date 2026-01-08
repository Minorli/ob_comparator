## MODIFIED Requirements

### Requirement: Execution order
The fixup executor SHALL support a default priority order and an optional dependency-aware order. When both grants_all and grants_miss directories exist, the executor SHALL default to grants_miss.

#### Scenario: Default order
- **WHEN** --smart-order is not provided
- **THEN** scripts execute in the legacy priority order

#### Scenario: Dependency-aware order
- **WHEN** --smart-order is enabled
- **THEN** scripts execute by dependency layers with grants before dependent objects

#### Scenario: Grants miss preferred
- **WHEN** fixup_scripts/grants_miss exists alongside grants_all
- **THEN** the executor runs grants_miss by default instead of grants_all
