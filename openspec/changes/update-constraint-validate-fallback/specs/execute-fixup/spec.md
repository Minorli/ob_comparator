## ADDED Requirements

### Requirement: ORA-02298 classification
The fixup executor SHALL classify ORA-02298 failures as constraint validation/data-quality failures.

#### Scenario: Constraint validate fails during execution
- **WHEN** run_fixup receives ORA-02298 for a script
- **THEN** it classifies the failure as constraint validate failure
- **AND** reports a remediation hint to clean data and run deferred validation scripts later

### Requirement: Deferred validate directory default exclusion
The fixup executor SHALL skip `constraint_validate_later` by default during bulk execution.

#### Scenario: Default run_fixup execution
- **WHEN** user runs run_fixup without explicit include/exclude overrides
- **THEN** scripts under `constraint_validate_later` are excluded from normal execution
