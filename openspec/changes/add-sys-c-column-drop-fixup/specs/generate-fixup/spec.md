## MODIFIED Requirements

### Requirement: Table ALTER scripts for column differences
The system SHALL generate ALTER TABLE statements for missing columns and length mismatches, and comment DROP suggestions for extra columns.

#### Scenario: Extra column
- **WHEN** a column exists only in the target
- **THEN** a commented DROP COLUMN suggestion is emitted

### Requirement: SYS_C extra column force (opt-in)
The system SHALL emit `ALTER TABLE ... FORCE` for extra columns matching the SYS_C\d+ pattern when enabled.

#### Scenario: SYS_C force enabled
- **WHEN** fixup_drop_sys_c_columns is true
- **AND** an extra column name matches SYS_C\d+
- **THEN** an ALTER TABLE ... FORCE statement is emitted for that table

#### Scenario: SYS_C force disabled (default)
- **WHEN** fixup_drop_sys_c_columns is false or missing
- **THEN** SYS_C extra columns follow the default commented DROP behavior
