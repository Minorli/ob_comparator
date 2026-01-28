## ADDED Requirements

### Requirement: SYS_C force fixup switch
The system SHALL support a `fixup_drop_sys_c_columns` switch to control whether SYS_C\d+ extra columns emit ALTER TABLE FORCE automatically.

#### Scenario: Switch enabled
- **WHEN** fixup_drop_sys_c_columns is true
- **THEN** SYS_C\d+ extra columns emit ALTER TABLE FORCE in fixup scripts

#### Scenario: Switch disabled
- **WHEN** fixup_drop_sys_c_columns is false or missing
- **THEN** extra columns remain commented in fixup scripts
