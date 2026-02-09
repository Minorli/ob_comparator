## MODIFIED Requirements
### Requirement: Missing object DDL generation
The system SHALL generate CREATE DDL scripts for missing objects by type and store them in fixup_scripts subdirectories.

#### Scenario: Status drift fixup disabled by default
- **WHEN** status drift checks detect trigger/constraint status differences
- **AND** `generate_status_fixup=false`
- **THEN** no status-fixup SQL files are generated

#### Scenario: Trigger status fixup generation
- **WHEN** `generate_status_fixup=true` and trigger status differs
- **THEN** the system emits `ALTER TRIGGER ... ENABLE|DISABLE` in `fixup_scripts/status/trigger`

#### Scenario: Constraint status fixup generation (enabled_only)
- **WHEN** `generate_status_fixup=true` and `constraint_status_sync_mode=enabled_only`
- **AND** source is ENABLED while target is DISABLED
- **THEN** the system emits `ALTER TABLE ... ENABLE CONSTRAINT ...` in `fixup_scripts/status/constraint`

#### Scenario: Constraint status fixup generation (full)
- **WHEN** `generate_status_fixup=true` and `constraint_status_sync_mode=full`
- **THEN** the system aligns ENABLED/DISABLED and VALIDATED/NOVALIDATE states using Oracle-compatible ALTER TABLE syntax
