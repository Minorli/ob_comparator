## ADDED Requirements
### Requirement: Status drift control switches
The system SHALL expose configuration switches to control status drift checking and fixup generation.

#### Scenario: Check scope switch
- **WHEN** `check_status_drift_types` is set
- **THEN** only listed types (`trigger`, `constraint`) are considered for status drift checks

#### Scenario: Fixup scope switch
- **WHEN** `status_fixup_types` is set
- **THEN** status fixup scripts are generated only for listed types

#### Scenario: Trigger validity sync mode
- **WHEN** `trigger_validity_sync_mode=compile`
- **THEN** trigger status fixup can append `ALTER TRIGGER ... COMPILE` for `source=VALID,target=INVALID` cases

#### Scenario: Constraint sync mode
- **WHEN** `constraint_status_sync_mode=enabled_only`
- **THEN** only `ENABLED` drift is actionable
- **WHEN** mode is `full`
- **THEN** both `ENABLED` and `VALIDATED` drift are actionable
