## MODIFIED Requirements

### Requirement: Constraint status drift control must be explicit
The toolkit SHALL expose `constraint_status_sync_mode` to control whether constraint status drift handling covers only `ENABLED/DISABLED` or also `VALIDATED/NOT VALIDATED`.

#### Scenario: Default configuration includes validated-state drift
- **WHEN** the operator does not explicitly configure `constraint_status_sync_mode`
- **THEN** the toolkit defaults to `full`
- **AND** it checks `VALIDATED / NOT VALIDATED` drift for supported constraint types

#### Scenario: Operator wants enabled-only status handling
- **WHEN** `constraint_status_sync_mode=enabled_only`
- **THEN** the toolkit only handles `ENABLED / DISABLED` drift
- **AND** it does not generate status-fixup SQL for `VALIDATED / NOT VALIDATED` drift
