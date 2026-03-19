## MODIFIED Requirements

### Requirement: Constraint status fixup generation must respect sync mode
The toolkit SHALL generate status-fixup SQL for existing constraints according to `constraint_status_sync_mode`, while preserving OceanBase safety guards for unsupported constraint classes.

#### Scenario: Existing FK validated-state drift under default mode
- **WHEN** an existing `FK` is `VALIDATED` on Oracle
- **AND** the same `FK` is `NOT VALIDATED` on OceanBase
- **AND** the operator uses default configuration
- **THEN** the toolkit detects the validated-state drift
- **AND** it emits `ALTER TABLE ... ENABLE VALIDATE CONSTRAINT ...` status-fixup SQL

#### Scenario: PK and UK still avoid validate-state SQL
- **WHEN** an existing `PK` or `UK` has validated-state drift
- **THEN** the toolkit keeps reporting the drift
- **AND** it does not emit `ENABLE/[NO]VALIDATE` status-fixup SQL for that object class
