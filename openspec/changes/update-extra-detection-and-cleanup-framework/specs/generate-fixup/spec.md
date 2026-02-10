## ADDED Requirements

### Requirement: Extra cleanup candidate generation
The system SHALL optionally generate cleanup candidates for target-extra objects.

#### Scenario: Cleanup candidates disabled
- **WHEN** `generate_extra_cleanup=false`
- **THEN** no cleanup candidate files are created

#### Scenario: Cleanup candidates enabled
- **WHEN** `generate_extra_cleanup=true`
- **THEN** cleanup candidate files are written under `fixup_scripts/cleanup_candidates/`
- **AND** statements are emitted as commented SQL for manual confirmation

#### Scenario: Constraint candidate drop uses parent table context
- **WHEN** an extra CONSTRAINT is reported from extra checks
- **THEN** candidate SQL uses `ALTER TABLE <owner>.<table> DROP CONSTRAINT <name>`
