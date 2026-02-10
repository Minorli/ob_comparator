## ADDED Requirements

### Requirement: Extra count warning alignment
The system SHALL avoid emitting early noisy INDEX/CONSTRAINT extra/missing warning signals based only on rough metadata totals.

#### Scenario: Pre-reconcile count stage
- **WHEN** INDEX/CONSTRAINT rough totals differ but semantic comparison has not completed
- **THEN** the early count-stage warning output does not treat these rough totals as final extra/missing conclusions

#### Scenario: Final summary stage
- **WHEN** semantic extra results are available
- **THEN** INDEX/CONSTRAINT missing/extra summary counts come from reconciled semantic comparison results
