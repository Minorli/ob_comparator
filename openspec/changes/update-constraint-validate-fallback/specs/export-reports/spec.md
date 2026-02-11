## ADDED Requirements

### Requirement: Deferred validation detail report
The system SHALL export a deferred validation detail report when missing constraints are generated with NOVALIDATE.

#### Scenario: Deferred validate rows exist
- **WHEN** one or more constraints are emitted as NOVALIDATE in fixup generation
- **THEN** the system writes `constraint_validate_deferred_detail_<timestamp>.txt`
- **AND** includes the file in `report_index_<timestamp>.txt`
