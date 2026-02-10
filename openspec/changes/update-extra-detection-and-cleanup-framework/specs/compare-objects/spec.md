## MODIFIED Requirements

### Requirement: Primary object coverage
The system SHALL compare source and target objects for the primary object set and track print-only types.

#### Scenario: Standard primary types
- **WHEN** the source contains TABLE, VIEW, PROCEDURE, FUNCTION, SYNONYM, JOB, SCHEDULE, TYPE, and TYPE BODY
- **THEN** the system includes them in the primary comparison scope

#### Scenario: Print-only primary types
- **WHEN** the source contains MATERIALIZED VIEW, PACKAGE, or PACKAGE BODY
- **THEN** the system records them as print-only and skips OceanBase validation

#### Scenario: Print-only MATERIALIZED VIEW participates in extra-target detection
- **WHEN** `MATERIALIZED VIEW` is enabled and exists on target but not in remap-expected targets
- **THEN** the object is included in target-extra detection output (`extra_targets`)
- **AND** it remains print-only (no fixup generation for missing MVIEW)

#### Scenario: PACKAGE and PACKAGE BODY excluded from extra-target detection
- **WHEN** target contains extra PACKAGE or PACKAGE BODY objects
- **THEN** those objects are not added to `extra_targets`
- **AND** package drift continues to be handled by package-compare reporting
