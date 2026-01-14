## MODIFIED Requirements
### Requirement: Primary object coverage
The system SHALL compare source and target objects for the primary object set and track print-only types.

#### Scenario: Standard primary types
- **WHEN** the source contains TABLE, VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, SYNONYM, JOB, SCHEDULE, TYPE, and TYPE BODY
- **THEN** the system includes them in the primary comparison scope

#### Scenario: Print-only primary types
- **WHEN** the source contains MATERIALIZED VIEW
- **THEN** the system records them as print-only and skips OceanBase validation

## ADDED Requirements
### Requirement: Package validity comparison
The system SHALL compare PACKAGE and PACKAGE BODY existence and VALID/INVALID status between source and target, and capture compile error details when available.

#### Scenario: Package exists and status matches
- **WHEN** a PACKAGE or PACKAGE BODY exists in both source and target with the same status
- **THEN** the system records the object as OK

#### Scenario: Target package missing
- **WHEN** a PACKAGE or PACKAGE BODY exists in the source but not in the target
- **THEN** the system records the object as missing in the target

#### Scenario: Source package invalid
- **WHEN** the source PACKAGE or PACKAGE BODY status is INVALID
- **THEN** the system records the object as source-invalid and excludes it from mismatch counts while still listing it

#### Scenario: Status mismatch or target invalid
- **WHEN** the source status is VALID and the target status is INVALID or the statuses differ
- **THEN** the system records the object as status mismatch and includes error details if available
