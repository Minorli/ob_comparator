## ADDED Requirements

### Requirement: Optional usability checks for VIEW and SYNONYM
The system SHALL optionally validate VIEW and SYNONYM usability using a lightweight query that does not return data.

#### Scenario: Usability checks enabled
- **WHEN** check_object_usability is true
- **THEN** the system runs `SELECT * FROM <obj> WHERE 1=2` for each VIEW and SYNONYM that exists in the target

#### Scenario: Usability checks disabled
- **WHEN** check_object_usability is false
- **THEN** the system does not execute any usability SQL and keeps existence-only behavior

#### Scenario: Source usability comparison enabled
- **WHEN** check_object_usability is true and check_source_usability is true
- **THEN** the system evaluates source and target usability and classifies results as OK, UNUSABLE, EXPECTED_UNUSABLE, or UNEXPECTED_USABLE

#### Scenario: Timeout during usability check
- **WHEN** a usability query exceeds usability_check_timeout
- **THEN** the system marks the object as TIMEOUT without classifying it as unusable
