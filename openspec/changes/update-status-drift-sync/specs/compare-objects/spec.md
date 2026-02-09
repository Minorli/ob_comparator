## MODIFIED Requirements
### Requirement: Extra object checks
The system SHALL validate INDEX, CONSTRAINT, SEQUENCE, and TRIGGER objects against source metadata when enabled.

#### Scenario: Trigger status drift detection
- **WHEN** a trigger exists on both source and target sides
- **THEN** the system compares trigger event, enabled status, and validity status
- **AND** records drift rows for non-equal fields

#### Scenario: Constraint status drift detection
- **WHEN** a constraint can be semantically matched between source and target (PK/UK/FK/CHECK)
- **THEN** the system compares constraint enabled status
- **AND** in full mode additionally compares validated status

#### Scenario: Constraint semantic match over name
- **WHEN** source and target constraint names differ but columns/expressions/references are equivalent
- **THEN** the system treats them as the same logical constraint for status drift comparison
