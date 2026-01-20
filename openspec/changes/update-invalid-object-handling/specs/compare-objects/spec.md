## ADDED Requirements

### Requirement: Invalid source objects block dependents
The system SHALL treat INVALID source objects as unsupported nodes and propagate blocked status to dependent objects during support classification.

#### Scenario: Invalid view blocks a dependent object
- **WHEN** a source VIEW is INVALID and another missing object depends on it
- **THEN** the dependent object is reported as BLOCKED with a dependency-invalid reason

#### Scenario: Invalid package blocks a dependent object
- **WHEN** a source PACKAGE or PACKAGE BODY is INVALID and another missing object depends on it
- **THEN** the dependent object is reported as BLOCKED with a dependency-invalid reason

### Requirement: Synonym blocked on invalid target
The system SHALL mark a missing SYNONYM as BLOCKED when its resolved target object is INVALID in the source.

#### Scenario: Synonym points to invalid view
- **WHEN** a SYNONYM references a VIEW that is INVALID in the source
- **THEN** the SYNONYM is reported as BLOCKED with a dependency-invalid reason

### Requirement: Trigger status checks ignore unsupported tables
The system SHALL omit trigger status comparisons for triggers whose parent tables are unsupported or blacklisted.

#### Scenario: Trigger status on blacklisted table
- **WHEN** a TRIGGER belongs to a blacklisted TABLE
- **THEN** trigger status differences for that TRIGGER are not included in trigger status reporting
