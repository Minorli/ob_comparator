## ADDED Requirements

### Requirement: View constraint compatibility classification
The system SHALL detect VIEW column-list constraints and classify their compatibility based on cleanup mode.

#### Scenario: Auto cleanable constraint
- **WHEN** view_constraint_cleanup=auto and a VIEW contains RELY DISABLE/DISABLE/NOVALIDATE constraints
- **THEN** the view is classified as supported (cleanable)

#### Scenario: Auto uncleanable constraint
- **WHEN** view_constraint_cleanup=auto and a VIEW contains ENABLE constraints or ambiguous constraint state
- **THEN** the view is classified as unsupported/blocked and recorded in uncleanable report

#### Scenario: Force clean
- **WHEN** view_constraint_cleanup=force and a VIEW contains column-list constraints
- **THEN** the view is classified as supported (cleaned)

#### Scenario: Off
- **WHEN** view_constraint_cleanup=off and a VIEW contains column-list constraints
- **THEN** the view is classified as unsupported/blocked
