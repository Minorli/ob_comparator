## ADDED Requirements

### Requirement: View constraint cleanup in fixup DDL
The system SHALL apply view constraint cleanup rules before generating VIEW fixup DDL.

#### Scenario: Auto cleanup
- **WHEN** view_constraint_cleanup=auto and the VIEW constraint is cleanable
- **THEN** the cleaned DDL is emitted

#### Scenario: Force cleanup
- **WHEN** view_constraint_cleanup=force
- **THEN** the DDL is emitted without column-list constraints

#### Scenario: Off
- **WHEN** view_constraint_cleanup=off
- **THEN** VIEW DDL is not cleaned and the object remains unsupported
